//! Quick note about the database table name due to Tailscale:
//! > Static array to hold the tables in the order of creation in the database.
//!   As we use TimescaleDB, each table get partitioned using a pattern like "_hyper_x_y_chunk",
//!   which don't give us the opportunity to detect which table is being updated/inserted.
//!   As the client will connect to the WS using the base table name, this array is used for lookup.
//!   The pattern always follow the same naming convention: "_hyper_(table_creation_order_from_1)_(partition_number)_chunk".
//!   So we use this array to derive the name of the table from the pattern naming chunk.

#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[macro_use]
extern crate log;

macro_rules! has_bit {
    ($a:expr,$b:expr) => {
        ($a & $b) != 0
    };
}

macro_rules! field_isset {
    ($value:expr, $name:literal) => {
        match $value {
            Some(x) => x,
            None => {
                error!(
                    "Config: optional field {} is not defined but is needed.",
                    $name
                );
                std::process::exit(1);
            }
        }
    };
}

use crate::utils::config::Config;
use crate::websockets::{forwarder::start_forwarder, ServerState};

use bastion::spawn;
use bastion::supervisor::{ActorRestartStrategy, RestartStrategy, SupervisorRef};
use bastion::{prelude::BastionContext, Bastion};
use cdc::{
    connection::db_client_start,
    replication::{replication_slot_create, replication_stream_poll, replication_stream_start},
    ExtConfig,
};
use clap::Parser;
use clap_verbosity_flag::InfoLevel;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;

mod cdc;
mod server;
mod utils;
mod websockets;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(short = 'c', long = "config")]
    config_path: Option<String>,

    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<InfoLevel>,
}

lazy_static::lazy_static! {
    // Which table are allowed (hard defined at startup for now)
    // Allow us to avoid accepting websocket which will never be triggered
    static ref TABLES: RwLock<Vec<String>> = {
        RwLock::new(Vec::new())
    };

    // Lazy static of the Config which is loaded from the config file
    static ref CONFIG: Config = match Config::new() {
        Ok(config) => config,
        Err(e) => {
            error!("Cannot build the Config: {:?}", e);
            std::process::exit(1);
        }
    };

    // Bastion supervisor used to define a custom restart policy for the children
    static ref SUPERVISOR: SupervisorRef = match Bastion::supervisor(|sp| {
        sp.with_restart_strategy(RestartStrategy::default().with_actor_restart_strategy(
            ActorRestartStrategy::LinearBackOff {
                timeout: Duration::from_secs(3),
            },
        ))
    }) {
        Ok(sp) => sp,
        Err(err) => {
            error!("Cannot create the Bastion supervisor: {:?}", err);
            std::process::exit(1);
        }
    };
}

fn prog() -> Option<String> {
    std::env::args()
        .next()
        .as_ref()
        .map(Path::new)
        .and_then(Path::file_name)
        .and_then(OsStr::to_str)
        .map(String::from)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Init logger
    env_logger::Builder::new()
        .filter_module(
            &prog().map_or_else(|| "speculare_pgcdc".to_owned(), |f| f.replace('-', "_")),
            args.verbose.log_level_filter(),
        )
        .init();

    // Construct our default server state
    let server_state = Arc::new(ServerState::default());

    // Init Bastion supervisor
    Bastion::init();
    Bastion::start();

    // Clone server_state for run_server
    let cserver_state = server_state.clone();

    // Start the children in Bastion (allow for restart if fails)
    SUPERVISOR
        .children(|child| {
            child.with_exec(move |_: BastionContext| {
                trace!("Starting the replication forwarder & listener");
                let server_state = server_state.clone();

                async move {
                    // A multi-producer, single-consumer channel queue. Using 128 buffers length.
                    let (tx, rx) = mpsc::channel(128);

                    // Start listening to the Sender & forward message when receiving one
                    let handle = spawn! {
                        start_forwarder(rx, server_state).await;
                    };

                    // Form replication connection & keep the connection open
                    let client = db_client_start().await;
                    client.detect_tables().await;
                    trace!("Main: Allowed tables are: {:?}", &TABLES.read().unwrap());
                    trace!("Main: Tables lookup are: {:?}", &CONFIG.lookup_table);

                    let slot_name = uuid_readable_rs::short().replace(' ', "_").to_lowercase();
                    let lsn = replication_slot_create(&client, &slot_name).await;
                    let duplex_stream = replication_stream_start(&client, &slot_name, &lsn).await;

                    select! {
                        _ = replication_stream_poll(duplex_stream, tx.clone()) => {
                            panic!("replication_stream_poll exited, panic to restart")
                        }
                        _ = handle => {
                            panic!("start_forwarder exited, panic to restart")
                        }
                    }
                }
            })
        })
        .expect("Cannot create the Children for Bastion");

    server::run_server(cserver_state).await
}
