//! Quick note about the database table name due to Tailscale:
//! > Static array to hold the tables in the order of creation in the database.
//!   As we use TimescaleDB, each table get partitioned using a pattern like "_hyper_x_y_chunk",
//!   which don't give us the opportunity to detect which table is being updated/inserted.
//!   As the client will connect to the WS using the base table name, this array is used for lookup.
//!   The pattern always follow the same naming convention: "_hyper_(table_creation_order_from_1)_(partition_number)_chunk".
//!   So we use this array to derive the name of the table from the pattern naming chunk.

#[macro_use]
extern crate log;

macro_rules! has_bit {
    ($a:expr,$b:expr) => {
        ($a & $b) != 0
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
#[cfg(feature = "timescale")]
use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;

mod api;
mod cdc;
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

/// Our global unique client id counter.
static NEXT_CLIENT_ID: AtomicUsize = AtomicUsize::new(1);

#[cfg(feature = "timescale")]
lazy_static::lazy_static! {
    // Used with TimescaleDB to lookup the table name (disks may be _hyper_1 for example)
    static ref TABLES_LOOKUP: RwLock<HashMap<i8, String>> = {
        RwLock::new(HashMap::new())
    };
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

    // Define log level
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var(
            "RUST_LOG",
            format!(
                "{}={level},tower_http={level}",
                &prog().map_or_else(|| "speculare_pgcdc".to_owned(), |f| f.replace('-', "_")),
                level = args.verbose.log_level_filter()
            ),
        )
    }

    // Init logger/tracing
    tracing_subscriber::fmt::init();

    // Construct our default server state
    let server_state = Arc::new(ServerState::default());

    // Init and Start Bastion supervisor
    Bastion::init();
    Bastion::start();

    // Clone server_state for run_server (below) as we use server_state
    // in our SUPERVISOR.children.
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

                    // Detect tables that we'll use to authorize or lookup with timescale
                    client.detect_tables().await;
                    trace!("Main: Allowed tables are: {:?}", &TABLES.read().unwrap());
                    #[cfg(feature = "timescale")]
                    {
                        client.detect_lookup().await;
                        trace!(
                            "Main: Tables lookup are: {:?}",
                            &TABLES_LOOKUP.read().unwrap()
                        );
                    }

                    let slot_name = uuid_readable_rs::short().replace(' ', "_").to_lowercase();
                    let lsn = replication_slot_create(&client, &slot_name).await;
                    let duplex_stream = replication_stream_start(&client, &slot_name, &lsn).await;

                    // call to panic allow us to exit this children and restart a new one
                    // in case any of the two (replication_stream_poll or handle) exit.
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

    api::run_server(cserver_state).await
}
