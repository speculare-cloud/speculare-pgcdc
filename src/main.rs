//! Quick note about the database table name due to Tailscale:
//! Static array to hold the tables in the order of creation in the database.
//! As we use TimescaleDB, each table get partitioned using a pattern like "_hyper_x_y_chunk",
//! which don't give us the opportunity to detect which table is being updated/inserted.
//! As the client will connect to the WS using the base table name, this array is used for lookup.
//! The pattern always follow the same naming convention: "_hyper_(table_creation_order_from_1)_(partition_number)_chunk".
//! So we use this array to derive the name of the table from the pattern naming chunk.

#[macro_use]
extern crate log;

macro_rules! has_bit {
    ($a:expr,$b:expr) => {
        ($a & $b) != 0
    };
}

use crate::api::server;
use crate::utils::config::Config;

use api::ws_utils::ServerState;
use bastion::supervisor::{ActorRestartStrategy, RestartStrategy, SupervisorRef};
use bastion::Bastion;
use clap::Parser;
use clap_verbosity_flag::InfoLevel;
use inner::start_inner;
use once_cell::sync::Lazy;
use sproot::prog;
#[cfg(feature = "timescale")]
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;

mod api;
mod cdc;
mod forwarder;
mod inner;
mod utils;

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    #[clap(short = 'c', long = "config")]
    config_path: Option<String>,

    #[clap(flatten)]
    verbose: clap_verbosity_flag::Verbosity<InfoLevel>,
}

/// Our global unique client id counter.
static ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[cfg(feature = "timescale")]
// Used with TimescaleDB to lookup the table name (disks may be _hyper_1 for example)
static TABLES_LOOKUP: Lazy<RwLock<HashMap<i8, String>>> = Lazy::new(|| RwLock::new(HashMap::new()));

// Lazy static of the Config which is loaded from the config file
static CONFIG: Lazy<Config> = Lazy::new(|| match Config::new() {
    Ok(config) => config,
    Err(e) => {
        error!("Cannot build the Config: {}", e);
        std::process::exit(1);
    }
});

// Which table are allowed (hard defined at startup for now)
// Allow us to avoid accepting websocket which will never be triggered
static TABLES: Lazy<RwLock<Vec<String>>> = Lazy::new(|| RwLock::new(Vec::new()));

// Bastion supervisor used to define a custom restart policy for the children
static SUPERVISOR: Lazy<SupervisorRef> = Lazy::new(|| {
    match Bastion::supervisor(|sp| {
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
    }
});

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

    // Start the inner work, replication, forwarder, ...
    start_inner(server_state);

    // Start the public api server
    server::serve(cserver_state).await
}
