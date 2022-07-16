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

use crate::api::server;
use crate::utils::config::Config;

use api::ws_utils::ServerState;
use bastion::supervisor::{ActorRestartStrategy, RestartStrategy, SupervisorRef};
use bastion::Bastion;
use clap::Parser;
use clap_verbosity_flag::InfoLevel;
#[cfg(feature = "auth")]
use diesel::r2d2::ConnectionManager;
#[cfg(feature = "auth")]
use diesel::PgConnection;
use inner::start_inner;
#[cfg(feature = "auth")]
use moka::future::Cache;
use sproot::prog;
#[cfg(feature = "auth")]
use sproot::Pool;
#[cfg(feature = "timescale")]
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

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
lazy_static::lazy_static! {
    // Used with TimescaleDB to lookup the table name (disks may be _hyper_1 for example)
    static ref TABLES_LOOKUP: RwLock<HashMap<i8, String>> = {
        RwLock::new(HashMap::new())
    };
}

#[cfg(feature = "auth")]
lazy_static::lazy_static! {
    // > time_to_live is set to one hour, after that the key will be evicted and
    //   we'll need to recheck it from the auth server.
    static ref CHECKSESSIONS_CACHE: Cache<String, String> = Cache::builder().time_to_live(Duration::from_secs(60 * 60)).build();
    static ref CHECKAPI_CACHE: Cache<String, Uuid> = Cache::builder().time_to_live(Duration::from_secs(60 * 60)).build();
    static ref AUTHPOOL: Pool = {
        // Init the connection to the postgresql
        let manager = ConnectionManager::<PgConnection>::new(&CONFIG.auth_database_url);
        // This step might spam for error CONFIG.database_max_connection of times, this is normal.
        match r2d2::Pool::builder()
            .max_size(CONFIG.auth_database_max_connection)
            .min_idle(Some((10 * CONFIG.auth_database_max_connection) / 100))
            .build(manager)
        {
            Ok(pool) => {
                info!("R2D2 PostgreSQL pool created");
                pool
            }
            Err(e) => {
                error!("Failed to create db pool: {}", e);
                std::process::exit(1);
            }
        }
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
