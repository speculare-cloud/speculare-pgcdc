#[macro_use]
extern crate log;

macro_rules! has_bit {
    ($a:expr,$b:expr) => {
        ($a & $b) != 0
    };
}

mod cdc;
mod server;
mod utils;
mod websockets;

use crate::websockets::{forwarder::start_forwarder, ServerState};

use cdc::{
    connection::db_client_start,
    replication::{replication_slot_create, replication_stream_poll, replication_stream_start},
    ExtConfig,
};
use config::Config;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use tokio::sync::mpsc;

// Static array to hold the tables in the order of creation in the database.
// As we use TimescaleDB, each table get partitioned using a patern like "_hyper_x_y_chunk",
// which don't give us the opportunity to detect which table is being updated/inserted.
// As the client will connect to the WS using the base table name, this array is used for lookup.
// The patern always follow the same naming convention: "_hyper_(table_creation_order_from_1)_(partition_number)_chunk".
// So we use this array to derive the name of the table from the patern naming chunk.
lazy_static::lazy_static! {
    static ref TABLES: RwLock<Vec<String>> = {
        RwLock::new(Vec::new())
    };

    static ref TABLES_BY_INDEX: RwLock<HashMap<usize, String>> = {
        RwLock::new([
            (0, "disks".into()),
            (1, "cputimes".into()),
            (2, "cpustats".into()),
            (3, "ioblocks".into()),
            (4, "loadavg".into()),
            (5, "memory".into()),
            (6, "swap".into()),
            (7, "ionets".into())
        ].iter().cloned().collect())
    };
}

// Lazy static of the Config which is loaded from Alerts.toml
lazy_static::lazy_static! {
    static ref CONFIG: Config = {
        let mut config = Config::default();
        config.merge(config::File::with_name("Pgcdc")).unwrap();
        config
    };
}

/// Configure the logger level for any binary calling it
fn configure_logger(level: String) {
    // Check if the RUST_LOG already exist in the sys
    if std::env::var_os("RUST_LOG").is_none() {
        // if it doesn't, assign a default value to RUST_LOG
        // Define RUST_LOG as trace for debug and error for prod
        std::env::set_var("RUST_LOG", level);
    }
    // Init the logger
    env_logger::init();
}

#[tokio::main]
async fn main() {
    // Init the logger and set the debug level correctly
    configure_logger(
        CONFIG
            .get_str("RUST_LOG")
            .unwrap_or_else(|_| "error,wrap=info".into()),
    );

    // Form replication connection & keep the connection open
    let client = db_client_start().await;

    client.populate_tables().await;
    trace!("Main: Allowed tables are: {:?}", &TABLES.read().unwrap());

    // A multi-producer, single-consumer channel queue. Using 124 buffers lenght.
    let (tx, rx) = mpsc::channel(124);

    // Construct our default server state
    let server_state = Arc::new(ServerState::default());

    // Start listening to the Sender & forward message when receiving one
    start_forwarder(rx, server_state.clone());

    // Init the replication slot and listen to the duplex_stream
    tokio::spawn(async move {
        let lsn = replication_slot_create(&client).await;
        let duplex_stream = replication_stream_start(&client, &lsn).await;
        replication_stream_poll(duplex_stream, tx).await;
    });

    server::run_server(server_state).await
}
