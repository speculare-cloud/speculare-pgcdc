#[macro_use]
extern crate log;

mod pg_cdc;
mod server;
mod websockets;

use actix::Actor;
use tokio::sync::broadcast;
use websockets::{server::ws_server::WsServer, ws_dispatcher};

// Static array to hold the tables in the order of creation in the database.
// As we use TimescaleDB, each table get partitioned using a patern like "_hyper_x_y_chunk",
// which don't give us the opportunity to detect which table is being updated/inserted.
// As the client will connect to the WS using the base table name, this array is used for lookup.
// The patern always follow the same naming convention: "_hyper_(table_creation_order_from_1)_(partition_number)_chunk".
// So we use this array to derive the name of the table from the patern naming chunk.
lazy_static::lazy_static! {
    static ref TABLES: [&'static str; 8] = {
        ["hosts", "cputimes", "cpustats", "ioblocks", "loadavg", "memory", "swap", "ionets"]
    };
}

fn configure_logger() {
    // Check if the RUST_LOG already exist in the sys
    if std::env::var_os("RUST_LOG").is_none() {
        // if it doesn't, assign a default value to RUST_LOG
        // Define RUST_LOG as trace for debug and error for prod
        std::env::set_var(
            "RUST_LOG",
            if cfg!(debug_assertions) {
                "info,actix_server=info,actix_web=info"
            } else {
                "error,actix_server=error,actix_web=error"
            },
        );
    }
    // Init the logger
    env_logger::init();
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load env variable from .env
    dotenv::dotenv().ok();

    // Init the logger and set the debug level correctly
    configure_logger();

    // Form replication connection & keep the connection open
    let rclient = pg_cdc::connect_replica().await;

    // A multi-producer, multi-consumer broadcast queue. Each sent value is seen by all consumers.
    // 16 is the number of messages that can be queued up before older messages get dropped.
    let (tx, _) = broadcast::channel(16);

    // Start WsServer actor
    let ws_server = WsServer::new().start();

    // Clone the Sender of the broadcast to allow it to be used in two async context
    // Init the ws_dispatcher before init_cdc_listener because the latter will fail if no subscriber are waiting
    ws_dispatcher::init_ws_dispatcher(ws_server.clone(), tx.clone());

    // Init the replication slot and read the stream of change
    pg_cdc::init_cdc_listener(rclient, tx);

    // Start the Actix server and so the websocket client
    server::server(ws_server).await?;

    // Return Ok
    Ok(())
}
