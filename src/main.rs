#[macro_use]
extern crate log;

mod pg_cdc;
mod server;
mod websockets;

use actix::Actor;
use config::Config;
use std::sync::RwLock;
use tokio::sync::broadcast;
use tokio_postgres::SimpleQueryMessage;
use websockets::{server::ws_server::WsServer, ws_dispatcher};

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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Init the logger and set the debug level correctly
    configure_logger(
        CONFIG
            .get_str("RUST_LOG")
            .unwrap_or_else(|_| "error,actix_server=info,actix_web=error".into()),
    );

    // Form replication connection & keep the connection open
    let rclient = pg_cdc::connect_replica().await;

    // A multi-producer, multi-consumer broadcast queue. Each sent value is seen by all consumers.
    // 16 is the number of messages that can be queued up before older messages get dropped.
    let (tx, _) = broadcast::channel(16);

    // Start WsServer actor
    let ws_server = WsServer::new().start();
    info!("Successfully started WsServer");

    // Clone the Sender of the broadcast to allow it to be used in two async context
    // Init the ws_dispatcher before init_cdc_listener because the latter will fail if no subscriber are waiting
    ws_dispatcher::init_ws_dispatcher(ws_server.clone(), tx.clone());

    // Get all tables contained in the Database
    let query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';";
    rclient
        .simple_query(&query)
        .await
        .unwrap()
        .into_iter()
        .for_each(|msg| {
            // And push them to the TABLES Vec
            if let SimpleQueryMessage::Row(row) = msg {
                if let Some(val) = row.get(0) {
                    TABLES.write().unwrap().push(val.to_owned())
                }
            }
        });

    // Init the replication slot and read the stream of change
    pg_cdc::init_cdc_listener(rclient, tx);

    info!("Allowed tables are: {:?}", &TABLES.read().unwrap());

    // Start the Actix server and so the websocket client
    server::server(ws_server).await?;

    // Return Ok
    Ok(())
}
