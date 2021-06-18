#[macro_use]
extern crate log;

mod pg_cdc;
mod server;
mod websockets;

use actix::Actor;
use tokio::sync::broadcast;
use tokio_postgres::SimpleQueryMessage;
use websockets::{server::ws_server::WsServer, ws_dispatcher};

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

    // Form replication connection
    // And keep the connection open
    let rclient = pg_cdc::connect_replica().await;

    // A multi-producer, multi-consumer broadcast queue. Each sent value is seen by all consumers.
    let (tx, _) = broadcast::channel(16);

    // Start WsServer actor
    let ws_server = WsServer::new().start();

    // Clone the Sender of the broadcast to allow it to be used in two async context
    // Init the ws_dispatcher before init_cdc_listener because the latter will fail if no subscriber are waiting
    ws_dispatcher::init_ws_dispatcher(ws_server.clone(), tx.clone());

    // Get all tables contained in the Database
    let query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';";
    let tables = rclient
        .simple_query(&query)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|msg| {
            if let SimpleQueryMessage::Row(row) = msg {
                match row.get(0) {
                    Some(val) => Some(val.to_owned()),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    // Init the replication slot and read the stream of change
    pg_cdc::init_cdc_listener(rclient, tx);

    // Start the Actix server and so the websocket client
    server::server(ws_server, tables).await?;

    // Return Ok
    Ok(())
}
