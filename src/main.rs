#[macro_use]
extern crate log;

use actix::Actor;
use tokio::sync::{broadcast, oneshot};

mod cdc;
mod logger;
mod server;
mod ws_client;
mod ws_server;
mod ws_utils;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load env variable from .env
    dotenv::dotenv().ok();

    // Init the logger and set the debug level correctly
    logger::configure();

    // Create a oneshot channel to handle postgresql connection failures
    let (txo, rxo) = oneshot::channel();

    // Form replication connection
    // And keep the connection open
    let rclient = cdc::connect_replica(txo).await;

    // A multi-producer, multi-consumer broadcast queue. Each sent value is seen by all consumers.
    let (tx, _) = broadcast::channel(16);

    // Start chat server actor
    let ws_server = ws_server::WsServer::new().start();

    // Clone the Sender of the broadcast to allow it to be used in two async context
    // Init the ws_dispatcher before init_cdc_listener because the latter will fail if no subscriber are waiting
    ws_utils::init_ws_dispatcher(ws_server.clone(), tx.clone());

    // Init the replication slot and read the stream of change
    cdc::init_cdc_listener(rclient, tx);

    // Start the Actix server and so the websocket client
    server::server(ws_server).await?;

    // Wait for the postgres connection to be closed
    // Allow us to panic! and shutdown all task if needed
    rxo.await.unwrap();
    Ok(())
}
