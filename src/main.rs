#[macro_use]
extern crate log;

use actix::Actor;
use tokio::sync::broadcast;
use tokio_postgres::SimpleQueryMessage;

mod cdc;
mod logger;
mod server;
mod websockets;

use websockets::server::ws_server::WsServer;
use websockets::ws_dispatcher;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load env variable from .env
    dotenv::dotenv().ok();

    // Init the logger and set the debug level correctly
    logger::configure();

    // Form replication connection
    // And keep the connection open
    let rclient = cdc::connect_replica().await;

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
    cdc::init_cdc_listener(rclient, tx);

    // Start the Actix server and so the websocket client
    server::server(ws_server, tables).await?;

    // Return Ok
    Ok(())
}
