#[macro_use]
extern crate log;

use actix::*;
use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::broadcast;
use tokio_postgres::replication_client::SnapshotMode;
use tokio_postgres::{connect_replication, NoTls, ReplicationMode};

mod logger;
mod server;
mod ws_client;
mod ws_server;

const TIME_SEC_CONVERSION: u64 = 946_684_800;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Load env variable from .env
    dotenv::dotenv().ok();
    // Init the logger and set the debug level correctly
    logger::configure();

    // Connection information to postgres
    let conninfo = "host=localhost user=postgres dbname=pgcdc password=password";

    // Form replication connection
    let (mut rclient, rconnection) = connect_replication(conninfo, NoTls, ReplicationMode::Logical)
        .await
        .unwrap();

    // Spawn connection to run on its own
    tokio::spawn(async move {
        if let Err(e) = rconnection.await {
            panic!("connection error: {}", e);
        }
    });

    // Spawn the broadcaster
    // Multi producer - multi consumer, each value will be seen by each consumer
    let (tx, _) = broadcast::channel(16);
    let txc = tx.clone();

    // // Spawn listener for changes
    // for x in 0..10 {
    //     // Clone the sender (tx) and create a new subsription to it
    //     // which will be passed to the tokio task
    //     let mut rxc = tx.subscribe();
    //     tokio::spawn(async move {
    //         // Wait for rxc.recv() to get message from tx (broadcast producer)
    //         loop {
    //             let value = rxc.recv().await;
    //             if value.is_err() {
    //                 error!("Task #{} just got an error: {}", x, value.err().unwrap());
    //             } else {
    //                 info!("Task #{} got: {}", x, value.unwrap());
    //             }
    //         }
    //     });
    // }

    // Define constants for the logical slot
    let slot_name = "pgcdc_repl";
    let plugin = "wal2json";
    let options = &[("pretty-print", "0")];

    // Clean up previous runs
    rclient
        .drop_replication_slot(slot_name, true)
        .await
        .unwrap();

    // We set NoExportSnapshot
    let no_export = Some(SnapshotMode::NoExportSnapshot);
    rclient
        .create_logical_replication_slot(slot_name, true, plugin, no_export)
        .await
        .unwrap();

    // Get info about the client we have
    let identify_system = rclient.identify_system().await.unwrap();

    // Compute the epoch and register the last_lsn of the stream
    // last_lsn will be updated everytime we read a new value
    let epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);
    let mut last_lsn = identify_system.xlogpos();

    // Start chat server actor
    let ws_server = ws_server::ChatServer::new().start();
    let wsc = ws_server.clone();

    tokio::spawn(async move {
        let mut rx = txc.subscribe();
        loop {
            let value = rx.recv().await;
            if value.is_err() {
                error!("Task just got an error: {}", value.err().unwrap());
            } else {
                info!("Task got: {}", value.unwrap());
            }
            ws_server.do_send(ws_server::ClientMessage {
                id: 1,
                msg: "Hello".to_owned(),
                room: "Main".to_owned(),
            });
        }
    });

    // TODO - Let this while loop run in his own task/thread
    //      - Run an actix server with webSocket which listen to the tokio broadcast
    //      - Filter out which message should be sent with which info, ...
    tokio::spawn(async move {
        // We now switch to consuming the stream
        let mut logical_stream = rclient
            .start_logical_replication(slot_name, identify_system.xlogpos(), options)
            .await
            .unwrap();
        // Listen for the replication stream
        while let Some(replication_message) = logical_stream.next().await {
            match replication_message.unwrap() {
                ReplicationMessage::XLogData(xlog_data) => {
                    // Extracting the json data from the ReplicationMessage
                    // converting to a String because he need to live longer than this scope
                    let json = String::from_utf8(xlog_data.data().to_vec()).unwrap();
                    info!("Json: {}", json);

                    // Send the JSON to every consumer (each on a different task)
                    tx.send(json.to_owned()).unwrap();
                    // ws_server.do_send(ws_server::ClientMessage {
                    //     id: 1,
                    //     msg: json,
                    //     room: "Main".to_owned(),
                    // });

                    // Update the last_lsn as we've sent the info
                    last_lsn = xlog_data.wal_end().into();
                    info!("Last_lsn == {}", last_lsn);
                }
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    // If the keepalive reply is 1, this means postgres is waiting for our reply
                    // before cutting off the connection
                    if keepalive.reply() == 1 {
                        warn!("sending keepalive reply with last_lsn == {}", last_lsn);
                        let ts = epoch.elapsed().unwrap().as_micros() as i64;
                        logical_stream
                            .as_mut()
                            .standby_status_update(last_lsn, last_lsn, last_lsn, ts, 0)
                            .await
                            .unwrap();
                    }
                }
                _ => (),
            }
        }
    });

    server::server(wsc).await
}
