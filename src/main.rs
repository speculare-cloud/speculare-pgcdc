#[macro_use]
extern crate log;

use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage;
use std::time::{Duration, UNIX_EPOCH};
use tokio_postgres::replication_client::SnapshotMode;
use tokio_postgres::{connect_replication, Error, NoTls, ReplicationMode};

mod logger;

const TIME_SEC_CONVERSION: u64 = 946_684_800;

#[tokio::main]
async fn main() -> Result<(), Error> {
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

    // Define constants for the logical slot
    let slot_name = "pgcdc_repl";
    let plugin = "wal2json";
    let options = &[("pretty-print", "0")];

    // Clean up previous runs
    rclient.drop_replication_slot(slot_name, true).await?;

    // We set NoExportSnapshot
    let no_export = Some(SnapshotMode::NoExportSnapshot);
    rclient
        .create_logical_replication_slot(slot_name, true, plugin, no_export)
        .await?;

    let identify_system = rclient.identify_system().await?;

    // We now switch to consuming the stream
    let mut logical_stream = rclient
        .start_logical_replication(slot_name, identify_system.xlogpos(), options)
        .await?;

    let epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);
    let mut last_lsn = identify_system.xlogpos();

    while let Some(replication_message) = logical_stream.next().await {
        match replication_message? {
            ReplicationMessage::XLogData(xlog_data) => {
                let json = std::str::from_utf8(xlog_data.data()).unwrap();
                info!("Json: {}", json);

                last_lsn = xlog_data.wal_end().into();
                info!("Last_lsn == {}", last_lsn);
            }
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                if keepalive.reply() == 1 {
                    warn!("sending keepalive reply with last_lsn == {}", last_lsn);
                    let ts = epoch.elapsed().unwrap().as_micros() as i64;
                    logical_stream
                        .as_mut()
                        .standby_status_update(last_lsn, last_lsn, last_lsn, ts, 0)
                        .await?;
                }
            }
            _ => (),
        }
    }
    Ok(())
}
