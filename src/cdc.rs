use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::broadcast::Sender;
use tokio_postgres::replication_client::{ReplicationClient, SnapshotMode};
use tokio_postgres::{connect_replication, NoTls, ReplicationMode};

const TIME_SEC_CONVERSION: u64 = 946_684_800;

/// Open a replication connection to the Postgresql server and maintain the connection open on a task.
pub async fn connect_replica() -> ReplicationClient {
    // Connection information to postgres
    let conninfo = &std::env::var("CONNINFO").expect("BINDING must be set");

    // Form replication connection
    let (rclient, rconnection) = connect_replication(conninfo, NoTls, ReplicationMode::Logical)
        .await
        .unwrap();

    // Spawn connection to run on its own
    tokio::spawn(async move {
        // rconnection will never return except on Error
        // as long as no error, the connection is open.
        if let Err(e) = rconnection.await {
            panic!("connection error: {}", e);
        }
    });

    rclient
}

/// Drop previous replication slot and create a new one
pub async fn init_replication_slot(rclient: &mut ReplicationClient, slot_name: &str) {
    // Clean up previous replication slot (if any) named slot_name
    let resp = rclient.drop_replication_slot(slot_name, true).await;
    // Assert that the drop was done successfully
    assert_eq!(
        resp.is_ok(),
        true,
        "Error while dropping previous replication slot: {:?}",
        resp.err()
    );

    // We set NoExportSnapshot and create the replication slot
    let plugin = "wal2json";
    let no_export = Some(SnapshotMode::NoExportSnapshot);
    let resp = rclient
        .create_logical_replication_slot(slot_name, true, plugin, no_export)
        .await;
    // Assert that the creation was done successfully
    assert_eq!(
        resp.is_ok(),
        true,
        "Error while creating the replication slot: {:?}",
        resp.err()
    );
}

/// Call init_replication_slot() and start the stream + read it.
pub fn init_cdc_listener(mut rclient: ReplicationClient, tx: Sender<String>) {
    tokio::spawn(async move {
        // Define constants for the logical slot
        let slot_name = "pgcdc_repl";
        let options = &[("pretty-print", "0")];

        // Drop previous and create new replication slot
        init_replication_slot(&mut rclient, &slot_name).await;

        // Get info about the server (xlogpos, dbname, timeline, systemid)
        let identify_system = rclient.identify_system().await.unwrap();

        // Compute the epoch and register the last_lsn of the stream
        // last_lsn will be updated everytime we read a new value
        let epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);
        let mut last_lsn = identify_system.xlogpos();

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
                    trace!("Json: {}", json);

                    // Send the JSON to dispatcher consumer using this solution for the queue of the mpmc
                    // This fail only if no receiver are waiting for the sender, in our case it's safe to assume
                    // that if tx.send() fail, we panic! because our program is fucked up at this point
                    // TODO - If this fail, create a new Receiver by calling tx.subscribe in the appropriate task.
                    if let Err(err) = tx.send(json.to_owned()) {
                        panic!(err)
                    }
                    trace!("Json sent");

                    // Update the last_lsn as we've sent the info
                    last_lsn = xlog_data.wal_end().into();
                    trace!("Last_lsn == {}", last_lsn);
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
}
