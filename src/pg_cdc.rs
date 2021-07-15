use super::CONFIG;

use futures::StreamExt;
use postgres_protocol::message::backend::ReplicationMessage;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::broadcast::Sender;
use tokio_postgres::{
    connect_replication,
    replication_client::{ReplicationClient, SnapshotMode},
    NoTls, ReplicationMode,
};

/// This represent the time between the real EPOCH and the EPOCH Postgres is using.
const TIME_SEC_CONVERSION: u64 = 946_684_800;

/// Open a replication connection to the Postgresql server and maintain the connection open on a task.
pub async fn connect_replica() -> ReplicationClient {
    // Connection information to postgres
    let conninfo = &CONFIG.get_str("CONNINFO").expect("Missing CONNINFO");

    // Form replication connection
    let (rclient, rconnection) =
        match connect_replication(conninfo, NoTls, ReplicationMode::Logical).await {
            Ok((rc, rco)) => (rc, rco),
            Err(err) => {
                error!("Fatal error, postgres connection: {}", err);
                std::process::exit(1);
            }
        };
    info!("Successfully connected to the replication");

    // Spawn connection to run on its own
    tokio::spawn(async move {
        // rconnection will never return except on Error
        // as long as no error, the connection is open.
        if let Err(err) = rconnection.await {
            error!("Fatal error, postgres connection: {}", err);
            std::process::exit(1);
        }
    });

    rclient
}

/// Drop previous replication slot and create a new one
pub async fn init_replication_slot(rclient: &mut ReplicationClient, slot_name: &str) {
    // Clean up previous replication slot (if any) named slot_name
    let resp = rclient.drop_replication_slot(slot_name, true).await;
    // Assert that the drop was done successfully
    assert!(
        resp.is_ok(),
        "Error while dropping previous replication slot: {:?}",
        resp.err()
    );
    info!(
        "Successfully dropped the previous replication slot with name {}",
        slot_name
    );

    // We set NoExportSnapshot and create the replication slot
    let plugin = "wal2json";
    let no_export = Some(SnapshotMode::NoExportSnapshot);
    let resp = rclient
        .create_logical_replication_slot(slot_name, true, plugin, no_export)
        .await;
    // Assert that the creation was done successfully
    assert!(
        resp.is_ok(),
        "Error while creating the replication slot: {:?}",
        resp.err()
    );
    info!(
        "Successfully created the logical replication slot with name {}",
        slot_name
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
        info!(
            "identify_system: postgres answered with: {:?}",
            identify_system
        );

        // Compute the epoch and register the last_lsn of the stream
        // last_lsn will be updated everytime we read a new value
        let epoch = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);
        let mut last_lsn = identify_system.xlogpos();

        // We now switch to consuming the stream
        let mut logical_stream = rclient
            .start_logical_replication(slot_name, last_lsn, options)
            .await
            .unwrap();
        info!(
            "Started the logical replication at {} with options: {:?}",
            last_lsn, options
        );

        // Keepalive sent count before a successfull keepalive
        // A successfull keepalive is when Postgres ask us for a reply of 1
        // we send it and after that we recieve a reply of 0.
        // This mean we successfully sent our keepalive packet.
        let mut keepalive_sent_count: u8 = 0;
        // Listen for the replication stream
        while let Some(replication_message) = logical_stream.next().await {
            match replication_message {
                Ok(ReplicationMessage::XLogData(xlog_data)) => {
                    // Extracting the json data from the ReplicationMessage
                    // converting to a String because he need to live longer than this scope
                    let json = String::from_utf8(xlog_data.data().to_vec()).unwrap();
                    trace!("Json: {}", json);

                    // Send the JSON to dispatcher consumer using this solution for the queue of the mpmc
                    // This fail only if no receiver are waiting for the sender, in our case it's safe to assume
                    // that if tx.send() fail, we panic! because our program is fucked up at this point
                    if let Err(err) = tx.send(json.to_owned()) {
                        error!("Fatal error, can't send to the channel: {}", err);
                        std::process::exit(1);
                    }
                    trace!("Json sent");

                    // Update the last_lsn as we've sent the info
                    last_lsn = xlog_data.wal_end().into();
                    trace!("Last_lsn == {}", last_lsn);
                }
                Ok(ReplicationMessage::PrimaryKeepAlive(keepalive)) => {
                    // If the keepalive reply is 1, this means postgres is waiting for our reply
                    // before cutting off the connection
                    if keepalive.reply() == 1 {
                        // Increment the keepalive count
                        keepalive_sent_count += 1;
                        // If more than 5 keepalive were sent before one success
                        // we just exit and crash because we're prolly spamming the CPU and the network.
                        if keepalive_sent_count > 5 {
                            error!(
                                "Fatal error, too much keepalive == 1: {}",
                                keepalive_sent_count
                            );
                            std::process::exit(1);
                        }
                        // Calculating the epoch for the packet
                        info!("sending keepalive reply with last_lsn == {}", last_lsn);
                        let ts = epoch.elapsed().unwrap().as_micros() as i64;
                        // Send the keepalive with the last lsn we got
                        match logical_stream
                            .as_mut()
                            .standby_status_update(last_lsn, last_lsn, last_lsn, ts, 0)
                            .await
                        {
                            Ok(_) => info!("keepalive sent"),
                            Err(err) => error!("failed to deliver the keepalive due to: {}", err),
                        }
                    } else if keepalive_sent_count != 0 {
                        // Reset the counter because if we got a reply of 0 it's all good
                        keepalive_sent_count = 0;
                    }
                }
                Err(err) => error!("Replication stream error: {}", err),
                _ => (),
            }
        }
    });
}
