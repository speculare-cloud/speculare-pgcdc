use crate::CONFIG;

use tokio_postgres::{
    connect_replication,
    replication_client::{ReplicationClient, SnapshotMode},
    NoTls, ReplicationMode,
};

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
    info!("Dropped the previous replication slot {}", slot_name);

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
    info!("Created the logical replication slot {}", slot_name);
}
