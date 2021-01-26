#[macro_use]
extern crate log;

use postgres_protocol::message::backend::ReplicationMessage;
use tokio::stream::StreamExt;
use tokio_postgres::{
    connect_replication, replication_client::SnapshotMode, types::Lsn, Error, NoTls,
    ReplicationMode,
};
// use rand::Rng;

mod logger;

// fn random_slot_name() -> String {
//     let mut rng = rand::thread_rng();
//     let array: [&str; 6] = [
//         &"gigantic",
//         &"scold",
//         &"greasy",
//         &"shaggy",
//         &"wasteful",
//         &"ablaze",
//     ];

//     let rnbr = rng.gen_range(0..6);
//     format!("spc_{}{}", array[rnbr], rng.gen_range(10..100))
// }

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Init the logger and set the debug level correctly
    logger::configure();

    let conninfo = "host=localhost user=postgres dbname=pgcdc password=password";

    // form replication connection
    let (mut rc, rco) = connect_replication(conninfo, NoTls, ReplicationMode::Logical)
        .await
        .unwrap();

    // this connection will stay alive as long as we keep sending status update (keepalive)
    tokio::spawn(async move {
        if let Err(e) = rco.await {
            panic!("connection error: {}", e);
        }
    });

    let identify_system = rc.identify_system().await?;

    let slot = "fixed_name_for_now";

    let _ = rc.drop_replication_slot(slot, false).await.unwrap();

    let _ = rc
        .create_logical_replication_slot(
            slot,
            true,
            "wal2json",
            Some(SnapshotMode::NoExportSnapshot),
        )
        .await?;

    let mut rcc = rc.clone();
    let mut logical_stream = rcc
        .start_logical_replication(
            slot,
            identify_system.xlogpos(),
            &vec![("pretty-print", "0")],
        )
        .await?;

    while let Some(replication_message) = logical_stream.next().await {
        match replication_message? {
            ReplicationMessage::XLogData(xlog_data) => {
                let json = std::str::from_utf8(xlog_data.data()).unwrap();
                info!("JSON  text: {}", json);
            }
            ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                if keepalive.reply() == 1 {
                    warn!("Keepalive is equals to 1");
                    let wal_end: Lsn = keepalive.wal_end().into();
                    rc.standby_status_update(wal_end, wal_end, wal_end, keepalive.timestamp(), 0)
                        .await?;
                    info!("Status update sent");
                }
            }
            _ => (),
        }
    }
    Ok(())
}
