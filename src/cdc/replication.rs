use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, Bytes, BytesMut};
use futures::{SinkExt, StreamExt};
use std::{
    io::{Cursor, Read},
    pin::Pin,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc::Sender;
use tokio_postgres::{Client, CopyBothDuplex, SimpleQueryMessage, SimpleQueryRow};

const TIME_SEC_CONVERSION: u64 = 946_684_800;
const XLOG_DATA_TAG: u8 = b'w';
const PRIMARY_KEEPALIVE_TAG: u8 = b'k';

lazy_static::lazy_static! {
    static ref EPOCH: SystemTime = UNIX_EPOCH + Duration::from_secs(TIME_SEC_CONVERSION);
}

#[inline]
pub fn current_time() -> u64 {
    EPOCH.elapsed().unwrap().as_micros() as u64
}

/// Send a CREATE_REPLICATION_SLOT ... TEMPORARY LOGICAL to the server.
/// The response to the CREATE_REPLICATION is not documented but based
/// on the code, it's an HashMap containing the following:
///
/// 1. "slot_name": name of the slot that was created, as requested
/// 2. "consistent_point": LSN at which we became consistent
/// 3. "snapshot_name": exported snapshot's name
/// 4. "output_plugin": name of the output plugin, as requested
pub async fn replication_slot_create(client: &Client, slot_name: &str) -> String {
    let slot_query = &format!(
        "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL wal2json NOEXPORT_SNAPSHOT",
        slot_name
    );

    let resp: Vec<SimpleQueryRow> = match client.simple_query(slot_query).await {
        Ok(result) => result
            .into_iter()
            .filter_map(|data| match data {
                SimpleQueryMessage::Row(row) => Some(row),
                _ => None,
            })
            .collect(),
        Err(e) => {
            error!(
                "Replication: '{}' cannot get the consistent_point: {}",
                slot_name, e
            );
            std::process::exit(1);
        }
    };

    let lsn = resp[0].get("consistent_point").unwrap().to_owned();

    trace!(
        "Replication: slot {} created and got lsn {}",
        slot_name,
        lsn
    );

    lsn
}

/// Starts streaming logical changes from replication slot pgcdc_repl,
/// starting from position start_lsn.
pub async fn replication_stream_start(
    client: &Client,
    slot_name: &str,
    start_lsn: &str,
) -> CopyBothDuplex<Bytes> {
    let repl_query = format!("START_REPLICATION SLOT {} LOGICAL {}", slot_name, start_lsn);
    let copy_both_result = client.copy_both_simple::<bytes::Bytes>(&repl_query).await;
    let duplex_stream = match copy_both_result {
        Ok(result) => result,
        Err(e) => {
            error!("Replication: cannot get the a CopyBothDuplex: {}", e);
            std::process::exit(1);
        }
    };

    trace!(
        "Replication: started successfully using slot {} from lsn {}",
        slot_name,
        start_lsn
    );

    duplex_stream
}

/// Tries to read and process one message from a replication stream, using async I/O.
pub async fn replication_stream_poll(duplex_stream: CopyBothDuplex<Bytes>, tx: Sender<String>) {
    let mut boxed = Box::pin(duplex_stream);

    let mut sync_lsn: u64 = 0;
    while let Some(data) = boxed.next().await {
        match data {
            Ok(bytes) => {
                let mut buf = Cursor::new(bytes);
                let tag = match buf.read_u8() {
                    Ok(tag) => tag,
                    Err(e) => {
                        error!("Replication: cannot read_u8 for tag: {}", e);
                        continue;
                    }
                };

                match tag {
                    XLOG_DATA_TAG => {
                        parse_xlogdata_message(&mut buf, &mut sync_lsn, &tx).await;
                    }
                    PRIMARY_KEEPALIVE_TAG => {
                        parse_keepalive_message(&mut boxed, &mut buf, &mut sync_lsn).await;
                    }
                    tag => {
                        error!("Replication: Unknown streaming message type: `{}`", tag);
                        continue;
                    }
                }
            }
            Err(e) => {
                error!("Replication: unknown error: {}", e);
            }
        }
    }
}

/// Parses a XLogData message received from the server. It is packed binary with the
/// following structure:
/// - u64: The starting point of the WAL data in this message.
/// - u64: The current end of WAL on the server.
/// - u64: The server's system clock at the time of transmission, as microseconds
///        since midnight on 2000-01-01.
/// - Byte(n): The output from the logical replication output plugin.
async fn parse_xlogdata_message(buf: &mut Cursor<Bytes>, sync_lsn: &mut u64, tx: &Sender<String>) {
    let wal_pos = match buf.read_u64::<BigEndian>() {
        Ok(wal) => wal,
        Err(e) => {
            error!("XLogData: cannot read_u64 wal_pos: {}", e);
            return;
        }
    };
    let _wal_end = buf.read_u64::<BigEndian>();
    let _ts = buf.read_u64::<BigEndian>();

    trace!("XLogData: wal_pos {}/{:X}", wal_pos >> 32, wal_pos);

    let mut data: String = String::with_capacity(32);
    match buf.read_to_string(&mut data) {
        Ok(size) => {
            if size == 0 {
                return;
            }
        }
        Err(e) => {
            error!("XLogData: cannot read_to_string: {}", e);
            return;
        }
    };
    // Broadcast data to the transmitter
    // send can fail if the other half of the channel is closed, either due to close
    // or because the Receiver has been dropped. In addition send will also block until
    // there is a room for the message into the queue.
    if let Err(e) = tx.send(data).await {
        error!("XLogData: can't send to the channel due to: {}", e);
        std::process::exit(1);
    }

    *sync_lsn = wal_pos;
}

/// Parses a "Primary keepalive message" received from the server. It is packed binary
/// with the following structure:
///
/// - u64: The current end of WAL on the server.
/// - u64: The server's system clock at the time of transmission, as microseconds
///        since midnight on 2000-01-01.
/// - u8: 1 means that the client should reply to this message as soon as possible,
///       to avoid a timeout disconnect. 0 otherwise.
async fn parse_keepalive_message(
    conn: &mut Pin<Box<CopyBothDuplex<Bytes>>>,
    buf: &mut Cursor<Bytes>,
    sync_lsn: &mut u64,
) {
    let wal_pos = buf.read_u64::<BigEndian>().unwrap();
    let _ = buf.read_i64::<BigEndian>(); // timestamp
    let reply_requested = match buf.read_u8() {
        Ok(reply) => reply == 1,
        Err(e) => {
            error!("Keepalive: cannot read_u8 reply_requested: {}", e);
            return;
        }
    };

    // Not 100% sure whether it's semantically correct to update our LSN position here --
    // the keepalive message indicates the latest position on the server, which might not
    // necessarily correspond to the latest position on the client. But this is what
    // pg_recvlogical does, so it's probably ok.
    *sync_lsn = std::cmp::max(wal_pos, *sync_lsn);

    trace!(
        "Keepalive: wal_pos {}/{:X}, reply_requested {}",
        wal_pos >> 32,
        wal_pos,
        reply_requested
    );

    if reply_requested {
        send_checkpoint(conn, *sync_lsn).await;
    }
}

/// Send a "Standby status update" message to server, indicating the LSN up to which we
/// have received logs. This message is packed binary with the following structure:
///
/// - u8('r'): Identifies the message as a receiver status update.
/// - u64: The location of the last WAL byte + 1 received by the client.
/// - u64: The location of the last WAL byte + 1 stored durably by the client.
/// - u64: The location of the last WAL byte + 1 applied to the client DB.
/// - u64: The client's system clock, as microseconds since midnight on 2000-01-01.
/// - u8: If 1, the client requests the server to reply to this message immediately.
async fn send_checkpoint(conn: &mut Pin<Box<CopyBothDuplex<Bytes>>>, lsn: u64) {
    let mut ka_buf = BytesMut::with_capacity(34);

    ka_buf.put_u8(b'r');
    ka_buf.put_u64(lsn);
    ka_buf.put_u64(lsn);
    ka_buf.put_u64(0); // Only used by physical replication
    ka_buf.put_u64(current_time());
    ka_buf.put_u8(0);

    let _ = (*conn).send(ka_buf.freeze()).await;

    trace!("Checkpoint: lsn: {}/{:X}", lsn >> 32, lsn);
}
