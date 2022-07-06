use crate::websockets;
#[cfg(feature = "timescale")]
use crate::{cdc::extract_hyper_idx, TABLES_LOOKUP};

use super::{ServerState, DELETE, INSERT, UPDATE};

use ahash::AHashSet;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;
use warp::ws::Message;

/// Get the table name from an &str, returning a String
/// This is used due to TimescaleDB renaming the hypertable using a
/// pattern '_hyper_' with some number and all. If we can't convert the pattern
/// back to it's original table name, return the pattern name.
#[cfg(feature = "timescale")]
fn get_table_name(table_name: &str) -> String {
    if table_name.starts_with("_hyper_") {
        let idx = match extract_hyper_idx(table_name) {
            Ok(idx) => idx,
            Err(_) => {
                error!(
                    "Match table: table {} cannot be deconstructed into an idx",
                    table_name
                );
                return table_name.to_owned();
            }
        };
        // Get the table name from the index and return an owned String
        // or continue the loop and skip this value if not found.
        match TABLES_LOOKUP.read().unwrap().get(&idx) {
            Some(val) => return val.to_owned(),
            None => {
                error!(
                    "Match table: table not found inside using index: {}:{}",
                    idx, table_name,
                );
                return table_name.to_owned();
            }
        }
    }
    table_name.to_owned()
}

/// Send a message to a specific group of sessions (insert, update or delete)
fn send_message(
    message: &serde_json::Value,
    sessions: Option<&AHashSet<usize>>,
    server_state: &Arc<ServerState>,
) {
    // If no session were defined, skip
    if sessions.is_none() {
        return;
    }
    // For every sessions'id in the tables HashMap
    for id in sessions.unwrap() {
        // Get the client from the clients list inside the server_state
        if let Some(client) = server_state.clients.read().unwrap().get(id) {
            // Check if the client asked for a particular filter
            let to_send = match &client.watch_for.specific {
                Some(specific) => specific.match_filter(message),
                None => true,
            };

            if to_send {
                // Send the message to the client
                if let Err(_disconnected) = client.gate.send(Ok(Message::text(message.to_string())))
                {
                    error!("Send_message: client disconnected, should be removed soon");
                }
            }
        }
    }
}

/// Start a new task which loop over the Receiver's value it may get and forward them to websockets.
pub async fn start_forwarder(mut rx: Receiver<String>, server_state: Arc<ServerState>) {
    trace!("Forwarder: Started and waiting for a message");

    loop {
        match rx.recv().await {
            Some(mut value) => {
                // Convert the data to a Value enum of serde_json
                // Using simd optimization through simd_json crate.
                let data: Value = simd_json::from_str(&mut value).unwrap();
                // Extract what we really want and assert that it exists
                let changes = match data["change"].as_array() {
                    Some(val) => val,
                    None => {
                        error!("Forwarder: The message is invalid: {}", data);
                        continue;
                    }
                };
                // For each change inside of changes, we do the following treatment
                for change in changes {
                    // Check the table (to str (using a match for safety))
                    if let (Some(table_name), Some(change_type)) =
                        (change["table"].as_str(), change["kind"].as_str())
                    {
                        // Get the table name from the _hyper_x_x_chunk
                        // See comment in the main.rs for more information.
                        #[cfg(feature = "timescale")]
                        let table_name = get_table_name(table_name);
                        #[cfg(not(feature = "timescale"))]
                        let table_name = table_name.to_owned();
                        // Construct the change_flag
                        let mut change_flag = 0u8;
                        // At this stage, the change_flag can be only be one of INSERT, UPDATE, DELETE
                        // but not multiple of them.
                        websockets::apply_flag(&mut change_flag, change_type);
                        // Only send the message to those interested in the change_type
                        if has_bit!(change_flag, INSERT) {
                            // First get the lock over the RwLock guard
                            let lock = server_state.inserts.read().unwrap();
                            // Then get the sessions out of it
                            let sessions = lock.get(&table_name);
                            // And finally send the message to each client inside that sessions AHashSet
                            send_message(change, sessions, &server_state);
                        } else if has_bit!(change_flag, UPDATE) {
                            let lock = server_state.updates.read().unwrap();
                            let sessions = lock.get(&table_name);
                            send_message(change, sessions, &server_state);
                        } else if has_bit!(change_flag, DELETE) {
                            let lock = server_state.deletes.read().unwrap();
                            let sessions = lock.get(&table_name);
                            send_message(change, sessions, &server_state);
                        } else {
                            error!("Forwarder: change_flag {:?} not handled.", change_flag);
                            continue;
                        };
                    } else {
                        error!(
                            "Forwarder: table ({:?}) or change_type ({:?}) not present.",
                            change["table"], change["kind"]
                        );
                    }
                }
            }
            None => {
                trace!("Channel returned None");
                return;
            }
        }
    }
}
