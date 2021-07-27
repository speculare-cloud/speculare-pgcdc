use super::{ServerState, DELETE, INSERT, UPDATE};
use crate::{websockets, TABLES_BY_INDEX};

use serde_json::Value;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::mpsc::Receiver;
use warp::ws::Message;

/// Get the table name from an &str, returning a String
/// This is used due to TimescaleDB renaming the hypertable using a
/// pattern '_hyper_' with some number and all. If we can't convert the pattern
/// back to it's original table name, return the pattern name.
fn get_table_name(table_name: &str) -> String {
    if table_name.starts_with("_hyper_") {
        let mut parts = table_name.splitn(4, '_');
        let idx = match parts.nth(2) {
            Some(val) => val.parse::<i8>().unwrap() - 1,
            None => {
                error!("Table {} cannot be deconstructed into an idx", table_name);
                return table_name.to_owned();
            }
        };
        // Get the table name from the index and return an owned String
        // or continue the loop and skip this value if not found.
        match TABLES_BY_INDEX.read().unwrap().get(&(idx as usize)) {
            Some(val) => return val.to_owned(),
            None => {
                error!("Table not found inside using index: {}:{}", idx, table_name,);
                return table_name.to_owned();
            }
        }
    }
    table_name.to_owned()
}

/// Send a message to a specific group of sessions (insert, update or delete)
fn send_message(
    message: &serde_json::Value,
    sessions: Option<&HashSet<usize>>,
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
                    error!("Client disconnected, should be dropped soon");
                }
            }
        }
    }
}

/// Start a new task which loop over the Receiver's value it may get and forward them to websockets.
pub fn start_forwarder(mut rx: Receiver<String>, server_state: Arc<ServerState>) {
    tokio::spawn(async move {
        trace!("Forwarder: Started and waiting for a message");
        while let Some(mut value) = rx.recv().await {
            // Convert the data to a Value enum of serde_json
            // Using simd optimization through simd_json crate.
            let data: Value = simd_json::from_str(&mut value).unwrap();
            // Extract what we really want and assert that it exists
            let changes = match data["change"].as_array() {
                Some(val) => val,
                None => {
                    error!("The message we got doesn't contains a change: {}", data);
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
                    let table_name = get_table_name(table_name);
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
                        // And finally send the message to each client inside that sessions HashSet
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
                        error!("Change {:?} not handled (yet).", change_flag);
                        continue;
                    };
                } else {
                    error!(
                        "Table ({:?}) or change_type ({:?}) not present.",
                        change["table"], change["kind"]
                    );
                }
            }
        }
    });
}
