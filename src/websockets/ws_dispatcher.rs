use crate::TABLES;

use super::server::{handler_message, ws_server};

use serde_json::Value;
use tokio::sync::broadcast::Sender;

/// Start a new task which loop over the broadcast's value it may send and dispatch them to websocket.
pub fn init_ws_dispatcher(ws_server: actix::Addr<ws_server::WsServer>, tx: Sender<String>) {
    // Create the Receiver for the broadcast
    let mut rx = tx.subscribe();
    // Spawn the task handling the rest
    tokio::spawn(async move {
        loop {
            // Wait until we recv the data
            let value = rx.recv().await;
            if value.is_err() {
                error!("Task just got an error: {}", value.err().unwrap());
                continue;
            }
            let value = value.unwrap();
            trace!("Dispatcher task got: {}", value);
            // Convert the data to a Value enum of serde_json
            let data: Value = serde_json::from_str(&value).unwrap();
            // If the change is not an array, don't handle it
            if !data["change"].is_array() {
                continue;
            }
            // For every change in the changes, we do as follow
            for change in data["change"].as_array().unwrap() {
                // Check the table (to str (using a match for safety))
                match change["table"].as_str() {
                    // If the table name exist, we match for the change kind
                    Some(table_name) => match change["kind"].as_str() {
                        // If the change kind exist
                        Some(change_type) => {
                            // Get the table name from the _hyper_x_x_chunk
                            // See comment in the main.rs for more information.
                            let table_name = if table_name.starts_with("_hyper_") {
                                let mut parts = table_name.splitn(4, '_');
                                let idx = match parts.nth(2) {
                                    Some(val) => val.parse::<usize>().unwrap() - 1,
                                    None => return table_name.to_owned(),
                                };
                                TABLES[idx].to_owned()
                            } else {
                                table_name.to_owned()
                            };
                            // We just send the info to the ws_server which will then broadcast
                            // the change to all the websocket listening for it
                            ws_server.do_send(handler_message::ClientMessage {
                                msg: change.to_owned(),
                                change_table: table_name.to_string(),
                                change_type: super::str_to_change_type(change_type),
                            });
                        }
                        None => error!("Dispatcher don't know the type of the change"),
                    },
                    None => error!("Dispatcher don't know the targeted table"),
                };
            }
        }
    });
}
