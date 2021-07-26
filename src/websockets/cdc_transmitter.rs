use crate::TABLES_BY_INDEX;

use crate::websockets::{
    self,
    server::{handler_message, ws_server},
};

use serde_json::Value;
use tokio::sync::broadcast::Sender;

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

/// Start a new task which loop over the broadcast's value it may send and dispatch them to websocket.
pub fn launch_broadcaster(ws_server: actix::Addr<ws_server::WsServer>, tx: Sender<String>) {
    // Create the Receiver for the broadcast
    let mut rx = tx.subscribe();
    // Spawn the task handling the rest
    tokio::spawn(async move {
        info!("Successfully started the WsDispatcher");
        loop {
            // Wait until we recv the data
            let value = rx.recv().await;
            if value.is_err() {
                error!("Task just got an error: {}", value.err().unwrap());
                continue;
            }
            let mut value = value.unwrap();
            trace!("Dispatcher task got: {}", value);
            // Convert the data to a Value enum of serde_json
            let data: Value = simd_json::from_str(&mut value).unwrap();
            // Extract what we really want
            let changes = data["change"].as_array();
            // If the changes is None, we don't continue
            if changes.is_none() {
                continue;
            }
            // For each change inside of changes, we do the following treatment
            for change in changes.unwrap() {
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
                    // We just send the info to the ws_server which will then broadcast
                    // the change to all the websocket listening for it
                    // Send a message using:
                    // => server/handler_message.rs -> fn handle
                    ws_server.do_send(handler_message::ClientMessage {
                        msg: change.to_owned(),
                        change_table: table_name.to_string(),
                        change_flag,
                    });
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