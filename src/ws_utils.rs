use crate::ws_server;

use serde_json::Value;
use tokio::sync::broadcast::Sender;

/// Representation of SQL Change for CDC
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum ChangeType {
    INSERT = 0,
    UPDATE = 1,
    DELETE = 2,
    UNKNOWN = 3,
    ALL = 4,
}

impl PartialEq for ChangeType {
    fn eq(&self, other: &ChangeType) -> bool {
        *self as u8 == *other as u8
    }
}

/// Convert str typed SQL change to ChangeType
pub fn str_to_change_type(change_type: &str) -> ChangeType {
    match change_type {
        "insert" => ChangeType::INSERT,
        "update" => ChangeType::UPDATE,
        "delete" => ChangeType::DELETE,
        "*" => ChangeType::ALL,
        _ => ChangeType::UNKNOWN,
    }
}

/// Start a new task which loop over the broadcast's value it may send and dispatch them to websocket.
pub fn init_ws_dispatcher(ws_server: actix::Addr<ws_server::WsServer>, tx: Sender<String>) {
    // Create the Receiver for the broadcast
    let mut rx = tx.subscribe();
    // Spawn the task handling the rest
    tokio::spawn(async move {
        loop {
            // TODO - Document
            let value = rx.recv().await;
            if value.is_err() {
                error!("Task just got an error: {}", value.err().unwrap());
                continue;
            }
            let value = value.unwrap();
            trace!("Dispatcher task got: {}", value);
            let data: Value = serde_json::from_str(&value).unwrap();
            if !data["change"].is_array() {
                continue;
            }
            for change in data["change"].as_array().unwrap() {
                match change["table"].as_str() {
                    Some(table_name) => match change["kind"].as_str() {
                        Some(change_type) => {
                            ws_server.do_send(ws_server::ClientMessage {
                                msg: change.to_owned(),
                                change_table: table_name.to_string(),
                                change_type: str_to_change_type(change_type),
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
