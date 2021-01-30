use crate::ws_server;

use serde_json::Value;
use tokio::sync::broadcast::Sender;

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
            let value = rx.recv().await;
            match value {
                Ok(val) => {
                    trace!("Dispatcher task got: {}", val);
                    let data: Value = serde_json::from_str(&val).unwrap();
                    match data["change"][0]["table"].as_str() {
                        Some(table_name) => match data["change"][0]["kind"].as_str() {
                            Some(change_type) => {
                                ws_server.do_send(ws_server::ClientMessage {
                                    msg: val,
                                    change_table: table_name.to_string(),
                                    change_type: str_to_change_type(change_type),
                                });
                            }
                            None => error!("Dispatcher don't know the type of the change"),
                        },
                        None => error!("Dispatcher don't know the targeted table"),
                    };
                }
                Err(err) => error!("Task just got an error: {}", err),
            }
        }
    });
}
