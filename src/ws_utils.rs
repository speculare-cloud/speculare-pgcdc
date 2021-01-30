use crate::ws_server;

use serde_json::Value;
use tokio::sync::broadcast::Sender;

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
                        Some(table_name) => {
                            ws_server.do_send(ws_server::ClientMessage {
                                msg: val,
                                table: table_name.to_string(),
                            });
                        }
                        None => error!("Dispatcher don't know the targeted table"),
                    };
                }
                Err(err) => error!("Task just got an error: {}", err),
            }
        }
    });
}
