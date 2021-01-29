use crate::ws_server;

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
                    ws_server.do_send(ws_server::ClientMessage {
                        msg: val,
                        table: "test_table".to_owned(),
                    });
                }
                Err(err) => error!("Task just got an error: {}", err),
            }
        }
    });
}
