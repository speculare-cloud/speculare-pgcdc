use crate::websockets::SessionInfo;

use super::{ServerState, WsWatchFor, DELETE, INSERT, NEXT_CLIENT_ID, UPDATE};

use futures::{FutureExt, StreamExt};
use std::collections::HashSet;
use std::sync::{atomic::Ordering, Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp;
use warp::ws::WebSocket;

pub async fn client_connected(
    ws: WebSocket,
    server_state: Arc<ServerState>,
    watch_for: WsWatchFor,
) {
    // Use a counter to assign a new unique ID for this user.
    let my_id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
    info!("Client connected: {}", my_id);

    // Split the socket into a sender and receive of messages.
    let (user_ws_tx, mut user_ws_rx) = ws.split();

    // Use an unbounded channel to handle buffering and flushing of messages
    // to the websocket...
    let (tx, rx) = mpsc::unbounded_channel();
    let rx = UnboundedReceiverStream::new(rx);
    tokio::task::spawn(rx.forward(user_ws_tx).map(|result| {
        if let Err(e) = result {
            error!("websocket send error for: {}", e);
        }
    }));

    // Save the sender in our list of connected clients.
    server_state.clients.write().unwrap().insert(
        my_id,
        SessionInfo {
            gate: tx,
            watch_for: watch_for.to_owned(),
        },
    );

    // Insert in the right category depending on the ChangeType
    if has_bit!(watch_for.change_flag, INSERT) {
        server_state
            .inserts
            .write()
            .unwrap()
            .entry(watch_for.change_table.to_owned())
            .or_insert_with(HashSet::new)
            .insert(my_id);
    }
    if has_bit!(watch_for.change_flag, UPDATE) {
        server_state
            .updates
            .write()
            .unwrap()
            .entry(watch_for.change_table.to_owned())
            .or_insert_with(HashSet::new)
            .insert(my_id);
    }
    if has_bit!(watch_for.change_flag, DELETE) {
        server_state
            .deletes
            .write()
            .unwrap()
            .entry(watch_for.change_table)
            .or_insert_with(HashSet::new)
            .insert(my_id);
    }

    // Return a `Future` that is basically a state machine managing
    // this specific user's connection.
    // Every time the client sends a message, do nothing
    while let Some(result) = user_ws_rx.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("websocket error(uid={}): {}", my_id, e);
                break;
            }
        };
    }

    // user_ws_rx stream will keep processing as long as the user stays
    // connected. Once they disconnect, then...
    user_disconnected(my_id, &server_state, watch_for.change_flag).await;
}

async fn user_disconnected(my_id: usize, server_state: &Arc<ServerState>, change_flag: u8) {
    info!("Client disconnected: {}", my_id);

    // Stream closed up, so remove from the user list
    server_state.clients.write().unwrap().remove(&my_id);
    if has_bit!(change_flag, INSERT) {
        // For each table entries, remove the id of the ws_session
        for list_sessions in server_state.inserts.write().unwrap().values_mut() {
            // Even if the event.id is not in the list_sessions, it will try
            list_sessions.remove(&my_id);
        }
    }
    if has_bit!(change_flag, UPDATE) {
        for list_sessions in server_state.updates.write().unwrap().values_mut() {
            list_sessions.remove(&my_id);
        }
    }
    if has_bit!(change_flag, DELETE) {
        for list_sessions in server_state.deletes.write().unwrap().values_mut() {
            list_sessions.remove(&my_id);
        }
    }
}
