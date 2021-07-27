use crate::websockets::SessionInfo;

use super::{ServerState, WsWatchFor, DELETE, INSERT, NEXT_CLIENT_ID, UPDATE};

use futures::{FutureExt, StreamExt};
use std::collections::HashSet;
use std::sync::{atomic::Ordering, Arc};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::{
    self,
    ws::{Message, WebSocket},
};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(40);

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

    // Use an unbounded channel to handle buffering and flushing of messages to the websocket...
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
            gate: tx.clone(),
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

    // Construct the heartbeat interval, used to know when to send Ping to the client
    let mut hb_tick = tokio::time::interval(HEARTBEAT_INTERVAL);

    // When was the last time the server answered us with a Pong ?
    let mut hb_pong: Instant = Instant::now();

    // Return a `Future` that is basically a state machine managing
    // this specific user's connection.
    // Every time the client sends a message, do nothing
    // Run an infinite loop that only stop when the connection is closed or should be
    loop {
        // Tokio select will wait on multiple branch, here user_ws_rx.next() and hb_tick.tick()
        tokio::select! {
            Some(msg) = user_ws_rx.next() => {
                match msg {
                    Ok(msg) => {
                        match msg {
                            // If the client sent us a pong message
                            e if e.is_pong() => {
                                info!("Pong receive (uid={})", my_id);
                                hb_pong = Instant::now();
                            },
                            // If the client send us a close message
                            e if e.is_close() => {
                                info!("Websocket closed (uid={})", my_id);
                                break;
                            },
                            _ => {}
                        }
                    },
                    Err(e) => {
                        error!("Websocket error (uid={}): {}", my_id, e);
                        break;
                    }
                }
            }
            _ = hb_tick.tick() => {
                // If the lastest hb_pong from the client is oldest than the TIMEOUT
                if Instant::now().duration_since(hb_pong) > CLIENT_TIMEOUT {
                    // Breaking will auto close the WS connection
                    break;
                }
                // If we can't send the ping message, break !
                match tx.send(Ok(Message::ping(""))) {
                    Ok(_) => info!("Ping sent (uid={})", my_id),
                    Err(e) => {
                        error!("Cannot send the ping (uid={}) due to: {}", my_id, e);
                        break;
                    }
                }
            }
        }
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
