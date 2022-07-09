use crate::{
    utils::{
        specific_filter::{DataType, SpecificFilter},
        ws_utils::{apply_flag, ServerState, SessionInfo, WsWatchFor, DELETE, INSERT, UPDATE},
    },
    CONFIG, NEXT_CLIENT_ID, TABLES,
};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, WebSocketUpgrade,
    },
    routing::{any, get},
    Extension, Router,
};
use futures::{stream::SplitStream, FutureExt, StreamExt};
use sproot::apierrors::ApiError;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{atomic::Ordering, Arc},
};
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower_http::trace::{DefaultMakeSpan, TraceLayer};

pub async fn run_server(state: Arc<ServerState>) {
    // build our application with some routes
    let app = Router::new()
        .route("/ping", any(|| async { "zpour" }))
        .route("/ws", get(ws_handler))
        // logging so we can see whats going on
        .layer(TraceLayer::new_for_http().make_span_with(DefaultMakeSpan::default()))
        .layer(Extension(state));

    // Convert the binding into a SocketAddr
    let socket: SocketAddr = match CONFIG.binding.parse() {
        Ok(val) => val,
        Err(e) => {
            error!("The BINDING is not a valid SocketAddr: {}", e);
            std::process::exit(1);
        }
    };

    info!("API served on {}", socket);
    // Run the axum server
    axum::Server::bind(&socket)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn ws_handler(
    Extension(state): Extension<Arc<ServerState>>,
    Query(params): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Result<(), ApiError> {
    // Extract the query params or return a bad request
    let query = match params.get("query") {
        Some(q) => q,
        None => {
            return Err(ApiError::ExplicitError(String::from(
                "missing the query params",
            )))
        }
    };

    // Construct the watch_for from the query and if error, bad request
    let watch_for = parse_ws_query(query)?;

    ws.on_upgrade(|socket: WebSocket| async {
        // TODO - Determine if using a UUID would be better (faster)?
        let id = NEXT_CLIENT_ID.fetch_add(1, Ordering::Relaxed);
        trace!("Websocket: client connected: {}", id);

        // Split the socket into a sender and receive of messages.
        let (user_ws_tx, user_ws_rx) = socket.split();

        // Use an bounded channel (256) to handle buffering and flushing of messages to the websocket.
        let (tx, rx) = mpsc::unbounded_channel();
        let rx = UnboundedReceiverStream::new(rx);
        tokio::task::spawn(rx.forward(user_ws_tx).map(|result| {
            if let Err(err) = result {
                error!("Websocket: send error for: {}", err);
            }
        }));

        ws_connected(id, tx, user_ws_rx, watch_for, state).await;
    });

    Ok(())
}

fn parse_ws_query(query: &str) -> Result<WsWatchFor, ApiError> {
    let mut parts = query.split(':');
    let mut change_flag = 0;

    // Apply bit operation to the change_flag based on the query type
    match parts.next() {
        Some(val) => val
            .split(',')
            .for_each(|ctype| apply_flag(&mut change_flag, ctype)),
        None => {
            return Err(ApiError::ExplicitError(String::from(
                "the change_type params is not present",
            )));
        }
    }

    // If change_flag is 0, we have an error because we don't listen to any known event types
    if change_flag == 0 {
        return Err(ApiError::ExplicitError(String::from(
            "the change_type params does not match requirements",
        )));
    }

    // Get the change_table and check if the table is valid
    let change_table = match parts.next() {
        Some(table) => {
            // Check if the table exists inside TABLES
            if !TABLES.read().unwrap().iter().any(|v| v == table) {
                return Err(ApiError::ExplicitError(String::from(
                    "the table asked for does not exists",
                )));
            }
            table.to_owned()
        }
        None => {
            return Err(ApiError::ExplicitError(String::from(
                "the change_table params is not present",
            )));
        }
    };

    // Construct the SpecificFilter from the request
    let specific: Option<SpecificFilter> = if let Some(filter) = parts.next() {
        let mut fparts = filter.splitn(3, '.');
        // let mut fparts = filter.splitn(2, ".eq.");
        match (fparts.next(), fparts.next(), fparts.next()) {
            (Some(col), Some(eq), Some(val)) => match eq {
                "eq" => Some(SpecificFilter {
                    column: serde_json::Value::String(col.to_owned()),
                    value: DataType::String(val.to_owned()),
                }),
                "in" => {
                    let items = val
                        .split(',')
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>();
                    Some(SpecificFilter {
                        column: serde_json::Value::String(col.to_owned()),
                        value: DataType::Array(items),
                    })
                }
                _ => None,
            },
            _ => None,
        }
    } else {
        None
    };

    // Construct what the client is listening to
    Ok(WsWatchFor {
        change_table,
        change_flag,
        specific,
    })
}

async fn ws_connected(
    id: usize,
    tx: UnboundedSender<Result<Message, axum::Error>>,
    mut user_ws_rx: SplitStream<WebSocket>,
    watch_for: WsWatchFor,
    state: Arc<ServerState>,
) {
    let change_flag = watch_for.change_flag;
    let change_table = watch_for.change_table.to_owned();
    // Save the sender in our list of connected clients.
    state.clients.write().unwrap().insert(
        id,
        SessionInfo {
            gate: tx.clone(),
            watch_for,
        },
    );

    // Insert in the right category depending on the ChangeType
    if has_bit!(change_flag, INSERT) {
        state
            .inserts
            .write()
            .unwrap()
            .entry(change_table.clone())
            .or_insert_with(HashSet::new)
            .insert(id);
    }
    if has_bit!(change_flag, UPDATE) {
        state
            .updates
            .write()
            .unwrap()
            .entry(change_table.clone())
            .or_insert_with(HashSet::new)
            .insert(id);
    }
    if has_bit!(change_flag, DELETE) {
        state
            .deletes
            .write()
            .unwrap()
            .entry(change_table)
            .or_insert_with(HashSet::new)
            .insert(id);
    }

    while let Some(event) = user_ws_rx.next().await {
        match event {
            Ok(payload) => {
                debug!("Websocket: msg: {:?}", payload);
                if let Message::Close(_) = payload {
                    info!("Websocket: client closed");
                    break;
                }
            }
            Err(err) => {
                error!("Websocket: error: {}", err);
                break;
            }
        }
    }

    ws_disconnected(id, state, change_flag);
}

fn ws_disconnected(id: usize, state: Arc<ServerState>, change_flag: u8) {
    info!("Websocket: client disconnected: {}", id);
    // Stream closed up, so remove from the user list
    state.clients.write().unwrap().remove(&id);

    if has_bit!(change_flag, INSERT) {
        // For each table entries, remove the id of the ws_session
        for list_sessions in state.inserts.write().unwrap().values_mut() {
            // Even if the event.id is not in the list_sessions, it will try
            list_sessions.remove(&id);
        }
    }
    if has_bit!(change_flag, UPDATE) {
        for list_sessions in state.updates.write().unwrap().values_mut() {
            list_sessions.remove(&id);
        }
    }
    if has_bit!(change_flag, DELETE) {
        for list_sessions in state.deletes.write().unwrap().values_mut() {
            list_sessions.remove(&id);
        }
    }
}
