#[cfg(feature = "auth")]
use crate::utils::auth::{self, AuthInfo};
use crate::{
    utils::{
        query::parse_ws_query,
        ws_utils::{ServerState, SessionInfo, WsWatchFor, DELETE, INSERT, UPDATE},
    },
    CONFIG, NEXT_CLIENT_ID,
};

use axum::{
    extract::{
        ws::{Message, WebSocket},
        Query, WebSocketUpgrade,
    },
    response::Response,
    routing::{any, get},
    Extension, Router,
};
#[cfg(feature = "auth")]
use axum_extra::extract::cookie::Key;
use axum_server::tls_rustls::RustlsConfig;
use futures::{stream::SplitStream, FutureExt, StreamExt};
use sproot::{apierrors::ApiError, field_isset};
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

    #[cfg(feature = "auth")]
    let app = app.layer(Extension(Key::from(CONFIG.cookie_secret.as_bytes())));

    // Convert the binding into a SocketAddr
    let socket: SocketAddr = match CONFIG.binding.parse() {
        Ok(val) => val,
        Err(e) => {
            error!("The BINDING is not a valid SocketAddr: {}", e);
            std::process::exit(1);
        }
    };

    // Run the axum server
    if CONFIG.https {
        info!("API served on {} (HTTPS)", socket);
        axum_server::bind_rustls(
            socket,
            RustlsConfig::from_pem_file(
                field_isset!(CONFIG.key_cert.as_ref(), "key_cert").unwrap(),
                field_isset!(CONFIG.key_priv.as_ref(), "key_priv").unwrap(),
            )
            .await
            .unwrap(),
        )
        .serve(app.into_make_service())
        .await
        .unwrap();
    } else {
        info!("API served on {} (HTTP)", socket);
        axum_server::bind(socket)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

async fn ws_handler(
    #[cfg(feature = "auth")] auth: AuthInfo,
    Extension(state): Extension<Arc<ServerState>>,
    Query(params): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Result<Response, ApiError> {
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

    #[cfg(feature = "auth")]
    {
        if !auth.is_admin {
            if watch_for.specific.is_none() {
                return Err(ApiError::InvalidRequestError(None));
            }

            let specific = watch_for.specific.clone().unwrap();
            auth::restrict_auth(auth, specific).await?;
        }
    }

    Ok(ws.on_upgrade(|socket: WebSocket| async {
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
    }))
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
    trace!("Websocket: client disconnected: {}", id);
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
