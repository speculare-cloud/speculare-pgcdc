#[cfg(feature = "auth")]
use super::AppState;
use super::{ws_handler, ws_utils::ServerState};

use crate::CONFIG;

use axum::{
    routing::{any, get},
    Extension, Router,
};
#[cfg(feature = "auth")]
use axum_extra::extract::cookie::Key;
use axum_server::tls_rustls::RustlsConfig;
use sproot::{apierrors::ApiError, field_isset};
use std::{net::SocketAddr, sync::Arc};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};

pub async fn serve(serv_state: Arc<ServerState>) {
    #[cfg(feature = "auth")]
    let state = AppState {
        key: Key::from(CONFIG.cookie_secret.as_bytes()),
    };

    // build our application with some routes
    let app = Router::new()
        .route("/ping", any(|| async { "zpour" }))
        .route("/ws", get(ws_handler::accept_conn))
        // logging so we can see whats going on
        .layer(TraceLayer::new_for_http().make_span_with(DefaultMakeSpan::default()))
        .layer(Extension(serv_state));

    #[cfg(feature = "auth")]
    let app = app.with_state(state);

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
