use crate::{
    utils::specific_filter::{DataType, SpecificFilter},
    websockets::{self, client::client_connected, ListQueryParams, ServerState, WsWatchFor},
    CONFIG,
};

use std::{net::SocketAddr, sync::Arc};
use warp::{
    self,
    http::{Response, StatusCode},
    hyper::Body,
    Filter, Reply,
};

fn parse_ws_query(params: &ListQueryParams) -> Result<WsWatchFor, Response<Body>> {
    let mut parts = params.query.split(':');
    let mut change_flag = 0;
    match parts.next() {
        Some(val) => val
            .split(',')
            .for_each(|ctype| websockets::apply_flag(&mut change_flag, ctype)),
        None => {
            return Err(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("The change_type params is not present.")
                .into_response())
        }
    }
    if change_flag == 0 {
        return Err(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("The change_type params does not match requirements.")
            .into_response());
    }
    let change_table = match parts.next() {
        Some(table) => {
            // Check if the table exists inside TABLES
            // if !TABLES.read().unwrap().iter().any(|v| v == table) {
            //     return Response::builder()
            //         .status(StatusCode::BAD_REQUEST)
            //         .body("The table asked for does not exists.")
            //         .into_response();
            // }
            table.to_owned()
        }
        None => {
            return Err(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("The change_table params is not present.")
                .into_response());
        }
    };
    let specific: Option<SpecificFilter> = if let Some(filter) = parts.next() {
        let mut fparts = filter.splitn(2, ".eq.");
        match (fparts.next(), fparts.next()) {
            (Some(col), Some(val)) => Some(SpecificFilter {
                column: serde_json::Value::String(col.to_owned()),
                value: DataType::String(val.to_owned()),
            }),
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

pub async fn run_server(server_state: Arc<ServerState>) {
    // Turn our "states" into a new Filter...
    let map_server_state = warp::any().map(move || server_state.clone());

    // Define the route handling our WebSocket
    let ws_handlers = warp::get()
        .and(warp::path("ws"))
        .and(warp::ws())
        .and(warp::query::<ListQueryParams>())
        .and(map_server_state)
        .map(
            |ws: warp::ws::Ws, params: ListQueryParams, map_server_state: Arc<ServerState>| {
                let watch_for = match parse_ws_query(&params) {
                    Ok(wsf) => wsf,
                    Err(e) => return e,
                };

                ws.on_upgrade(move |socket| client_connected(socket, map_server_state, watch_for))
                    .into_response()
            },
        );

    let binding = CONFIG.get_str("BINDING").expect("BINDING must be set");
    // Convert the binding into a SocketAddr
    let socket: SocketAddr = match binding.parse() {
        Ok(val) => val,
        Err(e) => {
            error!("The BINDING is not a valid SocketAddr: {}", e);
            return;
        }
    };
    // Check if we should enable https
    let https = CONFIG.get_bool("HTTPS").unwrap_or(false);
    let serv = warp::serve(
        warp::any()
            .and(warp::path("ping"))
            .map(|| "zpour")
            .or(ws_handlers),
    );
    if https {
        let cert_path = CONFIG
            .get_str("KEY_CERT")
            .expect("Missing KEY_CERT but HTTPS is true");
        let key_path = CONFIG
            .get_str("KEY_PRIV")
            .expect("Missing KEY_PRIV but HTTPS is true");
        // Run the Warp server infinitly
        serv.tls()
            .cert_path(cert_path)
            .key_path(key_path)
            .run(socket)
            .await;
    } else {
        // Run the Warp server infinitly
        serv.run(socket).await;
    }
}
