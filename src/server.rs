use crate::{
    utils::specific_filter::{DataType, SpecificFilter},
    websockets::{self, client::client_connected, ListQueryParams, ServerState, WsWatchFor},
    CONFIG, TABLES,
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

    // Apply bit operation to the change_flag based on the query type
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

    // If change_flag is 0, we have an error
    if change_flag == 0 {
        return Err(Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body("The change_type params does not match requirements.")
            .into_response());
    }

    // Get the change_table and check if the table is valid
    let change_table = match parts.next() {
        Some(table) => {
            // Check if the table exists inside TABLES
            if !TABLES.read().unwrap().iter().any(|v| v == table) {
                return Err(Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body("The table asked for does not exists.")
                    .into_response());
            }
            table.to_owned()
        }
        None => {
            return Err(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("The change_table params is not present.")
                .into_response());
        }
    };

    // Construct the SpecificFilter from the request
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

    // Convert the binding into a SocketAddr
    let socket: SocketAddr = match CONFIG.binding.parse() {
        Ok(val) => val,
        Err(e) => {
            error!("The BINDING is not a valid SocketAddr: {}", e);
            std::process::exit(1);
        }
    };

    let serv = warp::serve(
        warp::any()
            .and(warp::path("ping"))
            .map(|| "zpour")
            .or(ws_handlers),
    );

    if CONFIG.https {
        let key_priv = field_isset!(CONFIG.key_priv.as_ref(), "key_priv");
        let key_cert = field_isset!(CONFIG.key_cert.as_ref(), "key_cert");

        // Run the Warp server infinitely
        serv.tls()
            .cert_path(key_cert)
            .key_path(key_priv)
            .run(socket)
            .await;
    } else {
        // Run the Warp server infinitely
        serv.run(socket).await;
    }
}
