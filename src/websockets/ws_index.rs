use super::ws_dispatcher::str_to_change_type;
use super::ws_server::WsServer;
use super::ws_session::WsSession;
use super::ws_specific_filter::{DataType, Op, SpecificFilter};
use super::WsWatchFor;

use actix::prelude::*;
use actix_web::{web, web::Query, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use serde::Deserialize;
use std::time::Instant;

/// Contains info for which table/event/filter we're listening
#[derive(Deserialize)]
pub struct ListQueryParams {
    pub change_table: String,
    pub change_type: Option<String>,
    pub specific_filter: Option<String>,
}

// TODO - Split and optimize
/// Entry point for our websocket route
pub async fn ws_index(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Addr<WsServer>>,
    tables: web::Data<Vec<String>>,
    params: Query<ListQueryParams>,
) -> Result<HttpResponse, Error> {
    // If no params for the change_type, default to ALL
    let change_type = if params.change_type.is_none() {
        "*"
    } else {
        &params.change_type.as_ref().unwrap()
    };
    // Change_table as String owned by this scope
    // TODO - Handle multiple table separated by a comma
    // ex: ?change_tables=table_one, table_two
    // Will register the client in table_one and table_two for the changes requested
    let change_table = params.change_table.to_owned();
    // Check if table is * or tables contains the table we asked for
    if !tables.contains(&change_table) {
        error!("The TABLE the client asked for does not exists");
        return Ok(HttpResponse::BadRequest().json("The TABLE asked for does not exists"));
    }
    // Check if event type is * or one of the SQL changes type
    if !matches!(change_type, "*" | "insert" | "update" | "delete") {
        error!("The TYPE params does not match requirements.");
        return Ok(HttpResponse::BadRequest().json("The TYPE params does not match requirements."));
    }

    // Parse the SpecificFilter <col>.<op>.<val>
    let specific: Option<SpecificFilter> = match &params.specific_filter {
        Some(filter) => {
            // Split the filter by '.'
            // col  = [0]
            // op   = [1]
            // val  = [2]
            let parts: Vec<&str> = filter.split('.').collect();
            if parts.len() != 3 {
                error!("The FILTER params does not match requirements.");
                return Ok(HttpResponse::BadRequest()
                    .json("The FILTER params does not match requirements."));
            } else {
                // As we only handle a small number of OP,
                // determine which one we're asking for, and if not found, return 400
                let op = match parts[1] {
                    "eq" => Op::Eq,
                    "pl" => Op::Higher,
                    "lw" => Op::Lower,
                    _ => {
                        error!("The OP params does not match requirements.");
                        return Ok(HttpResponse::BadRequest()
                            .json("The OP params does not match requirements."));
                    }
                };
                // Convert the column str to a owned String for latter use
                let column = parts[0].to_owned();

                // Convert the number if it's a number, else create a String from the str
                let value = match parts[2].parse::<i64>() {
                    Ok(nbr) => DataType::Number(nbr),
                    Err(_) => DataType::String(parts[2].to_owned()),
                };

                // Return the SpecificFilter object (struct)
                Some(SpecificFilter { column, value, op })
            }
        }
        None => None,
    };

    // Upgrade the HTTP connection to a WebSocket one
    ws::start(
        // Construct the WebSocket session with srv addr and WsWatchFor
        WsSession {
            id: 0,
            hb: Instant::now(),
            addr: srv.get_ref().clone(),
            watch_for: WsWatchFor {
                change_table,
                change_type: str_to_change_type(change_type),
                specific,
            },
        },
        &req,
        stream,
    )
}
