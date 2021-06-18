use crate::websockets::{
    server::ws_server::WsServer,
    specific_filter::{DataType, SpecificFilter},
    ws_session::WsSession,
    ChangeType, WsWatchFor,
};

use actix::prelude::*;
use actix_web::{web, web::Query, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use serde::Deserialize;
use std::time::Instant;

/// Contains info for which table/event/filter we're listening
#[derive(Deserialize)]
pub struct ListQueryParams {
    pub query: String,
}

/// Entry point for our websocket route
///
/// Use this route like /ws?query=change_type:table:col.eq.val
///
/// Will get change_type event from table where col is equals to val
pub async fn ws_index(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Addr<WsServer>>,
    tables: web::Data<Vec<String>>,
    params: Query<ListQueryParams>,
) -> Result<HttpResponse, Error> {
    let parts: Vec<&str> = params.query.split(':').collect();
    let lenght = parts.len();
    if !(2..=3).contains(&lenght) {
        error!("The request doesn't have the correct number of args.");
        return Ok(HttpResponse::BadRequest().json("Your request must follow: query=change_type:table:col.eq.val (change_type and table are mandatory)"));
    };
    // We're sure that the parts[0] exist as any string splitted at : will give us someting
    // Except if the String == ":" => but that's ok.
    let change_type = crate::websockets::str_to_change_type(parts[0]);
    // Check if the change_type is not unknown
    if change_type == ChangeType::Unknown {
        error!("The TYPE params does not match requirements.");
        return Ok(HttpResponse::BadRequest().json("The change_type param does not match requirements, valid are: *, insert, update, delete"));
    }
    // If the Vec has more or eq than 2 items, means we have a table specified.
    // We're sure that the [1] exists has we checked for the lenght before.
    let change_table = parts[1].to_owned();
    // Check if the request table exists
    // TODO - Fix this if the table does not exists due to PARTMAN
    if !tables.contains(&change_table) {
        error!("The TABLE the client asked for does not exists");
        return Ok(HttpResponse::BadRequest().json("The TABLE asked for does not exists"));
    }
    // Now we need to check if it's needed to specify a filter
    let specific_filter: Option<SpecificFilter> = if lenght == 3 {
        let filter = parts[2];
        let filter_parts: Vec<&str> = filter.splitn(2, ".eq.").collect();
        // If the filter doesn't hold 2 parts (col & val), fail the request.
        if filter_parts.len() != 2 {
            return Ok(HttpResponse::BadRequest().json("The filter part of your request does not comply with: query=change_type:table:col.eq.val"));
        }

        Some(SpecificFilter {
            column: filter_parts[0].to_owned(),
            value: DataType::String(filter_parts[1].to_owned()),
        })
    } else {
        None
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
                change_type,
                specific: specific_filter,
            },
        },
        &req,
        stream,
    )
}
