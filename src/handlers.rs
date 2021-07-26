use crate::TABLES;
use crate::{
    utils::specific_filter::{DataType, SpecificFilter},
    websockets::{self, client::ws_client::WsClient, server::ws_server::WsServer, WsWatchFor},
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
    params: Query<ListQueryParams>,
) -> Result<HttpResponse, Error> {
    let parts: Vec<&str> = params.query.split(':').collect();
    let lenght = parts.len();
    if !(2..=3).contains(&lenght) {
        error!("The request doesn't have the correct number of args.");
        return Ok(HttpResponse::BadRequest().json("Your request must follow: query=change_type:table:col.eq.val (change_type and table are mandatory)"));
    };
    // Construct the u8 value holding our flags
    let mut change_flag = 0u8;
    // We're sure that the parts[0] exist as any string splitted at : will give us someting
    parts[0]
        .split(',')
        .for_each(|ctype| websockets::apply_flag(&mut change_flag, ctype));
    // Check if the change_type is not unknown
    if change_flag == 0 {
        error!("The TYPE params does not match requirements.");
        return Ok(HttpResponse::BadRequest().json("The change_type param does not match requirements, valid are: *, insert, update, delete"));
    }
    // If the Vec has more or eq than 2 items, means we have a table specified.
    // We're sure that the [1] exists has we checked for the lenght before.
    let change_table = parts[1].to_owned();
    // Check if the request table exists
    if !TABLES.read().unwrap().contains(&change_table) {
        // Check where is the '_' char in the table name (if any)
        let udr_idx = change_table.find('_');
        // If '_' is found, check if one table exists with only the first part of the table name
        // ex: disks_p2020 => disks, so check if disks_template exists. (template because it's used in PARTMAN).
        let idx = udr_idx.unwrap_or_default();
        if udr_idx.is_none()
            || !TABLES
                .read()
                .unwrap()
                .iter()
                .any(|x| &x[idx..] == "_template" && x[..idx] == change_table[..idx])
        {
            error!("The TABLE the client asked for does not exists");
            return Ok(HttpResponse::BadRequest().json("The TABLE asked for does not exists"));
        }
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
            column: serde_json::Value::String(filter_parts[0].to_owned()),
            value: DataType::String(filter_parts[1].to_owned()),
        })
    } else {
        None
    };

    // Upgrade the HTTP connection to a WebSocket one
    ws::start(
        // Construct the WebSocket session with srv addr and WsWatchFor
        WsClient {
            id: 0,
            hb: Instant::now(),
            addr: srv.get_ref().clone(),
            watch_for: WsWatchFor {
                change_table,
                change_flag,
                specific: specific_filter,
            },
        },
        &req,
        stream,
    )
}
