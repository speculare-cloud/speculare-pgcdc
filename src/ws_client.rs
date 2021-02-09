use crate::ws_server;
use crate::ws_utils::{str_to_change_type, ChangeType};

use actix::prelude::*;
use actix_web::{web, web::Query, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use serde::Deserialize;
use std::time::{Duration, Instant};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

// TODO - Handle more Type
/// List of supported data type
#[derive(Clone)]
pub enum DataType {
    Number(i64),
    String(String),
}

// TODO - Handle more OP
/// List of supported type
#[derive(Clone)]
pub enum Op {
    Eq,
    Lower,
    Higher,
}

/// Contains the specific filter applied to the Ws
#[derive(Clone)]
pub struct SpecificFilter {
    pub column: String,
    pub value: DataType,
    pub op: Op,
}

impl SpecificFilter {
    /// Determine if the filter match the message passed as parameter
    pub fn match_specific_filter(&self, message: &serde_json::Value) -> bool {
        // Get the SpecificFilter object, firstly by a ref and then unwrap
        let filter = self;
        // Define easy to use variable
        let column = &filter.column;
        let value = &filter.value;
        // TODO - Implement OP (operation)
        //let op = &specific.op;

        if message["columnnames"].is_array() {
            // Determine if the column is present in this change
            let columns = message["columnnames"].as_array().unwrap();
            // Check if the cloumns we asked for exist in this data change
            let value_index = match (*columns)
                .iter()
                // Iterate and get the position back if it match the condition
                .position(|r| r == &serde_json::Value::String(column.to_owned()))
            {
                Some(index) => index,
                None => return false,
            };
            // This part is to optimize and rewrite, but basically it just
            // match, filter and sort around the criteria of the column value.
            if message["columnvalues"].is_array() {
                let values = message["columnvalues"].as_array().unwrap();
                let targeted_value = &values[value_index];
                // If the value we asked for is a String or a Number
                match value {
                    DataType::String(val) => {
                        // Check if is_string, and if so, convert it then check
                        if targeted_value.is_string() && targeted_value.as_str().unwrap() == val {
                            return true;
                        }
                    }
                    DataType::Number(val) => {
                        // Check if is_number and convert to i64 (might place a TODO for latter handling more type)
                        // For number we currently only support i64
                        if targeted_value.is_number() && targeted_value.as_i64().unwrap() == *val {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}

/// Contains info for what does the Ws is listening to
#[derive(Clone)]
pub struct WsWatchFor {
    pub change_table: String,
    pub change_type: ChangeType,
    pub specific: Option<SpecificFilter>,
}

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
    srv: web::Data<Addr<ws_server::WsServer>>,
    tables: web::Data<Vec<String>>,
    params: Query<ListQueryParams>,
) -> Result<HttpResponse, Error> {
    let change_type: &str;
    // If no params for the change_type, default to ALL
    if params.change_type.is_none() {
        change_type = "*";
    } else {
        change_type = &params.change_type.as_ref().unwrap();
    }
    // Change_table as String owned by this scope
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

struct WsSession {
    /// unique session id
    id: usize,
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT), otherwise we drop connection.
    hb: Instant,
    /// WsServer addr
    addr: Addr<ws_server::WsServer>,
    /// Table for which the Ws listen the changes
    watch_for: WsWatchFor,
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Start the heartbeat for this session
        self.hb(ctx);
        // Get the addr of this session Context
        let addr = ctx.address();
        // Send the info a new connection has been opened to the server
        self.addr
            .send(ws_server::Connect {
                addr: addr.recipient(),
                watch_for: self.watch_for.to_owned(),
            })
            .into_actor(self)
            // Update the session id by the one the server gave us
            // else we crash this session
            .then(|res, act, ctx| {
                match res {
                    Ok(res) => act.id = res,
                    _ => ctx.stop(),
                }
                fut::ready(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // Just send a disconnect message to the server
        self.addr.do_send(ws_server::Disconnect {
            id: self.id,
            watch_for: self.watch_for.to_owned(),
        });
        Running::Stop
    }
}

/// Handle messages from WsServer, we simply send it to peer websocket
impl Handler<ws_server::WsData> for WsSession {
    type Result = ();

    fn handle(&mut self, msg: ws_server::WsData, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

/// WebSocket message handler
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            Ok(ws::Message::Continuation(_)) => {
                ctx.stop();
            }
            Err(_) => ctx.stop(),
            _ => (),
        }
    }
}

impl WsSession {
    /// Helper method that sends ping to client every second.
    ///
    /// Also this method checks heartbeats from client
    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                // heartbeat timed out
                error!("Websocket Client heartbeat failed, disconnecting!");
                // notify WsServer to drop the current act.id
                act.addr.do_send(ws_server::Disconnect {
                    id: act.id,
                    watch_for: act.watch_for.to_owned(),
                });
                // stop actor
                ctx.stop();
                // don't send ping anymore
                return;
            }
            ctx.ping(b"");
        });
    }
}
