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

/// Contains info for what does the Ws is listening to
#[derive(Clone)]
pub struct WsWatchFor {
    pub change_table: String,
    pub change_type: ChangeType,
}

#[derive(Deserialize)]
pub struct ListQueryParams {
    pub change_table: Option<String>,
    pub change_type: Option<String>,
}

/// Entry point for our websocket route
pub async fn ws_index(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Addr<ws_server::WsServer>>,
    tables: web::Data<Vec<String>>,
    params: Query<ListQueryParams>,
) -> Result<HttpResponse, Error> {
    let change_table: &str;
    let change_type: &str;
    // If no params for the change_table, default to ALL
    if params.change_table.is_none() {
        change_table = "*";
    } else {
        change_table = &params.change_table.as_ref().unwrap();
    }
    // If no params for the change_type, default to ALL
    if params.change_type.is_none() {
        change_type = "*";
    } else {
        change_type = &params.change_type.as_ref().unwrap();
    }
    // Change_table as String owned by this scope
    let change_table = change_table.to_owned();
    // Check if table is * or tables contains the table we asked for
    if change_table != "*" && !tables.contains(&change_table) {
        error!("The TABLE the client asked for does not exists");
        return Ok(HttpResponse::BadRequest().json("The TABLE asked for does not exists"));
    }
    // Check if event type is * or one of the SQL changes type
    if !matches!(change_type, "*" | "insert" | "update" | "delete") {
        error!("The TYPE params does not match requirements.");
        return Ok(HttpResponse::BadRequest().json("The TYPE params does not match requirements."));
    }
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
        self.hb(ctx);

        let addr = ctx.address();
        self.addr
            .send(ws_server::Connect {
                addr: addr.recipient(),
                watch_for: self.watch_for.to_owned(),
            })
            .into_actor(self)
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
