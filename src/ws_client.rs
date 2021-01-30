use crate::ws_server;

use actix::prelude::*;
use actix_web::{web, web::Path, Error, HttpRequest, HttpResponse};
use actix_web_actors::ws;
use std::time::{Duration, Instant};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Entry point for our websocket route
pub async fn ws_index(
    req: HttpRequest,
    stream: web::Payload,
    srv: web::Data<Addr<ws_server::WsServer>>,
    tables: web::Data<Vec<String>>,
    info: Path<String>,
) -> Result<HttpResponse, Error> {
    if !tables.contains(&info.0) {
        return Ok(HttpResponse::BadRequest().finish());
    }
    ws::start(
        WsSession {
            id: 0,
            hb: Instant::now(),
            addr: srv.get_ref().clone(),
            table: info.0,
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
    table: String,
}

impl Actor for WsSession {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);

        let addr = ctx.address();
        self.addr
            .send(ws_server::Connect {
                addr: addr.recipient(),
                table: self.table.to_owned(),
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
        self.addr.do_send(ws_server::Disconnect { id: self.id });
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
                act.addr.do_send(ws_server::Disconnect { id: act.id });
                // stop actor
                ctx.stop();
                // don't send ping anymore
                return;
            }
            ctx.ping(b"");
        });
    }
}
