use crate::websockets::{
    server::{handler_connect, handler_disconnect, ws_server},
    WsWatchFor,
};

use actix::prelude::*;
use actix_web_actors::ws;
use std::time::{Duration, Instant};

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Struct holding serssion information such as id, heartbeat time, ...
pub struct WsClient {
    /// unique session id
    pub id: usize,
    /// Client must send ping at least once per 10 seconds (CLIENT_TIMEOUT), otherwise we drop connection.
    pub hb: Instant,
    /// WsServer addr
    pub addr: Addr<ws_server::WsServer>,
    /// Table for which the Ws listen the changes
    pub watch_for: WsWatchFor,
}

impl Actor for WsClient {
    type Context = ws::WebsocketContext<Self>;

    /// Function called at the begining of the Ws connection between Client & Server
    fn started(&mut self, ctx: &mut Self::Context) {
        // Start the heartbeat for this session
        self.hb(ctx);
        // Get the addr of this session Context
        let addr = ctx.address();
        // Send the info a new connection has been opened to the server
        self.addr
            .send(handler_connect::Connect {
                addr: addr.recipient(),
                watch_for: self.watch_for.to_owned(),
            })
            .into_actor(self)
            // Update the session id by the one the server gave us else we crash this session
            .then(|res, act, ctx| {
                match res {
                    Ok(res) => act.id = res,
                    _ => ctx.stop(),
                }
                fut::ready(())
            })
            .wait(ctx);
    }

    /// Called when the Actor is stopped
    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // Just send a disconnect message to the server
        self.addr.do_send(handler_disconnect::Disconnect {
            id: self.id,
            change_type: self.watch_for.change_type,
        });
        Running::Stop
    }
}

/// Handle messages from WsServer, we simply send it to peer websocket
impl Handler<ws_server::WsData> for WsClient {
    type Result = ();

    // Send the msg to the Client
    fn handle(&mut self, msg: ws_server::WsData, ctx: &mut Self::Context) {
        ctx.text(msg.0);
    }
}

/// WebSocket message handler
impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsClient {
    // Handle message received from the Client
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

impl WsClient {
    /// Helper method that sends ping to client every second.
    ///
    /// Also this method checks heartbeats from client
    fn hb(&self, ctx: &mut ws::WebsocketContext<Self>) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            // Check client heartbeats
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
                // Heartbeat timed out
                error!("Websocket Client heartbeat failed, disconnecting!");
                // Notify WsServer to drop the current act.id
                act.addr.do_send(handler_disconnect::Disconnect {
                    id: act.id,
                    change_type: act.watch_for.change_type,
                });
                // Stop actor
                ctx.stop();
                // Don't send ping anymore
                return;
            }
            ctx.ping(b"");
        });
    }
}
