use super::ws_server::WsServer;

use crate::websockets::ChangeType;

use actix::prelude::*;

#[derive(Message)]
#[rtype(result = "()")]
pub struct ClientMessage {
    /// Message
    pub msg: serde_json::Value,
    /// Table name
    pub change_table: String,
    /// Change type
    pub change_type: ChangeType,
}

/// Handler for Message message.
impl Handler<ClientMessage> for WsServer {
    type Result = ();

    fn handle(&mut self, msg: ClientMessage, _: &mut Context<Self>) {
        // Send the message
        self.send_message(&msg.change_table, msg.change_type, msg.msg);
    }
}
