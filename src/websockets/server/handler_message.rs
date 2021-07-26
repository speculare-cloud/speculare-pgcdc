use crate::websockets::{
    server::ws_server::{WsData, WsServer},
    DELETE, INSERT, UPDATE,
};

use actix::prelude::*;
use std::collections::HashSet;

#[derive(Message)]
#[rtype(result = "()")]
pub struct ClientMessage {
    /// Message
    pub msg: serde_json::Value,
    /// Table name
    pub change_table: String,
    /// Change type
    pub change_flag: u8,
}

/// Handler for Message message.
impl Handler<ClientMessage> for WsServer {
    type Result = ();

    fn handle(&mut self, msg: ClientMessage, _: &mut Context<Self>) {
        // Send the message using the function defined in ws_server.rs
        self.send_message(msg.msg, &msg.change_table, msg.change_flag);
    }
}

impl WsServer {
    /// Send message to all websocket listening for table
    fn send_message(&self, message: serde_json::Value, change_table: &str, change_flag: u8) {
        // Get all sessions for the table we're sending event
        // It's safe to assume the flag will only have one of them at this stage.
        // Because the flag has been constructed from a CDC changes, which has only 1 type max.
        let sessions: Option<&HashSet<usize>> = if has_bit!(change_flag, INSERT) {
            self.insert_tables.get(change_table)
        } else if has_bit!(change_flag, UPDATE) {
            self.update_tables.get(change_table)
        } else if has_bit!(change_flag, DELETE) {
            self.delete_tables.get(change_table)
        } else {
            error!("Change {:?} not handled (yet).", change_flag);
            return;
        };

        // If no session were defined, skip
        if sessions.is_none() {
            return;
        }

        // For every sessions ID in the tables HashMap
        for id in sessions.unwrap() {
            // Send the message to the ID (checking the specific filter, ...)
            // Get the Addr of the WS from the sessions hashmap by the id
            if let Some(info) = self.sessions.get(id) {
                // Check if specific filter applies
                let to_send = match &info.watch_for.specific {
                    Some(specific) => specific.match_filter(&message),
                    None => true,
                };
                // If we need to send the info, just send it
                if to_send {
                    // This send the message to:
                    // => client/ws_client.rs -> Handler<ws_server::WsData> for WsClient -> fn handle
                    let _ = info.recipient.do_send(WsData(message.to_string()));
                }
            }
        }
    }
}
