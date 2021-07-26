use crate::websockets::{DELETE, INSERT, UPDATE};

use super::ws_server::WsServer;

use actix::prelude::*;

/// Session is disconnected
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: usize,
    pub change_flag: u8,
}

/// Handler for Disconnect message.
///
/// De-registering a session and removing it from the listener for his table
impl Handler<Disconnect> for WsServer {
    type Result = ();

    fn handle(&mut self, event: Disconnect, _: &mut Context<Self>) {
        info!("WS: someone disconnected");
        // Check if the event.id is still in the sessions list
        if self.sessions.remove(&event.id).is_some() {
            // Remove session from all tables registered
            // Dumb var to get less verbose code in the following IFs
            let change_flag = event.change_flag;
            // Delete in the right category depending on the type
            if has_bit!(change_flag, INSERT) {
                // For each table entries, remove the id of the ws_session
                for list_sessions in self.insert_tables.values_mut() {
                    // Even if the event.id is not in the list_sessions, it will try
                    list_sessions.remove(&event.id);
                }
            }
            if has_bit!(change_flag, UPDATE) {
                for list_sessions in self.update_tables.values_mut() {
                    list_sessions.remove(&event.id);
                }
            }
            if has_bit!(change_flag, DELETE) {
                for list_sessions in self.delete_tables.values_mut() {
                    list_sessions.remove(&event.id);
                }
            }
        }
    }
}
