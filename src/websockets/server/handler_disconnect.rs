use super::ws_server::WsServer;

use crate::websockets::ChangeType;

use actix::prelude::*;

/// Session is disconnected
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: usize,
    pub change_type: ChangeType,
}

/// Handler for Disconnect message.
///
/// De-registering a session and removing it from the listener for his table
impl Handler<Disconnect> for WsServer {
    type Result = ();

    fn handle(&mut self, event: Disconnect, _: &mut Context<Self>) {
        info!("WS: someone disconnected");
        if self.sessions.remove(&event.id).is_some() {
            // Remove session from all tables registered
            // Dumb var to get less verbose code in the following IFs
            let ct = event.change_type;
            // Insert in the right category depending on the type
            if ct == ChangeType::AllTypes || ct == ChangeType::Insert {
                for list_sessions in self.insert_tables.values_mut() {
                    list_sessions.remove(&event.id);
                }
            }
            if ct == ChangeType::AllTypes || ct == ChangeType::Update {
                for list_sessions in self.update_tables.values_mut() {
                    list_sessions.remove(&event.id);
                }
            }
            if ct == ChangeType::AllTypes || ct == ChangeType::Delete {
                for list_sessions in self.delete_tables.values_mut() {
                    list_sessions.remove(&event.id);
                }
            }
        }
    }
}
