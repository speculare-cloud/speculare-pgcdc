use super::ws_server::{SessionInfo, WsData, WsServer};

use crate::websockets::{WsWatchFor, DELETE, INSERT, UPDATE};

use actix::prelude::*;
use rand::{self, Rng};
use std::collections::HashSet;

/// Data used when a new session is created
#[derive(Message)]
#[rtype(usize)]
pub struct Connect {
    pub addr: Recipient<WsData>,
    pub watch_for: WsWatchFor,
}

/// Handler for Connect message.
///
/// Register new session and assign unique id to this session
impl Handler<Connect> for WsServer {
    type Result = usize;

    fn handle(&mut self, event: Connect, _: &mut Context<Self>) -> Self::Result {
        info!("WS: new connection");
        // Generate random usize id
        let id = self.rng.gen::<usize>();
        // Insert the id into the list of sessions and construct SessionInfo
        self.sessions.insert(
            id,
            SessionInfo {
                watch_for: event.watch_for.to_owned(),
                recipient: event.addr,
            },
        );
        // Dumb var to get less verbose code in the following IFs
        let change_flag = event.watch_for.change_flag;
        // Insert in the right category depending on the ChangeType
        if has_bit!(change_flag, INSERT) {
            self.insert_tables
                .entry(event.watch_for.change_table.to_owned())
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        if has_bit!(change_flag, UPDATE) {
            self.update_tables
                .entry(event.watch_for.change_table.to_owned())
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        if has_bit!(change_flag, DELETE) {
            self.delete_tables
                .entry(event.watch_for.change_table)
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        // Send the new id back so the ws_session can save it in the actor
        id
    }
}
