use super::ws_server::{WsData, WsServer};
use crate::websockets::ChangeType;
use crate::websockets::WsWatchFor;

use actix::prelude::*;
use rand::{self, Rng};
use std::collections::HashSet;

/// New session is created
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
        // Define it as session id
        self.sessions
            .insert(id, (event.watch_for.to_owned(), event.addr));
        // Insert in the right category depending on the type
        match event.watch_for.change_type {
            ChangeType::Insert => {
                self.insert_tables
                    .entry(event.watch_for.change_table)
                    .or_insert_with(HashSet::new)
                    .insert(id);
            }
            ChangeType::Update => {
                self.update_tables
                    .entry(event.watch_for.change_table)
                    .or_insert_with(HashSet::new)
                    .insert(id);
            }
            ChangeType::Delete => {
                self.delete_tables
                    .entry(event.watch_for.change_table)
                    .or_insert_with(HashSet::new)
                    .insert(id);
            }
            _ => {}
        }
        // Send the new id back
        id
    }
}
