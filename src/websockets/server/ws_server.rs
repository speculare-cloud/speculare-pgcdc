use crate::websockets::ChangeType;
use crate::websockets::WsWatchFor;

use actix::prelude::*;
use rand::{self, rngs::ThreadRng};
use std::collections::{HashMap, HashSet};

/// Sends this messages to session
#[derive(Message)]
#[rtype(result = "()")]
pub struct WsData(pub String);

pub struct WsServer {
    /// Contains the id and the addr of the Ws reciever
    pub sessions: HashMap<usize, (WsWatchFor, Recipient<WsData>)>,
    /// HashMap of who is listening which table, and name depend on the change type
    pub insert_tables: HashMap<String, HashSet<usize>>,
    pub update_tables: HashMap<String, HashSet<usize>>,
    pub delete_tables: HashMap<String, HashSet<usize>>,
    /// Random generator thread
    pub rng: ThreadRng,
}

/// Make actor from `WsServer`
impl Actor for WsServer {
    type Context = Context<Self>;
}

impl WsServer {
    /// Construct a new instance of WsServer Actor
    pub fn new() -> WsServer {
        WsServer {
            // Can hold 128 clients before realloc
            sessions: HashMap::with_capacity(128),
            // Can hold 16 different tables before realloc
            insert_tables: HashMap::with_capacity(16),
            update_tables: HashMap::with_capacity(16),
            delete_tables: HashMap::with_capacity(16),
            // Random thread-local number generator
            rng: rand::thread_rng(),
        }
    }

    /// Sending message to the target id respecting the specific filter (if some)
    fn send_to_id(&self, id: &usize, message: &serde_json::Value) {
        // Get the Addr of the WS from the sessions hashmap by the id
        if let Some(info) = self.sessions.get(id) {
            // Check if specific filter applies
            let to_send = if info.0.specific.is_none() {
                true
            } else {
                info.0
                    .specific
                    .as_ref()
                    .unwrap()
                    .match_specific_filter(message)
            };
            // If we need to send the info, just send it
            if to_send {
                let _ = info.1.do_send(WsData(message.to_string()));
            }
        }
    }

    /// Send message to all websocket listening for table
    pub fn send_message(
        &self,
        change_table: &str,
        change_type: ChangeType,
        message: serde_json::Value,
    ) {
        let sessions: Option<&HashSet<usize>> = match change_type {
            // Get all sessions for the table we're sending event
            ChangeType::Insert => self.insert_tables.get(change_table),
            ChangeType::Update => self.update_tables.get(change_table),
            ChangeType::Delete => self.delete_tables.get(change_table),
            // If none of the above, we don't handle it
            _ => {
                error!("Change {:?} not handled (yet).", change_type);
                return;
            }
        };
        // If no session were defined, skip
        if sessions.is_none() {
            return;
        }
        // For every sessions ID in the tables HashMap
        for id in sessions.unwrap() {
            // Send the message to the ID (checking the specific filter, ...)
            self.send_to_id(id, &message);
        }
    }
}
