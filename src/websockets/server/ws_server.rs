use crate::websockets::WsWatchFor;

use actix::prelude::*;
use rand::{self, rngs::ThreadRng};
use std::collections::{HashMap, HashSet};

/// Sends this messages to session
#[derive(Message)]
#[rtype(result = "()")]
pub struct WsData(pub String);

/// Struct used to hold session information
pub struct SessionInfo {
    pub watch_for: WsWatchFor,
    pub recipient: Recipient<WsData>,
}

/// Server struct holding what the server need to run correctly
pub struct WsServer {
    /// Contains the id and the addr of the Ws reciever
    pub sessions: HashMap<usize, SessionInfo>,
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
}
