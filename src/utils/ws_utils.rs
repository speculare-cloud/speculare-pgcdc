use crate::utils::specific_filter::SpecificFilter;

use ahash::{AHashMap, AHashSet};
use axum::extract::ws::Message;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

pub const INSERT: u8 = 1 << 1;
pub const UPDATE: u8 = 1 << 2;
pub const DELETE: u8 = 1 << 3;

pub struct SessionInfo {
    pub gate: mpsc::UnboundedSender<Result<Message, axum::Error>>,
    pub watch_for: WsWatchFor,
}

/// Our state of currently connected clients.
///
/// - Key is their id
/// - Value is a sender of `warp::ws::Message`
type Clients = Arc<RwLock<AHashMap<usize, SessionInfo>>>;

/// Our state of currently connected clients listening for a particular table.
///
/// - Key is the table name
/// - Value is a AHashSet containing all the client's id listening to that table.
type TypeList = Arc<RwLock<AHashMap<String, AHashSet<usize>>>>;

/// Contains info for what does the Ws is listening to
pub struct WsWatchFor {
    pub change_table: String,
    pub change_flag: u8,
    pub specific: Option<SpecificFilter>,
}

pub fn apply_flag(flag: &mut u8, ctype: &str) {
    match ctype {
        "insert" => {
            *flag |= INSERT;
        }
        "update" => {
            *flag |= UPDATE;
        }
        "delete" => {
            *flag |= DELETE;
        }
        "*" => {
            *flag |= INSERT;
            *flag |= UPDATE;
            *flag |= DELETE;
        }
        _ => {
            error!("parts[0] (change_type) don't match any of the available types.")
        }
    }
}

/// Contains info for which table/event/filter we're listening
#[derive(serde::Deserialize)]
pub struct ListQueryParams {
    pub query: String,
}

/// Server state for WebSocket
#[derive(Default, Clone)]
pub struct ServerState {
    pub clients: Clients,
    pub inserts: TypeList,
    pub updates: TypeList,
    pub deletes: TypeList,
}
