use crate::ws_client::{DataType, WsWatchFor};
use crate::ws_utils::ChangeType;

use actix::prelude::*;
use rand::{self, rngs::ThreadRng, Rng};
use std::collections::{HashMap, HashSet};

/// Sends this messages to session
#[derive(Message)]
#[rtype(result = "()")]
pub struct WsData(pub String);

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

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        info!("WS: new connection");
        // generate random usize id
        let id = self.rng.gen::<usize>();
        // define it as session id
        self.sessions
            .insert(id, (msg.watch_for.to_owned(), msg.addr));
        // determine in which tables I have to insert
        // insert id and table in the corresponding tables HashMap
        let change_type = msg.watch_for.change_type;
        if change_type == ChangeType::ALL || change_type == ChangeType::INSERT {
            self.insert_tables
                .entry(msg.watch_for.change_table.to_owned())
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        if change_type == ChangeType::ALL || change_type == ChangeType::UPDATE {
            self.update_tables
                .entry(msg.watch_for.change_table.to_owned())
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        if change_type == ChangeType::ALL || change_type == ChangeType::DELETE {
            self.delete_tables
                .entry(msg.watch_for.change_table)
                .or_insert_with(HashSet::new)
                .insert(id);
        }
        // send id back
        id
    }
}

/// Session is disconnected
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: usize,
    pub watch_for: WsWatchFor,
}

/// Handler for Disconnect message.
///
/// De-registering a session and removing it from the listener for his table
impl Handler<Disconnect> for WsServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        info!("WS: someone disconnected");
        if self.sessions.remove(&msg.id).is_some() {
            // remove session from all tables registered
            let change_type = msg.watch_for.change_type;
            if change_type == ChangeType::ALL || change_type == ChangeType::INSERT {
                for sessions in self.insert_tables.values_mut() {
                    sessions.remove(&msg.id);
                }
            }
            if change_type == ChangeType::ALL || change_type == ChangeType::UPDATE {
                for sessions in self.update_tables.values_mut() {
                    sessions.remove(&msg.id);
                }
            }
            if change_type == ChangeType::ALL || change_type == ChangeType::DELETE {
                for sessions in self.delete_tables.values_mut() {
                    sessions.remove(&msg.id);
                }
            }
        }
    }
}

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
        self.send_message(&msg.change_table, msg.change_type, msg.msg);
    }
}

pub struct WsServer {
    /// Contains the id and the addr of the Ws reciever
    sessions: HashMap<usize, (WsWatchFor, Recipient<WsData>)>,
    /// HashMap of who is listening which table, and name depend on the change type
    insert_tables: HashMap<String, HashSet<usize>>,
    update_tables: HashMap<String, HashSet<usize>>,
    delete_tables: HashMap<String, HashSet<usize>>,
    /// Random generator thread
    rng: ThreadRng,
}

/// Make actor from `WsServer`
impl Actor for WsServer {
    type Context = Context<Self>;
}

impl WsServer {
    /// Construct a new instance of WsServer Actor
    pub fn new() -> WsServer {
        WsServer {
            sessions: HashMap::new(),
            insert_tables: HashMap::new(),
            update_tables: HashMap::new(),
            delete_tables: HashMap::new(),
            rng: rand::thread_rng(),
        }
    }

    /// Send message to all websocket listening for table
    fn send_message(
        &self,
        change_table: &str,
        change_type: ChangeType,
        message: serde_json::Value,
    ) {
        let sessions: Option<&HashSet<usize>>;
        match change_type {
            // Get all sessions for the table we're sending event
            // TODO - Fix if table listened is "*"
            ChangeType::INSERT => {
                sessions = self.insert_tables.get(change_table);
            }
            ChangeType::UPDATE => {
                sessions = self.update_tables.get(change_table);
            }
            ChangeType::DELETE => {
                sessions = self.delete_tables.get(change_table);
            }
            // If none of the above, we don't handle it
            _ => {
                error!("Change {:?} not handled (yet).", change_type);
                return;
            }
        }
        // If no session were defined, skip
        if sessions.is_none() {
            return;
        }
        // For every sessions ID in the tables HashMap
        for id in sessions.unwrap() {
            // Get the Addr of the WS from the sessions hashmap by the id
            if let Some(info) = self.sessions.get(id) {
                // Check if specific filter applies
                if info.0.specific.is_some() {
                    let specific = info.0.specific.as_ref().unwrap();
                    let column = &specific.column;
                    let value = &specific.value;
                    
                    // TODO - Implement OP (operation)
                    //let op = &specific.op;

                    if message["columnnames"].is_array() {
                        let value_index: usize;
                        // Determine if the column is present in this change
                        let columns = message["columnnames"].as_array().unwrap();
                        match (*columns)
                            .iter()
                            .position(|r| r == &serde_json::Value::String(column.to_owned()))
                        {
                            Some(index) => {
                                value_index = index;
                            }
                            None => continue,
                        }
                        let mut to_send = false;
                        // This part is to optimize and rewrite, but basically it just
                        // match, filter and sort around the criteria of the column value.
                        if message["columnvalues"].is_array() {
                            let values = message["columnvalues"].as_array().unwrap();
                            let targeted_value = &values[value_index];
                            match value {
                                DataType::String(val) => {
                                    if targeted_value.is_string() {
                                        if targeted_value.as_str().unwrap() == val {
                                            to_send = true;
                                        } else {
                                            continue;
                                        }
                                    } else {
                                        continue;
                                    }
                                }
                                DataType::Number(val) => {
                                    if targeted_value.is_number() {
                                        if targeted_value.as_i64().unwrap() == *val {
                                            to_send = true;
                                        } else {
                                            continue;
                                        }
                                    } else {
                                        continue;
                                    }
                                }
                            }
                        }
                        if to_send {
                            let _ = info.1.do_send(WsData(message.to_string()));
                        }
                    }
                } else {
                    // Send the message
                    let _ = info.1.do_send(WsData(message.to_string()));
                }
            }
        }
    }
}
