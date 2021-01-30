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
    pub table: String,
}

/// Session is disconnected
#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect {
    pub id: usize,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ClientMessage {
    /// Message
    pub msg: String,
    /// Table name
    pub table: String,
}

pub struct WsServer {
    sessions: HashMap<usize, Recipient<WsData>>,
    tables: HashMap<String, HashSet<usize>>,
    rng: ThreadRng,
}

impl WsServer {
    pub fn new() -> WsServer {
        WsServer {
            sessions: HashMap::new(),
            tables: HashMap::new(),
            rng: rand::thread_rng(),
        }
    }
}

impl WsServer {
    /// Send message to all websocket listening for table
    fn send_message(&self, table: &str, message: &str) {
        if let Some(sessions) = self.tables.get(table) {
            for id in sessions {
                if let Some(addr) = self.sessions.get(id) {
                    let _ = addr.do_send(WsData(message.to_owned()));
                }
            }
        }
    }
}

/// Make actor from `WsServer`
impl Actor for WsServer {
    type Context = Context<Self>;
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
        self.sessions.insert(id, msg.addr);
        // insert id and table in the tables HashMap
        self.tables
            .entry(msg.table)
            .or_insert_with(HashSet::new)
            .insert(id);
        // send id back
        id
    }
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
            for sessions in self.tables.values_mut() {
                sessions.remove(&msg.id);
            }
        }
    }
}

/// Handler for Message message.
impl Handler<ClientMessage> for WsServer {
    type Result = ();

    fn handle(&mut self, msg: ClientMessage, _: &mut Context<Self>) {
        self.send_message(&msg.table, msg.msg.as_str());
    }
}
