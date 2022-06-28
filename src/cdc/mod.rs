use crate::TABLES;

use async_trait::async_trait;
use tokio_postgres::{Client, SimpleQueryMessage};

pub mod connection;
pub mod replication;

#[async_trait]
pub trait ExtConfig {
    async fn detect_tables(&self) {}
}

#[async_trait]
impl ExtConfig for Client {
    /// Fill the global TABLES Vec with the tables available inside the database
    async fn detect_tables(&self) {
        let query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';";
        TABLES.write().unwrap().clear();

        self.simple_query(query)
            .await
            .unwrap()
            .into_iter()
            .for_each(|msg| {
                // And push them to the TABLES Vec
                if let SimpleQueryMessage::Row(row) = msg {
                    if let Some(val) = row.get(0) {
                        TABLES.write().unwrap().push(val.to_owned())
                    }
                }
            });
    }
}
