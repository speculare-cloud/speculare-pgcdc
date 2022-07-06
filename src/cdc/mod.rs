use crate::TABLES;
#[cfg(feature = "timescale")]
use crate::TABLES_LOOKUP;

use async_trait::async_trait;
use tokio_postgres::{Client, SimpleQueryMessage};

pub mod connection;
pub mod replication;

#[cfg(feature = "timescale")]
pub fn extract_hyper_idx(table_name: &str) -> Result<i8, ()> {
    let mut parts = table_name.splitn(4, '_');
    match parts.nth(2) {
        Some(val) => Ok(val.parse::<i8>().unwrap()),
        None => Err(()),
    }
}

#[async_trait]
pub trait ExtConfig {
    async fn detect_tables(&self) {}
    #[cfg(feature = "timescale")]
    async fn detect_lookup(&self) {}
}

#[async_trait]
impl ExtConfig for Client {
    /// Fill the global TABLES Vec with the tables available inside the database
    async fn detect_tables(&self) {
        let query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name!='__diesel_schema_migrations';";
        TABLES.write().unwrap().clear();

        match self.simple_query(query).await {
            Ok(res) => res.into_iter().for_each(|msg| {
                // And push them to the TABLES Vec
                if let SimpleQueryMessage::Row(row) = msg {
                    if let Some(val) = row.get(0) {
                        TABLES.write().unwrap().push(val.to_owned())
                    }
                }
            }),
            Err(err) => {
                error!("Cannot check the tables, continuing without them: {}", err);
            }
        }
    }

    #[cfg(feature = "timescale")]
    async fn detect_lookup(&self) {
        let query =
            "select table_name,associated_table_prefix from _timescaledb_catalog.hypertable;";
        TABLES_LOOKUP.write().unwrap().clear();

        match self.simple_query(query).await {
            Ok(res) => res.into_iter().for_each(|msg| {
                if let SimpleQueryMessage::Row(row) = msg {
                    if let (Some(table), Some(prefix)) = (row.get(0), row.get(1)) {
                        if let Ok(idx) = extract_hyper_idx(prefix) {
                            TABLES_LOOKUP.write().unwrap().insert(idx, table.to_owned());
                        }
                    }
                }
            }),
            Err(err) => {
                error!(
                    "Cannot check the lookup tables, continuing without them: {}",
                    err
                );
            }
        }
    }
}
