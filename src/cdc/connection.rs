use crate::CONFIG;

use tokio_postgres::{Client, NoTls};

/// Connects to the Postgres server (using conn_string for server info)
pub async fn db_client_start() -> Client {
    let conn_string = format!(
        "host={} user={} dbname={} replication=database password={}",
        CONFIG
            .get_str("POSTGRES_HOST")
            .expect("Missing POSTGRES_HOST inside the config"),
        CONFIG
            .get_str("POSTGRES_USER")
            .expect("Missing POSTGRES_USER inside the config"),
        CONFIG
            .get_str("POSTGRES_DATABASE")
            .expect("Missing POSTGRES_DATABASE inside the config"),
        CONFIG
            .get_str("POSTGRES_PASSWORD")
            .expect("Missing POSTGRES_PASSWORD inside the config")
    );

    let (client, connection) = match tokio_postgres::connect(&conn_string, NoTls).await {
        Ok((rc, rco)) => (rc, rco),
        Err(e) => {
            error!("Postgres: connection failed: {}", e);
            std::process::exit(1);
        }
    };
    trace!("Postgres: connection etablished");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("Postgres: connection broken due to: {}", e);
            std::process::exit(1);
        }
    });

    client
}
