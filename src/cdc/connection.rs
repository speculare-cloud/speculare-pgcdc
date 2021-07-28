use crate::CONFIG;

use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::{Client, NoTls};

/// Connects to the Postgres server (using conn_string for server info)
pub async fn db_client_start() -> Client {
    let tls = CONFIG.get_bool("POSTGRES_TLS").unwrap_or(true);
    let conn_string = format!(
        "host={} user={} dbname={} replication=database password={} sslmode={}",
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
            .expect("Missing POSTGRES_PASSWORD inside the config"),
        if tls { "require" } else { "disable" }
    );

    // TODO - Avoid all the duplicated code - Maybe using a macro ?
    let client = match tls {
        true => {
            let connector =
                MakeTlsConnector::new(SslConnector::builder(SslMethod::tls()).unwrap().build());
            let (rc, rco) = match tokio_postgres::connect(&conn_string, connector).await {
                Ok((rc, rco)) => (rc, rco),
                Err(e) => {
                    error!("Postgres: connection failed: {}", e);
                    std::process::exit(1);
                }
            };

            tokio::spawn(async move {
                if let Err(e) = rco.await {
                    error!("Postgres: connection broken due to: {}", e);
                    std::process::exit(1);
                }
            });

            rc
        }
        false => {
            let connector = NoTls;
            let (rc, rco) = match tokio_postgres::connect(&conn_string, connector).await {
                Ok((rc, rco)) => (rc, rco),
                Err(e) => {
                    error!("Postgres: connection failed: {}", e);
                    std::process::exit(1);
                }
            };

            tokio::spawn(async move {
                if let Err(e) = rco.await {
                    error!("Postgres: connection broken due to: {}", e);
                    std::process::exit(1);
                }
            });

            rc
        }
    };
    trace!("Postgres: connection etablished");

    client
}
