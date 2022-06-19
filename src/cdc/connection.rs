use crate::CONFIG;

use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::{Client, NoTls};

/// Connects to the Postgres server (using conn_string for server info)
pub async fn db_client_start() -> Client {
    let conn_string =
        format!(
        "host={} user={} dbname={} replication=database password={} sslmode={} connect_timeout=10",
        CONFIG.database_host,
        CONFIG.database_user,
        CONFIG.database_dbname,
        CONFIG.database_password,
        if CONFIG.database_tls { "require" } else { "disable" }
    );

    // TODO - Avoid all the duplicated code - Maybe using a macro ?
    // Someone can help if he want, as connector is a different type in both case
    // I don't know how to use common code for them. Unsafe here would be ok.
    let client = match CONFIG.database_tls {
        true => {
            let connector =
                MakeTlsConnector::new(SslConnector::builder(SslMethod::tls()).unwrap().build());
            let (rc, rco) = match tokio_postgres::connect(&conn_string, connector).await {
                Ok((rc, rco)) => (rc, rco),
                Err(e) => {
                    error!("Postgres: connection failed: {}, {:?}", e, e.as_db_error());
                    std::process::exit(1);
                }
            };

            tokio::spawn(async move {
                if let Err(e) = rco.await {
                    // Don't exit after this because we handle the "reconnection"
                    error!("Postgres: connection broken due to: {}", e);
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
                    // Don't exit after this because we handle the "reconnection"
                    error!("Postgres: connection broken due to: {}", e);
                }
            });

            rc
        }
    };
    info!("Postgres: connection established");

    client
}
