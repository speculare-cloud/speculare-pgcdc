use super::{handlers, websockets::server::ws_server::WsServer, CONFIG};

use actix_web::{middleware, App, HttpServer};
use rustls::internal::pemfile::{certs, pkcs8_private_keys, rsa_private_keys};
use rustls::{NoClientAuth, ServerConfig};
use std::fs::File;
use std::io::BufReader;

/// Return the ServerConfig needed for Actix to be binded on HTTPS
///
/// Use key and cert for the path to find the files.
pub fn get_ssl_builder(key: String, cert: String) -> ServerConfig {
    // Init the ServerConfig with no Client's cert verifiction
    let mut config = ServerConfig::new(NoClientAuth::new());
    // Open BufReader on the key and cert files to read their content
    let cert_file = &mut BufReader::new(
        File::open(&cert).unwrap_or_else(|_| panic!("Certificate file not found at {}", cert)),
    );
    let key_file = &mut BufReader::new(
        File::open(&key).unwrap_or_else(|_| panic!("Key file not found at {}", key)),
    );
    // Create a Vec of certificate by extracting all cert from cert_file
    let cert_chain = certs(cert_file).unwrap();
    // Extract all PKCS8-encoded private key from key_file and generate a Vec from them
    let mut keys = pkcs8_private_keys(key_file).unwrap();
    // If no keys are found, we try using the rsa type
    if keys.is_empty() {
        // Reopen a new BufReader as pkcs8_private_keys took over the previous one
        let key_file = &mut BufReader::new(
            File::open(&key).unwrap_or_else(|_| panic!("Key file not found at {}", key)),
        );
        keys = rsa_private_keys(key_file).unwrap();
    }
    // Set a single certificate to be used for all subsequent request
    config.set_single_cert(cert_chain, keys.remove(0)).unwrap();

    config
}

/// Construct and run the actix server instance.
pub async fn server(wsc: actix::Addr<WsServer>) -> std::io::Result<()> {
    // Construct the HttpServer instance.
    // Passing the pool of PgConnection and defining the logger / compress middleware.
    let serv = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::default())
            .app_data(actix_web::web::Data::new(wsc.clone()))
            .route("/ping", actix_web::web::get().to(|| async { "zpour" }))
            .route("/ping", actix_web::web::head().to(|| async { "zpour" }))
            .route("/ws", actix_web::web::get().to(handlers::ws_index))
    });
    // Bind and run the server on HTTP or HTTPS depending on the mode of compilation.
    let binding = CONFIG.get_str("BINDING").expect("BINDING must be set");
    // Check if we should enable https
    let https = CONFIG.get_bool("HTTPS");
    // Bind the server (https or no)
    if https.is_err() || !https.unwrap() {
        if !cfg!(debug_assertions) {
            warn!("You're starting speculare-server as HTTP on a production build")
        } else {
            info!("Server started as HTTP");
        }
        serv.bind(binding)?.run().await
    } else {
        info!("Server started as HTTPS");
        serv.bind_rustls(
            binding,
            get_ssl_builder(
                CONFIG.get_str("KEY_PRIV").unwrap(),
                CONFIG.get_str("KEY_CERT").unwrap(),
            ),
        )?
        .run()
        .await
    }
}
