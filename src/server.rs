use super::CONFIG;
use super::{websockets::handlers, websockets::server::ws_server::WsServer, TABLES};

use actix_web::{middleware, App, HttpServer};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};

/// Return the SslAcceptorBuilder needed for Actix to be binded on HTTPS
///
/// Use KEY_PRIV and KEY_CERT environement variable for the path to find the files.
fn get_ssl_builder() -> openssl::ssl::SslAcceptorBuilder {
    // Getting the KEY path for both cert & priv key
    let key = CONFIG.get_str("KEY_PRIV").expect("BINDING must be set");
    let cert = CONFIG.get_str("KEY_CERT").expect("BINDING must be set");
    // Construct the SslAcceptor builder by setting the SslMethod as tls.
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    // Add the files (key & cert) to the builder
    builder.set_private_key_file(key, SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file(cert).unwrap();

    builder
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
            .app_data(actix_web::web::Data::new(*TABLES))
            .route("/ping", actix_web::web::get().to(|| async { "zpour" }))
            .route("/ping", actix_web::web::head().to(|| async { "zpour" }))
            .route("/ws", actix_web::web::get().to(handlers::ws_index))
    });
    // Bind and run the server on HTTP or HTTPS depending on the mode of compilation.
    let binding = CONFIG.get_str("CONNINFO").expect("BINDING must be set");
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
        serv.bind_openssl(binding, get_ssl_builder())?.run().await
    }
}
