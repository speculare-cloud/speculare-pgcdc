use actix_web::{middleware, App, HttpServer};
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use tokio::sync::broadcast;

/// Return the SslAcceptorBuilder needed for Actix to be binded on HTTPS
///
/// Use KEY_PRIV and KEY_CERT environement variable for the path to find the files.
fn get_ssl_builder() -> openssl::ssl::SslAcceptorBuilder {
    let key = std::env::var("KEY_PRIV").expect("KEY_PRIV must be set");
    let cert = std::env::var("KEY_CERT").expect("KEY_CERT must be set");
    let mut builder = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    builder.set_private_key_file(key, SslFiletype::PEM).unwrap();
    builder.set_certificate_chain_file(cert).unwrap();

    builder
}

/// Dummy hello_world HTTP response
async fn hello_world() -> impl actix_web::Responder {
    "Hello World!"
}

/// Construct and run the actix server instance
pub async fn server(tx: broadcast::Sender<std::string::String>) -> std::io::Result<()> {
    // Construct the HttpServer instance.
    // Passing the pool of PgConnection and defining the logger / compress middleware.
    let serv = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::default())
            .data(tx.clone())
            .route("/", actix_web::web::get().to(hello_world))
        //.configure(routes::routes)
    });
    // Bind and run the server on HTTP or HTTPS depending on the mode of compilation.
    let binding = std::env::var("BINDING").expect("BINDING must be set");
    // Check if we should enable https
    let https = std::env::var("HTTPS");
    // Bind the server (https or no)
    if https.is_err() || https.unwrap() == "NO" {
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
