use std::sync::Arc;

use actix_web::middleware::{self};
use actix_web::{web, App, HttpServer};

use crate::clusters;
use crate::clusters::store::CdrsClusterStore;
use crate::clusters::store::ClusterStore;
use crate::id;
use crate::logger;
use crate::session::create_session;
use crate::BANNER;

pub struct ServerConfig {
    pub log: logger::Level,
    pub host: String,
    pub port: u16,
}

pub struct ServerState {}

pub async fn run(config: ServerConfig) -> std::io::Result<()> {
    // Set the default log level
    logger::init(&config.log);

    // Output seeker banner
    info!("{}", BANNER);
    info!("starting server...");

    // Initialize server shared state
    // TODO - register worker ID
    let id_generator = id::Generator::new(0, 0);
    let session = create_session().await;
    let cluster_store: Arc<dyn ClusterStore + Send + Sync> =
        Arc::new(CdrsClusterStore::new(session, id_generator));

    // Start server
    let server = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .app_data(web::Data::new(cluster_store.clone()))
            .configure(routes)
    })
    .bind((config.host.clone(), config.port.clone()))?
    .run();

    info!("Server running at http://{}:{}", config.host, config.port);
    server.await
}

fn routes(config: &mut web::ServiceConfig) {
    config.service(web::scope("api/v1/clusters").configure(clusters::endpoints::v1::configure));
}
