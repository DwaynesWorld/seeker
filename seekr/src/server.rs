use std::sync::Arc;

use actix_web::middleware::{self};
use actix_web::{web, App, HttpServer};

use crate::clusters::endpoints::v1::configure as configure_cluster;
use crate::clusters::store::CdrsClusterStore;
use crate::clusters::store::ClusterStore;
use crate::id;
use crate::kafka::metadata::MetadataService;
use crate::logger;
use crate::session::create_session;
use crate::subscriptions::endpoints::v1::configure as configure_subscription;
use crate::subscriptions::store::CdrsSubscriptionStore;
use crate::subscriptions::store::SubscriptionStore;
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

    // Output seekr banner
    info!("{}", BANNER);
    info!("Starting server...");

    // Initialize server shared state
    // TODO: register worker ID
    let id_generator = Arc::new(id::Generator::new(0, 0));
    let session = Arc::new(create_session().await);

    let cluster_store: Arc<dyn ClusterStore + Send + Sync> =
        Arc::new(CdrsClusterStore::new(session.clone(), id_generator.clone()));

    let subscription_store: Arc<dyn SubscriptionStore + Send + Sync> = Arc::new(
        CdrsSubscriptionStore::new(session.clone(), id_generator.clone()),
    );

    // Start Metadata service
    MetadataService::new(cluster_store.clone())
        .start()
        .await
        .expect("unable to start metadata service.");

    // Start Http server
    let server = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .app_data(web::Data::new(cluster_store.clone()))
            .app_data(web::Data::new(subscription_store.clone()))
            .configure(routes)
    })
    .bind((config.host.clone(), config.port.clone()))?
    .run();

    info!("Server running at http://{}:{}", config.host, config.port);
    server.await
}

fn routes(config: &mut web::ServiceConfig) {
    config.service(web::scope("api/v1/clusters").configure(configure_cluster));
    config.service(web::scope("api/v1/subscriptions").configure(configure_subscription));
}
