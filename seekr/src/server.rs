use actix_web::middleware;
use actix_web::web::Data;
use actix_web::{web, App, HttpServer};

use crate::clusters::endpoints::v1::configure as configure_cluster;
use crate::clusters::store::init_cluster_store;
use crate::kafka::metadata::manager::MetadataManager;
use crate::logger;
use crate::subscriptions::endpoints::v1::configure as configure_subscription;
use crate::subscriptions::store::init_subscription_store;
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
    let clusters = init_cluster_store().await;
    let subscriptions = init_subscription_store().await;
    let metadata_service = Data::new(MetadataManager::new(clusters.clone()));

    // Start Metadata service
    metadata_service
        .clone()
        .into_inner()
        .start()
        .await
        .expect("unable to start metadata service");

    // Start Http server
    let metadata_service_ = metadata_service.clone();
    let server = HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .wrap(middleware::Compress::default())
            .app_data(Data::new(clusters.clone()))
            .app_data(Data::new(subscriptions.clone()))
            .app_data(metadata_service_.clone())
            .configure(routes)
    })
    .bind((config.host.clone(), config.port.clone()))?
    .disable_signals()
    .run();

    let server_handle = server.handle();
    let server_task = tokio::spawn(async move {
        info!("Server running at http://{}:{}", config.host, config.port);
        server.await
    });

    let shutdown_task = tokio::spawn(async move {
        // Listen for ctrl-c
        tokio::signal::ctrl_c().await.unwrap();
        info!("Global shutdown has been initiated...");

        // Start shutdown of tasks
        metadata_service.clone().into_inner().stop().await;
        debug!("Metadata service shutdown completed...");

        server_handle.stop(true).await;
        debug!("HTTP server shutdown completed...");
    });

    let _ = tokio::try_join!(server_task, shutdown_task).expect("unable to join tasks");

    Ok(())
}

fn routes(config: &mut web::ServiceConfig) {
    config.service(web::scope("api/v1/clusters").configure(configure_cluster));
    config.service(web::scope("api/v1/subscriptions").configure(configure_subscription));
}
