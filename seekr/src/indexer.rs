use crate::{logger, BANNER};

pub struct IndexerConfig {
    pub log: logger::Level,
}

pub async fn run(config: IndexerConfig) -> std::io::Result<()> {
    // Set the default log level
    logger::init(&config.log);

    // Output seekr banner
    info!("{}", BANNER);
    info!("Starting indexer...");

    Ok(())
}
