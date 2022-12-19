use clap::Args;

use seekr::logger::Level;

#[derive(Args, Debug)]
pub struct IndexerConfig {
    #[clap(
        short,
        long,
        env = "SEEKER_LOG",
        default_value = "info",
        forbid_empty_values = true,
        help = "The logging level",
        value_enum
    )]
    /// The logging level
    pub log: Level,
}

impl From<seekr::indexer::IndexerConfig> for IndexerConfig {
    fn from(c: seekr::indexer::IndexerConfig) -> Self {
        Self { log: c.log }
    }
}

impl From<IndexerConfig> for seekr::indexer::IndexerConfig {
    fn from(c: IndexerConfig) -> Self {
        Self { log: c.log }
    }
}
