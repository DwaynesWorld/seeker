pub mod metadata;
pub mod streams;

pub mod config {
    pub const BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
    pub const SEEKR_GROUP_ID: &str = "seekr.group.id";
    pub const METADATA_POLL_INTERVAL: &str = "metadata.poll.interval.ms";
    pub const METRICS_POLL_INTERVAL: &str = "metrics.poll.interval.ms";
}
