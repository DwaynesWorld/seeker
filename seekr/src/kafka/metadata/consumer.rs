use std::time::Duration;
use std::{result::Result, sync::Arc};

use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::groups::GroupInfo;
use rdkafka::metadata::{MetadataBroker, MetadataTopic};
use rdkafka::ClientConfig;
use tokio::sync::Mutex;

use crate::clusters::cluster::Cluster;
use crate::errors::AnyError;
use crate::kafka::config;

use super::{
    BrokerMetadata, ClusterMetadata, GroupMember, GroupMetadata, PartitionMetadata, TopicMetadata,
};

/// Timeout for fetching metadata.
pub const FETCH_METADATA_TIMEOUT_MS: Duration = Duration::from_millis(15_000);

#[async_trait]
pub trait MetadataConsumer {
    async fn fetch_meta(&self) -> Result<ClusterMetadata, AnyError>;
}

pub struct KafkaMetadataConsumer {
    pub inner: Arc<Mutex<BaseConsumer>>,
}

impl KafkaMetadataConsumer {
    pub fn create(cluster: &Cluster) -> Result<Self, AnyError> {
        debug!("cluster config: {:?}", cluster.config);

        let bootstraps = cluster
            .config
            .get(config::BOOTSTRAP_SERVERS)
            .unwrap_or(&String::from("localhost:9092"))
            .to_owned();

        let group_id = cluster
            .config
            .get(config::SEEKR_GROUP_ID)
            .unwrap_or(&String::from("seekr.io"))
            .to_owned();

        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstraps)
            .set("group.id", &group_id)
            .set("api.version.request", "true")
            .create::<BaseConsumer>()?;

        Ok(Self {
            inner: Arc::new(Mutex::new(consumer)),
        })
    }
}

#[async_trait]
impl MetadataConsumer for KafkaMetadataConsumer {
    async fn fetch_meta(&self) -> Result<ClusterMetadata, AnyError> {
        let inner = self.inner.lock().await;
        let metadata = inner.fetch_metadata(None, FETCH_METADATA_TIMEOUT_MS)?;

        let mut brokers = metadata
            .brokers()
            .iter()
            .map(parse_broker)
            .collect::<Vec<_>>();
        brokers.sort_by(|a, b| a.host.cmp(&b.host));

        let mut topics = metadata
            .topics()
            .iter()
            .map(parse_topic)
            .collect::<Vec<_>>();
        topics.sort_by(|a, b| a.name.cmp(&b.name));

        let mut groups = inner
            .fetch_group_list(None, FETCH_METADATA_TIMEOUT_MS)?
            .groups()
            .iter()
            .map(parse_group)
            .collect::<Vec<_>>();
        groups.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(ClusterMetadata {
            brokers,
            groups,
            topics,
        })
    }
}

fn parse_broker(b: &MetadataBroker) -> BrokerMetadata {
    BrokerMetadata {
        id: b.id(),
        host: b.host().to_owned(),
        port: b.port(),
    }
}

fn parse_topic(t: &MetadataTopic) -> TopicMetadata {
    let mut partitions = t
        .partitions()
        .iter()
        .map(|p| PartitionMetadata {
            id: p.id(),
            leader: p.leader(),
            replicas: p.replicas().to_owned(),
            isr: p.isr().to_owned(),
            error: p
                .error()
                .map(|e| KafkaError::MetadataFetch(e.into()).to_string()),
        })
        .collect::<Vec<_>>();
    partitions.sort_by(|a, b| a.id.cmp(&b.id));

    TopicMetadata {
        name: t.name().to_string(),
        partitions,
    }
}

fn parse_group(g: &GroupInfo) -> GroupMetadata {
    let members = g
        .members()
        .iter()
        .map(|m| GroupMember {
            id: m.id().to_owned(),
            client_id: m.client_id().to_owned(),
            client_host: m.client_host().to_owned(),
        })
        .collect::<Vec<_>>();

    GroupMetadata {
        name: g.name().to_owned(),
        state: g.state().to_owned(),
        members,
    }
}
