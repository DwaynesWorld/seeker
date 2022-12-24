use std::error::Error;
use std::{result::Result, sync::Arc};

use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};
use rdkafka::ClientConfig;
use tokio::sync::Mutex;

use crate::clusters::{cluster::config, cluster::Cluster};

use super::{
    BrokerMetadata, ClusterMetadata, GroupMember, GroupMetadata, PartitionMetadata, TopicMetadata,
};

/// Timeout for fetching metadata.
pub const FETCH_METADATA_TIMEOUT_MS: i32 = 15_000;

#[async_trait]
pub trait MetadataConsumer {
    async fn fetch_meta(&self) -> Result<ClusterMetadata, Box<dyn Error + Send + Sync>>;
}

pub struct KafkaMetadataConsumer {
    pub inner: Arc<Mutex<BaseConsumer<EmptyConsumerContext>>>,
}

impl KafkaMetadataConsumer {
    pub fn create(cluster: &Cluster) -> Result<Self, Box<dyn Error + Send + Sync>> {
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

        debug!("bootstraps: {}", bootstraps);
        debug!("group_id: {}", group_id);

        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstraps)
            .set("group.id", &group_id)
            .set("api.version.request", "true")
            .create::<BaseConsumer<EmptyConsumerContext>>()?;

        Ok(Self {
            inner: Arc::new(Mutex::new(consumer)),
        })
    }
}

#[async_trait]
impl MetadataConsumer for KafkaMetadataConsumer {
    async fn fetch_meta(&self) -> Result<ClusterMetadata, Box<dyn Error + Send + Sync>> {
        let inner = self.inner.lock().await;
        let metadata = inner.fetch_metadata(None, FETCH_METADATA_TIMEOUT_MS)?;

        let mut brokers = metadata
            .brokers()
            .iter()
            .map(|b| BrokerMetadata {
                id: b.id(),
                host: b.host().to_owned(),
                port: b.port(),
            })
            .collect::<Vec<_>>();
        brokers.sort_by(|a, b| a.host.cmp(&b.host));

        let mut topics = metadata
            .topics()
            .iter()
            .map(|t| {
                let mut partitions = t
                    .partitions()
                    .iter()
                    .map(|p| {
                        let partition_err = p
                            .error()
                            .map(|e| rdkafka::types::RDKafkaError::from(e).to_string());
                        PartitionMetadata {
                            id: p.id(),
                            leader: p.leader(),
                            replicas: p.replicas().to_owned(),
                            isr: p.isr().to_owned(),
                            error: partition_err,
                        }
                    })
                    .collect::<Vec<_>>();
                partitions.sort_by(|a, b| a.id.cmp(&b.id));

                TopicMetadata {
                    name: t.name().to_string(),
                    partitions,
                }
            })
            .collect::<Vec<_>>();
        topics.sort_by(|a, b| a.name.cmp(&b.name));

        let mut groups = inner
            .fetch_group_list(None, FETCH_METADATA_TIMEOUT_MS)?
            .groups()
            .iter()
            .map(|g| {
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
            })
            .collect::<Vec<_>>();
        groups.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(ClusterMetadata {
            brokers,
            groups,
            topics,
        })
    }
}
