use std::error::Error;
use std::time::Duration;
use std::{collections::HashMap, result::Result, sync::Arc};

use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;

use crate::clusters::{cluster::Cluster, store::ClusterStore};

/// Timeout for fetching metadata.
pub const FETCH_METADATA_TIMEOUT_MS: i32 = 60_000;

#[async_trait]
pub trait MetadataConsumer {
    async fn fetch_meta(&self) -> Result<ClusterMetadata, Box<dyn Error + Send + Sync>>;
}

pub struct KafkaMetadataConsumer {
    pub inner: Arc<Mutex<BaseConsumer<EmptyConsumerContext>>>,
}

impl KafkaMetadataConsumer {
    pub fn create(cluster: &Cluster) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let bootstraps = cluster.meta.get("bootstrap.servers").unwrap();

        let consumer = ClientConfig::new()
            .set("bootstrap.servers", &bootstraps)
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

        let brokers = metadata
            .brokers()
            .iter()
            .map(|b| BrokerMetadata {
                id: b.id(),
                host: b.host().to_owned(),
                port: b.port(),
            })
            .collect::<Vec<_>>();

        let topics = metadata
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

        let groups = inner
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

        Ok(ClusterMetadata {
            brokers,
            groups,
            topics,
        })
    }
}

pub struct MetadataService {
    store: Arc<dyn ClusterStore + Send + Sync>,
    state: Arc<RwLock<State>>,
}

struct State {
    consumers: HashMap<i64, Arc<dyn MetadataConsumer + Send + Sync>>,
    cache: HashMap<i64, ClusterMetadata>,
}

impl MetadataService {
    pub fn new(store: Arc<dyn ClusterStore + Send + Sync>) -> Self {
        let state = State {
            consumers: HashMap::new(),
            cache: HashMap::new(),
        };
        MetadataService {
            store,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Acquire lock for startup
        let mut state = self.state.write().await;

        // Fetch all registered clusters from db
        let clusters = self.store.list().await?;

        for c in clusters {
            if state.consumers.contains_key(&c.id) {
                warn!("Meta consumer is already registered for cluster: {}", c.id);
                continue;
            }

            // Create consumer for each cluster
            let consumer = KafkaMetadataConsumer::create(&c)?;
            let consumer = Arc::new(consumer);

            // Track consumers
            state.consumers.insert(c.id, consumer.clone());

            // FIXME: borrowed data escapes outside of associated function
            // `self` escapes the associated function body here
            // tokio::spawn(async move { self.poll(c, consumer).await });
        }

        Ok(())
    }

    pub async fn register() {
        todo!()
    }

    pub async fn remove() {
        todo!()
    }

    pub async fn poll(&self, cluster: Cluster, consumer: Arc<dyn MetadataConsumer + Send + Sync>) {
        let refresh: u64 = cluster
            .meta
            .get("metadata.refresh.seconds")
            .unwrap()
            .parse()
            .unwrap();
        let mut interval = interval(Duration::from_secs(refresh));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let result = consumer.fetch_meta().await;
                    if result.is_err() {
                        error!("Error: Failed to fetch metadata for cluster {} - {:?}", cluster.id, result.err());
                        continue;
                    }

                    let metadata = result.unwrap();
                    let mut state = self.state.write().await;
                    state.cache.insert(cluster.id, metadata);
                }
            }
        }
    }
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct ClusterMetadata {
    pub brokers: Vec<BrokerMetadata>,
    pub groups: Vec<GroupMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct BrokerMetadata {
    /// The id of the broker.
    pub id: i32,
    /// The host name of the broker.
    pub host: String,
    /// The port of the broker.
    pub port: i32,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct GroupMetadata {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub error: Option<String>,
}
