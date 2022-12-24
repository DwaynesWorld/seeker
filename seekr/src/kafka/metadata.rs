use std::error::Error;
use std::time::Duration;
use std::{collections::HashMap, result::Result, sync::Arc};

use async_trait::async_trait;
use rdkafka::consumer::{BaseConsumer, Consumer, EmptyConsumerContext};
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tokio::time::interval;

use crate::clusters::{cluster::config, cluster::Cluster, store::ClusterStore};
use crate::shutdown::Shutdown;

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

#[derive(Clone)]
pub struct ConsumerContext {
    consumer: Arc<dyn MetadataConsumer + Send + Sync>,
    sd: Arc<Shutdown>,
}

pub struct MetadataService {
    store: Arc<dyn ClusterStore + Send + Sync>,
    state: Arc<RwLock<State>>,
}

struct State {
    context: HashMap<i64, ConsumerContext>,
    cache: HashMap<i64, ClusterMetadata>,
}

impl MetadataService {
    pub fn new(store: Arc<dyn ClusterStore + Send + Sync>) -> Self {
        let state = State {
            context: HashMap::new(),
            cache: HashMap::new(),
        };
        MetadataService {
            store,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Fetch all registered clusters from db
        let clusters = self.store.list().await?;

        for c in clusters {
            self.clone().init(c).await?;
        }

        Ok(())
    }

    pub async fn stop(self: Arc<Self>) {
        debug!("Metadata service shutdown has been initiated...");

        let state = self.state.read().await;
        let contexts = state.context.values();

        for c in contexts {
            c.sd.begin();
            c.sd.wait_complete().await;
        }

        debug!("Metadata service shutdown has been completed...");
    }

    pub async fn register(self: Arc<Self>, c: Cluster) {
        let result = self.init(c).await;
        if result.is_err() {
            error!(
                "Error: registering cluster: {}",
                result.unwrap_err().source().unwrap()
            );
        }
    }

    pub async fn remove(self: Arc<Self>, id: i64) {
        let mut state = self.state.write().await;
        let context = state.context.remove(&id);
        if context.is_some() {
            context.unwrap().sd.begin();
        }
    }

    async fn init(self: Arc<Self>, c: Cluster) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("Initializing metadata consumer for cluster {}...", c.id);

        // Acquire read lock and check if consumer exist
        let this = self.clone();
        let state = this.state.read().await;
        let exists = state.context.contains_key(&c.id);
        drop(state);

        if exists {
            warn!("Meta consumer is already registered for cluster: {}", c.id);
            return Ok(());
        }

        // Create consumer for each cluster
        let consumer = KafkaMetadataConsumer::create(&c)?;
        let consumer = Arc::new(consumer);
        let sd = Arc::new(Shutdown::new());
        let context = ConsumerContext { consumer, sd };

        // Acquire write lock and Track consumers
        let mut state = this.state.write().await;
        state.context.insert(c.id, context.clone());
        drop(state);

        // Poll metadata in the background
        let this = self.clone();
        tokio::spawn(async move { this.poll(c, context).await });
        Ok(())
    }

    async fn poll(self: Arc<Self>, cluster: Cluster, context: ConsumerContext) {
        let refresh: u64 = cluster
            .config
            .get(config::METADATA_POLL_INTERVAL)
            .unwrap_or(&String::from("30000"))
            .parse()
            .unwrap_or(30_000);
        let mut interval = interval(Duration::from_millis(refresh));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    trace!("Fetching metadata for cluster {}...", cluster.id);
                    let result = context.consumer.fetch_meta().await;
                    if result.is_err() {
                        error!("Error: Failed to fetch metadata for cluster {} - {:?}", cluster.id, result.err());
                        continue;
                    }

                    let metadata = result.unwrap();
                    trace!("Metadata: {:?}", metadata);

                    let mut state = self.state.write().await;
                    state.cache.insert(cluster.id, metadata);
                }
                _ = context.sd.wait_begin() => {
                    debug!("Metadata service poll shutdown started...");
                    drop(context.consumer);
                    context.sd.complete();
                    break;
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
    pub id: i32,
    pub host: String,
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
