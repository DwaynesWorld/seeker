use std::time::Duration;
use std::{collections::HashMap, result::Result, sync::Arc};

use serde::Serialize;
use tokio::sync::RwLock;
use tokio::time::interval;

use crate::clusters::{cluster::Cluster, store::ClusterStore};
use crate::errors::AnyError;
use crate::kafka::config;
use crate::shutdown::Shutdown;

use super::consumer::{KafkaMetadataConsumer, MetadataConsumer};
use super::ClusterMetadata;

#[derive(Debug, Clone, Serialize)]
pub enum CachedMetadataEntry {
    Unknown,
    Processing,
    Meta(ClusterMetadata),
    Failed(String),
}

#[derive(Clone)]
pub struct ConsumerContext {
    consumer: Arc<dyn MetadataConsumer + Send + Sync>,
    sd: Arc<Shutdown>,
}

pub struct MetadataManager {
    store: Arc<dyn ClusterStore + Send + Sync>,
    state: Arc<RwLock<State>>,
}

struct State {
    context: HashMap<i64, ConsumerContext>,
    cache: HashMap<i64, CachedMetadataEntry>,
}

impl MetadataManager {
    pub fn new(store: Arc<dyn ClusterStore + Send + Sync>) -> Self {
        let state = State {
            context: HashMap::new(),
            cache: HashMap::new(),
        };
        MetadataManager {
            store,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<(), AnyError> {
        debug!("Starting Metadata service...");

        // Fetch all registered clusters from db
        let clusters = self.store.list(None).await?;

        for c in clusters {
            self.clone().init(c).await?;
        }

        Ok(())
    }

    pub async fn stop(self: Arc<Self>) {
        debug!("Stopping Metadata manager...");
        debug!("Metadata manager shutdown has been initiated...");

        let state = self.state.read().await;
        let contexts = state.context.values();

        for c in contexts {
            c.sd.begin();
            c.sd.wait_complete().await;
        }

        debug!("Metadata manager shutdown has been completed...");
    }

    pub async fn register(self: Arc<Self>, c: Cluster) {
        info!("Registering metadata consumer for cluster {}", c.id);

        let result = self.init(c).await;
        if result.is_err() {
            error!(
                "Error: registering cluster: {}",
                result.unwrap_err().source().unwrap()
            );
        }
    }

    pub async fn remove(self: Arc<Self>, id: i64) {
        info!("Removing metadata consumer for cluster {}", id);

        let mut state = self.state.write().await;
        let context = state.context.remove(&id);
        if context.is_some() {
            context.unwrap().sd.begin();
        }
    }

    pub async fn get(self: Arc<Self>, id: i64) -> Result<Option<CachedMetadataEntry>, AnyError> {
        info!("Fetching cached metadata for cluster {}", id);

        let state = self.state.read().await;
        let meta = state.cache.get(&id);
        Ok(meta.map(|m| m.to_owned()))
    }

    async fn init(self: Arc<Self>, c: Cluster) -> Result<(), AnyError> {
        info!("Initializing metadata consumer for cluster {}...", c.id);

        // Acquire read lock and check if consumer exist
        let manager = self.clone();
        let state = manager.state.read().await;
        let exists = state.context.contains_key(&c.id);
        drop(state);

        if exists {
            warn!("Meta consumer is already registered for cluster: {}", c.id);
            return Ok(());
        }

        // Create consumer for cluster
        let consumer = Arc::new(KafkaMetadataConsumer::create(&c)?);
        let sd = Arc::new(Shutdown::new());
        let context = ConsumerContext { consumer, sd };

        // Acquire write lock and track consumers
        let mut state = manager.state.write().await;
        state.context.insert(c.id, context.clone());
        state.cache.insert(c.id, CachedMetadataEntry::Processing);
        drop(state);

        // Spawn thread to poll metadata in the background
        let manager = self.clone();
        tokio::spawn(async move { manager.poll(c, context).await });
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
                    trace!("Polling metadata for cluster {}...", cluster.id);

                    let result = context.consumer.fetch_meta().await;
                    if result.is_err() {
                        let msg =  format!("Error: Failed to fetch metadata for cluster {} - {:?}", cluster.id, result.err());
                        error!("{}", msg);

                        let mut state = self.state.write().await;
                        state.cache.insert(cluster.id, CachedMetadataEntry::Failed(msg));
                        continue;
                    }

                    let metadata = result.unwrap();
                    trace!("Metadata: {:?}", metadata);

                    let mut state = self.state.write().await;
                    state.cache.insert(cluster.id, CachedMetadataEntry::Meta(metadata));
                }
                _ = context.sd.wait_begin() => {
                    debug!("Metadata manager poll shutdown started...");

                    drop(context.consumer);
                    context.sd.complete();
                    break;
                }
            }
        }
    }
}
