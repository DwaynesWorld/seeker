use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::clusters::store::{init_cluster_store, ClusterStore};
use crate::errors::AnyError;
use crate::kafka::streams::service::StreamsService;
use crate::logger;
use crate::subscriptions::store::{init_subscription_store, SubscriptionStore};
use crate::BANNER;

pub struct IndexerConfig {
    pub log: logger::Level,
}

pub async fn run(config: IndexerConfig) -> std::io::Result<()> {
    // Set the default log level
    logger::init(&config.log);

    // Output seekr banner
    info!("{}", BANNER);
    info!("Starting indexer...");

    // Initialize shared state
    let clusters = init_cluster_store().await;
    let subscriptions = init_subscription_store().await;
    let scheduler = Arc::new(Scheduler::new(clusters.clone(), subscriptions.clone()));

    // Start index scheduler
    let scheduler_clone = scheduler.clone();
    let scheduler_task = tokio::spawn(async move {
        info!("Indexer running ...");
        scheduler_clone.start().await
    });

    // Listen for shutdown
    let shutdown_task = tokio::spawn(async move {
        // Listen for ctrl-c
        tokio::signal::ctrl_c().await.unwrap();
        info!("Global shutdown has been initiated...");

        // Start shutdown of tasks
        scheduler.stop().await;
        debug!("Scheduler service shutdown completed...");
    });

    let _ = tokio::try_join!(scheduler_task, shutdown_task).expect("unable to join tasks");

    Ok(())
}

struct State {
    workers: HashMap<i64, Arc<StreamsService>>,
}

pub struct Scheduler {
    cs: Arc<dyn ClusterStore + Send + Sync>,
    ss: Arc<dyn SubscriptionStore + Send + Sync>,
    state: Arc<RwLock<State>>,
}

impl Scheduler {
    pub fn new(
        cs: Arc<dyn ClusterStore + Send + Sync>,
        ss: Arc<dyn SubscriptionStore + Send + Sync>,
    ) -> Self {
        let state = State {
            workers: HashMap::new(),
        };
        Self {
            cs,
            ss,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<(), AnyError> {
        debug!("Starting stream scheduler...");

        // Acquire lock to prevent multiple starts
        let mut state = self.state.write().await;

        // Fetch all subscriptions
        let subs = self.ss.list(None).await?;
        let ids = subs.iter().map(|x| x.cluster_id).collect::<Vec<i64>>();
        let clusters = self
            .cs
            .list(Some(ids))
            .await?
            .iter()
            .map(|x| (x.id, x.clone()))
            .collect::<HashMap<_, _>>();

        for sub in subs {
            // Create StreamService for each subscription
            let cluster = clusters
                .get(&sub.cluster_id)
                .expect("unable to find for sub")
                .clone();
            let service = Arc::new(StreamsService::new(cluster, sub.clone()));

            // Track service
            state.workers.insert(sub.id, service.clone());

            // Spawn thread in the background
            tokio::spawn(async move { service.start().await });
        }

        Ok(())
    }

    pub async fn stop(self: Arc<Self>) {
        debug!("Stopping streams scheduler...");
        debug!("Streams scheduler shutdown has been initiated...");

        let state = self.state.read().await;
        let workers = state.workers.values();

        todo!("shutdown workers");

        debug!("Streams scheduler shutdown has been completed...");
    }
}
