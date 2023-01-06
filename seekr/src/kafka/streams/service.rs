use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;
use tokio::time::sleep;

use crate::clusters::cluster::Cluster;
use crate::errors::AnyError;
use crate::shutdown::Shutdown;
use crate::subscriptions::subscription::Subscription;

use super::consumer::StreamsConsumer;

#[derive(Clone)]
pub struct StreamsContext {
    consumer: Arc<dyn StreamsConsumer + Send + Sync>,
    sd: Arc<Shutdown>,
}

struct State {
    context: HashMap<i64, StreamsContext>,
}

pub struct StreamsService {
    clusters: Cluster,
    subscriptions: Subscription,
    state: Arc<RwLock<State>>,
}

impl StreamsService {
    pub fn new(clusters: Cluster, subscriptions: Subscription) -> Self {
        let state = State {
            context: HashMap::new(),
        };

        Self {
            clusters,
            subscriptions,
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn start(self: Arc<Self>) {
        info!("starting stream service");

        loop {
            sleep(Duration::from_millis(1100)).await;
            info!("1100 ms have elapsed");
        }
    }

    pub async fn stop(self: Arc<Self>) {
        info!("stopping stream service");
    }
}
