use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer};
use rdkafka::message::Headers;
use rdkafka::{ClientConfig, Message};

use crate::clusters::cluster::Cluster;
use crate::errors::AnyError;
use crate::kafka::config;
use crate::subscriptions::subscription::Subscription;

use super::StreamsMessage;

/// Timeout for fetching message.
pub const POLL_TIMEOUT_MS: i32 = 5_000;

#[async_trait]
pub trait StreamsConsumer {
    async fn consume(&self) -> Result<Option<StreamsMessage>, AnyError>;
}

pub struct KafkaStreamsConsumer {
    pub inner: Arc<StreamConsumer>,
}

impl KafkaStreamsConsumer {
    pub fn create(cluster: &Cluster, subscription: &Subscription) -> Result<Self, AnyError> {
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
            .create::<StreamConsumer>()?;

        consumer.subscribe(&[&subscription.topic_name])?;

        Ok(Self {
            inner: Arc::new(consumer),
        })
    }
}

#[async_trait]
impl StreamsConsumer for KafkaStreamsConsumer {
    async fn consume(&self) -> Result<Option<StreamsMessage>, AnyError> {
        match self.inner.recv().await {
            Err(e) => {
                warn!("Kafka error: {}", e);
                Err(e.into())
            }
            Ok(m) => {
                let headers: HashMap<_, _> = m
                    .headers()
                    .unwrap()
                    .iter()
                    .map(|h| {
                        (
                            h.key.to_string(),
                            String::from_utf8(h.value.unwrap_or(b"").to_vec()).unwrap(),
                        )
                    })
                    .collect();

                let payload = match m.payload_view::<str>() {
                    None => None,
                    Some(Ok(s)) => Some(s.to_string()),
                    Some(Err(e)) => {
                        warn!("Error while deserializing message payload: {:?}", e);
                        None
                    }
                };

                debug!("key: '{:?}', payload: '{:?}', topic: {}, partition: {}, offset: {}, timestamp: {:?}",
					  m.key(), payload, m.topic(), m.partition(), m.offset(), m.timestamp());

                self.inner.commit_message(&m, CommitMode::Async).unwrap();

                Ok(Some(StreamsMessage { payload, headers }))
            }
        }
    }
}
