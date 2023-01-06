use std::collections::HashMap;
use std::option::Option;
use std::result;
use std::sync::Arc;
use std::vec::Vec;

use async_trait::async_trait;
use cdrs_tokio::error::Error;
use cdrs_tokio::frame::Frame;
use cdrs_tokio::query_values;
use cdrs_tokio::types::prelude::{Map, Row};
use cdrs_tokio::types::{AsRustType, ByName};
use chrono::{DateTime, Utc};
use meilisearch_sdk::indexes::Index;
use meilisearch_sdk::Client;

use crate::errors::AnyError;
use crate::session::CdrsSession;
use crate::{id, ID_GENERATOR, MS_CLIENT};

use super::subscription::Subscription;

#[async_trait]
pub trait SubscriptionStore {
    async fn list(&self, cluster_id: Option<i64>) -> Result<Vec<Subscription>, AnyError>;
    async fn get(&self, cluster_id: i64, id: i64)
        -> result::Result<Option<Subscription>, AnyError>;
    async fn insert(&self, subscription: Subscription) -> result::Result<i64, AnyError>;
    async fn update(&self, subscription: Subscription) -> result::Result<i64, AnyError>;
    async fn remove(&self, cluster_id: i64, id: i64) -> result::Result<i64, AnyError>;
}

pub const INDEX_NAME: &str = "subscriptions";

pub struct MSSubscriptionStore {
    /// Meilisearch client that provides an interface for interacting with the DB.
    client: Arc<Client>,
    /// A Distributed Unique ID generator.
    generator: Arc<id::Generator>,
}

impl MSSubscriptionStore {
    pub async fn new(client: Arc<Client>, generator: Arc<id::Generator>) -> Self {
        match client.clone().create_index(INDEX_NAME, Some("id")).await {
            Ok(task) => {
                task.wait_for_completion(&client, None, None).await.unwrap();
            }
            Err(_) => {
                // Noop
            }
        };

        Self { client, generator }
    }

    fn index(&self) -> Index {
        self.client.index(INDEX_NAME)
    }
}

#[async_trait]
impl SubscriptionStore for MSSubscriptionStore {
    async fn list(&self, cluster_id: Option<i64>) -> Result<Vec<Subscription>, AnyError> {
        if cluster_id.is_none() {
            let subs = self.index().get_documents::<Subscription>().await?;
            return Ok(subs.results);
        }

        let results = self
            .index()
            .search()
            .with_filter(&format!("cluster_id = {}", cluster_id.unwrap()))
            .execute::<Subscription>()
            .await?;

        let subs = results
            .hits
            .iter()
            .map(|h| h.result.clone())
            .collect::<Vec<_>>();

        Ok(subs)
    }

    async fn get(
        &self,
        _cluster_id: i64,
        id: i64,
    ) -> result::Result<Option<Subscription>, AnyError> {
        let cluster = self
            .index()
            .get_document::<Subscription>(&id.to_string())
            .await?;

        Ok(Some(cluster))
    }

    async fn insert(&self, s: Subscription) -> result::Result<i64, AnyError> {
        let sub = Subscription {
            id: self.generator.next_id().unwrap(),
            cluster_id: s.cluster_id,
            topic_name: s.topic_name,
            config: s.config,
            created_at: s.created_at,
            updated_at: s.updated_at,
        };

        self.index()
            .add_or_replace(&vec![&sub], Some("id"))
            .await?
            .wait_for_completion(&self.client, None, None)
            .await?;

        Ok(sub.id)
    }

    async fn update(&self, s: Subscription) -> result::Result<i64, AnyError> {
        self.index()
            .add_or_replace(&vec![&s], Some("id"))
            .await?
            .wait_for_completion(&self.client, None, None)
            .await?;

        Ok(s.id)
    }

    async fn remove(&self, _cluster_id: i64, id: i64) -> result::Result<i64, AnyError> {
        self.index().delete_document(id.to_string()).await?;
        Ok(id)
    }
}

pub struct CdrsSubscriptionStore {
    /// Cassandra session that holds a pool of connections to nodes
    /// and provides an interface for interacting with the cluster.
    session: Arc<CdrsSession>,

    /// A Distributed Unique ID generator.
    generator: Arc<id::Generator>,
}

impl CdrsSubscriptionStore {
    pub fn new(session: Arc<CdrsSession>, generator: Arc<id::Generator>) -> Self {
        Self { session, generator }
    }

    fn parse(&self, result: Result<Frame, Error>) -> Result<Vec<Row>, Error> {
        let rows = result?
            .response_body()?
            .into_rows()
            .ok_or(Error::General(format!("Failed to parse database response")))?;
        Ok(rows)
    }

    fn map(&self, row: &Row) -> Subscription {
        let id = row.r_by_name::<i64>(&"id").unwrap();
        let cluster_id = row.r_by_name::<i64>(&"cluster_id").unwrap();
        let topic_name = row.r_by_name::<String>(&"topic_name").unwrap();

        let config: HashMap<String, String> = match row.r_by_name::<Map>(&"config") {
            Ok(m) => m.as_r_type().unwrap(),
            Err(_) => HashMap::new(),
        };

        let created_at = row.r_by_name::<DateTime<Utc>>(&"created_at").unwrap();
        let updated_at = row.r_by_name::<DateTime<Utc>>(&"updated_at").unwrap();

        Subscription::init(id, cluster_id, topic_name, config, created_at, updated_at)
    }
}

#[async_trait]
impl SubscriptionStore for CdrsSubscriptionStore {
    async fn list(&self, cluster_id: Option<i64>) -> Result<Vec<Subscription>, AnyError> {
        let stmt = "SELECT * FROM adm.subscriptions WHERE cluster_id = ? LIMIT 100;";
        let values = query_values!(cluster_id.unwrap());
        let rows = self.session.query_with_values(stmt, values).await;
        let rows = self.parse(rows)?;
        let subs = rows.iter().map(|r| self.map(r)).collect::<Vec<_>>();

        Ok(subs)
    }

    async fn get(
        &self,
        cluster_id: i64,
        id: i64,
    ) -> result::Result<Option<Subscription>, AnyError> {
        let stmt = "SELECT * FROM adm.subscriptions WHERE cluster_id = ? AND id = ?;";
        let values = query_values!(cluster_id, id);
        let rows = self.session.query_with_values(stmt, values).await;
        let rows = self.parse(rows)?;
        if rows.len() == 0 {
            return Ok(None);
        }

        Ok(Some(self.map(rows.get(0).unwrap())))
    }

    async fn insert(&self, s: Subscription) -> result::Result<i64, AnyError> {
        let stmt = "
            INSERT INTO adm.subscriptions (id, cluster_id, topic_name, config, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?);";

        let mut s = s.clone();
        s.id = self.generator.next_id().unwrap();

        let values = query_values!(
            s.id,
            s.cluster_id,
            s.topic_name,
            s.config,
            s.created_at,
            s.updated_at
        );

        self.session.query_with_values(stmt, values).await?;

        Ok(s.id)
    }

    async fn update(&self, s: Subscription) -> result::Result<i64, AnyError> {
        let stmt = "
			UPDATE adm.subscriptions
			SET topic_name = ?, config = ?, updated_at = ?
            WHERE cluster_id = ? AND id = ?;";

        let values = query_values!(s.topic_name, s.config, s.updated_at, s.cluster_id, s.id);
        self.session.query_with_values(stmt, values).await?;

        Ok(s.id)
    }

    async fn remove(&self, cluster_id: i64, id: i64) -> result::Result<i64, AnyError> {
        let stmt = "DELETE FROM adm.subscriptions WHERE cluster_id = ? AND id = ?;";
        let values = query_values!(cluster_id, id);
        self.session.query_with_values(stmt, values).await?;

        Ok(id)
    }
}

pub async fn init_subscription_store() -> Arc<dyn SubscriptionStore + Send + Sync> {
    // Arc::new(CdrsSubscriptionStore::new(session, generator))
    Arc::new(MSSubscriptionStore::new(MS_CLIENT.clone(), ID_GENERATOR.clone()).await)
}
