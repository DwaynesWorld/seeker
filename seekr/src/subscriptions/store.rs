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

use crate::id;
use crate::session::CdrsSession;

use super::subscription::Subscription;

#[async_trait]
pub trait SubscriptionStore {
    async fn list(&self, cluster_id: i64) -> Result<Vec<Subscription>, Error>;
    async fn get(&self, cluster_id: i64, id: i64) -> result::Result<Option<Subscription>, Error>;
    async fn insert(&self, subscription: Subscription) -> result::Result<i64, Error>;
    async fn update(&self, subscription: Subscription) -> result::Result<i64, Error>;
    async fn remove(&self, cluster_id: i64, id: i64) -> result::Result<i64, Error>;
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
    async fn list(&self, cluster_id: i64) -> Result<Vec<Subscription>, Error> {
        let stmt = "SELECT * FROM adm.subscriptions WHERE cluster_id = ? LIMIT 100;";
        let values = query_values!(cluster_id);
        let rows = self.session.query_with_values(stmt, values).await;
        let rows = self.parse(rows)?;
        let subs = rows.iter().map(|r| self.map(r)).collect::<Vec<_>>();

        Ok(subs)
    }

    async fn get(&self, cluster_id: i64, id: i64) -> result::Result<Option<Subscription>, Error> {
        let stmt = "SELECT * FROM adm.subscriptions WHERE cluster_id = ? AND id = ?;";
        let values = query_values!(cluster_id, id);
        let rows = self.session.query_with_values(stmt, values).await;
        let rows = self.parse(rows)?;

        if rows.len() == 0 {
            return Ok(None);
        }

        Ok(Some(self.map(rows.get(0).unwrap())))
    }

    async fn insert(&self, s: Subscription) -> result::Result<i64, Error> {
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

        let result = self.session.query_with_values(stmt, values).await;
        if result.is_ok() {
            return Ok(s.id);
        }

        Err(result.unwrap_err())
    }

    async fn update(&self, s: Subscription) -> result::Result<i64, Error> {
        let stmt = "
			UPDATE adm.subscriptions
			SET topic_name = ?, config = ?, updated_at = ?
            WHERE cluster_id = ? AND id = ?;";

        let values = query_values!(s.topic_name, s.config, s.updated_at, s.cluster_id, s.id);
        let result = self.session.query_with_values(stmt, values).await;

        if result.is_ok() {
            return Ok(s.id);
        }

        Err(result.unwrap_err())
    }

    async fn remove(&self, cluster_id: i64, id: i64) -> result::Result<i64, Error> {
        let stmt = "DELETE FROM adm.subscriptions WHERE cluster_id = ? AND id = ?;";
        let values = query_values!(cluster_id, id);
        let result = self.session.query_with_values(stmt, values).await;

        if result.is_ok() {
            return Ok(id);
        }

        Err(result.unwrap_err())
    }
}
