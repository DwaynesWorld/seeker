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
use meilisearch_sdk::Client;

use crate::errors::AnyError;
use crate::id;
use crate::session::CdrsSession;

use super::cluster::{Cluster, Kind};

#[async_trait]
pub trait ClusterStore {
    async fn list(&self) -> Result<Vec<Cluster>, AnyError>;
    async fn get(&self, id: i64) -> result::Result<Option<Cluster>, AnyError>;
    async fn insert(&self, cluster: Cluster) -> result::Result<i64, AnyError>;
    async fn update(&self, cluster: Cluster) -> result::Result<i64, AnyError>;
    async fn remove(&self, id: i64) -> result::Result<i64, AnyError>;
}

pub const INDEX_NAME: &str = "clusters";

pub struct MSClusterStore {
    /// Meilisearch client that provides an interface for interacting with the DB.
    client: Arc<Client>,
    /// A Distributed Unique ID generator.
    generator: Arc<id::Generator>,
}

impl MSClusterStore {
    pub fn new(client: Arc<Client>, generator: Arc<id::Generator>) -> Self {
        Self { client, generator }
    }
}

#[async_trait]
impl ClusterStore for MSClusterStore {
    async fn list(&self) -> Result<Vec<Cluster>, AnyError> {
        let docs = self
            .client
            .index(INDEX_NAME)
            .get_documents::<Cluster>()
            .await?;
        Ok(docs.results)
    }

    async fn get(&self, id: i64) -> result::Result<Option<Cluster>, AnyError> {
        let cluster = self
            .client
            .index(INDEX_NAME)
            .get_document::<Cluster>(&id.to_string())
            .await?;

        Ok(Some(cluster))
    }

    async fn insert(&self, c: Cluster) -> result::Result<i64, AnyError> {
        let cluster = Cluster {
            id: self.generator.next_id().unwrap(),
            kind: c.kind,
            name: c.name,
            config: c.config,
            created_at: c.created_at,
            updated_at: c.updated_at,
        };

        self.client
            .index(INDEX_NAME)
            .add_or_replace(&vec![&cluster], Some("id"))
            .await?
            .wait_for_completion(&self.client, None, None)
            .await?;

        Ok(cluster.id)
    }

    async fn update(&self, c: Cluster) -> result::Result<i64, AnyError> {
        self.client
            .index(INDEX_NAME)
            .add_or_replace(&vec![&c], Some("id"))
            .await?
            .wait_for_completion(&self.client, None, None)
            .await?;

        Ok(c.id)
    }

    async fn remove(&self, id: i64) -> result::Result<i64, AnyError> {
        self.client
            .index(INDEX_NAME)
            .delete_document(id.to_string())
            .await?;

        Ok(id)
    }
}

pub struct CdrsClusterStore {
    /// Cassandra session that holds a pool of connections to nodes and provides an interface for
    /// interacting with the cluster.
    session: Arc<CdrsSession>,

    /// A Distributed Unique ID generator.
    generator: Arc<id::Generator>,
}

impl CdrsClusterStore {
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

    fn map(&self, row: &Row) -> Cluster {
        let id = row.r_by_name::<i64>(&"id").unwrap();
        let name = row.r_by_name::<String>(&"name").unwrap();

        let kind = match row.r_by_name::<i32>(&"kind").unwrap() {
            1 => Kind::Kafka,
            _ => Kind::Unknown,
        };

        let config: HashMap<String, String> = match row.r_by_name::<Map>(&"config") {
            Ok(m) => m.as_r_type().unwrap(),
            Err(_) => HashMap::new(),
        };

        let created_at = row.r_by_name::<DateTime<Utc>>(&"created_at").unwrap();
        let updated_at = row.r_by_name::<DateTime<Utc>>(&"updated_at").unwrap();

        Cluster::init(id, kind, name, config, created_at, updated_at)
    }
}

#[async_trait]
impl ClusterStore for CdrsClusterStore {
    async fn list(&self) -> Result<Vec<Cluster>, AnyError> {
        let stmt = "SELECT * FROM adm.clusters LIMIT 100;";
        let rows = self.session.query(stmt).await;
        let rows = self.parse(rows)?;
        let clusters = rows.iter().map(|r| self.map(r)).collect::<Vec<_>>();
        Ok(clusters)
    }

    async fn get(&self, id: i64) -> result::Result<Option<Cluster>, AnyError> {
        let stmt = "SELECT * FROM adm.clusters WHERE id = ?;";
        let values = query_values!(id);
        let rows = self.session.query_with_values(stmt, values).await;
        let rows = self.parse(rows)?;
        if rows.len() == 0 {
            return Ok(None);
        }

        Ok(Some(self.map(rows.get(0).unwrap())))
    }

    async fn insert(&self, c: Cluster) -> result::Result<i64, AnyError> {
        let stmt = "
            INSERT INTO adm.clusters (id, kind, name, config, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?);";

        let id = self.generator.next_id().unwrap();
        let values = query_values!(
            id,
            c.kind.clone() as i32,
            c.name.clone(),
            c.config.clone(),
            c.created_at,
            c.updated_at
        );

        self.session.query_with_values(stmt, values).await?;

        Ok(id)
    }

    async fn update(&self, c: Cluster) -> result::Result<i64, AnyError> {
        let stmt = "
			UPDATE adm.clusters
			SET name = ?, config = ?, updated_at = ?
            WHERE id = ?;";

        let values = query_values!(c.name.to_owned(), c.config.to_owned(), c.updated_at, c.id);
        self.session.query_with_values(stmt, values).await?;

        Ok(c.id)
    }

    async fn remove(&self, id: i64) -> result::Result<i64, AnyError> {
        let stmt = "DELETE FROM adm.clusters WHERE id = ?;";
        let values = query_values!(id);
        self.session.query_with_values(stmt, values).await?;

        Ok(id)
    }
}
