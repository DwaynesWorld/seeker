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

use super::cluster::{Cluster, Kind};

#[async_trait]
pub trait ClusterStore {
    async fn list(&self) -> Result<Vec<Cluster>, Error>;
    async fn get(&self, id: i64) -> result::Result<Option<Cluster>, Error>;
    async fn insert(&self, entry: Cluster) -> result::Result<i64, Error>;
    async fn update(&self, entry: Cluster) -> result::Result<i64, Error>;
    async fn remove(&self, id: i64) -> result::Result<i64, Error>;
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

        let meta: HashMap<String, String> = match row.r_by_name::<Map>(&"meta") {
            Ok(m) => m.as_r_type().unwrap(),
            Err(_) => HashMap::new(),
        };

        let created_at = row.r_by_name::<DateTime<Utc>>(&"created_at").unwrap();
        let updated_at = row.r_by_name::<DateTime<Utc>>(&"updated_at").unwrap();

        Cluster::init(id, kind, name, meta, created_at, updated_at)
    }
}

#[async_trait]
impl ClusterStore for CdrsClusterStore {
    async fn list(&self) -> Result<Vec<Cluster>, Error> {
        let stmt = "SELECT * FROM adm.clusters LIMIT 100;";
        let rows = self.session.query(stmt).await;
        let rows = self.parse(rows)?;
        let clusters = rows.iter().map(|r| self.map(r)).collect::<Vec<_>>();

        Ok(clusters)
    }

    async fn get(&self, id: i64) -> result::Result<Option<Cluster>, Error> {
        let stmt = "SELECT * FROM adm.clusters WHERE id = ?;";
        let values = query_values!(id);
        let rows = self.session.query_with_values(stmt, values).await;
        let rows = self.parse(rows)?;

        if rows.len() == 0 {
            return Ok(None);
        }

        Ok(Some(self.map(rows.get(0).unwrap())))
    }

    async fn insert(&self, c: Cluster) -> result::Result<i64, Error> {
        let stmt = "
            INSERT INTO adm.clusters (id, kind, name, meta, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?);";

        let mut c = c.clone();
        c.id = self.generator.next_id().unwrap();

        let values = query_values!(
            c.id,
            c.kind as i32,
            c.name,
            c.meta,
            c.created_at,
            c.updated_at
        );

        let result = self.session.query_with_values(stmt, values).await;
        if result.is_ok() {
            return Ok(c.id);
        }

        Err(result.unwrap_err())
    }

    async fn update(&self, c: Cluster) -> result::Result<i64, Error> {
        let stmt = "
			UPDATE adm.clusters
			SET name = ?, meta = ?, updated_at = ?
            WHERE id = ?;";

        let values = query_values!(c.name, c.meta, c.updated_at, c.id);
        let result = self.session.query_with_values(stmt, values).await;

        if result.is_ok() {
            return Ok(c.id);
        }

        Err(result.unwrap_err())
    }

    async fn remove(&self, id: i64) -> result::Result<i64, Error> {
        let stmt = "DELETE FROM adm.clusters WHERE id = ?;";
        let values = query_values!(id);
        let result = self.session.query_with_values(stmt, values).await;

        if result.is_ok() {
            return Ok(id);
        }

        Err(result.unwrap_err())
    }
}
