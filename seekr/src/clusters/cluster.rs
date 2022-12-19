use chrono::prelude::*;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[repr(i32)]
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Kind {
    Unknown,
    Kafka,
}

impl Kind {
    pub fn to_str(&self) -> &str {
        match self {
            Kind::Kafka => "KAFKA",
            _ => "UNKNOWN",
        }
    }
}

impl TryFrom<i32> for Kind {
    type Error = ();

    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            x if x == Kind::Unknown as i32 => Ok(Kind::Unknown),
            x if x == Kind::Kafka as i32 => Ok(Kind::Kafka),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Cluster {
    /// The id of cluster entry.
    pub id: i64,

    /// The kind of cluster entry.
    pub kind: Kind,

    /// Cluster name.
    pub name: String,

    /// A key/value pair collection of cluster config options.
    pub meta: HashMap<String, String>,

    /// Represents the point in time in UTC Epoch time, when the entry was created.
    pub created_at: DateTime<Utc>,

    /// Represents the point in time in UTC Epoch time, when the entry was modified.
    pub updated_at: DateTime<Utc>,
}

impl Cluster {
    pub fn new(id: Option<i64>, kind: Kind, name: String, meta: HashMap<String, String>) -> Self {
        Cluster {
            id: id.unwrap_or(0),
            kind,
            name,
            meta,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub fn init(
        id: i64,
        kind: Kind,
        name: String,
        meta: HashMap<String, String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Cluster {
            id,
            kind,
            name,
            meta,
            created_at,
            updated_at,
        }
    }
}
