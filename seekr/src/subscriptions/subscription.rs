use std::collections::HashMap;

use chrono::prelude::*;

// The subscription for a topic with the given name.
#[derive(Clone, Debug, PartialEq)]
pub struct Subscription {
    // Specifies the unique identifier of the subscription.
    pub id: i64,

    // Specifies the unique identifier of the subscription cluster.
    pub cluster_id: i64,

    // Specifies the topic name for the subscription.
    pub topic_name: String,

    /// A key/value pair collection of topic config options.
    pub config: HashMap<String, String>,

    /// Represents the point in time in UTC Epoch time, when the subscription was created.
    pub created_at: DateTime<Utc>,

    /// Represents the point in time in UTC Epoch time, when the subscription was modified.
    pub updated_at: DateTime<Utc>,
}

impl Subscription {
    pub fn new(
        id: Option<i64>,
        cluster_id: i64,
        topic_name: String,
        config: HashMap<String, String>,
    ) -> Self {
        Subscription {
            id: id.unwrap_or(0),
            cluster_id,
            topic_name,
            config,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    pub fn init(
        id: i64,
        cluster_id: i64,
        topic_name: String,
        config: HashMap<String, String>,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Self {
        Subscription {
            id,
            cluster_id,
            topic_name,
            config,
            created_at,
            updated_at,
        }
    }
}
