use serde::{Deserialize, Serialize};

pub mod consumer;
pub mod manager;

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct ClusterMetadata {
    pub brokers: Vec<BrokerMetadata>,
    pub groups: Vec<GroupMetadata>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct BrokerMetadata {
    pub id: i32,
    pub host: String,
    pub port: i32,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct GroupMember {
    pub id: String,
    pub client_id: String,
    pub client_host: String,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct GroupMetadata {
    pub name: String,
    pub state: String,
    pub members: Vec<GroupMember>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub error: Option<String>,
}
