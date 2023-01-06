use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod consumer;
pub mod service;

#[derive(PartialEq, Serialize, Deserialize, Debug, Clone)]
pub struct StreamsMessage {
    pub payload: Option<String>,
    pub headers: HashMap<String, String>,
}
