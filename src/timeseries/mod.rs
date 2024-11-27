
use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeries {
    pub id: u64,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub unit: String,
    pub description: Option<String>,
    #[serde(rename = "unitExternalId")]
    pub unit_external_id: Option<String>,
    #[serde(rename = "securityCategories")]
    pub security_categories: Option<Vec<u64>>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "valueType")]
    pub value_type: String,
    #[serde(rename = "createdTime")]
    pub created_time: Option<u64>,
    #[serde(rename = "lastUpdatedTime")]
    pub last_updated_time: Option<u64>,
    #[serde(rename = "relationsFrom")]
    pub relations_from: Vec<u64>,
    #[serde(rename = "isString")]
    pub is_string: bool,
    #[serde(rename = "isStep")]
    pub is_step: bool
}

impl TimeSeries {

}

#[derive(Deserialize, Debug, Clone)]
pub struct TimeSeriesResponse {
    items: Vec<TimeSeries>,
}

impl TimeSeriesResponse {

    pub fn get_items(&self) -> Vec<TimeSeries> {
        self.items.clone()
    }

    pub fn length(&self) -> u64 {
        self.items.len() as u64
    }

}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LimitParam {
    limit: u64,
}

impl LimitParam {

    pub fn new() -> Self {
        LimitParam { limit: 100 }
    }

    pub fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
    }

    pub fn get_limit(&self) -> u64 {
        self.limit
    }
}