use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::collections::HashMap;
use std::rc::{Weak};
use crate::ApiService;
use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtIdCollection};
use crate::http::{ResponseError};

pub struct UnitsService<'a>{
    pub(crate) api_service: Weak<ApiService<'a>>,
    base_url: String
}

impl<'a> UnitsService<'a>{

    pub fn new(api_service: Weak<ApiService<'a>>, base_url: &String) -> Self {
        let unit_base_url = format!("{}/units", base_url);
        UnitsService {api_service, base_url: unit_base_url}
    }

    pub async fn list(&self) -> Result<DataWrapper<Unit>, ResponseError> {
        self.execute_get_request(&self.base_url).await
    }

    pub async fn by_external_id(&self, value: &str) -> Result<DataWrapper<Unit>, ResponseError> {
        let path = &format!("{}/{value}", self.base_url, value = value);
        self.execute_get_request(path).await
    }

    pub async fn by_ids(&self, json: &IdAndExtIdCollection) -> Result<DataWrapper<Unit>, ResponseError> {
        let path = &format!("{}/byids", &self.base_url);
        self.execute_post_request(path, json).await
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Unit{
    pub id: u64,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub name: String,
    #[serde(rename = "longName")]
    pub long_name: String,
    pub symbol: String,
    pub description: String,
    #[serde(rename = "aliasNames")]
    pub alias_names: Vec<String>,
    pub quantity: String,
    pub conversion: HashMap<String, f64>,
    pub source: String,
    #[serde(rename = "sourceReference")]
    pub source_reference: String
}
