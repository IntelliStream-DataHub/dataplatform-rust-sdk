use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::collections::HashMap;
use std::rc::{Weak};
use crate::ApiService;
use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtIdCollection};
use crate::http::{process_response, ResponseError};

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

        // Create and send an HTTP GET request
        let response = self.get_api_service()?
            .http_client
            .get(&self.base_url) // Correctly access `base_url`
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;

        // Process the HTTP response and deserialize it as `DataWrapper<UnitResponse>`
        process_response::<DataWrapper<Unit>>(response).await
    }

    pub async fn by_external_id(&self, value: &str) -> Result<DataWrapper<Unit>, ResponseError> {
        let path = format!("{}/{value}", self.base_url, value = value);

        // Create and send an HTTP GET request
        let response = self.get_api_service()?
            .http_client
            .get(&path) // Correctly access `base_url`
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;

        // Process the HTTP response and deserialize it
        process_response::<DataWrapper<Unit>>(response).await
    }

    pub async fn by_ids(&self, json: &IdAndExtIdCollection) -> Result<DataWrapper<Unit>, ResponseError> {
        let path = format!("{}/byids", self.base_url);

        // Create and send an HTTP GET request
        let path = format!("{}/byids", &self.base_url);
        let response = self.get_api_service()?
            .http_client
            .post(path) // Correctly access `base_url`
            .body(serde_json::to_string(json).unwrap())
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;

        // Process the HTTP response and deserialize it
        process_response::<DataWrapper<Unit>>(response).await
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
