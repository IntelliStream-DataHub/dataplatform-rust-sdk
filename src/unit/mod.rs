use serde::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;
use std::ops::Deref;
use std::rc::{Rc, Weak};
use reqwest::{Method, Response, StatusCode};
use crate::ApiService;
use crate::generic::IdAndExtIdCollection;

pub struct UnitsService<'a>{
    pub(crate) api_service: Weak<ApiService<'a>>,
    base_url: String
}

impl<'a> UnitsService<'a>{

    pub fn new(api_service: Weak<ApiService<'a>>) -> Self {
        let base_url = format!("{}/units", api_service.upgrade().unwrap().config.base_url.clone().deref().clone());
        UnitsService {api_service, base_url}
    }

    pub async fn list(&self) -> Option<Response<>> {
        // Attempt to upgrade the Weak reference to access ApiService
        if let Some(api_service) = self.api_service.upgrade() {
            // Create an HTTP GET request using the client in `api_service`
            let response = api_service
                .http_client
                .get(self.base_url.as_str()) // Ensure `base_url` is properly initialized
                .send()
                .await;

            // Check for a valid HTTP response
            match response {
                Ok(resp) => {
                    if resp.status() == StatusCode::OK {
                        // Access and process the response body
                        if let Ok(body) = resp.text().await {
                            println!("Response body: {}", body); // Debug output (replace with actual handling logic)
                        } else {
                            eprintln!("Failed to read response body");
                        }
                    } else {
                        eprintln!("Request failed with status: {}", resp.status());
                    }
                }
                Err(err) => {
                    eprintln!("HTTP request failed: {}", err);
                }
            }
        } else {
            eprintln!("Failed to upgrade Weak reference to ApiService");
        }

        None
    }

    pub async fn by_external_id(&self, value: &str) -> Option<Response<>> {
        const METHOD: &str = "GET";
        const PATH: &str = format!("/units/{value}", value = value).as_str();
        None
    }

    pub async fn by_ids(&self, json: &IdAndExtIdCollection) -> Option<Response<>>{
        const METHOD: &str = "POST";
        const PATH: &str = "/units/byids";
        None
    }
}

#[derive(Debug, Deserialize, Clone)]
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

#[derive(Deserialize, Debug, Clone)]
pub struct UnitResponse {
    items: Vec<Unit>,
}

impl UnitResponse {

    pub fn get_items(&self) -> Vec<Unit> {
        self.items.clone()
    }

    pub fn length(&self) -> usize {
        self.items.len()
    }

}
