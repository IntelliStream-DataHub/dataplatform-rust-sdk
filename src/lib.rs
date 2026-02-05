use std::rc::{Rc, Weak};
use reqwest::{ClientBuilder};
use reqwest::Client;
use dotenv::dotenv;

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
use crate::datahub::DataHubApi;
use crate::events::EventsService;
use crate::files::FileService;
use crate::resources::ResourceService;
use crate::timeseries::{TimeSeriesService};
use crate::unit::{UnitsService};

mod unit;
mod generic;
mod timeseries;
mod datahub;
mod fields;
mod events;
mod http;
mod files;
mod filters;
mod serde_helper;
mod errors;
mod resources;
mod graph_data_wrapper;
#[cfg(test)]
mod tests;

pub struct ApiService{
    config: Box<DataHubApi>,
    pub time_series: TimeSeriesService,
    pub units: UnitsService,
    pub events: EventsService,
    pub resources: ResourceService,
    pub files: FileService,
    http_client: Client,
}

pub fn create_api_service() -> Rc<ApiService> {
    dotenv().ok(); // Reads the .env file
    let dataplatform_api:DataHubApi /* Type */ = DataHubApi::create_default();
    let mut headers = HeaderMap::new();
    //if let Some(token) = dataplatform_api.get_api_token().await{
    //    let auth_header =format!("Bearer {token}");
    //headers.insert(AUTHORIZATION, HeaderValue::from_str(auth_header.as_str()).unwrap());
    //};
    headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers.insert(ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let http_client = ClientBuilder::new().default_headers(headers).build().unwrap();
    let boxed_config = Box::new(dataplatform_api.clone());
    // Clone the base_url before moving boxed_config into ApiService
    let base_url_clone = boxed_config.base_url.clone();

    let api_service = Rc::new_cyclic(|weak_self| {
        ApiService {
            config: boxed_config,
            time_series: TimeSeriesService::new(Weak::clone(weak_self), &base_url_clone), // Initialize any other services here
            units: UnitsService::new ( Weak::clone(weak_self), &base_url_clone ), // Pass the Weak reference
            events: EventsService::new ( Weak::clone(weak_self), &base_url_clone ),
            resources: ResourceService::new (Weak::clone(weak_self), base_url_clone.clone() ),
            files: FileService::new ( Weak::clone(weak_self), &base_url_clone ),
            http_client,
        }
    });

    api_service

}

