use dotenv::dotenv;
use reqwest::Client;
use reqwest::ClientBuilder;
use std::sync::{Arc, Weak};

use crate::datahub::DataHubConfig;
pub use crate::events::EventsService;
pub use crate::files::{FileService, FileUpload};
pub use crate::resources::ResourceService;
pub use crate::timeseries::TimeSeriesService;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
pub use unit::{Unit, UnitsService};
pub use crate::subscriptions::SubscriptionsService;

#[cfg(feature = "blocking")]
pub mod blocking;
pub mod buffer;
#[cfg(test)]
mod buffer_integration;
pub mod datahub;
pub mod datasets;
pub mod errors;
pub mod events;
pub mod fields;
pub mod files;
pub mod filters;
pub mod generic;
pub mod graph_data_wrapper;
pub mod http;
pub mod relations;
pub mod resources;
pub mod serde_helper;
pub mod subscriptions;
#[cfg(test)]
pub mod tests;
pub mod timeseries;
pub mod unit;
pub mod functions;

pub use resources::*;
pub use events::*;
pub use timeseries::*;
pub use relations::{EdgeProxy, RelForm};
use crate::datasets::*;

pub use subscriptions::{
    DataCollectionString, DataSort, DataWrapperMessage, EventAction, EventObject, ListenError,
    Subscription, SubscriptionFilter, SubscriptionListener, SubscriptionMessage,
    SubscriptionRetriever, WsDatapoint,
};
use crate::functions::FunctionsService;
//pub use filters::Filter;

pub struct ApiService {
    config: Box<DataHubConfig>,
    pub time_series: TimeSeriesService,
    pub units: UnitsService,
    pub events: EventsService,
    pub resources: ResourceService,
    pub datasets: DatasetsService,
    pub files: FileService,
    pub subscriptions: SubscriptionsService,
    pub functions: FunctionsService,
    pub(crate) http_client: Client,
}

/// Drive a future to completion on a self-contained, single-threaded Tokio runtime.
///
/// The SDK is async inside, but binaries that only talk to DataHub shouldn't have to
/// depend on Tokio themselves. Wrap your async entry point in this instead:
///
/// ```no_run
/// fn main() {
///     dataplatform_rust_sdk::block_on(async {
///         let api = dataplatform_rust_sdk::create_api_service();
///         // .await SDK calls here
///     });
/// }
/// ```
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build the SDK's internal Tokio runtime")
        .block_on(future)
}

pub fn create_api_service() -> Arc<ApiService> {
    dotenv().ok(); // Reads the .env file
    let dataplatform_api: DataHubConfig /* Type */ = DataHubConfig::create_default();
    let mut headers = HeaderMap::new();

    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str("application/json").unwrap(),
    );
    headers.insert(ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let http_client = ClientBuilder::new()
        .default_headers(headers)
        .build()
        .unwrap();
    let boxed_config = Box::new(dataplatform_api.clone());
    // Clone the base_url before moving boxed_config into ApiService
    let base_url_clone = boxed_config.base_url.clone();

    let api_service = Arc::new_cyclic(|weak_self| {
        ApiService {
            config: boxed_config,
            time_series: TimeSeriesService::new(Weak::clone(weak_self), &base_url_clone), // Initialize any other services here
            units: UnitsService::new(Weak::clone(weak_self), &base_url_clone), // Pass the Weak reference
            events: EventsService::new(Weak::clone(weak_self), &base_url_clone),
            resources: ResourceService::new(Weak::clone(weak_self), &base_url_clone),
            datasets: DatasetsService::new(Weak::clone(weak_self), &base_url_clone),
            files: FileService::new(Weak::clone(weak_self), &base_url_clone),
            subscriptions: SubscriptionsService::new(Weak::clone(weak_self), &base_url_clone),
            functions: FunctionsService::new(Weak::clone(weak_self), &base_url_clone),
            http_client,
        }
    });
    api_service
}
impl ApiService {
    pub fn new(config: DataHubConfig) -> Arc<ApiService> {
        let mut headers = HeaderMap::new();

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );
        headers.insert(ACCEPT, HeaderValue::from_str("application/json").unwrap());

        let http_client = ClientBuilder::new()
            .default_headers(headers)
            .build()
            .unwrap();
        let boxed_config = Box::new(config);
        // Clone the base_url before moving boxed_config into ApiService
        let base_url_clone = boxed_config.base_url.clone();

        let api_service = Arc::new_cyclic(|weak_self| {
            ApiService {
                config: boxed_config,
                time_series: TimeSeriesService::new(Weak::clone(weak_self), &base_url_clone), // Initialize any other services here
                units: UnitsService::new(Weak::clone(weak_self), &base_url_clone), // Pass the Weak reference
                events: EventsService::new(Weak::clone(weak_self), &base_url_clone),
                resources: ResourceService::new(Weak::clone(weak_self), &base_url_clone),
                datasets: DatasetsService::new(Weak::clone(weak_self), &base_url_clone),
                files: FileService::new(Weak::clone(weak_self), &base_url_clone),
                subscriptions: SubscriptionsService::new(Weak::clone(weak_self), &base_url_clone),
                functions: FunctionsService::new(Weak::clone(weak_self), &base_url_clone),
                http_client,
            }
        });

        api_service
    }
    pub fn api_service_from_env() -> Arc<ApiService> {
        let dataplatform_api: DataHubConfig /* Type */ = DataHubConfig::from_env().unwrap();
        let mut headers = HeaderMap::new();

        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_str("application/json").unwrap(),
        );
        headers.insert(ACCEPT, HeaderValue::from_str("application/json").unwrap());

        let http_client = ClientBuilder::new()
            .default_headers(headers)
            .build()
            .unwrap();
        let boxed_config = Box::new(dataplatform_api.clone());
        // Clone the base_url before moving boxed_config into ApiService
        let base_url_clone = boxed_config.base_url.clone();

        let api_service = Arc::new_cyclic(|weak_self| {
            ApiService {
                config: boxed_config,
                time_series: TimeSeriesService::new(Weak::clone(weak_self), &base_url_clone), // Initialize any other services here
                units: UnitsService::new(Weak::clone(weak_self), &base_url_clone), // Pass the Weak reference
                events: EventsService::new(Weak::clone(weak_self), &base_url_clone),
                resources: ResourceService::new(Weak::clone(weak_self), &base_url_clone),
                datasets: DatasetsService::new(Weak::clone(weak_self), &base_url_clone),
                files: FileService::new(Weak::clone(weak_self), &base_url_clone),
                subscriptions: SubscriptionsService::new(Weak::clone(weak_self), &base_url_clone),
                functions: FunctionsService::new(Weak::clone(weak_self), &base_url_clone),
                http_client,
            }
        });

        api_service
    }
}
