//! An async Rust client for the **IntelliStream DataHub** REST API — time series and
//! datapoints, events, the resource graph, files, datasets and subscriptions.
//!
//! Everything starts at [`create_api_service()`], which reads its configuration from the
//! environment and returns an [`ApiService`]. Each API family is a field on it:
//!
//! ```no_run
//! use intellistream_datahub_sdk::create_api_service;
//!
//! # async fn run() {
//! let api = create_api_service();
//! let hits = api.time_series.search_by_query("engine").await.unwrap();
//! for ts in hits.get_items() {
//!     println!("{}", ts.external_id);
//! }
//! # }
//! ```
//!
//! # The services
//!
//! | Field | Module | Covers |
//! |---|---|---|
//! | `time_series` | [`timeseries`] | Time series CRUD, plus datapoint ingestion and retrieval |
//! | `events` | [`events`] | Event CRUD, filter and search, and the vocabulary endpoints |
//! | `resources` | [`resources`] | The generic node service — reads span every node type |
//! | `assets` | [`assets`] | The typed `/assets` family |
//! | `datasets` | [`datasets`] | Data sets, the unit access is granted on |
//! | `files` | [`files`] | Upload, download, directory listing, trash and restore |
//! | `functions` | [`functions`] | Function nodes |
//! | `edges` | [`relations`] | Relationship edges between resources, and the type catalogue |
//! | `labels` | [`labels`] | The tenant's label catalogue |
//! | `units` | [`mod@unit`] | The unit catalogue |
//! | `subscriptions` | [`subscriptions`] | Subscription CRUD and WebSocket listening |
//!
//! # Reading a response
//!
//! Collection endpoints answer `{ "items": [...] }`, which this crate models as
//! [`DataWrapper<T>`](generic::DataWrapper) — call
//! [`get_items()`](generic::DataWrapper::get_items) for the rows. Graph endpoints answer
//! [`GraphDataWrapper<T>`](graph_data_wrapper::GraphDataWrapper), which carries edges
//! alongside the nodes.
//!
//! `/resources` spans six node types and answers each row in the shape of its own kind, so
//! its reads return the [`Node`] enum rather than one flat struct. Match on it, or narrow
//! with [`as_time_series()`](nodes::Node::as_time_series),
//! [`into_asset()`](nodes::Node::into_asset) and their siblings.
//!
//! # Identifying things
//!
//! Most entities have both a server-assigned numeric `id` and a caller-supplied
//! `externalId`, and endpoints accept either. [`IdAndExtId`](generic::IdAndExtId) models
//! that choice for delete and by-id calls.
//!
//! # Errors
//!
//! Service methods return [`ResponseError`](http::ResponseError), which carries the status
//! and the raw body. When the API refused in the documented way, the body is an RFC 9457
//! problem document: reach it with
//! [`ResponseError::problem()`](http::ResponseError::problem) and branch on
//! [`ProblemDetail::slug()`](problem::ProblemDetail::slug) — never on the prose in `title`
//! or `detail`, which may be reworded at any time.
//!
//! Configuration and authentication failures surface separately, as
//! [`DataHubError`](errors::DataHubError).
//!
//! # Filtering, searching and listing
//!
//! Each collection offers three reads, and they are not interchangeable:
//!
//! - **`list(limit)`** — the criteria-free first page. No paging, ever.
//! - **`filter(form)`** — structured criteria, with `sort` and a keyset `cursor`. This is
//!   the one that pages: echo `next_cursor` back until it stops coming.
//! - **`search(form)`** — a free-text phrase, optionally narrowed by the same criteria. The
//!   phrase selects and the filter only removes; it can never widen a search.
//!
//! The [`filters`] module explains the `XFilter` (criteria) versus `XFilterForm` (request
//! body) split that all three share.
//!
//! # Configuration
//!
//! [`create_api_service()`] loads a local `.env` file and the process environment. It needs
//! `BASE_URL`, plus either `TOKEN` or the OAuth2 client-credentials set `CLIENT_ID` /
//! `CLIENT_SECRET` / `TOKEN_URI`. See [`datahub::DataHubConfig`] for the full set, including
//! the `SCOPE` a Keycloak Organizations realm requires and the RFC 7523 assertion flow.
//!
//! # Cargo features
//!
//! - **`blocking`** — a synchronous mirror of the whole API in [`blocking`], the same split
//!   as `reqwest` / `reqwest::blocking`. Every call delegates to the async implementation on
//!   a runtime the client owns, so there is one implementation of each call. It must not be
//!   constructed or used from inside an async context.
//!
//! If you only need to make a handful of calls from a synchronous `main`, [`block_on`] runs
//! a future on a self-contained runtime without taking a Tokio dependency of your own.

// `doc_cfg` is nightly-only; docs.rs builds on nightly with `--cfg docsrs` (see Cargo.toml),
// so stable builds simply skip the feature badges.
#![cfg_attr(docsrs, feature(doc_cfg))]

use dotenv::dotenv;
use reqwest::Client;
use reqwest::ClientBuilder;
use std::sync::{Arc, Weak};

use crate::datahub::DataHubConfig;
pub use crate::assets::AssetsService;
pub use crate::events::EventsService;
pub use crate::files::{FileService, FileUpload};
pub use crate::resources::ResourceService;
pub use crate::timeseries::TimeSeriesService;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, CONTENT_TYPE};
pub use unit::{Unit, UnitsService};
// Only the service is re-exported at the crate root: `resources::*` already brings a
// (different) `Label` graph DTO here, so the label entity stays addressed as `labels::Label`.
pub use crate::labels::LabelsService;
pub use crate::relations::EdgesService;
pub use crate::subscriptions::SubscriptionsService;

#[cfg(feature = "blocking")]
#[cfg_attr(docsrs, doc(cfg(feature = "blocking")))]
pub mod blocking;
pub mod assets;
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
pub mod labels;
#[cfg(test)]
mod mcp_integration;
#[cfg(test)]
mod multi_tenant_integration;
mod problem_integration;
pub mod nodes;
pub mod problem;
pub mod relations;
pub mod resources;
#[doc(hidden)] // id-as-string serde adapters; see the module doc for why they stay `pub`
pub mod serde_helper;
pub mod subscriptions;
#[cfg(test)]
pub mod tests;
pub mod timeseries;
pub mod unit;
pub mod functions;

pub use resources::*;
pub use nodes::{Asset, Node, NodeType, Policy};
pub use problem::{FieldProblem, ProblemDetail, Retry, UnknownField};
/// GeoJSON geometry type used by [`Resource::geolocation`]; re-exported so callers
/// don't need a direct dependency on the `geojson` crate.
pub use geojson::Geometry;
pub use events::*;
pub use timeseries::*;
pub use relations::{EdgeProxy, RelForm, RelatedNode, RelationDirection};
use crate::datasets::*;
pub use crate::datasets::Dataset;

pub use filters::DataSort;
pub use subscriptions::{
    DataCollectionString, DataWrapperMessage, EventAction, EventObject, ListenError,
    Subscription, SubscriptionFilter, SubscriptionListener, SubscriptionMessage,
    SubscriptionFilterForm, WsDatapoint,
};
use crate::functions::FunctionsService;
//pub use filters::Filter;

/// The client: one configured connection to a DataHub backend, with every API family on it.
///
/// Build one with [`create_api_service()`] and call through its fields — `api.events.create(…)`,
/// `api.time_series.insert_datapoints(…)`. It is held in an [`Arc`] and every service borrows
/// the same HTTP client and token cache, so clone the `Arc` freely rather than building a
/// second one; a second client means a second token cache.
///
/// One `ApiService` is one tenant. Tenant identity rides in the access token's `organization`
/// claim and there is no per-call override, so talking to two tenants means two clients, each
/// with its own [`datahub::DataHubConfig`] and scope.
pub struct ApiService {
    config: Box<DataHubConfig>,
    /// Time series and their datapoints — see [`timeseries`].
    pub time_series: TimeSeriesService,
    /// The unit catalogue — see [`mod@unit`].
    pub units: UnitsService,
    /// Events — see [`events`].
    pub events: EventsService,
    /// The generic node service, spanning every node type — see [`resources`].
    pub resources: ResourceService,
    /// The typed `/assets` family — see [`assets`].
    pub assets: AssetsService,
    /// Data sets — see [`datasets`].
    pub datasets: DatasetsService,
    /// Files — see [`files`].
    pub files: FileService,
    /// Subscriptions, including WebSocket listening — see [`subscriptions`].
    pub subscriptions: SubscriptionsService,
    /// Function nodes — see [`functions`].
    pub functions: FunctionsService,
    /// The label catalogue — see [`labels`].
    pub labels: LabelsService,
    /// Relationship edges and their type catalogue — see [`relations`].
    pub edges: EdgesService,
    pub(crate) http_client: Client,
}

/// Drive a future to completion on a self-contained, single-threaded Tokio runtime.
///
/// The SDK is async inside, but binaries that only talk to DataHub shouldn't have to
/// depend on Tokio themselves. Wrap your async entry point in this instead:
///
/// ```no_run
/// fn main() {
///     intellistream_datahub_sdk::block_on(async {
///         let api = intellistream_datahub_sdk::create_api_service();
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

/// Build an [`ApiService`] from the environment.
///
/// Loads a local `.env` file if there is one, then reads the process environment. It needs
/// `BASE_URL`, plus either `TOKEN` or the OAuth2 client-credentials trio `CLIENT_ID` /
/// `CLIENT_SECRET` / `TOKEN_URI`. [`datahub::DataHubConfig`] documents the rest, including the
/// `SCOPE` a Keycloak Organizations realm needs and the RFC 7523 assertion flow.
///
/// ```no_run
/// use intellistream_datahub_sdk::create_api_service;
///
/// # async fn run() {
/// let api = create_api_service();
/// let units = api.units.list().await.unwrap();
/// # }
/// ```
///
/// # Panics
///
/// Configuration is read eagerly, so a missing `BASE_URL` or an unusable credential set panics
/// here rather than failing on the first call. Build the [`datahub::DataHubConfig`]
/// yourself with [`DataHubConfig::from_env`](datahub::DataHubConfig::from_env), which returns a
/// `Result`, and pass it to [`ApiService::new`].
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
            assets: AssetsService::new(Weak::clone(weak_self), &base_url_clone),
            datasets: DatasetsService::new(Weak::clone(weak_self), &base_url_clone),
            files: FileService::new(Weak::clone(weak_self), &base_url_clone),
            subscriptions: SubscriptionsService::new(Weak::clone(weak_self), &base_url_clone),
            functions: FunctionsService::new(Weak::clone(weak_self), &base_url_clone),
            labels: LabelsService::new(Weak::clone(weak_self), &base_url_clone),
            edges: EdgesService::new(Weak::clone(weak_self), &base_url_clone),
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
                assets: AssetsService::new(Weak::clone(weak_self), &base_url_clone),
                datasets: DatasetsService::new(Weak::clone(weak_self), &base_url_clone),
                files: FileService::new(Weak::clone(weak_self), &base_url_clone),
                subscriptions: SubscriptionsService::new(Weak::clone(weak_self), &base_url_clone),
                functions: FunctionsService::new(Weak::clone(weak_self), &base_url_clone),
                labels: LabelsService::new(Weak::clone(weak_self), &base_url_clone),
                edges: EdgesService::new(Weak::clone(weak_self), &base_url_clone),
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
                assets: AssetsService::new(Weak::clone(weak_self), &base_url_clone),
                datasets: DatasetsService::new(Weak::clone(weak_self), &base_url_clone),
                files: FileService::new(Weak::clone(weak_self), &base_url_clone),
                subscriptions: SubscriptionsService::new(Weak::clone(weak_self), &base_url_clone),
                functions: FunctionsService::new(Weak::clone(weak_self), &base_url_clone),
                labels: LabelsService::new(Weak::clone(weak_self), &base_url_clone),
                edges: EdgesService::new(Weak::clone(weak_self), &base_url_clone),
                http_client,
            }
        });

        api_service
    }
}
