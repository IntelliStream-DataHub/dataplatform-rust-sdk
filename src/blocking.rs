//! Blocking (synchronous) client, mirroring the async API — the same split as
//! `reqwest` / `reqwest::blocking`.
//!
//! Enable it with the `blocking` cargo feature. Every wrapper delegates to the async
//! implementation on a dedicated Tokio runtime owned by the client, so behavior
//! (durable buffering, retries, OAuth token refresh) is identical to the async API —
//! there is exactly one implementation of each call.
//!
//! ```no_run
//! use dataplatform_rust_sdk::blocking;
//!
//! let api = blocking::create_api_service();
//! let series = api.time_series.search_by_name("engine").unwrap();
//! for ts in series.get_items() {
//!     println!("{}", ts.external_id);
//! }
//! ```
//!
//! # Panics
//!
//! Like `reqwest::blocking`, this client must not be constructed or called from inside
//! an async context: building its runtime on a Tokio runtime thread panics. Use the
//! async [`crate::ApiService`] there instead.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::runtime::Runtime;

use crate::datahub::DataHubApi;
use crate::generic::{
    DataWrapper, Datapoint, DatapointString, DatapointsCollection, DeleteFilter, IdAndExtId,
    RetrieveFilter, SearchAndFilterForm,
};
use crate::graph_data_wrapper::GraphDataWrapper;
use crate::http::ResponseError;
use crate::relations::RelForm;
use crate::resources::{RelatedResourcesForm, Resource, ResourceNetwork};
use crate::timeseries::{TimeSeries, TimeSeriesUpdateCollection};

/// Generate blocking methods that delegate to the same-named async method on one of
/// the inner [`crate::ApiService`] services. Signatures are stated once here and must
/// match the async ones; a mismatch is a compile error, so the two surfaces can't
/// drift silently. Generic methods are delegated by hand below the macro calls.
macro_rules! delegate {
    ($field:ident => $(
        $(#[$meta:meta])*
        fn $name:ident ( $($arg:ident : $ty:ty),* $(,)? ) -> $ret:ty;
    )*) => {
        $(
            $(#[$meta])*
            pub fn $name(&self, $($arg: $ty),*) -> $ret {
                self.rt.block_on(self.api.$field.$name($($arg),*))
            }
        )*
    };
}

/// The blocking counterpart of [`crate::ApiService`]. Construct it once and reuse it;
/// it owns the Tokio runtime that drives all of its services.
pub struct ApiService {
    api: Arc<crate::ApiService>,
    pub time_series: TimeSeriesService,
    pub resources: ResourceService,
}

/// The blocking counterpart of [`crate::create_api_service`]: configuration from the
/// environment (and a `.env` file, if present).
pub fn create_api_service() -> ApiService {
    ApiService::wrap(crate::create_api_service())
}

impl ApiService {
    /// The blocking counterpart of [`crate::ApiService::new`].
    pub fn new(config: DataHubApi) -> ApiService {
        Self::wrap(crate::ApiService::new(config))
    }

    /// The blocking counterpart of [`crate::ApiService::api_service_from_env`].
    pub fn from_env() -> ApiService {
        Self::wrap(crate::ApiService::api_service_from_env())
    }

    fn wrap(api: Arc<crate::ApiService>) -> ApiService {
        let rt = Arc::new(
            Runtime::new().expect("failed to build the blocking client's Tokio runtime"),
        );
        ApiService {
            time_series: TimeSeriesService {
                api: api.clone(),
                rt: rt.clone(),
            },
            resources: ResourceService {
                api: api.clone(),
                rt,
            },
            api,
        }
    }

    /// Escape hatch: the async service this client wraps, for the few places (e.g.
    /// subscriptions) that only exist on the async API.
    pub fn async_api(&self) -> Arc<crate::ApiService> {
        self.api.clone()
    }
}

/// Blocking counterpart of [`crate::TimeSeriesService`].
pub struct TimeSeriesService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl TimeSeriesService {
    delegate! { time_series =>
        fn list() -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn list_with_limit(limit: Option<u64>) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn create(json: &DataWrapper<TimeSeries>) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn create_one(ts: &TimeSeries) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn create_from_list(ts_list: &Vec<TimeSeries>) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn delete(json: &DataWrapper<IdAndExtId>) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn update(json: &TimeSeriesUpdateCollection) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn by_ids(json: &DataWrapper<IdAndExtId>) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn search(form: &SearchAndFilterForm) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn search_by_name(name: &str) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn search_by_query(query: &str) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn search_by_description(query: &str) -> Result<DataWrapper<TimeSeries>, ResponseError>;
        fn insert_datapoint(id: Option<u64>, external_id: Option<String>, timestamp: DateTime<Utc>, value: String) -> Result<DataWrapper<String>, ResponseError>;
        fn insert_datapoints(json: &mut DataWrapper<DatapointsCollection<DatapointString>>) -> Result<DataWrapper<String>, ResponseError>;
        fn retrieve_datapoints(json: &DataWrapper<RetrieveFilter>) -> Result<DataWrapper<DatapointsCollection<Datapoint>>, ResponseError>;
        fn delete_datapoints(json: &DataWrapper<DeleteFilter>) -> Result<DataWrapper<String>, ResponseError>;
        fn retrieve_latest_datapoint(json: &DataWrapper<IdAndExtId>) -> Result<DataWrapper<DatapointsCollection<Datapoint>>, ResponseError>;
    }

    /// Already synchronous on the async service; passed through directly.
    pub fn buffered_count(&self) -> u64 {
        self.api.time_series.buffered_count()
    }
}

/// Blocking counterpart of [`crate::ResourceService`].
pub struct ResourceService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl ResourceService {
    delegate! { resources =>
        fn create(nodes: Vec<Resource>, relations: Vec<RelForm>) -> Result<GraphDataWrapper<Resource>, ResponseError>;
        fn search(payload: &SearchAndFilterForm) -> Result<DataWrapper<Resource>, ResponseError>;
        fn fetch_related(form: &RelatedResourcesForm) -> Result<ResourceNetwork, ResponseError>;
    }

    // The generic methods don't fit the macro; delegated by hand.

    pub fn by_ids<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        self.rt.block_on(self.api.resources.by_ids(input))
    }

    pub fn delete<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        self.rt.block_on(self.api.resources.delete(input))
    }
}
