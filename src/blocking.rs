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

use crate::datahub::DataHubConfig;
use crate::datasets::{Dataset, DatasetFilter, DatasetSearch};
use crate::events::{Event, EventIdCollection};
use crate::files::FileUpload;
use crate::filters::EventFilter;
use crate::functions::Function;
use crate::generic::{
    DataWrapper, Datapoint, DatapointString, DatapointsCollection, DeleteFilter, INode,
    IdAndExtId, RetrieveFilter, SearchAndFilterForm,
};
use crate::graph_data_wrapper::GraphDataWrapper;
use crate::http::ResponseError;
use crate::labels::Label;
use crate::relations::RelForm;
use crate::resources::{RelatedResourcesForm, Resource, ResourceNetwork};
use crate::timeseries::{TimeSeries, TimeSeriesUpdateCollection};
use crate::unit::Unit;

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

/// Like [`delegate!`], for the SDK's recurring generic shape
/// `fn name<I>(&self, arg: &I) where for<'a> &'a I: Into<DataWrapper<T>>`.
macro_rules! delegate_into {
    ($field:ident => $(
        $(#[$meta:meta])*
        fn $name:ident ( $arg:ident : Into<DataWrapper<$into_ty:ty>> ) -> $ret:ty;
    )*) => {
        $(
            $(#[$meta])*
            pub fn $name<I>(&self, $arg: &I) -> $ret
            where
                for<'a> &'a I: Into<DataWrapper<$into_ty>>,
            {
                self.rt.block_on(self.api.$field.$name($arg))
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
    pub events: EventsService,
    pub datasets: DatasetsService,
    pub units: UnitsService,
    pub files: FileService,
    pub functions: FunctionsService,
    pub labels: LabelsService,
}

/// The blocking counterpart of [`crate::create_api_service`]: configuration from the
/// environment (and a `.env` file, if present).
pub fn create_api_service() -> ApiService {
    ApiService::wrap(crate::create_api_service())
}

impl ApiService {
    /// The blocking counterpart of [`crate::ApiService::new`].
    pub fn new(config: DataHubConfig) -> ApiService {
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
        macro_rules! service {
            ($name:ident) => {
                $name {
                    api: api.clone(),
                    rt: rt.clone(),
                }
            };
        }
        ApiService {
            time_series: service!(TimeSeriesService),
            resources: service!(ResourceService),
            events: service!(EventsService),
            datasets: service!(DatasetsService),
            units: service!(UnitsService),
            files: service!(FileService),
            functions: service!(FunctionsService),
            labels: service!(LabelsService),
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

    // These two return GraphDataWrapper, not DataWrapper; delegated by hand.

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

/// Blocking counterpart of [`crate::EventsService`].
pub struct EventsService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl EventsService {
    delegate! { events =>
        fn filter(filter: &EventFilter) -> Result<DataWrapper<Event>, ResponseError>;
    }

    delegate_into! { events =>
        fn create(data: Into<DataWrapper<Event>>) -> Result<DataWrapper<Event>, ResponseError>;
        fn delete(json: Into<DataWrapper<EventIdCollection>>) -> Result<DataWrapper<Event>, ResponseError>;
        fn by_ids(id_collection: Into<DataWrapper<EventIdCollection>>) -> Result<DataWrapper<Event>, ResponseError>;
    }

    /// Already synchronous on the async service; passed through directly.
    pub fn buffered_count(&self) -> u64 {
        self.api.events.buffered_count()
    }
}

/// Blocking counterpart of [`crate::datasets::DatasetsService`].
/// (`list`, `update` and `policies` are unimplemented on the async service and so
/// have no blocking mirror yet.)
pub struct DatasetsService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl DatasetsService {
    delegate! { datasets =>
        fn filter(filter: &DatasetFilter) -> Result<DataWrapper<Dataset>, ResponseError>;
        fn search(search: &DatasetSearch) -> Result<DataWrapper<Dataset>, ResponseError>;
    }

    delegate_into! { datasets =>
        fn create(data: Into<DataWrapper<Dataset>>) -> Result<DataWrapper<Dataset>, ResponseError>;
        fn delete(json: Into<DataWrapper<IdAndExtId>>) -> Result<DataWrapper<Dataset>, ResponseError>;
        fn by_ids(id_collection: Into<DataWrapper<IdAndExtId>>) -> Result<DataWrapper<Dataset>, ResponseError>;
    }
}

/// Blocking counterpart of [`crate::UnitsService`].
pub struct UnitsService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl UnitsService {
    delegate! { units =>
        fn list() -> Result<DataWrapper<Unit>, ResponseError>;
        fn by_external_id(value: &str) -> Result<DataWrapper<Unit>, ResponseError>;
        fn by_ids(json: &DataWrapper<IdAndExtId>) -> Result<DataWrapper<Unit>, ResponseError>;
    }
}

/// Blocking counterpart of [`crate::FileService`].
pub struct FileService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl FileService {
    delegate! { files =>
        fn upload_file(file_upload: FileUpload) -> Result<DataWrapper<INode>, ResponseError>;
        fn list_root_directory() -> Result<DataWrapper<INode>, ResponseError>;
        fn list_directory_by_path(path: &str) -> Result<DataWrapper<INode>, ResponseError>;
        fn delete(id_collection: &DataWrapper<IdAndExtId>) -> Result<DataWrapper<Event>, ResponseError>;
    }
}

/// Blocking counterpart of [`crate::functions::FunctionsService`].
pub struct FunctionsService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl FunctionsService {
    delegate! { functions =>
        fn list() -> Result<DataWrapper<Function>, ResponseError>;
        fn by_ids(ids: &[IdAndExtId]) -> Result<DataWrapper<Function>, ResponseError>;
        fn by_external_id(external_id: &str) -> Result<Function, ResponseError>;
    }

    delegate_into! { functions =>
        fn create(data: Into<DataWrapper<Function>>) -> Result<DataWrapper<Function>, ResponseError>;
        fn delete(json: Into<DataWrapper<IdAndExtId>>) -> Result<DataWrapper<Function>, ResponseError>;
    }
}

/// Blocking counterpart of [`crate::labels::LabelsService`].
pub struct LabelsService {
    api: Arc<crate::ApiService>,
    rt: Arc<Runtime>,
}

impl LabelsService {
    delegate! { labels =>
        fn list() -> Result<DataWrapper<Label>, ResponseError>;
        fn get(id: u64) -> Result<DataWrapper<Label>, ResponseError>;
    }

    delegate_into! { labels =>
        fn create(data: Into<DataWrapper<Label>>) -> Result<DataWrapper<Label>, ResponseError>;
        fn update(data: Into<DataWrapper<Label>>) -> Result<DataWrapper<Label>, ResponseError>;
        fn delete(json: Into<DataWrapper<IdAndExtId>>) -> Result<DataWrapper<Label>, ResponseError>;
    }
}
