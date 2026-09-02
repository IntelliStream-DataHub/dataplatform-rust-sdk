use crate::StringOrList;
pub(crate) mod async_service;
pub(crate) mod sync_service;

use crate::PyIdCollection;
use crate::events::{PyEvent, PyTimeFilter};
use crate::resources::PyResourceNetwork;
use crate::{PyFieldBool, PyFieldStr, PyListFieldStr, PyMapField};
use intellistream_datahub_sdk::filters::{EventFilter, EventFilterForm};
use intellistream_datahub_sdk::datahub::to_snake_lower_cased_allow_start_with_digits;
use intellistream_datahub_sdk::datasets::{
    DatasetFilter, Dataset, DatasetFilterForm, DatasetUpdate, DatasetUpdateFields,
};
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::resources::RelatedResourcesForm;
use intellistream_datahub_sdk::ApiService;
use pyo3::prelude::*;
use pyo3::{Bound, PyResult, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[pyclass(module = "intellistream_datahub_sdk", name = "Dataset", from_py_object)]
#[derive(Clone)]
pub struct PyDataset {
    pub inner: Dataset,
    /// The client this object was returned by, enabling navigation methods
    /// (`neighbors`). `None` on locally-constructed datasets — navigation then raises.
    pub client: Option<Arc<ApiService>>,
}

impl From<Dataset> for PyDataset {
    fn from(ts: Dataset) -> Self {
        Self {
            inner: ts,
            client: None,
        }
    }
}
impl From<PyDataset> for Dataset {
    fn from(ts: PyDataset) -> Self {
        ts.inner
    }
}

impl PyDataset {
    /// Wrap a dataset returned by the API, stamping the client so navigation methods work.
    pub fn with_client(inner: Dataset, client: Arc<ApiService>) -> Self {
        Self {
            inner,
            client: Some(client),
        }
    }

    /// Build the graph-traversal form for this dataset (the unified graph — datasets are nodes).
    fn related_form(
        &self,
        depth: i32,
        relationship_types: Option<Vec<String>>,
        limit: i32,
    ) -> RelatedResourcesForm {
        RelatedResourcesForm {
            id: self.inner.id,
            external_id: Some(self.inner.external_id.clone()),
            depth,
            relationship_types,
            limit,
            excluded_labels: vec![],
        }
    }
}
#[pymethods]
impl PyDataset {
    /// Create a datasets entity.
    ///
    /// parameters
    /// ----------
    #[new]
    #[pyo3(signature=(
        external_id,
        name=None,
        id=None,
        //@NotNull
        //@Size(min= 3, max = 256)
        //@NotNull
        //3, max = 512)
        description = None,
        policies= None,
        metadata= None,
        connected_data_sets=None
    ))]
    pub fn __init__(
        external_id: String,
        name: Option<String>,
        id: Option<u64>,
        description: Option<String>,
        policies: Option<Vec<String>>,
        metadata: Option<HashMap<String, String>>,
        connected_data_sets: Option<Vec<u64>>,
    ) -> Self {
        let name = name.unwrap_or(external_id.clone());
        PyDataset {
            inner: Dataset {
                name,
                id,
                external_id,
                description,
                policies,
                metadata: metadata.unwrap_or_default(),
                connected_data_sets: connected_data_sets.unwrap_or_default(),
                labels: None,
                source: None,
                data_set_id: None,
                related_resources: vec![],
                created_time: None,
                last_updated_time: None,
            },
            client: None,
        }
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        &self.inner.external_id
    }
    #[setter]
    pub fn set_external_id(&mut self, value: String) {
        self.inner.external_id = value;
    }
    #[getter]
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    #[setter]
    pub fn set_name(&mut self, value: String) {
        self.inner.name = value;
    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[setter]
    pub fn set_id(&mut self, value: Option<u64>) {
        self.inner.id = value;
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[setter]
    pub fn set_description(&mut self, value: Option<String>) {
        self.inner.description = value;
    }
    #[getter]
    pub fn policies(&self) -> Option<&Vec<String>> {
        self.inner.policies.as_ref()
    }
    #[setter]
    pub fn set_policies(&mut self, value: Option<Vec<String>>) {
        self.inner.policies = value;
    }
    #[getter]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.inner.metadata
    }
    #[setter]
    pub fn set_metadata(&mut self, value: HashMap<String, String>) {
        self.inner.metadata = value;
    }
    #[getter]
    pub fn connected_data_sets(&self) -> &Vec<u64> {
        self.inner.connected_data_sets.as_ref()
    }
    #[setter]
    pub fn set_connected_data_sets(&mut self, value: Vec<u64>) {
        self.inner.connected_data_sets = value;
    }
    /// The labels on this node, always including the intrinsic `DATASET` type-label. It is what
    /// identifies a data set in a heterogeneous `resources.filter()` result.
    #[getter]
    pub fn labels(&self) -> Option<&Vec<String>> {
        self.inner.labels.as_ref()
    }
    #[setter]
    pub fn set_labels(&mut self, value: Option<Vec<String>>) {
        self.inner.labels = value;
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }
    #[setter]
    pub fn set_source(&mut self, value: Option<String>) {
        self.inner.source = value;
    }
    #[getter]
    pub fn related_resources(&self) -> Vec<crate::relations::PyRelatedNode> {
        self.inner
            .related_resources
            .iter()
            .cloned()
            .map(crate::relations::PyRelatedNode::from)
            .collect()
    }
    /// Always `"dataset"`. Present on every node class so data-driven code can dispatch without
    /// an `isinstance` ladder.
    #[getter]
    pub fn node_type(&self) -> &'static str {
        crate::nodes::node_type_name(intellistream_datahub_sdk::nodes::NodeType::Dataset)
    }
}

#[derive(FromPyObject)]
pub enum DatasetIdentifiable {
    Dataset(PyDataset),
    IdCollection(PyIdCollection),
    ExternalId(String),
    Id(u64),
}

impl DatasetIdentifiable {
    pub fn id(&self) -> Option<u64> {
        match self {
            DatasetIdentifiable::IdCollection(id) => id.id(),
            DatasetIdentifiable::Dataset(dataset) => dataset.id(),
            DatasetIdentifiable::ExternalId(_) => None,
            DatasetIdentifiable::Id(id) => Some(id.clone()),
        }
    }
    pub fn external_id(&self) -> Option<&str> {
        // todo! decide if we want to return Option<&str> or &str would require IdAndExtId to be changed to always force external_id to be Some
        match self {
            DatasetIdentifiable::IdCollection(id) => id.external_id(),
            DatasetIdentifiable::Dataset(dataset) => Some(dataset.external_id()),
            DatasetIdentifiable::ExternalId(id) => Some(id),
            DatasetIdentifiable::Id(_) => None,
        }
    }
}
impl From<PyDataset> for DatasetIdentifiable {
    fn from(dataset: PyDataset) -> Self {
        DatasetIdentifiable::Dataset(dataset)
    }
}
impl From<PyIdCollection> for DatasetIdentifiable {
    fn from(event: PyIdCollection) -> Self {
        DatasetIdentifiable::IdCollection(event)
    }
}
impl From<DatasetIdentifiable> for IdAndExtId {
    fn from(value: DatasetIdentifiable) -> Self {
        match value {
            DatasetIdentifiable::IdCollection(id) => Self {
                id: id.id(),
                external_id: id.external_id().map(|id| id.to_string()),
            },
            DatasetIdentifiable::Dataset(event) => Self {
                id: event.id(),
                external_id: Some(event.external_id().to_string()),
            },
            DatasetIdentifiable::ExternalId(id) => Self {
                id: None,
                external_id: Some(id.to_string()),
            },
            DatasetIdentifiable::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDataset>()?;
    Ok(())
}

/// Object-level graph navigation. Available only on datasets returned by the API (which carry a
/// client); calling these on a locally-constructed `Dataset` raises a clear error.
#[pymethods]
impl PyDataset {
    /// Walk the graph from this dataset and return the connected sub-graph (its `nodes`, the
    /// `edges` between them, and their `labels`). `depth` bounds the traversal in hops
    /// (`-1`, the default, = the whole connected component); `relationship_types` filters which
    /// edge types to follow (`None` = all); `limit` caps the node count. Neighbour nodes are
    /// modelled as `Resource`. Blocking; see [`neighbors_async`] for the awaitable variant.
    #[pyo3(signature = (depth=-1, relationship_types=None, limit=5000))]
    fn neighbors(
        &self,
        py: Python<'_>,
        depth: i32,
        relationship_types: Option<Vec<String>>,
        limit: i32,
    ) -> PyResult<PyResourceNetwork> {
        let service = self.client.clone().ok_or_else(crate::missing_client_err)?;
        let form = self.related_form(depth, relationship_types, limit);
        py.detach(|| {
            let result = crate::nav_runtime()
                .block_on(service.resources.fetch_related(&form))
                .map_err(crate::datahub_err)?;
            Ok(PyResourceNetwork::from_network(result, service.clone()))
        })
    }

    /// Awaitable variant of [`neighbors`].
    #[pyo3(signature = (depth=-1, relationship_types=None, limit=5000))]
    fn neighbors_async<'py>(
        &self,
        py: Python<'py>,
        depth: i32,
        relationship_types: Option<Vec<String>>,
        limit: i32,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.client.clone().ok_or_else(crate::missing_client_err)?;
        let form = self.related_form(depth, relationship_types, limit);
        future_into_py(py, async move {
            let result = service
                .resources
                .fetch_related(&form)
                .await
                .map_err(crate::datahub_err)?;
            Ok(PyResourceNetwork::from_network(result, service.clone()))
        })
    }
}

/// Reverse lookup: the events that reference this dataset. (The other direction —
/// `Event.related_resource_nodes()` — resolves an event's resources.) Available only on
/// datasets returned by the API; calling on a locally-constructed one raises.
#[pymethods]
impl PyDataset {
    /// Fetch events whose `related_resources` include this
    /// dataset (matched by graph-node id when present, else external id), via `events.filter`.
    /// `limit` caps the results (default 100). Blocking; see [`related_events_async`].
    #[pyo3(signature = (limit=100))]
    fn related_events(&self, py: Python<'_>, limit: u64) -> PyResult<Vec<PyEvent>> {
        let service = self.client.clone().ok_or_else(crate::missing_client_err)?;
        let filter = self.related_events_filter(limit);
        py.detach(|| {
            let result = crate::nav_runtime()
                .block_on(service.events.filter(&filter))
                .map_err(crate::datahub_err)?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|e| PyEvent::with_client(e, service.clone()))
                .collect())
        })
    }

    /// Awaitable variant of [`related_events`].
    #[pyo3(signature = (limit=100))]
    fn related_events_async<'py>(&self, py: Python<'py>, limit: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.client.clone().ok_or_else(crate::missing_client_err)?;
        let filter = self.related_events_filter(limit);
        future_into_py(py, async move {
            let result = service
                .events
                .filter(&filter)
                .await
                .map_err(crate::datahub_err)?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|e| PyEvent::with_client(e, service.clone()))
                .collect::<Vec<_>>())
        })
    }
}

impl PyDataset {
    /// Build the events filter selecting events that reference this node (by id when present,
    /// else external id).
    fn related_events_filter(&self, limit: u64) -> EventFilterForm {
        let mut basic = EventFilter::default();
        match self.inner.id {
            Some(id) => {
                basic.set_related_resource_ids(&[id]);
            }
            None => {
                basic.set_related_resource_external_ids(&[self.inner.external_id.as_str()]);
            }
        }
        let mut filter = EventFilterForm::default();
        filter.set_filter(basic);
        filter.set_limit(limit);
        filter
    }
}

// --------------------------------------------------------------------------- //
// Filter, search and update forms
// --------------------------------------------------------------------------- //

/// Criteria for `datasets.filter`. Every field is optional and they AND together, so an
/// argument-free `DatasetFilter()` places no restriction at all.
///
/// An **empty** list or dict is also no restriction rather than "match nothing" — the backend
/// reads a list it was handed with nothing in it as "I had no ids to filter on".
#[pyclass(module = "intellistream_datahub_sdk", name = "DatasetFilter", from_py_object)]
#[derive(Clone)]
pub struct PyDatasetFilter {
    pub inner: DatasetFilter,
}

impl From<DatasetFilter> for PyDatasetFilter {
    fn from(f: DatasetFilter) -> Self {
        Self { inner: f }
    }
}
impl From<PyDatasetFilter> for DatasetFilter {
    fn from(f: PyDatasetFilter) -> Self {
        f.inner
    }
}

#[pymethods]
impl PyDatasetFilter {
    /// `external_id`, `name` and `source` are **pattern** lists: `*` and `%` are wildcards, `_`
    /// is literal, matching is case-insensitive, and an entry with no wildcard matches exactly. So
    /// `name=["SAP*", "Plant A"]` mixes a prefix search with one exact name, and
    /// `external_id=["sap_*"]` replaces the retired `external_id_prefix`. Entries OR within a
    /// list; the fields AND. Each also accepts a bare string.
    ///
    /// `labels` must **all** be present; names are canonicalised, so `"pump a"` finds the label
    /// stored as `PUMP_A`. `metadata` entries must all be present too, and a `None` value matches
    /// the key alone.
    ///
    /// There is no `data_set_id`: a dataset is the thing other nodes are scoped by. There is no
    /// `write_protected` or `deactivated` either — both were removed server-side as inert, so a
    /// filter carrying them looked like it was narrowing and was not.
    #[new]
    #[pyo3(signature = (
        id = None,
        external_id = None,
        name = None,
        source = None,
        labels = None,
        metadata = None,
        created_time = None,
        last_updated_time = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: Option<Vec<u64>>,
        external_id: Option<StringOrList>,
        name: Option<StringOrList>,
        source: Option<StringOrList>,
        labels: Option<StringOrList>,
        metadata: Option<HashMap<String, Option<String>>>,
        created_time: Option<PyTimeFilter>,
        last_updated_time: Option<PyTimeFilter>,
    ) -> Self {
        let mut filter = DatasetFilter::new();
        if let Some(id) = id {
            filter.set_id(id);
        }
        if let Some(external_id) = external_id {
            filter.set_external_id(external_id.into());
        }
        if let Some(name) = name {
            filter.set_name(name.into());
        }
        if let Some(source) = source {
            filter.set_source(source.into());
        }
        if let Some(labels) = labels {
            filter.set_labels(labels.into());
        }
        if let Some(metadata) = metadata {
            filter.set_metadata(metadata);
        }
        if let Some(created_time) = created_time {
            filter.set_created_time(created_time.into());
        }
        if let Some(last_updated_time) = last_updated_time {
            filter.set_last_updated_time(last_updated_time.into());
        }
        Self {
            inner: filter.build(),
        }
    }
}

/// Build the request body for `datasets.filter` from either form of its arguments.
///
/// Shared by the sync and async services so the accepted keywords cannot drift apart between them.
///
/// `limit` defaults to the server's 100 and may not exceed 10000 — above that the request is
/// rejected.
#[allow(clippy::too_many_arguments)]
pub fn dataset_filter_form(
    filter: Option<PyDatasetFilter>,
    id: Option<Vec<u64>>,
    external_id: Option<StringOrList>,
    name: Option<StringOrList>,
    source: Option<StringOrList>,
    labels: Option<StringOrList>,
    metadata: Option<HashMap<String, Option<String>>>,
    created_time: Option<PyTimeFilter>,
    last_updated_time: Option<PyTimeFilter>,
    limit: Option<u64>,
    sort_by: Option<crate::StringOrList>,
    sort_order: Option<String>,
    cursor: Option<String>,
) -> PyResult<DatasetFilterForm> {
    let any_keyword = id.is_some()
        || external_id.is_some()
        || name.is_some()
        || source.is_some()
        || labels.is_some()
        || metadata.is_some()
        || created_time.is_some()
        || last_updated_time.is_some();
    let from_keywords = PyDatasetFilter::new(
        id,
        external_id,
        name,
        source,
        labels,
        metadata,
        created_time,
        last_updated_time,
    )
    .inner;

    let mut form = DatasetFilterForm::new();
    form.set_filter(crate::resolve_filter(
        filter.map(Into::into),
        from_keywords,
        any_keyword,
    )?);
    if let Some(limit) = limit {
        form.set_limit(limit);
    }
    form.set_paging(crate::build_page_request(sort_by, sort_order, cursor));
    Ok(form.build())
}

/// A partial update for one dataset, mirroring the server's update form.
///
/// `dataset` names the target — a `Dataset`, an `IdCollection`, an external id or a numeric id.
/// Every other argument is a field wrapper and only the ones you pass are sent; anything omitted
/// is left untouched.
///
/// There is deliberately no `policies` or `connected_data_sets` here: the update endpoint does not
/// accept them, whatever a `Dataset` can carry on create.
#[pyclass(module = "intellistream_datahub_sdk", name = "DatasetUpdate", from_py_object)]
#[derive(Clone)]
pub struct PyDatasetUpdate {
    pub inner: DatasetUpdate,
}

impl From<PyDatasetUpdate> for DatasetUpdate {
    fn from(u: PyDatasetUpdate) -> Self {
        u.inner
    }
}

#[pymethods]
impl PyDatasetUpdate {
    #[new]
    #[pyo3(signature = (
        dataset,
        external_id = None,
        name = None,
        description = None,
        metadata = None,
        labels = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        dataset: DatasetIdentifiable,
        external_id: Option<PyFieldStr>,
        name: Option<PyFieldStr>,
        description: Option<PyFieldStr>,
        metadata: Option<PyMapField>,
        labels: Option<PyListFieldStr>,
    ) -> Self {
        // Target by numeric id when we have one, else by external id — the server accepts either
        // and `DatasetUpdate` carries exactly one.
        let mut update = match dataset.id() {
            Some(id) => DatasetUpdate::by_id(id),
            None => DatasetUpdate::by_external_id(dataset.external_id().unwrap_or_default()),
        };
        update.update = DatasetUpdateFields {
            external_id: external_id.map(Into::into),
            name: name.map(Into::into),
            description: description.map(Into::into),
            metadata: metadata.map(Into::into),
            labels: labels.map(Into::into),
        };
        Self { inner: update }
    }

    #[getter]
    fn target_id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    fn target_external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
}
