pub(crate) mod async_service;
pub(crate) mod sync_service;

use crate::PyIdCollection;
use crate::events::{
    PyBasicEventFilter, PyEvent, PyEventFilter, PyEventIdCollection, PyTimeFilter,
};
use crate::resources::PyResourceNetwork;
use dataplatform_rust_sdk::filters::{BasicEventFilter, EventFilter};
use dataplatform_rust_sdk::datahub::to_snake_lower_cased_allow_start_with_digits;
use dataplatform_rust_sdk::datasets::Dataset;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::resources::RelatedResourcesForm;
use dataplatform_rust_sdk::ApiService;
use pyo3::prelude::*;
use pyo3::{Bound, PyResult, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

#[pyclass(module = "datahub_sdk", name = "Dataset", from_py_object)]
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
    fn related_events_filter(&self, limit: u64) -> EventFilter {
        let mut basic = BasicEventFilter::default();
        match self.inner.id {
            Some(id) => {
                basic.set_related_resource_ids(&[id]);
            }
            None => {
                basic.set_related_resource_external_ids(&[self.inner.external_id.as_str()]);
            }
        }
        let mut filter = EventFilter::default();
        filter.set_filter(basic);
        filter.set_limit(limit);
        filter
    }
}
