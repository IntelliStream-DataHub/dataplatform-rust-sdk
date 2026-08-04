use crate::relations::{PyEdgeProxy, PyRelatedNode};
use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::datahub::to_snake_lower_cased_allow_start_with_digits;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::RelatedNode;
use dataplatform_rust_sdk::resources::RelatedResourcesForm;
use dataplatform_rust_sdk::{ApiService, Resource};
use geojson::Geometry;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyAny;
use pyo3::{Bound, FromPyObject, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::{depythonize, pythonize};
use std::collections::HashMap;
use std::sync::Arc;

/// Convert a Python object (a GeoJSON geometry `dict`) into a [`Geometry`], surfacing
/// conversion failures as `ValueError`.
fn geometry_from_py(obj: Bound<'_, PyAny>) -> PyResult<Geometry> {
    depythonize(&obj).map_err(|e| PyValueError::new_err(format!("invalid geolocation: {e}")))
}

pub mod async_service;
pub mod sync_service;

use crate::{PyFieldStr, PyFieldU64, PyListFieldStr, PyMapField};
use dataplatform_rust_sdk::resources::{ResourceUpdate, ResourceUpdateFields};

/// One resource's update for `resources.update`. Target the resource by a `Resource`, its numeric
/// id, or its external id; every field is optional and uses the same wrappers as elsewhere
/// (`FieldStr` for scalars, `ListFieldStr` for labels, `MapField` for metadata). Mirrors
/// `TimeSeriesUpdate`.
#[pyclass(module = "datahub_sdk", name = "ResourceUpdate")]
#[derive(Clone)]
pub struct PyResourceUpdate {
    pub inner: ResourceUpdate,
}

impl From<PyResourceUpdate> for ResourceUpdate {
    fn from(v: PyResourceUpdate) -> Self {
        v.inner
    }
}

#[pymethods]
impl PyResourceUpdate {
    #[new]
    #[pyo3(signature = (
        resource,
        external_id = None,
        name = None,
        description = None,
        data_set_id = None,
        metadata = None,
        source = None,
        labels = None,
    ))]
    pub fn __init__(
        resource: ResourceIdentifiable,
        external_id: Option<PyFieldStr>,
        name: Option<PyFieldStr>,
        description: Option<PyFieldStr>,
        data_set_id: Option<PyFieldU64>,
        metadata: Option<PyMapField>,
        source: Option<PyFieldStr>,
        labels: Option<PyListFieldStr>,
    ) -> Self {
        let ident = IdAndExtId::from(resource);
        Self {
            inner: ResourceUpdate {
                id: ident.id,
                external_id: ident.external_id,
                update: ResourceUpdateFields {
                    external_id: external_id.map(Into::into),
                    name: name.map(Into::into),
                    description: description.map(Into::into),
                    data_set_id: data_set_id.map(Into::into),
                    metadata: metadata.map(Into::into),
                    source: source.map(Into::into),
                    labels: labels.map(Into::into),
                },
            },
        }
    }

    #[getter]
    fn target_id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    fn target_external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
    #[getter]
    fn labels(&self) -> Option<PyListFieldStr> {
        self.inner.update.labels.clone().map(PyListFieldStr::from)
    }
}

/// Things accepted as a resource identifier when fetching by_ids or deleting.
/// Mirrors the `FunctionIdentifyable` pattern so callers can pass a `Resource`,
/// an external id string, or a numeric id directly.
#[derive(Clone, FromPyObject)]
pub enum ResourceIdentifiable {
    Resource(PyResource),
    ExternalId(String),
    Id(u64),
}

impl From<ResourceIdentifiable> for IdAndExtId {
    fn from(value: ResourceIdentifiable) -> Self {
        match value {
            ResourceIdentifiable::Resource(r) => Self {
                id: r.inner.id,
                external_id: Some(r.inner.external_id.clone()),
            },
            ResourceIdentifiable::ExternalId(ext) => Self {
                id: None,
                external_id: Some(ext),
            },
            ResourceIdentifiable::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}

#[pyclass(module = "datahub_sdk", name = "Resource", from_py_object)]
#[derive(Clone)]
pub struct PyResource {
    pub inner: Resource,
    /// The client this object was returned by, enabling navigation methods
    /// (`related`). `None` on locally-constructed resources — navigation then raises.
    pub client: Option<Arc<ApiService>>,
}

impl From<Resource> for PyResource {
    fn from(ts: Resource) -> Self {
        Self {
            inner: ts,
            client: None,
        }
    }
}
impl From<PyResource> for Resource {
    fn from(ts: PyResource) -> Self {
        ts.inner
    }
}

impl PyResource {
    /// Wrap a resource returned by the API, stamping the client so navigation methods work.
    pub fn with_client(inner: Resource, client: Arc<ApiService>) -> Self {
        Self {
            inner,
            client: Some(client),
        }
    }

    /// Build the graph-traversal form for this resource, identified by id (preferred) and
    /// external id.
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
impl PyResource {
    #[new]
    #[pyo3(signature=(
    name=None,
    external_id=None,
    id=None,
    metadata=None,
    description=None,
    is_root=false,
    data_set_id=None,
    source=None,
    labels=None,
    related_resources=None,
    geolocation=None))]
    pub fn new(
        // todo implement a smooth way to convert "datahub entities" to id-collections
        name: Option<String>,
        external_id: Option<String>,
        id: Option<u64>,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        is_root: bool,
        data_set_id: Option<u64>,
        source: Option<String>,
        labels: Option<Vec<String>>,
        related_resources: Option<Vec<PyRelatedNode>>,
        geolocation: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let geolocation = geolocation.map(geometry_from_py).transpose()?;
        let (final_name, final_ext_id) = match (name, external_id) {
            (Some(name), Some(external_id)) => (name, external_id),
            (None, Some(external_id)) => (external_id.clone(), external_id),
            (Some(name), None) => (
                name.clone(),
                to_snake_lower_cased_allow_start_with_digits(&name),
            ),
            (None, None) => {
                return Err(PyValueError::new_err(
                    "name or external_id must be provided",
                ));
            }
        };
        Ok(Self {
            inner: Resource {
                name: final_name,
                external_id: final_ext_id,
                id,
                metadata,
                description,
                is_root,
                data_set_id,
                source,
                labels,
                related_resources: related_resources
                    .map(|v| v.into_iter().map(RelatedNode::from).collect())
                    .unwrap_or_default(),
                geolocation,
                created_time: None,
                last_updated_time: None,
            },
            client: None,
        })
    }
    #[getter]
    pub fn name(&self) -> &str {
        self.inner.name.as_str()
    }
    #[setter]
    pub fn set_name(&mut self, value: String) {
        self.inner.name = value;
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        self.inner.external_id.as_str()
    }
    #[setter]
    pub fn set_external_id(&mut self, value: String) {
        self.inner.external_id = value;
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
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }
    #[setter]
    pub fn set_metadata(&mut self, value: Option<HashMap<String, String>>) {
        self.inner.metadata = value;
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
    pub fn is_root(&self) -> bool {
        self.inner.is_root
    }
    #[setter]
    pub fn set_is_root(&mut self, value: bool) {
        self.inner.is_root = value;
    }
    #[getter]
    pub fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
    #[setter]
    pub fn set_data_set_id(&mut self, value: Option<u64>) {
        self.inner.data_set_id = value;
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
    pub fn labels(&self) -> Option<&Vec<String>> {
        self.inner.labels.as_ref()
    }
    #[setter]
    pub fn set_labels(&mut self, value: Option<Vec<String>>) {
        self.inner.labels = value;
    }
    #[getter]
    pub fn related_resources(&self) -> Vec<PyRelatedNode> {
        self.inner
            .related_resources
            .iter()
            .cloned()
            .map(PyRelatedNode::from)
            .collect()
    }
    #[setter]
    pub fn set_related_resources(&mut self, value: Option<Vec<PyRelatedNode>>) {
        self.inner.related_resources = value
            .map(|v| v.into_iter().map(RelatedNode::from).collect())
            .unwrap_or_default();
    }
    /// The GeoJSON geometry as a Python `dict` (e.g.
    /// `{"type": "Point", "coordinates": [10.75, 59.91]}`), or `None`.
    #[getter]
    pub fn geolocation<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match &self.inner.geolocation {
            Some(geom) => Ok(Some(pythonize(py, geom).map_err(|e| {
                PyValueError::new_err(format!("could not serialize geolocation: {e}"))
            })?)),
            None => Ok(None),
        }
    }
    #[setter]
    pub fn set_geolocation(&mut self, value: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        self.inner.geolocation = value.map(geometry_from_py).transpose()?;
        Ok(())
    }
    #[getter]
    pub fn created_time(&self) -> Option<DateTime<Utc>> {
        self.inner.created_time
    }
    #[getter]
    pub fn last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.inner.last_updated_time
    }
}

// The label shape returned inside a `ResourceNetwork` is the unified `Label` entity from the
// labels module (`crate::labels::PyLabel`); the graph DTO is widened into it via `From`.
use crate::labels::PyLabel;

/// Result of a graph traversal (`resources.fetch_related(...)`): the connected
/// sub-graph of `nodes`, the `edges` between them, and their `labels`.
#[pyclass(module = "datahub_sdk", name = "ResourceNetwork")]
#[derive(Clone)]
pub struct PyResourceNetwork {
    pub nodes: Vec<PyResource>,
    pub edges: Vec<PyEdgeProxy>,
    pub labels: Vec<PyLabel>,
}

impl PyResourceNetwork {
    /// Build the Python view of a graph traversal, stamping `client` onto every node so
    /// callers can chain navigation off the returned resources.
    pub fn from_network(
        network: dataplatform_rust_sdk::resources::ResourceNetwork,
        client: Arc<ApiService>,
    ) -> Self {
        Self {
            nodes: network
                .nodes
                .into_iter()
                .map(|r| PyResource::with_client(r, client.clone()))
                .collect(),
            edges: network.edges.into_iter().map(PyEdgeProxy::from).collect(),
            labels: network.labels.into_iter().map(PyLabel::from).collect(),
        }
    }
}

#[pymethods]
impl PyResourceNetwork {
    #[getter]
    fn nodes(&self) -> Vec<PyResource> {
        self.nodes.clone()
    }
    #[getter]
    fn edges(&self) -> Vec<PyEdgeProxy> {
        self.edges.clone()
    }
    #[getter]
    fn labels(&self) -> Vec<PyLabel> {
        self.labels.clone()
    }
}

/// Object-level graph navigation. Available only on resources returned by the API (which carry
/// a client); calling these on a locally-constructed `Resource` raises a clear error.
#[pymethods]
impl PyResource {
    /// Walk the graph from this resource and return the connected sub-graph (its `nodes`, the
    /// `edges` between them, and their `labels`). `depth` bounds the traversal in hops
    /// (`-1`, the default, = the whole connected component); `relationship_types` filters which
    /// edge types to follow (`None` = all); `limit` caps the node count. Blocking; see
    /// [`related_async`] for the awaitable variant.
    #[pyo3(signature = (depth=-1, relationship_types=None, limit=5000))]
    fn related(
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

    /// Awaitable variant of [`related`].
    #[pyo3(signature = (depth=-1, relationship_types=None, limit=5000))]
    fn related_async<'py>(
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
