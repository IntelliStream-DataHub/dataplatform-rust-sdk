use crate::relations::PyEdgeProxy;
use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::Resource;
use dataplatform_rust_sdk::datahub::to_snake_lower_cased_allow_start_with_digits;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::EdgeProxy;
use pyo3::exceptions::PyValueError;
use pyo3::{FromPyObject, PyResult, pyclass, pymethods};
use std::collections::HashMap;

pub mod async_service;
pub mod sync_service;

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
}

impl From<Resource> for PyResource {
    fn from(ts: Resource) -> Self {
        Self { inner: ts }
    }
}
impl From<PyResource> for Resource {
    fn from(ts: PyResource) -> Self {
        ts.inner
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
    relations=None,
    geolocation=None))]
    pub fn new(
        // todo implement a smooth way to convert "datahub entities" to id-collections
        name: Option<String>,
        external_id: Option<String>,
        id: Option<u64>,
        metadata: Option<crate::string_map::StringMap>,
        description: Option<String>,
        is_root: bool,
        data_set_id: Option<u64>,
        source: Option<String>,
        labels: Option<Vec<String>>,
        relations: Option<Vec<PyEdgeProxy>>,
        geolocation: Option<HashMap<String, f64>>, // todo implement GEOJSON, not prio atm
    ) -> PyResult<Self> {
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
                metadata: metadata.map(Into::into),
                description,
                is_root,
                data_set_id,
                source,
                labels,
                relations: relations
                    .map(|v| v.into_iter().map(EdgeProxy::from).collect()),
                geolocation,
                created_time: None,
                last_updated_time: None,
                relations_form: None,
            },
        })
    }
    #[getter]
    pub fn name(&self) -> &str {
        self.inner.name.as_str()
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        self.inner.external_id.as_str()
    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[getter]
    pub fn is_root(&self) -> bool {
        self.inner.is_root
    }
    #[getter]
    pub fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }
    #[getter]
    pub fn labels(&self) -> Option<&Vec<String>> {
        self.inner.labels.as_ref()
    }
    #[getter]
    pub fn relations(&self) -> Option<Vec<PyEdgeProxy>> {
        self.inner
            .relations
            .as_ref()
            .map(|v| v.iter().cloned().map(PyEdgeProxy::from).collect())
    }
    #[getter]
    pub fn geolocation(&self) -> Option<&HashMap<String, f64>> {
        self.inner.geolocation.as_ref()
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
