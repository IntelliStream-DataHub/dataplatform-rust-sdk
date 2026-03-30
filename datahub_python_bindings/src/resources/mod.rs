use std::collections::HashMap;
use chrono::{DateTime, Utc};
use pyo3::{pyclass, pymethods};
use dataplatform_rust_sdk::Resource;

pub mod async_service;
pub mod sync_service;
pub mod general;

#[pyclass(module="datahub_python_sdk",name="Resource",from_py_object)]
#[derive(Clone)]
pub struct PyResource{
    pub inner: Resource
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
    name,
    external_id,
    id=None,
    metadata=None,
    description=None,
    is_root=false,
    data_set_id=None,
    source=None,
    labels=None,
    relations=None,
    geolocation=None,
    created_time=None,
    last_updated_time=None))]
    pub fn new(
        // todo implement a smooth way to convert "datahub entities" to id-collections
        name: String,
        external_id: String,
        id: Option<u64>,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        is_root: bool,
        data_set_id: Option<u64>,
        source: Option<String>,
        labels: Option<Vec<String>>,
        relations: Option<Vec<String>>,
        geolocation: Option<HashMap<String, f64>>, // todo implement GEOJSON, not prio atm
        created_time: Option<DateTime<Utc>>,
        last_updated_time: Option<DateTime<Utc>>,

    ) -> Self {
        Self {
            inner: Resource{
                name,
                external_id,
                id,
                metadata,
                description,
                is_root,
                data_set_id,
                source,
                labels,
                relations,
                geolocation,
                created_time,
                last_updated_time,
                relations_form:None},
        }
    }
    #[getter]
    pub fn name(&self) -> &str{
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
    pub fn relations(&self) -> Option<&Vec<String>> {
        self.inner.relations.as_ref()
    }
    #[getter]
    pub fn geolocation(&self) -> Option<&HashMap<String, f64>> {
        self.inner.geolocation.as_ref()
    }
    #[getter]
    pub fn created_time(&self) -> Option<DateTime<Utc>>{
        self.inner.created_time
    }
    #[getter]
    pub fn last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.inner.last_updated_time
    }
}
