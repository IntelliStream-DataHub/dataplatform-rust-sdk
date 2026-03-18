use pyo3::pyclass;
use dataplatform_rust_sdk::Resource;

pub mod async_service;
pub mod sync_service;


#[pyclass(module="datahub_python_sdk")]
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

impl PyResource {
    pub(crate) fn from_resource(inner: Resource) -> Self {
        Self { inner }
    }
}