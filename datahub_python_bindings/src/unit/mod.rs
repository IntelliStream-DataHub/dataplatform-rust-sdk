use std::collections::HashMap;
use pyo3::{pyclass, pymethods};
use dataplatform_rust_sdk::{TimeSeries, Unit};

pub mod async_service;
pub mod sync_service;
pub mod general;

#[pyclass(module="datahub_python_sdk")]
#[derive(Clone)]
pub struct PyUnit{
    pub inner:  Unit
}



impl From<Unit> for PyUnit {
    fn from(ts: Unit) -> Self {
        Self { inner: ts }
    }
}
impl From<PyUnit> for Unit {
    fn from(ts: PyUnit) -> Self {
        ts.inner
    }
}

impl PyUnit {
    pub(crate) fn from_inner(inner: Unit) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyUnit {
    #[new]
    fn new(id: u64,
            external_id: String,
            name: String,
            long_name: String,
            symbol: String,
            description: String,
            alias_names: Vec<String>,
            quantity: String,
            conversion: HashMap<String, f64>,
            source: String,
            source_reference: String) -> Self {
        Self { inner: Unit{
            id,
            external_id,
            name,
            long_name,
            symbol,
            description,
            alias_names,
            quantity,
            conversion,
            source,
            source_reference
        } }
    }
}