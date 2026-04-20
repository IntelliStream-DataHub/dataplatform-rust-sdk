pub(crate) mod async_service;
pub(crate) mod sync_service;

use crate::PyIdCollection;
use crate::events::{
    PyBasicEventFilter, PyEvent, PyEventFilter, PyEventIdCollection, PyTimeFilter,
};
use dataplatform_rust_sdk::datahub::to_snake_lower_cased_allow_start_with_digits;
use dataplatform_rust_sdk::datasets::Dataset;
use dataplatform_rust_sdk::generic::IdAndExtId;
use pyo3::prelude::*;
use pyo3::{Bound, PyResult, pyclass, pymethods};
use std::collections::HashMap;
use uuid::Uuid;

#[pyclass(module = "datahub_python_sdk", name = "Dataset", from_py_object)]
#[derive(Clone)]
pub struct PyDataset {
    pub inner: Dataset,
}

impl From<Dataset> for PyDataset {
    fn from(ts: Dataset) -> Self {
        Self { inner: ts }
    }
}
impl From<PyDataset> for Dataset {
    fn from(ts: PyDataset) -> Self {
        ts.inner
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
        }
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        &self.inner.external_id
    }
    #[getter]
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[getter]
    pub fn policies(&self) -> Option<&Vec<String>> {
        self.inner.policies.as_ref()
    }
    #[getter]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.inner.metadata
    }
    #[getter]
    pub fn connected_data_sets(&self) -> &Vec<u64> {
        self.inner.connected_data_sets.as_ref()
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
