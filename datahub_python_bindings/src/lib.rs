mod datasets;
mod events;
mod files;
mod resources;
mod subscriptions;
pub mod timeseries;
pub mod units;

use crate::datasets::PyDataset;
use crate::datasets::async_service::PyDatasetsServiceAsync;
use crate::datasets::sync_service::PyDatasetsServiceSync;
use crate::events::PyEvent;
use crate::events::async_service::PyEventsServiceAsync;
use crate::events::sync_service::PyEventsServiceSync;
use crate::resources::PyResource;
use crate::resources::async_service::PyResourcesServiceAsync;
use crate::resources::sync_service::PyResourcesServiceSync;
use crate::subscriptions::async_service::PySubscriptionsServiceAsync;
use crate::subscriptions::sync_service::PySubscriptionsServiceSync;
use crate::timeseries::async_service::PyTimeSeriesServiceAsync;
use crate::timeseries::datapoints::PyRetrieveFilter;
use crate::timeseries::sync_service::PyTimeSeriesServiceSync;
use crate::timeseries::{PyDeleteFilter, PyTimeSeries};
use crate::units::PyUnit;
use crate::units::async_service::PyUnitServiceAsync;
use crate::units::sync_service::PyUnitServiceSync;
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::datahub::DataHubApi;
use dataplatform_rust_sdk::fields::{Field, ListField, MapField};
use dataplatform_rust_sdk::generic::*;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use units::*;

#[pyclass(module = "datahub_python_sdk", name = "DataHubClient")]
pub struct PySyncClient {
    inner: Arc<ApiService>,
    runtime: Arc<tokio::runtime::Runtime>,
}
#[pymethods]
impl PySyncClient {
    #[new]
    fn new(
        base_url: String,
        token: Option<String>,
        auth_url: Option<String>,
        token_url: Option<String>,
        redirect_url: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        project_name: Option<String>,
    ) -> Self {
        PySyncClient {
            inner: ApiService::new(DataHubApi::from_vars(
                base_url,
                token,
                auth_url,
                token_url,
                redirect_url,
                client_id,
                client_secret,
                project_name,
            )),
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        }
    }
    #[classmethod]
    fn from_env(py: Py<PyType>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubApi::from_env().unwrap()),
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        })
    }
    #[classmethod]
    fn from_envfile(py: Py<PyType>, path: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubApi::from_envfile(path).unwrap()),
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        })
    }

    #[getter]
    fn timeseries(&self) -> PyTimeSeriesServiceSync {
        PyTimeSeriesServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }

    #[getter]
    fn units(&self) -> PyUnitServiceSync {
        PyUnitServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }

    #[getter]
    fn events(&self) -> PyEventsServiceSync {
        PyEventsServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }

    #[getter]
    fn files(&self) -> PyFileService {
        PyFileService {
            api_service: self.inner.clone(),
        }
    }

    #[getter]
    fn resources(&self) -> PyResourcesServiceSync {
        PyResourcesServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }
    #[getter]
    fn datasets(&self) -> PyDatasetsServiceSync {
        PyDatasetsServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }

    #[getter]
    fn subscriptions(&self) -> PySubscriptionsServiceSync {
        PySubscriptionsServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

#[pyclass(module = "datahub_python_sdk", name = "AsyncDataHubClient")]
struct PyAsyncClient {
    inner: Arc<ApiService>,
}

#[pymethods]
impl PyAsyncClient {
    #[new]
    fn new(
        base_url: String,
        token: Option<String>,
        auth_url: Option<String>,
        token_url: Option<String>,
        redirect_url: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        project_name: Option<String>,
    ) -> Self {
        Self {
            inner: ApiService::new(DataHubApi::from_vars(
                base_url,
                token,
                auth_url,
                token_url,
                redirect_url,
                client_id,
                client_secret,
                project_name,
            )),
        }
    }
    #[classmethod]
    fn from_env(py: Py<PyType>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubApi::from_env().unwrap()),
        })
    }
    #[classmethod]
    fn from_envfile(py: Py<PyType>, path: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubApi::from_envfile(path).unwrap()),
        })
    }
    #[getter]
    fn timeseries(&self) -> PyTimeSeriesServiceAsync {
        PyTimeSeriesServiceAsync {
            api_service: self.inner.clone(),
        }
    }

    #[getter]
    fn units(&self) -> PyUnitServiceAsync {
        PyUnitServiceAsync {
            api_service: self.inner.clone(),
        }
    }

    #[getter]
    fn events(&self) -> PyEventsServiceAsync {
        PyEventsServiceAsync {
            api_service: self.inner.clone(),
        }
    }

    #[getter]
    fn files(&self) -> PyFileService {
        PyFileService {
            api_service: self.inner.clone(),
        }
    }

    #[getter]
    fn resources(&self) -> PyResourcesServiceAsync {
        PyResourcesServiceAsync {
            api_service: self.inner.clone(),
        }
    }

    #[getter]
    fn subscriptions(&self) -> PySubscriptionsServiceAsync {
        PySubscriptionsServiceAsync {
            api_service: self.inner.clone(),
        }
    }
}

#[pyclass(module = "datahub_python_sdk", name = "IdCollection")]
#[derive(Clone)]
pub(crate) struct PyIdCollection {
    inner: IdAndExtId,
}
impl From<IdAndExtId> for PyIdCollection {
    fn from(form: IdAndExtId) -> Self {
        Self { inner: form }
    }
}
impl From<PyIdCollection> for IdAndExtId {
    fn from(value: PyIdCollection) -> Self {
        value.inner
    }
}

#[pymethods]
impl PyIdCollection {
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    pub fn external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
}

#[pyclass(module = "datahub_python_sdk", name = "SearchAndFilterForm")]
#[derive(Clone)]
pub struct PySearchAndFilterForm {
    pub inner: SearchAndFilterForm,
}
impl From<SearchAndFilterForm> for PySearchAndFilterForm {
    fn from(form: SearchAndFilterForm) -> Self {
        Self { inner: form }
    }
}
impl From<PySearchAndFilterForm> for SearchAndFilterForm {
    fn from(value: PySearchAndFilterForm) -> Self {
        value.inner
    }
}
impl PySearchAndFilterForm {
    pub fn new(inner: SearchAndFilterForm) -> Self {
        Self { inner }
    }
}

#[derive(FromPyObject)]
pub enum Identifiable {
    #[pyo3(transparent)]
    Collection(PyIdCollection),
    #[pyo3(transparent)]
    TimeSeries(PyTimeSeries),
    Resource(PyResource),
    Unit(PyUnit),
    Event(PyEvent),
}
pub trait DatahubIdentity {
    fn id_collection(&self) -> IdAndExtId;
}
impl DatahubIdentity for Identifiable {
    fn id_collection(&self) -> IdAndExtId {
        match self {
            Identifiable::Collection(c) => c.inner.clone(),
            Identifiable::TimeSeries(timeseries) => IdAndExtId {
                id: timeseries.inner.id,
                external_id: Some(timeseries.inner.external_id.clone()),
            },
            Identifiable::Resource(resource) => IdAndExtId {
                id: resource.inner.id,
                external_id: Some(resource.inner.external_id.clone()),
            },
            Identifiable::Unit(u) => IdAndExtId {
                id: Some(u.inner.id),
                external_id: Some(u.inner.external_id.clone()),
            },
            Identifiable::Event(event) => IdAndExtId {
                id: None,
                external_id: Some(event.inner.external_id.clone()),
            },
        }
    }
}

#[pyclass(module = "datahub_python_sdk", name = "ListFieldU64")]
#[derive(Clone, Debug)]
pub struct PyListFieldU64(ListField<u64>);
impl From<ListField<u64>> for PyListFieldU64 {
    fn from(ts: ListField<u64>) -> Self {
        Self(ts)
    }
}
impl From<PyListFieldU64> for ListField<u64> {
    fn from(ts: PyListFieldU64) -> Self {
        ts.0
    }
}
#[pymethods]
impl PyListFieldU64 {
    #[new]
    #[pyo3(signature=(remove=None, add=None, set=None))]
    pub fn new(
        remove: Option<Vec<u64>>,
        add: Option<Vec<u64>>,
        set: Option<Vec<u64>>,
    ) -> PyResult<Self> {
        Ok(Self(ListField::new(set, add, remove)))
    }
    #[getter]
    pub fn set(&self) -> Option<&Vec<u64>> {
        self.0.set.as_ref()
    }
    #[getter]
    pub fn add(&self) -> Option<&Vec<u64>> {
        self.0.add.as_ref()
    }
    #[getter]
    pub fn remove(&self) -> Option<&Vec<u64>> {
        self.0.remove.as_ref()
    }
}
#[pyclass(module = "datahub_python_sdk", name = "ListFieldStr")]
#[derive(Clone, Debug)]
pub struct PyListFieldStr(ListField<String>);
impl From<ListField<String>> for PyListFieldStr {
    fn from(ts: ListField<String>) -> Self {
        Self(ts)
    }
}
impl From<PyListFieldStr> for ListField<String> {
    fn from(ts: PyListFieldStr) -> Self {
        ts.0
    }
}
#[pymethods]
impl PyListFieldStr {
    #[new]
    #[pyo3(signature=(remove=None, add=None, set=None))]
    pub fn new(
        remove: Option<Vec<String>>,
        add: Option<Vec<String>>,
        set: Option<Vec<String>>,
    ) -> PyResult<Self> {
        Ok(Self(ListField::new(set, add, remove)))
    }
    #[getter]
    pub fn set(&self) -> Option<&Vec<String>> {
        self.0.set.as_ref()
    }
    #[getter]
    pub fn add(&self) -> Option<&Vec<String>> {
        self.0.add.as_ref()
    }
    #[getter]
    pub fn remove(&self) -> Option<&Vec<String>> {
        self.0.remove.as_ref()
    }
}

#[pyclass(module = "datahub_python_sdk", name = "MapField")]
#[derive(Clone, Debug)]
pub struct PyMapField(pub MapField);

impl From<MapField> for PyMapField {
    fn from(ts: MapField) -> Self {
        Self(ts)
    }
}
impl From<PyMapField> for MapField {
    fn from(ts: PyMapField) -> Self {
        ts.0
    }
}
#[pymethods]
impl PyMapField {
    #[new]
    #[pyo3(signature=(remove=None, add=None, set=None))]
    pub fn new(
        remove: Option<Vec<String>>,
        add: Option<HashMap<String, String>>,
        set: Option<HashMap<String, String>>,
    ) -> PyResult<Self> {
        let map_field = MapField::new(set, add, remove);

        Ok(Self(map_field))
    }

    #[getter]
    pub fn set(&self) -> Option<&HashMap<String, String>> {
        self.0.set.as_ref()
    }
    #[getter]
    pub fn add(&self) -> Option<&HashMap<String, String>> {
        self.0.add.as_ref()
    }
    #[getter]
    pub fn remove(&self) -> Option<&Vec<String>> {
        self.0.remove.as_ref()
    }
}
#[pyclass(module = "datahub_python_sdk", name = "FieldStr")]
#[derive(Clone, Debug)]
pub struct PyFieldStr(Field<String>);

impl From<Field<String>> for PyFieldStr {
    fn from(field: Field<String>) -> Self {
        PyFieldStr(field)
    }
}
impl From<PyFieldStr> for Field<String> {
    fn from(field: PyFieldStr) -> Self {
        field.0
    }
}
#[pymethods]
impl PyFieldStr {
    #[new]
    #[pyo3(signature=(value=None,set_null=false))]
    pub fn new(value: Option<String>, set_null: bool) -> PyResult<Self> {
        Ok(Self(Field::new(value, set_null)))
    }

    #[getter]
    pub fn value(&self) -> Option<&str> {
        self.0.set.as_deref()
    }
    #[getter]
    pub fn set_null(&self) -> bool {
        self.0.set_null
    }
}

#[pyclass(module = "datahub_python_sdk", name = "FieldU64")]
#[derive(Clone, Debug)]
pub struct PyFieldU64(Field<u64>);

impl From<Field<u64>> for PyFieldU64 {
    fn from(field: Field<u64>) -> Self {
        PyFieldU64(field)
    }
}
impl From<PyFieldU64> for Field<u64> {
    fn from(field: PyFieldU64) -> Self {
        field.0
    }
}

#[pymethods]
impl PyFieldU64 {
    #[new]
    #[pyo3(signature=(value=None,set_null=false))]
    pub fn new(value: Option<u64>, set_null: bool) -> PyResult<Self> {
        Ok(Self(Field::new(value, set_null)))
    }
    #[getter]
    pub fn value(&self) -> Option<u64> {
        self.0.set
    }
    #[getter]
    pub fn set_null(&self) -> bool {
        self.0.set_null
    }
}

// --- Files ---
#[pyclass(module = "datahub_python_sdk")]
struct PyFileService {
    api_service: Arc<ApiService>,
}

#[pymethods]
impl PyFileService {
    fn list_root_directory<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .files
                .list_root_directory()
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
            // Mapping INode might be complex, simplified for now
            Ok(format!("{:?}", result.get_items()))
        })
    }
}

// --- Resources ---

#[pymodule]
fn datahub_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyAsyncClient>()?;
    m.add_class::<PySyncClient>()?;
    m.add_class::<PyIdCollection>()?;
    m.add_class::<PyUnitServiceSync>()?;
    m.add_class::<PyUnitServiceAsync>()?;
    m.add_class::<PyUnit>()?;
    m.add_class::<PyFileService>()?;
    m.add_class::<PyResource>()?;
    m.add_class::<PyFieldU64>()?;
    m.add_class::<PyListFieldU64>()?;
    m.add_class::<PyFieldStr>()?;
    m.add_class::<PyListFieldStr>()?;
    m.add_class::<PyMapField>()?;
    timeseries::register(m)?;
    events::register(m)?;
    datasets::register(m)?;
    subscriptions::register(m)?;
    Ok(())
}
