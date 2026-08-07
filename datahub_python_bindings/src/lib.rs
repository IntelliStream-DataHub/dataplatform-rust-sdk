mod datasets;
mod datetime;
mod events;
mod files;
mod labels;
mod relations;
mod resources;
mod subscriptions;
pub mod timeseries;
pub mod units;
mod functions;

use crate::datasets::PyDataset;
use crate::datasets::async_service::PyDatasetsServiceAsync;
use crate::datasets::sync_service::PyDatasetsServiceSync;
use crate::files::async_service::PyFilesServiceAsync;
use crate::files::sync_service::PyFilesServiceSync;
use crate::events::PyEvent;
use crate::events::async_service::PyEventsServiceAsync;
use crate::events::sync_service::PyEventsServiceSync;
use crate::resources::PyResource;
use crate::resources::async_service::PyResourcesServiceAsync;
use crate::resources::sync_service::PyResourcesServiceSync;
use crate::labels::PyLabel;
use crate::labels::async_service::PyLabelsServiceAsync;
use crate::labels::sync_service::PyLabelsServiceSync;
use crate::subscriptions::async_service::PySubscriptionsServiceAsync;
use crate::subscriptions::sync_service::PySubscriptionsServiceSync;
use crate::timeseries::async_service::PyTimeSeriesServiceAsync;
use crate::timeseries::datapoints::PyRetrieveFilter;
use crate::timeseries::sync_service::PyTimeSeriesServiceSync;
use crate::timeseries::{PyDeleteFilter, PyTimeSeries};
use crate::units::PyUnit;
use crate::functions::async_service::PyFunctionsServiceAsync;
use crate::functions::sync_service::PyFunctionsServiceSync;
use crate::units::async_service::PyUnitServiceAsync;
use crate::units::sync_service::PyUnitServiceSync;
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::datahub::DataHubConfig;
use dataplatform_rust_sdk::fields::{Field, ListField, MapField};
use dataplatform_rust_sdk::generic::*;
use dataplatform_rust_sdk::http::ResponseError;
use dataplatform_rust_sdk::{TimeSeriesFilter, TimeSeriesFilterForm};
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyType;
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use units::*;

create_exception!(
    datahub_sdk,
    DataHubException,
    PyException,
    "Error returned by the DataHub API. Carries the HTTP `status_code` and the raw response `message`."
);

/// Convert an SDK `ResponseError` into a `DataHubException` that exposes the HTTP
/// `status_code` and `message` as attributes, so Python callers can branch on the
/// status code (e.g. `except DataHubException as e: if e.status_code == 409: ...`).
pub(crate) fn datahub_err(e: ResponseError) -> PyErr {
    Python::attach(|py| {
        let err = DataHubException::new_err(e.get_message());
        let value = err.value(py);
        let _ = value.setattr("status_code", e.get_status().as_u16());
        let _ = value.setattr("message", e.get_message());
        err
    })
}

/// Shared Tokio runtime backing the blocking navigation twins (`event.related_resources()`,
/// `resource.related()`, ...). Mirrors the Cognite Python SDK's single managed event loop:
/// one process-wide runtime, independent of which client produced the object. Must not be
/// called from inside an async context — `block_on` would panic — use the `*_async` twin there
/// (same restriction as `src/blocking.rs`).
pub(crate) fn nav_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Runtime::new().expect("failed to build navigation runtime")
    })
}

/// Error raised when a navigation method is called on an object that carries no client — i.e.
/// one constructed locally rather than returned by the API. Mirrors Cognite's
/// `CogniteMissingClientError`.
pub(crate) fn missing_client_err() -> PyErr {
    pyo3::exceptions::PyRuntimeError::new_err(
        "This object has no client attached; navigation methods only work on objects returned \
         by the API (e.g. from create/by_ids/search/filter/fetch_related), not ones constructed \
         locally.",
    )
}

/// Build a `DataHubConfig` from explicit vars and apply optional durable-buffering settings. Setting any
/// of `buffer_retention_secs` / `buffer_max_bytes` (or `enable_buffering=True`) turns buffering on;
/// unset bounds fall back to the defaults (72h / 5 GiB).
///
/// `scope` / `audience` are added to the token request only when set. An assertion source
/// (`assertion`, or the `assertion_client_id`/`assertion_client_secret`/`assertion_token_url`
/// triple) switches that request to the RFC 7523 `jwt-bearer` grant.
#[allow(clippy::too_many_arguments)]
fn build_buffered_config(
    base_url: String,
    token: Option<String>,
    token_url: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    project_name: Option<String>,
    enable_buffering: bool,
    buffer_retention_secs: Option<i64>,
    buffer_max_bytes: Option<u64>,
    buffer_dir: Option<String>,
    scope: Option<String>,
    audience: Option<String>,
    assertion: Option<String>,
    assertion_token_url: Option<String>,
    assertion_client_id: Option<String>,
    assertion_client_secret: Option<String>,
    assertion_scope: Option<String>,
    assertion_audience: Option<String>,
    assertion_grant: Option<String>,
) -> DataHubConfig {
    let mut config = DataHubConfig::from_vars(
        base_url,
        token,
        token_url,
        client_id,
        client_secret,
        project_name,
    );
    if let Some(secs) = buffer_retention_secs {
        config.set_buffer_retention_secs(secs);
    }
    if let Some(bytes) = buffer_max_bytes {
        config.set_buffer_max_bytes(bytes);
    }
    if let Some(dir) = buffer_dir {
        config.set_buffer_dir(dir);
    }
    if enable_buffering {
        config.enable_buffering();
    }
    if let Some(scope) = scope {
        config.set_scope(scope);
    }
    if let Some(audience) = audience {
        config.set_audience(audience);
    }
    if let Some(assertion) = assertion {
        config.set_assertion(assertion);
    }
    if let (Some(id), Some(secret), Some(uri)) =
        (assertion_client_id, assertion_client_secret, assertion_token_url)
    {
        config.set_assertion_credentials(id, secret, uri);
    }
    if let Some(scope) = assertion_scope {
        config.set_assertion_scope(scope);
    }
    if let Some(audience) = assertion_audience {
        config.set_assertion_audience(audience);
    }
    if let Some(grant) = assertion_grant {
        config.set_assertion_grant(grant);
    }
    config
}

#[pyclass(module = "datahub_sdk", name = "DataHubClient")]
pub struct PySyncClient {
    inner: Arc<ApiService>,
    runtime: Arc<tokio::runtime::Runtime>,
}
#[pymethods]
impl PySyncClient {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        base_url,
        token=None,
        token_url=None,
        client_id=None,
        client_secret=None,
        project_name=None,
        enable_buffering=false,
        buffer_retention_secs=None,
        buffer_max_bytes=None,
        buffer_dir=None,
        scope=None,
        audience=None,
        assertion=None,
        assertion_token_url=None,
        assertion_client_id=None,
        assertion_client_secret=None,
        assertion_scope=None,
        assertion_audience=None,
        assertion_grant=None,
    ))]
    fn new(
        base_url: String,
        token: Option<String>,
        token_url: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        project_name: Option<String>,
        enable_buffering: bool,
        buffer_retention_secs: Option<i64>,
        buffer_max_bytes: Option<u64>,
        buffer_dir: Option<String>,
        scope: Option<String>,
        audience: Option<String>,
        assertion: Option<String>,
        assertion_token_url: Option<String>,
        assertion_client_id: Option<String>,
        assertion_client_secret: Option<String>,
        assertion_scope: Option<String>,
        assertion_audience: Option<String>,
        assertion_grant: Option<String>,
    ) -> Self {
        PySyncClient {
            inner: ApiService::new(build_buffered_config(
                base_url,
                token,
                token_url,
                client_id,
                client_secret,
                project_name,
                enable_buffering,
                buffer_retention_secs,
                buffer_max_bytes,
                buffer_dir,
                scope,
                audience,
                assertion,
                assertion_token_url,
                assertion_client_id,
                assertion_client_secret,
                assertion_scope,
                assertion_audience,
                assertion_grant,
            )),
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        }
    }
    #[classmethod]
    fn from_env(py: Py<PyType>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubConfig::from_env().unwrap()),
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        })
    }
    #[classmethod]
    fn from_envfile(py: Py<PyType>, path: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubConfig::from_envfile(path).unwrap()),
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
    fn files(&self) -> PyFilesServiceSync {
        PyFilesServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
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
    #[getter]
    fn functions(&self) -> PyFunctionsServiceSync {
        PyFunctionsServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }

    #[getter]
    fn labels(&self) -> PyLabelsServiceSync {
        PyLabelsServiceSync {
            api_service: self.inner.clone(),
            runtime: self.runtime.clone(),
        }
    }

}

#[pyclass(module = "datahub_sdk", name = "AsyncDataHubClient")]
struct PyAsyncClient {
    inner: Arc<ApiService>,
}

#[pymethods]
impl PyAsyncClient {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        base_url,
        token=None,
        token_url=None,
        client_id=None,
        client_secret=None,
        project_name=None,
        enable_buffering=false,
        buffer_retention_secs=None,
        buffer_max_bytes=None,
        buffer_dir=None,
        scope=None,
        audience=None,
        assertion=None,
        assertion_token_url=None,
        assertion_client_id=None,
        assertion_client_secret=None,
        assertion_scope=None,
        assertion_audience=None,
        assertion_grant=None,
    ))]
    fn new(
        base_url: String,
        token: Option<String>,
        token_url: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        project_name: Option<String>,
        enable_buffering: bool,
        buffer_retention_secs: Option<i64>,
        buffer_max_bytes: Option<u64>,
        buffer_dir: Option<String>,
        scope: Option<String>,
        audience: Option<String>,
        assertion: Option<String>,
        assertion_token_url: Option<String>,
        assertion_client_id: Option<String>,
        assertion_client_secret: Option<String>,
        assertion_scope: Option<String>,
        assertion_audience: Option<String>,
        assertion_grant: Option<String>,
    ) -> Self {
        Self {
            inner: ApiService::new(build_buffered_config(
                base_url,
                token,
                token_url,
                client_id,
                client_secret,
                project_name,
                enable_buffering,
                buffer_retention_secs,
                buffer_max_bytes,
                buffer_dir,
                scope,
                audience,
                assertion,
                assertion_token_url,
                assertion_client_id,
                assertion_client_secret,
                assertion_scope,
                assertion_audience,
                assertion_grant,
            )),
        }
    }
    #[classmethod]
    fn from_env(py: Py<PyType>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubConfig::from_env().unwrap()),
        })
    }
    #[classmethod]
    fn from_envfile(py: Py<PyType>, path: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            inner: ApiService::new(DataHubConfig::from_envfile(path).unwrap()),
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
    fn files(&self) -> PyFilesServiceAsync {
        PyFilesServiceAsync {
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
    #[getter]
    fn functions(&self) -> PyFunctionsServiceAsync {
        PyFunctionsServiceAsync {
            api_service: self.inner.clone()
        }
    }

    #[getter]
    fn labels(&self) -> PyLabelsServiceAsync {
        PyLabelsServiceAsync {
            api_service: self.inner.clone(),
        }
    }
}

#[pyclass(module = "datahub_sdk", name = "IdCollection")]
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
    #[new]
    #[pyo3(signature=(id=None, external_id=None))]
    pub fn new(id: Option<u64>, external_id: Option<String>) -> PyResult<Self> {
        if id.is_some() || external_id.is_some() {Ok(Self {
            inner: IdAndExtId {
                id,
                external_id,
            },
        })}
        else {Err(PyException::new_err("Either id or external_id must be provided"))}

    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    pub fn external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
}

#[pyclass(module = "datahub_sdk", name = "SearchAndFilterForm")]
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
#[pymethods]
impl PySearchAndFilterForm {
    #[new]
    #[pyo3(signature = (name=None, query=None, description=None, limit=None))]
    pub fn new(
        name: Option<String>,
        query: Option<String>,
        description: Option<String>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            inner: SearchAndFilterForm {
                filter: None,
                search: Some(SearchForm {
                    name,
                    description,
                    query,
                }),
                limit,
            },
        }
    }
}

#[pyclass(module = "datahub_sdk", name = "TimeSeriesFilterForm")]
#[derive(Clone)]
pub struct PyTimeSeriesFilterForm {
    pub inner: TimeSeriesFilterForm,
}
impl From<TimeSeriesFilterForm> for PyTimeSeriesFilterForm {
    fn from(form: TimeSeriesFilterForm) -> Self {
        Self { inner: form }
    }
}
impl From<PyTimeSeriesFilterForm> for TimeSeriesFilterForm {
    fn from(value: PyTimeSeriesFilterForm) -> Self {
        value.inner
    }
}
#[pymethods]
impl PyTimeSeriesFilterForm {
    /// AND-combined criteria for `timeseries.filter`. `data_set_id` expands down the dataset
    /// hierarchy server-side (a master dataset matches its children's timeseries too);
    /// `metadata_key`/`metadata_value` work together or alone.
    #[new]
    #[pyo3(signature = (data_set_id=None, unit=None, unit_external_id=None, metadata_key=None, metadata_value=None, limit=None))]
    pub fn new(
        data_set_id: Option<u64>,
        unit: Option<String>,
        unit_external_id: Option<String>,
        metadata_key: Option<String>,
        metadata_value: Option<String>,
        limit: Option<u64>,
    ) -> Self {
        Self {
            inner: TimeSeriesFilterForm {
                filter: TimeSeriesFilter {
                    data_set_id,
                    unit,
                    unit_external_id,
                    metadata_key,
                    metadata_value,
                },
                limit,
            },
        }
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
    #[pyo3(transparent)]
    Id(u64),
    #[pyo3(transparent)]
    ExternalId(String),
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
            Identifiable::Id(id) => IdAndExtId {
                id: Some(*id),
                external_id: None,
            },
            Identifiable::ExternalId(ext) => IdAndExtId {
                id: None,
                external_id: Some(ext.clone()),
            },
        }
    }
}

#[pyclass(module = "datahub_sdk", name = "ListFieldU64")]
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
    /// Replace the whole list.
    #[classmethod]
    pub fn set(_cls: Py<PyType>, values: Vec<u64>) -> Self {
        Self(ListField::set(values))
    }
    /// Add and/or remove entries, keeping the rest. Pass `add`, `remove`, or both.
    #[classmethod]
    #[pyo3(signature=(add=None, remove=None))]
    pub fn delta(_cls: Py<PyType>, add: Option<Vec<u64>>, remove: Option<Vec<u64>>) -> Self {
        Self(ListField::delta(add, remove))
    }
}
#[pyclass(module = "datahub_sdk", name = "ListFieldStr")]
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
    /// Replace the whole list.
    #[classmethod]
    pub fn set(_cls: Py<PyType>, values: Vec<String>) -> Self {
        Self(ListField::set(values))
    }
    /// Add and/or remove entries, keeping the rest. Pass `add`, `remove`, or both.
    #[classmethod]
    #[pyo3(signature=(add=None, remove=None))]
    pub fn delta(_cls: Py<PyType>, add: Option<Vec<String>>, remove: Option<Vec<String>>) -> Self {
        Self(ListField::delta(add, remove))
    }
}

/// The related-resource list of an `EventUpdate`. Entries are `IdCollection`s, so a resource can
/// be named by id, external_id, or both; `remove` matches on whichever side is given.
#[pyclass(module = "datahub_sdk", name = "ListFieldIdCollection")]
#[derive(Clone, Debug)]
pub struct PyListFieldIdCollection(ListField<IdAndExtId>);
impl From<ListField<IdAndExtId>> for PyListFieldIdCollection {
    fn from(ts: ListField<IdAndExtId>) -> Self {
        Self(ts)
    }
}
impl From<PyListFieldIdCollection> for ListField<IdAndExtId> {
    fn from(ts: PyListFieldIdCollection) -> Self {
        ts.0
    }
}
#[pymethods]
impl PyListFieldIdCollection {
    /// Replace the whole list.
    #[classmethod]
    pub fn set(_cls: Py<PyType>, values: Vec<PyIdCollection>) -> Self {
        Self(ListField::set(
            values.into_iter().map(IdAndExtId::from).collect(),
        ))
    }
    /// Add and/or remove entries, keeping the rest. Pass `add`, `remove`, or both.
    #[classmethod]
    #[pyo3(signature=(add=None, remove=None))]
    pub fn delta(
        _cls: Py<PyType>,
        add: Option<Vec<PyIdCollection>>,
        remove: Option<Vec<PyIdCollection>>,
    ) -> Self {
        let conv = |v: Vec<PyIdCollection>| -> Vec<IdAndExtId> {
            v.into_iter().map(IdAndExtId::from).collect()
        };
        Self(ListField::delta(add.map(conv), remove.map(conv)))
    }
}

#[pyclass(module = "datahub_sdk", name = "MapField")]
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
    /// Replace all entries.
    #[classmethod]
    pub fn set(_cls: Py<PyType>, values: HashMap<String, String>) -> Self {
        Self(MapField::set(values))
    }
    /// Add and/or remove entries, keeping the rest. Pass `add`, `remove`, or both.
    #[classmethod]
    #[pyo3(signature=(add=None, remove=None))]
    pub fn delta(
        _cls: Py<PyType>,
        add: Option<HashMap<String, String>>,
        remove: Option<Vec<String>>,
    ) -> Self {
        Self(MapField::delta(add, remove))
    }
}
#[pyclass(module = "datahub_sdk", name = "FieldStr")]
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

#[pyclass(module = "datahub_sdk", name = "FieldU64")]
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

// --- Resources ---

#[pymodule]
fn datahub_sdk(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("DataHubException", m.py().get_type::<DataHubException>())?;
    m.add_class::<PyAsyncClient>()?;
    m.add_class::<PySyncClient>()?;
    m.add_class::<PyIdCollection>()?;
    m.add_class::<PyUnitServiceSync>()?;
    m.add_class::<PyUnitServiceAsync>()?;
    m.add_class::<PyUnit>()?;
    m.add_class::<PyResource>()?;
    m.add_class::<crate::resources::PyResourceUpdate>()?;
    m.add_class::<crate::resources::PyResourceNetwork>()?;
    m.add_class::<PyLabel>()?;
    m.add_class::<PyLabelsServiceSync>()?;
    m.add_class::<PyLabelsServiceAsync>()?;
    m.add_class::<PyFieldU64>()?;
    m.add_class::<PyListFieldU64>()?;
    m.add_class::<PyFieldStr>()?;
    m.add_class::<PyListFieldStr>()?;
    m.add_class::<PyListFieldIdCollection>()?;
    m.add_class::<PyMapField>()?;
    m.add_class::<PySearchAndFilterForm>()?;
    m.add_class::<PyTimeSeriesFilterForm>()?;
    timeseries::register(m)?;
    events::register(m)?;
    datasets::register(m)?;
    files::register(m)?;
    subscriptions::register(m)?;
    functions::register(m)?;
    relations::register(m)?;
    Ok(())
}
