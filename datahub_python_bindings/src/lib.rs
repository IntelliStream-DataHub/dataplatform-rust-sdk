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
use crate::relations::async_service::PyEdgesServiceAsync;
use crate::relations::sync_service::PyEdgesServiceSync;
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
use dataplatform_rust_sdk::filters::NodeFilter;
use dataplatform_rust_sdk::{TimeSeriesFilter, TimeSeriesFilterForm};
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
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
/// Map a "not found" into Python's `None`.
///
/// Every single-resource `GET` in the API answers an unknown id with **404** (batch `/byids`
/// endpoints instead return an empty collection). A `get()` that raises for "it isn't there" is
/// unpythonic and contradicts the `X | None` signatures these bindings already publish, so the
/// 404 is absorbed here and every other error still propagates.
pub(crate) fn none_on_404<T>(result: Result<T, ResponseError>) -> PyResult<Option<T>> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(e) if e.get_status().as_u16() == 404 => Ok(None),
        Err(e) => Err(datahub_err(e)),
    }
}

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

    #[getter]
    fn edges(&self) -> PyEdgesServiceSync {
        PyEdgesServiceSync {
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

    #[getter]
    fn edges(&self) -> PyEdgesServiceAsync {
        PyEdgesServiceAsync {
            api_service: self.inner.clone(),
        }
    }

    #[getter]
    fn datasets(&self) -> PyDatasetsServiceAsync {
        PyDatasetsServiceAsync {
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

/// One page of a filter result: the rows, plus where to continue from.
///
/// Behaves as a list — `len()`, indexing, slicing, iteration, `in`, and `==` against a plain list
/// all work — so code written before paging existed keeps working unchanged. What it adds is
/// [`next_cursor`](Self::next_cursor), which is the only way to reach the next page: the api hands
/// out an opaque cursor and does not accept one a caller assembled.
///
/// The whole paging loop is therefore:
///
/// ```python
/// page = client.timeseries.filter(TimeSeriesFilterForm(limit=100, sort_by="name"))
/// while page:
///     for ts in page:
///         ...
///     if page.next_cursor is None:
///         break
///     page = client.timeseries.filter(TimeSeriesFilterForm(
///         limit=100, sort_by="name", cursor=page.next_cursor))
/// ```
///
/// `next_cursor` is `None` on the last page. A *full* page may still be the last one — the server
/// does not count the rows twice — so a walk ends with one request that comes back empty.
///
/// It is not a `list` subclass, so `isinstance(page, list)` is `False`; use `page.items` for a real
/// list when something demands one.
#[pyclass(module = "datahub_sdk", name = "Page", sequence)]
pub struct PyPage {
    items: Py<pyo3::types::PyList>,
    next_cursor: Option<String>,
}

impl PyPage {
    /// Wrap the rows of a response together with its cursor.
    pub(crate) fn new<'py, T>(
        py: Python<'py>,
        items: Vec<T>,
        next_cursor: Option<String>,
    ) -> PyResult<Self>
    where
        T: IntoPyObject<'py>,
    {
        Ok(Self {
            items: pyo3::types::PyList::new(py, items)?.unbind(),
            next_cursor,
        })
    }
}

#[pymethods]
impl PyPage {
    /// The rows, as a plain list.
    #[getter]
    fn items(&self, py: Python<'_>) -> Py<pyo3::types::PyList> {
        self.items.clone_ref(py)
    }

    /// The cursor for the next page, or `None` when the walk is done. Send it back as the
    /// request's `cursor`, with the same sort that produced it.
    #[getter]
    fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    fn __len__(&self, py: Python<'_>) -> usize {
        self.items.bind(py).len()
    }

    // Delegated to the list rather than reimplemented, so slicing and negative indices behave
    // exactly as they would on one.
    fn __getitem__<'py>(&self, key: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
        self.items.bind(key.py()).as_any().get_item(key)
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.items.bind(py).as_any().try_iter().map(|it| it.into_any())
    }

    fn __contains__(&self, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.items.bind(item.py()).as_any().contains(item)
    }

    fn __bool__(&self, py: Python<'_>) -> bool {
        !self.items.bind(py).is_empty()
    }

    /// Equal to a plain list of the same rows, so an assertion written against the old return type
    /// still holds.
    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.items.bind(other.py()).as_any().eq(other)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let cursor = match &self.next_cursor {
            Some(cursor) => format!("'{cursor}'"),
            None => "None".to_string(),
        };
        Ok(format!("Page({}, next_cursor={cursor})", self.items.bind(py).repr()?))
    }
}

/// Turn the `sort_by` / `sort_order` / `cursor` keywords into the request's paging half.
///
/// One place for all four endpoints, because the rule that matters is the same everywhere and does
/// not fail loudly when it is wrong: a cursor must travel with the sort that produced it.
pub(crate) fn build_page_request(
    sort_by: Option<StringOrList>,
    sort_order: Option<String>,
    cursor: Option<String>,
) -> dataplatform_rust_sdk::filters::PageRequest {
    use dataplatform_rust_sdk::filters::{DataSort, PageRequest};
    let sort = sort_by.map(|property| DataSort {
        property: property.into(),
        order: sort_order,
    });
    PageRequest { sort, cursor }
}

/// A filter's pattern list, as Python may write it: one string or a list of them.
///
/// The api accepts a bare scalar wherever it declares a list, because filter fields went plural
/// while most calls still pass one value. Mirroring that here keeps `names="pump_a"` from being a
/// `TypeError` the caller has to look up. The SDK always sends the canonical list form.
#[derive(FromPyObject)]
pub(crate) enum StringOrList {
    // A `str` is itself a sequence of `str` in Python, so it has to be tried first — otherwise
    // "pump" would extract as the four patterns p, u, m, p.
    One(String),
    Many(Vec<String>),
}

impl From<StringOrList> for Vec<String> {
    fn from(value: StringOrList) -> Self {
        match value {
            StringOrList::One(single) => vec![single],
            StringOrList::Many(many) => many,
        }
    }
}

/// A data set named in a filter's `data_set_ids`: a numeric id, an external id, or an explicit
/// `IdCollection` carrying either. The three spellings all reach the wire as `{"id": ...}` /
/// `{"externalId": ...}`.
#[derive(FromPyObject)]
pub(crate) enum DataSetRef {
    Id(u64),
    ExternalId(String),
    Collection(PyIdCollection),
}

impl From<DataSetRef> for IdAndExtId {
    fn from(value: DataSetRef) -> Self {
        match value {
            DataSetRef::Id(id) => IdAndExtId::from_id(id),
            DataSetRef::ExternalId(external_id) => IdAndExtId::from_external_id(&external_id),
            DataSetRef::Collection(collection) => collection.into(),
        }
    }
}

/// Turn an optional pattern-list argument into what the filter field expects.
pub(crate) fn opt_patterns(value: Option<StringOrList>) -> Option<Vec<String>> {
    value.map(Into::into)
}

/// Turn an optional data-set argument into the reference list the filter field expects.
///
/// An empty list is preserved rather than dropped: on `data_set_ids` alone, `[]` means "narrow to
/// no data sets" and `None` means "no restriction", and those are opposite answers.
pub(crate) fn opt_data_set_refs(value: Option<Vec<DataSetRef>>) -> Option<Vec<IdAndExtId>> {
    value.map(|refs| refs.into_iter().map(Into::into).collect())
}

/// The free-text half of a `search` request. The structured half is the `filter` argument of the
/// `search` method itself, because each entity's search declares its own filter type.
#[pyclass(module = "datahub_sdk", name = "SearchAndFilterForm")]
#[derive(Clone)]
pub struct PySearchAndFilterForm {
    pub search: SearchForm,
    pub limit: Option<u64>,
}

impl PySearchAndFilterForm {
    /// Combine the free-text half with an entity-specific filter into the request body.
    pub(crate) fn into_form<F>(self, filter: Option<F>) -> SearchAndFilterForm<F> {
        SearchAndFilterForm {
            filter,
            search: Some(self.search),
            limit: self.limit,
        }
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
            search: SearchForm {
                name,
                description,
                query,
            },
            limit,
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
    /// AND-combined criteria for `timeseries.filter` and the `filter` of `timeseries.search`.
    ///
    /// `external_ids`, `names`, `sources`, `units` and `unit_external_ids` are **pattern** lists:
    /// `*` and `%` are wildcards, `_` is literal, matching is case-insensitive, and an entry with
    /// no wildcard matches exactly. Entries within a list OR together; the fields AND. Each of
    /// them also accepts a bare string.
    ///
    /// `labels` must **all** be present. `metadata` entries must all be present too, and a `None`
    /// value matches the key alone — `{"health": None}` finds anything tagged `health`, which is
    /// what the retired `metadata_key`-without-`metadata_value` used to mean.
    ///
    /// `value_types` is matched exactly (case-insensitively) against the closed catalogue
    /// `BIGINT`, `FLOAT`, `FLOAT32`, `NUMERIC`, `DECIMAL32`, `TEXT`, `MIXED`.
    ///
    /// `data_set_ids` takes numeric ids, external ids, or `IdCollection`s, and expands down the
    /// dataset hierarchy server-side, so a master dataset matches its children's timeseries too.
    /// **`None` and `[]` differ here**: `None` places no restriction, `[]` narrows to no datasets
    /// and matches nothing. Every other list is "no restriction" when empty.
    ///
    /// `sort_by` names one property — `id`, `externalId`, `name`, `source`, `description`,
    /// `createdTime`, `lastUpdatedTime` or `dataSetId` — with `sort_order` of `"asc"` or `"desc"`;
    /// `id` is always appended so the order is total. An unrecognised property falls back to the
    /// default (newest created first) rather than failing. Nulls sort last ascending, first
    /// descending.
    ///
    /// `cursor` continues a previous page: pass the `next_cursor` of that response verbatim, with
    /// the **same** sort it came from — a mismatch is a 400, not a quietly short page.
    #[new]
    #[pyo3(signature = (
        ids=None,
        external_ids=None,
        names=None,
        sources=None,
        labels=None,
        metadata=None,
        created_time=None,
        last_updated_time=None,
        data_set_ids=None,
        units=None,
        unit_external_ids=None,
        value_types=None,
        limit=None,
        sort_by=None,
        sort_order=None,
        cursor=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ids: Option<Vec<u64>>,
        external_ids: Option<StringOrList>,
        names: Option<StringOrList>,
        sources: Option<StringOrList>,
        labels: Option<StringOrList>,
        metadata: Option<HashMap<String, Option<String>>>,
        created_time: Option<crate::events::PyTimeFilter>,
        last_updated_time: Option<crate::events::PyTimeFilter>,
        data_set_ids: Option<Vec<DataSetRef>>,
        units: Option<StringOrList>,
        unit_external_ids: Option<StringOrList>,
        value_types: Option<StringOrList>,
        limit: Option<u64>,
        sort_by: Option<StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> Self {
        Self {
            inner: TimeSeriesFilterForm {
                filter: TimeSeriesFilter {
                    node: NodeFilter {
                        ids,
                        external_ids: opt_patterns(external_ids),
                        names: opt_patterns(names),
                        sources: opt_patterns(sources),
                        labels: opt_patterns(labels),
                        metadata,
                        created_time: created_time.map(Into::into),
                        last_updated_time: last_updated_time.map(Into::into),
                    },
                    data_set_ids: opt_data_set_refs(data_set_ids),
                    units: opt_patterns(units),
                    unit_external_ids: opt_patterns(unit_external_ids),
                    value_types: opt_patterns(value_types),
                },
                limit,
                paging: build_page_request(sort_by, sort_order, cursor),
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

#[pyclass(module = "datahub_sdk", name = "FieldBool", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyFieldBool(Field<bool>);

impl From<Field<bool>> for PyFieldBool {
    fn from(field: Field<bool>) -> Self {
        PyFieldBool(field)
    }
}
impl From<PyFieldBool> for Field<bool> {
    fn from(field: PyFieldBool) -> Self {
        field.0
    }
}

#[pymethods]
impl PyFieldBool {
    #[new]
    #[pyo3(signature=(value=None,set_null=false))]
    pub fn new(value: Option<bool>, set_null: bool) -> PyResult<Self> {
        Ok(Self(Field::new(value, set_null)))
    }
    #[getter]
    pub fn value(&self) -> Option<bool> {
        self.0.set
    }
    #[getter]
    pub fn set_null(&self) -> bool {
        self.0.set_null
    }
}

/// The geolocation of a `ResourceUpdate` — the same `set`/`set_null` pair as `FieldStr`, carrying
/// a GeoJSON geometry `dict` (`{"type": "Point", "coordinates": [10.75, 59.91]}`) instead of a
/// string. It is its own class for the same reason `FieldU64` is: the wrappers are per-payload
/// type.
#[pyclass(module = "datahub_sdk", name = "FieldGeoJson", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyFieldGeoJson(Field<geojson::Geometry>);

impl From<Field<geojson::Geometry>> for PyFieldGeoJson {
    fn from(field: Field<geojson::Geometry>) -> Self {
        PyFieldGeoJson(field)
    }
}
impl From<PyFieldGeoJson> for Field<geojson::Geometry> {
    fn from(field: PyFieldGeoJson) -> Self {
        field.0
    }
}

#[pymethods]
impl PyFieldGeoJson {
    #[new]
    #[pyo3(signature=(value=None,set_null=false))]
    pub fn new(value: Option<Bound<'_, PyAny>>, set_null: bool) -> PyResult<Self> {
        let geometry = value
            .map(|obj| {
                pythonize::depythonize::<geojson::Geometry>(&obj)
                    .map_err(|e| PyValueError::new_err(format!("invalid geolocation: {e}")))
            })
            .transpose()?;
        Ok(Self(Field::new(geometry, set_null)))
    }
    #[getter]
    pub fn value<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match &self.0.set {
            Some(geometry) => Ok(Some(pythonize::pythonize(py, geometry).map_err(|e| {
                PyValueError::new_err(format!("could not serialize geolocation: {e}"))
            })?)),
            None => Ok(None),
        }
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
    m.add_class::<PyFieldBool>()?;
    m.add_class::<PyFieldGeoJson>()?;
    m.add_class::<crate::datasets::PyBasicDatasetFilter>()?;
    m.add_class::<crate::datasets::PyDatasetFilter>()?;
    m.add_class::<crate::datasets::PyDatasetUpdate>()?;
    m.add_class::<PySearchAndFilterForm>()?;
    m.add_class::<PyTimeSeriesFilterForm>()?;
    m.add_class::<crate::resources::PyResourceFilter>()?;
    m.add_class::<PyPage>()?;
    timeseries::register(m)?;
    events::register(m)?;
    datasets::register(m)?;
    files::register(m)?;
    subscriptions::register(m)?;
    functions::register(m)?;
    relations::register(m)?;
    Ok(())
}
