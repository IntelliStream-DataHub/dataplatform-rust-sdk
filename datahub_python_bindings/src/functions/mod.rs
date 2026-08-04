use crate::PyIdCollection;
use crate::events::PyEvent;
use crate::relations::PyRelatedNode;
use crate::resources::PyResourceNetwork;
use dataplatform_rust_sdk::filters::{BasicEventFilter, EventFilter};
use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::functions::Function;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::resources::RelatedResourcesForm;
use dataplatform_rust_sdk::ApiService;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::{Bound, PyResult, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use serde_json::Value as JsonValue;
use std::sync::Arc;

pub mod async_service;
pub mod sync_service;

#[pyclass(module = "datahub_sdk", name = "Function")]
#[derive(Clone)]
pub struct PyFunction {
    pub inner: Function,
    /// The client this object was returned by, enabling navigation methods
    /// (`neighbors`). `None` on locally-constructed functions — navigation then raises.
    pub client: Option<Arc<ApiService>>,
}

impl From<Function> for PyFunction {
    fn from(f: Function) -> Self {
        Self {
            inner: f,
            client: None,
        }
    }
}
impl From<PyFunction> for Function {
    fn from(f: PyFunction) -> Self {
        f.inner
    }
}

impl PyFunction {
    /// Wrap a function returned by the API, stamping the client so navigation methods work.
    pub fn with_client(inner: Function, client: Arc<ApiService>) -> Self {
        Self {
            inner,
            client: Some(client),
        }
    }

    /// Build the graph-traversal form for this function (the unified graph — functions are nodes).
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
impl PyFunction {
    #[new]
    #[pyo3(signature=(external_id, model_name, name=None, config=None))]
    fn __init__(
        py: Python<'_>,
        external_id: String,
        model_name: String,
        name: Option<String>,
        config: Option<Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let mut function = Function::new(external_id, model_name);
        function.name = name;
        if let Some(d) = config {
            function.config = py_to_json(&d.into_any())?;
        }
        Ok(Self {
            inner: function,
            client: None,
        })
    }

    #[getter]
    fn id(&self) -> Option<u64> {
        self.inner.id
    }

    #[getter]
    fn external_id(&self) -> &str {
        &self.inner.external_id
    }

    #[getter]
    fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }

    /// Stable identifier of the model template the function uses (e.g. `forecast-ema`,
    /// `anomaly-detection`). The function worker dispatches to a handler by this name.
    #[getter]
    fn model_name(&self) -> &str {
        &self.inner.model_name
    }

    /// Merged configuration: defaults from the server-side template plus any user-supplied
    /// overrides. Returned as a regular Python dict — keys present here override the
    /// template defaults of the same name.
    #[getter]
    fn config<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        json_to_py(py, &self.inner.config)
    }

    #[getter]
    fn labels(&self) -> Vec<String> {
        self.inner.labels.clone()
    }

    #[getter]
    fn metadata(&self) -> std::collections::HashMap<String, String> {
        self.inner.metadata.clone()
    }

    #[getter]
    fn created_time(&self) -> Option<DateTime<Utc>> {
        self.inner.created_time
    }

    #[getter]
    fn last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.inner.last_updated_time
    }

    /// The nodes bound into this function (e.g. its input timeseries via PROCESSED_BY
    /// edges). Populated by the server on `/functions/list`; the Python worker reads each
    /// entry's `id` and `relationship_type == "PROCESSED_BY"` to build its routing map.
    #[getter]
    fn related_resources(&self) -> Vec<PyRelatedNode> {
        self.inner
            .related_resources
            .iter()
            .cloned()
            .map(PyRelatedNode::from)
            .collect()
    }
}

/// Things accepted as a function identifier when fetching by_ids or deleting.
#[derive(Clone, FromPyObject)]
pub enum FunctionIdentifyable {
    Function(PyFunction),
    Collection(PyIdCollection),
    ExternalId(String),
    Id(u64),
}

impl From<FunctionIdentifyable> for IdAndExtId {
    fn from(value: FunctionIdentifyable) -> Self {
        match value {
            FunctionIdentifyable::Function(f) => Self {
                id: f.inner.id,
                external_id: Some(f.inner.external_id.clone()),
            },
            FunctionIdentifyable::Collection(c) => c.into(),
            FunctionIdentifyable::ExternalId(ext) => Self {
                id: None,
                external_id: Some(ext),
            },
            FunctionIdentifyable::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}

/// Convert a Python value (recursively) into `serde_json::Value`. Used so callers can
/// pass an idiomatic dict for `config` instead of having to JSON-encode it themselves.
pub(crate) fn py_to_json(value: &Bound<'_, PyAny>) -> PyResult<JsonValue> {
    if value.is_none() {
        return Ok(JsonValue::Null);
    }
    if let Ok(b) = value.cast::<PyBool>() {
        return Ok(JsonValue::Bool(b.is_true()));
    }
    if let Ok(i) = value.cast::<PyInt>() {
        if let Ok(n) = i.extract::<i64>() {
            return Ok(JsonValue::from(n));
        }
        if let Ok(n) = i.extract::<u64>() {
            return Ok(JsonValue::from(n));
        }
    }
    if let Ok(f) = value.cast::<PyFloat>() {
        let f: f64 = f.extract()?;
        return serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(
                    "non-finite float cannot be encoded as JSON",
                )
            });
    }
    if let Ok(s) = value.cast::<PyString>() {
        return Ok(JsonValue::String(s.extract()?));
    }
    if let Ok(list) = value.cast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(py_to_json(&item)?);
        }
        return Ok(JsonValue::Array(out));
    }
    if let Ok(dict) = value.cast::<PyDict>() {
        let mut out = serde_json::Map::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key: String = k.extract()?;
            out.insert(key, py_to_json(&v)?);
        }
        return Ok(JsonValue::Object(out));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(format!(
        "unsupported type for JSON conversion: {}",
        value.get_type().name()?
    )))
}

/// Convert a `serde_json::Value` into the equivalent Python object.
pub(crate) fn json_to_py<'py>(
    py: Python<'py>,
    value: &JsonValue,
) -> PyResult<Bound<'py, PyAny>> {
    match value {
        JsonValue::Null => Ok(py.None().into_bound(py)),
        JsonValue::Bool(b) => Ok(PyBool::new(py, *b).to_owned().into_any()),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.into_any())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.into_any())
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_pyobject(py)?.into_any())
            } else {
                Ok(py.None().into_bound(py))
            }
        }
        JsonValue::String(s) => Ok(s.as_str().into_pyobject(py)?.into_any()),
        JsonValue::Array(arr) => {
            let list = PyList::empty(py);
            for v in arr {
                list.append(json_to_py(py, v)?)?;
            }
            Ok(list.into_any())
        }
        JsonValue::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_to_py(py, v)?)?;
            }
            Ok(dict.into_any())
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyFunction>()?;
    m.add_class::<sync_service::PyFunctionsServiceSync>()?;
    m.add_class::<async_service::PyFunctionsServiceAsync>()?;
    Ok(())
}

/// Object-level graph navigation. Available only on functions returned by the API (which carry a
/// client); calling these on a locally-constructed `Function` raises a clear error.
#[pymethods]
impl PyFunction {
    /// Walk the graph from this function and return the connected sub-graph (its `nodes`, the
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

/// Reverse lookup: the events that reference this function. (The other direction —
/// `Event.related_resource_nodes()` — resolves an event's resources.) Available only on
/// functions returned by the API; calling on a locally-constructed one raises.
#[pymethods]
impl PyFunction {
    /// Fetch events whose `related_resource_ids` / `related_resource_external_ids` include this
    /// function (matched by graph-node id when present, else external id), via `events.filter`.
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

impl PyFunction {
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
