use crate::PyIdCollection;
use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::functions::{EdgeProxy, Function};
use dataplatform_rust_sdk::generic::IdAndExtId;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyList, PyString};
use pyo3::{Bound, PyResult, pyclass, pymethods};
use serde_json::Value as JsonValue;

pub mod async_service;
pub mod sync_service;

#[pyclass(module = "datahub_python_sdk", name = "Function")]
#[derive(Clone)]
pub struct PyFunction {
    pub inner: Function,
}

impl From<Function> for PyFunction {
    fn from(f: Function) -> Self {
        Self { inner: f }
    }
}
impl From<PyFunction> for Function {
    fn from(f: PyFunction) -> Self {
        f.inner
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
        Ok(Self { inner: function })
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

    /// PROCESSED_BY edges binding input timeseries into this function. Populated by the
    /// server on `/functions/list`; the Python worker reads `e.start` (timeseries id) and
    /// `e.edge_type == "PROCESSED_BY"` to build its routing map.
    #[getter]
    fn relations(&self) -> Vec<PyEdgeProxy> {
        self.inner
            .relations
            .iter()
            .cloned()
            .map(PyEdgeProxy::from)
            .collect()
    }
}

/// Read-only mirror of the SDK `EdgeProxy` for Python consumers — only the fields the
/// worker actually needs are exposed.
#[pyclass(module = "datahub_python_sdk", name = "EdgeProxy")]
#[derive(Clone)]
pub struct PyEdgeProxy {
    pub inner: EdgeProxy,
}

impl From<EdgeProxy> for PyEdgeProxy {
    fn from(e: EdgeProxy) -> Self {
        Self { inner: e }
    }
}

#[pymethods]
impl PyEdgeProxy {
    #[getter]
    fn id(&self) -> Option<u64> {
        self.inner.id
    }

    #[getter]
    fn start(&self) -> Option<u64> {
        self.inner.start
    }

    #[getter]
    fn end(&self) -> Option<u64> {
        self.inner.end
    }

    /// Relationship type, e.g. ``"PROCESSED_BY"``. Exposed under the Python-friendly name
    /// ``edge_type`` because ``type`` shadows the builtin and ``r#type`` doesn't survive
    /// pyo3's name mapping.
    #[getter]
    fn edge_type(&self) -> Option<&str> {
        self.inner.edge_type.as_deref()
    }

    #[getter]
    fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }

    #[getter]
    fn metadata(&self) -> std::collections::HashMap<String, String> {
        self.inner.metadata.clone()
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
    if let Ok(b) = value.downcast::<PyBool>() {
        return Ok(JsonValue::Bool(b.is_true()));
    }
    if let Ok(i) = value.downcast::<PyInt>() {
        if let Ok(n) = i.extract::<i64>() {
            return Ok(JsonValue::from(n));
        }
        if let Ok(n) = i.extract::<u64>() {
            return Ok(JsonValue::from(n));
        }
    }
    if let Ok(f) = value.downcast::<PyFloat>() {
        let f: f64 = f.extract()?;
        return serde_json::Number::from_f64(f)
            .map(JsonValue::Number)
            .ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(
                    "non-finite float cannot be encoded as JSON",
                )
            });
    }
    if let Ok(s) = value.downcast::<PyString>() {
        return Ok(JsonValue::String(s.extract()?));
    }
    if let Ok(list) = value.downcast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(py_to_json(&item)?);
        }
        return Ok(JsonValue::Array(out));
    }
    if let Ok(dict) = value.downcast::<PyDict>() {
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
    m.add_class::<PyEdgeProxy>()?;
    m.add_class::<sync_service::PyFunctionsServiceSync>()?;
    m.add_class::<async_service::PyFunctionsServiceAsync>()?;
    Ok(())
}
