use pyo3::FromPyObject;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyDict, PyFloat, PyInt, PyString};
use std::collections::HashMap;

/// A `dict` argument that maps to a Rust `HashMap<String, String>`, coercing
/// scalar values (`int`, `float`, `bool`) to their string form so callers can
/// write `metadata={"window length": 300}` instead of `{"window length": "300"}`.
///
/// Keys must be strings. Values that are not `str`/`int`/`float`/`bool`
/// (e.g. nested dicts or lists) are rejected with a `TypeError` rather than
/// being silently stringified, so genuine mistakes still surface.
#[derive(Clone, Debug, Default)]
pub struct StringMap(pub HashMap<String, String>);

impl From<StringMap> for HashMap<String, String> {
    fn from(m: StringMap) -> Self {
        m.0
    }
}

impl FromPyObject<'_,'_> for StringMap {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_,'_, PyAny>) -> PyResult<Self> {
        let dict = ob.downcast::<PyDict>().map_err(|_| {
            PyTypeError::new_err(format!("expected a dict, got '{:?}'", ob.get_type().name()))
        })?;
        let mut map = HashMap::with_capacity(dict.len());
        for (key, value) in dict.iter() {
            let key: String = key.extract().map_err(|_| {
                PyTypeError::new_err(format!(
                    "dict keys must be str, got '{:?}'",
                    key.get_type().name())
                )
            })?;
            map.insert(key, coerce_value(&value)?);
        }
        Ok(StringMap(map))
    }

}

/// Coerce a Python scalar to a string. `bool` is checked before `int` because
/// `bool` is a subclass of `int` in Python.
fn coerce_value(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = value.cast::<PyString>() {
        Ok(s.to_string())
    } else if let Ok(b) = value.cast::<PyBool>() {
        Ok(if b.is_true() { "true" } else { "false" }.to_owned())
    } else if value.is_instance_of::<PyInt>() {
        Ok(value.extract::<i64>()?.to_string())
    } else if value.is_instance_of::<PyFloat>() {
        Ok(value.extract::<f64>()?.to_string())
    } else {
        Err(PyTypeError::new_err(format!(
            "dict values must be str, int, float, or bool, got '{}'",
            value.get_type().name()?
        )))
    }
}
