pub(crate) mod async_service;
pub(crate) mod sync_service;

use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::labels::Label;
use pyo3::{pyclass, pymethods, FromPyObject, PyResult};
use pyo3::exceptions::PyValueError;

/// A DataHub label (the CRUD entity behind `client.labels`). `name` is the identifier callers
/// set; `id`/`color` are usually assigned by the server. Also the shape returned inside a
/// `ResourceNetwork` from `resources.fetch_related` (where `color`/`i18n_code` are `None`).
#[pyclass(module = "datahub_sdk", name = "Label", from_py_object)]
#[derive(Clone)]
pub struct PyLabel {
    pub inner: Label,
}

impl From<Label> for PyLabel {
    fn from(inner: Label) -> Self {
        Self { inner }
    }
}
impl From<PyLabel> for Label {
    fn from(v: PyLabel) -> Self {
        v.inner
    }
}

/// The graph-DTO label from a traversal carries only id/name/description; widen it to the unified
/// `Label` so `ResourceNetwork.labels` and `client.labels` return one and the same Python class.
impl From<dataplatform_rust_sdk::resources::Label> for PyLabel {
    fn from(l: dataplatform_rust_sdk::resources::Label) -> Self {
        PyLabel {
            inner: Label {
                id: l.id,
                name: l.name,
                description: l.description,
                i18n_code: None,
                color: None,
            },
        }
    }
}

#[pymethods]
impl PyLabel {
    #[new]
    #[pyo3(signature = (name=None, id=None, description=None, color=None, i18n_code=None))]
    pub fn __init__(
        name: Option<String>,
        id: Option<u64>,
        description: Option<String>,
        color: Option<String>,
        i18n_code: Option<String>,
    ) -> Self {
        Self {
            inner: Label { id, name, description, i18n_code, color },
        }
    }

    #[getter]
    fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[setter]
    fn set_id(&mut self, value: Option<u64>) {
        self.inner.id = value;
    }
    #[getter]
    fn name(&self) -> Option<&str> {
        self.inner.name.as_deref()
    }
    #[setter]
    fn set_name(&mut self, value: Option<String>) {
        self.inner.name = value;
    }
    #[getter]
    fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[setter]
    fn set_description(&mut self, value: Option<String>) {
        self.inner.description = value;
    }
    #[getter]
    fn color(&self) -> Option<&str> {
        self.inner.color.as_deref()
    }
    #[setter]
    fn set_color(&mut self, value: Option<String>) {
        self.inner.color = value;
    }
    #[getter]
    fn i18n_code(&self) -> Option<&str> {
        self.inner.i18n_code.as_deref()
    }
    #[setter]
    fn set_i18n_code(&mut self, value: Option<String>) {
        self.inner.i18n_code = value;
    }
}

/// Things accepted as a label identifier when deleting: a `Label`, its numeric id, or its name
/// (external id). Mirrors `ResourceIdentifiable`.
#[derive(Clone, FromPyObject)]
pub enum LabelIdentifiable {
    Label(PyLabel),
    ExternalId(String),
    Id(u64),
}

impl From<LabelIdentifiable> for IdAndExtId {
    fn from(value: LabelIdentifiable) -> Self {
        match value {
            LabelIdentifiable::Label(l) => Self {
                id: l.inner.id,
                external_id: l.inner.name.clone(),
            },
            LabelIdentifiable::ExternalId(ext) => Self {
                id: None,
                external_id: Some(ext),
            },
            LabelIdentifiable::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}

/// Reject label payloads that carry no id and no name — the server needs at least a name to
/// create and an id (or name) to update, and this gives a clear error instead of a 4xx.
pub(crate) fn require_named(labels: &[PyLabel]) -> PyResult<()> {
    if labels
        .iter()
        .any(|l| l.inner.id.is_none() && l.inner.name.is_none())
    {
        return Err(PyValueError::new_err(
            "each Label needs a name (for create) or an id (for update)",
        ));
    }
    Ok(())
}
