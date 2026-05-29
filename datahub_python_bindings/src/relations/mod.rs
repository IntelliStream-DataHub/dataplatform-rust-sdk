use crate::resources::PyResource;
use dataplatform_rust_sdk::graph_data_wrapper::GraphDataWrapper;
use dataplatform_rust_sdk::relations::{EdgeProxy, RelForm};
use dataplatform_rust_sdk::Resource;
use pyo3::prelude::*;
use pyo3::{Bound, PyResult, pyclass, pymethods};
use std::collections::HashMap;

/// Server-assigned edge between two resources. Constructible from Python primarily
/// for tests and round-trip serde — in normal use these are returned by the server
/// inside a `GraphResult`. The Python attribute is `relationship_type` even though
/// the wire field is `"type"`.
#[pyclass(module = "datahub_sdk", name = "EdgeProxy")]
#[derive(Clone)]
pub struct PyEdgeProxy {
    pub inner: EdgeProxy,
}

impl From<EdgeProxy> for PyEdgeProxy {
    fn from(e: EdgeProxy) -> Self {
        Self { inner: e }
    }
}

impl From<PyEdgeProxy> for EdgeProxy {
    fn from(e: PyEdgeProxy) -> Self {
        e.inner
    }
}

#[pymethods]
impl PyEdgeProxy {
    #[new]
    #[pyo3(signature = (
        id = None,
        start = None,
        end = None,
        relationship_type = None,
        description = None,
        relationship_type_id = None,
        metadata = None,
    ))]
    fn new(
        id: Option<u64>,
        start: Option<u64>,
        end: Option<u64>,
        relationship_type: Option<String>,
        description: Option<String>,
        relationship_type_id: Option<u64>,
        metadata: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            inner: EdgeProxy {
                id,
                start,
                end,
                relationship_type,
                description,
                relationship_type_id,
                metadata: metadata.unwrap_or_default(),
            },
        }
    }

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

    #[getter]
    fn relationship_type(&self) -> Option<&str> {
        self.inner.relationship_type.as_deref()
    }

    #[getter]
    fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }

    #[getter]
    fn relationship_type_id(&self) -> Option<u64> {
        self.inner.relationship_type_id
    }

    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.inner.metadata.clone()
    }
}

/// Request-side edge form mirroring server-side `RelForm`. Pair this with a list
/// of `Resource` and pass both to `ResourcesService.create()`. `relationship_type`
/// is keyword-required; the server snake-uppercases it.
#[pyclass(module = "datahub_sdk", name = "RelForm")]
#[derive(Clone)]
pub struct PyRelForm {
    pub inner: RelForm,
}

impl From<RelForm> for PyRelForm {
    fn from(r: RelForm) -> Self {
        Self { inner: r }
    }
}

impl From<PyRelForm> for RelForm {
    fn from(r: PyRelForm) -> Self {
        r.inner
    }
}

#[pymethods]
impl PyRelForm {
    #[new]
    #[pyo3(signature = (
        *,
        relationship_type,
        from_external_id = None,
        to_external_id = None,
        from_id = None,
        to_id = None,
        id = None,
        relationship_type_id = None,
        metadata = None,
        data_set_id = None,
        description = None,
    ))]
    fn new(
        relationship_type: String,
        from_external_id: Option<String>,
        to_external_id: Option<String>,
        from_id: Option<u64>,
        to_id: Option<u64>,
        id: Option<u64>,
        relationship_type_id: Option<u64>,
        metadata: Option<HashMap<String, String>>,
        data_set_id: Option<u64>,
        description: Option<String>,
    ) -> Self {
        Self {
            inner: RelForm {
                id,
                from_external_id,
                to_external_id,
                from_id,
                to_id,
                relationship_type,
                relationship_type_id,
                metadata: metadata.unwrap_or_default(),
                data_set_id,
                description,
            },
        }
    }

    #[classmethod]
    #[pyo3(name = "by_external_ids")]
    fn by_external_ids(
        _cls: &Bound<'_, pyo3::types::PyType>,
        from_external_id: String,
        to_external_id: String,
        relationship_type: String,
    ) -> Self {
        Self {
            inner: RelForm::by_external_ids(from_external_id, to_external_id, relationship_type),
        }
    }

    #[classmethod]
    #[pyo3(name = "by_ids")]
    fn by_ids(
        _cls: &Bound<'_, pyo3::types::PyType>,
        from_id: u64,
        to_id: u64,
        relationship_type: String,
    ) -> Self {
        Self {
            inner: RelForm::by_ids(from_id, to_id, relationship_type),
        }
    }

    #[getter]
    fn id(&self) -> Option<u64> {
        self.inner.id
    }

    #[getter]
    fn from_external_id(&self) -> Option<&str> {
        self.inner.from_external_id.as_deref()
    }

    #[getter]
    fn to_external_id(&self) -> Option<&str> {
        self.inner.to_external_id.as_deref()
    }

    #[getter]
    fn from_id(&self) -> Option<u64> {
        self.inner.from_id
    }

    #[getter]
    fn to_id(&self) -> Option<u64> {
        self.inner.to_id
    }

    #[getter]
    fn relationship_type(&self) -> &str {
        &self.inner.relationship_type
    }

    #[getter]
    fn relationship_type_id(&self) -> Option<u64> {
        self.inner.relationship_type_id
    }

    #[getter]
    fn metadata(&self) -> HashMap<String, String> {
        self.inner.metadata.clone()
    }

    #[getter]
    fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }

    #[getter]
    fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
}

/// Python view of `GraphDataWrapper<Resource>`: the nodes and relations returned
/// from a graph operation. `.nodes` is `list[Resource]`, `.relations` is `list[EdgeProxy]`.
#[pyclass(module = "datahub_sdk", name = "GraphResult")]
#[derive(Clone)]
pub struct PyGraphResult {
    pub nodes: Vec<PyResource>,
    pub relations: Vec<PyEdgeProxy>,
}

impl PyGraphResult {
    pub fn from_wrapper(wrapper: GraphDataWrapper<Resource>) -> Self {
        let nodes = wrapper
            .nodes()
            .unwrap_or_default()
            .into_iter()
            .map(|r| PyResource { inner: r })
            .collect();
        let relations = wrapper
            .relations()
            .map(|v| v.iter().cloned().map(PyEdgeProxy::from).collect())
            .unwrap_or_default();
        Self { nodes, relations }
    }
}

#[pymethods]
impl PyGraphResult {
    #[getter]
    fn nodes(&self) -> Vec<PyResource> {
        self.nodes.clone()
    }

    #[getter]
    fn relations(&self) -> Vec<PyEdgeProxy> {
        self.relations.clone()
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEdgeProxy>()?;
    m.add_class::<PyRelForm>()?;
    m.add_class::<PyGraphResult>()?;
    Ok(())
}
