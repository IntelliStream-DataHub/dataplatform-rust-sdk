//! The polymorphic node surface: `Asset`, `Policy`, and the dispatch that turns a Rust
//! [`Node`] into whichever Python class matches its type.
//!
//! `/resources` spans every node type, so its reads hand back a mixed list. Each element is the
//! *same* class a caller would get from the type's own endpoint — a timeseries from
//! `resources.filter()` is the `TimeSeries` class, not a lookalike — so `isinstance` works and
//! object-level navigation behaves identically wherever the object came from. Where dispatch is
//! data-driven rather than branching, every node class also exposes `node_type`.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use intellistream_datahub_sdk::nodes::{Asset, Node, NodeType, Policy};
use intellistream_datahub_sdk::relations::RelatedNode;
use intellistream_datahub_sdk::resources::RelatedResourcesForm;
use intellistream_datahub_sdk::ApiService;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAny, PyModuleMethods};
use pyo3::{Bound, IntoPyObject, PyErr, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use pythonize::{depythonize, pythonize};

use crate::datasets::PyDataset;
use crate::functions::PyFunction;
use crate::relations::PyRelatedNode;
use crate::resources::{PyResource, PyResourceNetwork};
use crate::timeseries::PyTimeSeries;

fn geometry_from_py(obj: Bound<'_, PyAny>) -> PyResult<geojson::Geometry> {
    depythonize(&obj).map_err(|e| PyValueError::new_err(format!("invalid geolocation: {e}")))
}

/// The name a node of this type answers to in Python's `node_type` and in the `node_type=`
/// filter argument.
pub fn node_type_name(kind: NodeType) -> &'static str {
    kind.filter_name()
}

/// One node of any type on its way to Python.
///
/// Deliberately **not** a `#[pyclass]`: Python never sees a `Node` object, it sees an `Asset`, a
/// `TimeSeries`, a `Dataset` and so on. This type exists only so `Vec<PyNode>` and
/// `Page::new(py, nodes, ..)` compose — both take anything that is `IntoPyObject`, which is what
/// the impl below provides. It is the first hand-written `IntoPyObject` in these bindings.
#[derive(Clone)]
pub struct PyNode {
    pub inner: Node,
    pub client: Arc<ApiService>,
}

impl PyNode {
    pub fn with_client(inner: Node, client: Arc<ApiService>) -> Self {
        Self { inner, client }
    }

    /// Wrap a whole list, stamping the client so navigation works off every element.
    pub fn many(nodes: Vec<Node>, client: Arc<ApiService>) -> Vec<PyNode> {
        nodes
            .into_iter()
            .map(|n| PyNode::with_client(n, client.clone()))
            .collect()
    }
}

impl<'py> IntoPyObject<'py> for PyNode {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        let client = self.client;
        Ok(match self.inner {
            Node::Asset(n) => PyAsset::with_client(n, client).into_pyobject(py)?.into_any(),
            Node::TimeSeries(n) => PyTimeSeries::with_client(n, client)
                .into_pyobject(py)?
                .into_any(),
            Node::Function(n) => PyFunction::with_client(n, client)
                .into_pyobject(py)?
                .into_any(),
            Node::Resource(n) => PyResource::with_client(n, client)
                .into_pyobject(py)?
                .into_any(),
            Node::Dataset(n) => PyDataset::with_client(n, client)
                .into_pyobject(py)?
                .into_any(),
            Node::Policy(n) => PyPolicy::with_client(n, client)
                .into_pyobject(py)?
                .into_any(),
            // `Node` is `#[non_exhaustive]`: a node type added to the api arrives here before
            // this crate knows about it. Say so plainly rather than mapping it onto the wrong
            // class.
            other => {
                return Err(PyValueError::new_err(format!(
                    "this SDK does not yet know the node type {:?}; upgrade the SDK",
                    other.kind()
                )));
            }
        })
    }
}

/// What `resources.create(...)` accepts as a node: any of the six node classes.
///
/// Each is dispatched server-side by its own type-label, so one heterogeneous list can create a
/// data set, an asset and a timeseries in a single call.
#[derive(Clone, pyo3::FromPyObject)]
pub enum NodeInput {
    Asset(PyAsset),
    TimeSeries(PyTimeSeries),
    Function(PyFunction),
    Dataset(PyDataset),
    Policy(PyPolicy),
    Resource(PyResource),
}

impl From<NodeInput> for Node {
    fn from(v: NodeInput) -> Self {
        match v {
            NodeInput::Asset(n) => Node::Asset(n.inner),
            NodeInput::TimeSeries(n) => Node::TimeSeries(n.inner),
            NodeInput::Function(n) => Node::Function(n.inner),
            NodeInput::Dataset(n) => Node::Dataset(n.inner),
            NodeInput::Policy(n) => Node::Policy(n.inner),
            NodeInput::Resource(n) => Node::Resource(n.inner),
        }
    }
}

/// Generate the graph-navigation methods every node class carries. `neighbors` walks outward
/// from this node and returns the connected sub-graph.
macro_rules! node_navigation {
    ($ty:ty) => {
        #[pymethods]
        impl $ty {
            /// Walk the graph from this node and return the connected sub-graph (its `nodes`, the
            /// `edges` between them, and their `labels`). `depth` bounds the traversal in hops
            /// (`-1`, the default, = the whole connected component); `relationship_types` filters
            /// which edge types to follow (`None` = all); `limit` caps the node count.
            ///
            /// Nodes reached this way are typed but sparse — the graph stores only a subset of
            /// each node's columns. Re-read one by id for its full field set.
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

            /// Awaitable variant of `neighbors`.
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
    };
}

/// An asset — a resource that carries a geographic location.
#[pyclass(module = "intellistream_datahub_sdk", name = "Asset", from_py_object)]
#[derive(Clone)]
pub struct PyAsset {
    pub inner: Asset,
    /// `None` on locally-constructed assets — navigation then raises.
    pub client: Option<Arc<ApiService>>,
}

impl From<Asset> for PyAsset {
    fn from(inner: Asset) -> Self {
        Self {
            inner,
            client: None,
        }
    }
}
impl From<PyAsset> for Asset {
    fn from(v: PyAsset) -> Self {
        v.inner
    }
}

impl PyAsset {
    pub fn with_client(inner: Asset, client: Arc<ApiService>) -> Self {
        Self {
            inner,
            client: Some(client),
        }
    }

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
impl PyAsset {
    #[new]
    #[pyo3(signature=(
        name=None,
        external_id=None,
        id=None,
        metadata=None,
        description=None,
        is_root=false,
        data_set_id=None,
        source=None,
        labels=None,
        related_resources=None,
        geolocation=None))]
    pub fn new(
        name: Option<String>,
        external_id: Option<String>,
        id: Option<u64>,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        is_root: bool,
        data_set_id: Option<u64>,
        source: Option<String>,
        labels: Option<Vec<String>>,
        related_resources: Option<Vec<PyRelatedNode>>,
        geolocation: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let geolocation = geolocation.map(geometry_from_py).transpose()?;
        let (final_name, final_ext_id) = crate::resources::name_and_external_id(name, external_id)?;
        Ok(Self {
            inner: Asset {
                id,
                external_id: final_ext_id,
                name: final_name,
                metadata,
                description,
                is_root,
                data_set_id,
                source,
                labels,
                related_resources: related_resources
                    .map(|v| v.into_iter().map(RelatedNode::from).collect())
                    .unwrap_or_default(),
                geolocation,
                created_time: None,
                last_updated_time: None,
            },
            client: None,
        })
    }

    /// Always `"asset"`. Present on every node class so data-driven code can dispatch without
    /// an `isinstance` ladder.
    #[getter]
    pub fn node_type(&self) -> &'static str {
        node_type_name(NodeType::Asset)
    }
    #[getter]
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    #[setter]
    pub fn set_name(&mut self, value: String) {
        self.inner.name = value;
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        &self.inner.external_id
    }
    #[setter]
    pub fn set_external_id(&mut self, value: String) {
        self.inner.external_id = value;
    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[setter]
    pub fn set_id(&mut self, value: Option<u64>) {
        self.inner.id = value;
    }
    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }
    #[setter]
    pub fn set_metadata(&mut self, value: Option<HashMap<String, String>>) {
        self.inner.metadata = value;
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[setter]
    pub fn set_description(&mut self, value: Option<String>) {
        self.inner.description = value;
    }
    #[getter]
    pub fn is_root(&self) -> bool {
        self.inner.is_root
    }
    #[setter]
    pub fn set_is_root(&mut self, value: bool) {
        self.inner.is_root = value;
    }
    #[getter]
    pub fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
    #[setter]
    pub fn set_data_set_id(&mut self, value: Option<u64>) {
        self.inner.data_set_id = value;
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }
    #[setter]
    pub fn set_source(&mut self, value: Option<String>) {
        self.inner.source = value;
    }
    #[getter]
    pub fn labels(&self) -> Option<&Vec<String>> {
        self.inner.labels.as_ref()
    }
    #[setter]
    pub fn set_labels(&mut self, value: Option<Vec<String>>) {
        self.inner.labels = value;
    }
    #[getter]
    pub fn related_resources(&self) -> Vec<PyRelatedNode> {
        self.inner
            .related_resources
            .iter()
            .cloned()
            .map(PyRelatedNode::from)
            .collect()
    }
    #[setter]
    pub fn set_related_resources(&mut self, value: Option<Vec<PyRelatedNode>>) {
        self.inner.related_resources = value
            .map(|v| v.into_iter().map(RelatedNode::from).collect())
            .unwrap_or_default();
    }
    /// The GeoJSON geometry as a Python `dict`, or `None`.
    ///
    /// On an asset reached through `neighbors()` this is reconstructed from the graph's native
    /// point, which is lossy for anything that is not a `Point` — read the asset by id when the
    /// geometry matters.
    #[getter]
    pub fn geolocation<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match &self.inner.geolocation {
            Some(geom) => Ok(Some(pythonize(py, geom).map_err(|e| {
                PyValueError::new_err(format!("could not serialize geolocation: {e}"))
            })?)),
            None => Ok(None),
        }
    }
    #[setter]
    pub fn set_geolocation(&mut self, value: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        self.inner.geolocation = value.map(geometry_from_py).transpose()?;
        Ok(())
    }
    #[getter]
    pub fn created_time(&self) -> Option<DateTime<Utc>> {
        self.inner.created_time
    }
    #[getter]
    pub fn last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.inner.last_updated_time
    }
    fn __repr__(&self) -> String {
        format!(
            "Asset(external_id={:?}, name={:?})",
            self.inner.external_id, self.inner.name
        )
    }
}

node_navigation!(PyAsset);

/// An access policy, as a node.
///
/// Sparse on every read: the api never sends a policy's `value`, `template_id` or `data_set_id`
/// back, so those are `None` regardless of what is stored.
#[pyclass(module = "intellistream_datahub_sdk", name = "Policy", from_py_object)]
#[derive(Clone)]
pub struct PyPolicy {
    pub inner: Policy,
    pub client: Option<Arc<ApiService>>,
}

impl From<Policy> for PyPolicy {
    fn from(inner: Policy) -> Self {
        Self {
            inner,
            client: None,
        }
    }
}
impl From<PyPolicy> for Policy {
    fn from(v: PyPolicy) -> Self {
        v.inner
    }
}

impl PyPolicy {
    pub fn with_client(inner: Policy, client: Arc<ApiService>) -> Self {
        Self {
            inner,
            client: Some(client),
        }
    }

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
impl PyPolicy {
    #[new]
    #[pyo3(signature=(
        name=None,
        external_id=None,
        id=None,
        r#type=None,
        value=None,
        deactivated=None,
        template_id=None,
        metadata=None,
        description=None,
        data_set_id=None,
        source=None,
        labels=None))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        name: Option<String>,
        external_id: Option<String>,
        id: Option<u64>,
        r#type: Option<String>,
        value: Option<Bound<'_, PyAny>>,
        deactivated: Option<bool>,
        template_id: Option<u64>,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        data_set_id: Option<u64>,
        source: Option<String>,
        labels: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let value = value
            .map(|v| {
                depythonize::<serde_json::Value>(&v)
                    .map_err(|e| PyValueError::new_err(format!("invalid policy value: {e}")))
            })
            .transpose()?;
        let (final_name, final_ext_id) = crate::resources::name_and_external_id(name, external_id)?;
        Ok(Self {
            inner: Policy {
                id,
                external_id: final_ext_id,
                name: final_name,
                description,
                policy_type: r#type,
                value,
                deactivated,
                template_id,
                data_set_id,
                source,
                metadata,
                labels,
                related_resources: vec![],
                created_time: None,
                last_updated_time: None,
            },
            client: None,
        })
    }

    /// Always `"policy"`.
    #[getter]
    pub fn node_type(&self) -> &'static str {
        node_type_name(NodeType::Policy)
    }
    #[getter]
    pub fn name(&self) -> &str {
        &self.inner.name
    }
    #[setter]
    pub fn set_name(&mut self, value: String) {
        self.inner.name = value;
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        &self.inner.external_id
    }
    #[setter]
    pub fn set_external_id(&mut self, value: String) {
        self.inner.external_id = value;
    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[setter]
    pub fn set_id(&mut self, value: Option<u64>) {
        self.inner.id = value;
    }
    /// The policy kind, e.g. `"IS_WRITE_PROTECTED"`.
    #[getter(r#type)]
    pub fn policy_type(&self) -> Option<&str> {
        self.inner.policy_type.as_deref()
    }
    #[setter(r#type)]
    pub fn set_policy_type(&mut self, value: Option<String>) {
        self.inner.policy_type = value;
    }
    /// The policy's value. Never populated on a read — the api does not send it back.
    #[getter]
    pub fn value<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match &self.inner.value {
            Some(v) => Ok(Some(pythonize(py, v).map_err(|e| {
                PyValueError::new_err(format!("could not serialize policy value: {e}"))
            })?)),
            None => Ok(None),
        }
    }
    #[getter]
    pub fn deactivated(&self) -> Option<bool> {
        self.inner.deactivated
    }
    #[setter]
    pub fn set_deactivated(&mut self, value: Option<bool>) {
        self.inner.deactivated = value;
    }
    /// Never populated on a read.
    #[getter]
    pub fn template_id(&self) -> Option<u64> {
        self.inner.template_id
    }
    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }
    #[setter]
    pub fn set_metadata(&mut self, value: Option<HashMap<String, String>>) {
        self.inner.metadata = value;
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[setter]
    pub fn set_description(&mut self, value: Option<String>) {
        self.inner.description = value;
    }
    /// Never populated on a read.
    #[getter]
    pub fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }
    #[setter]
    pub fn set_source(&mut self, value: Option<String>) {
        self.inner.source = value;
    }
    #[getter]
    pub fn labels(&self) -> Option<&Vec<String>> {
        self.inner.labels.as_ref()
    }
    #[setter]
    pub fn set_labels(&mut self, value: Option<Vec<String>>) {
        self.inner.labels = value;
    }
    #[getter]
    pub fn related_resources(&self) -> Vec<PyRelatedNode> {
        self.inner
            .related_resources
            .iter()
            .cloned()
            .map(PyRelatedNode::from)
            .collect()
    }
    #[getter]
    pub fn created_time(&self) -> Option<DateTime<Utc>> {
        self.inner.created_time
    }
    #[getter]
    pub fn last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.inner.last_updated_time
    }
    fn __repr__(&self) -> String {
        format!(
            "Policy(external_id={:?}, name={:?})",
            self.inner.external_id, self.inner.name
        )
    }
}

node_navigation!(PyPolicy);

pub fn register(m: &Bound<'_, pyo3::types::PyModule>) -> PyResult<()> {
    m.add_class::<PyAsset>()?;
    m.add_class::<PyPolicy>()?;
    Ok(())
}
