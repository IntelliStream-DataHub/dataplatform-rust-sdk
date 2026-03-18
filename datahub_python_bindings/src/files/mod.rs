mod async_service;

use pyo3::pyclass;
use dataplatform_rust_sdk::generic::INode;

#[pyclass]
#[derive(Clone)]
pub struct PyINode{
    inner: INode
}

impl From<INode> for PyINode {
    fn from(ts: INode) -> Self {
        Self { inner: ts }
    }
}
impl From<PyINode> for INode {
    fn from(ts: PyINode) -> Self {
        ts.inner
    }
}

impl PyINode {
    pub(crate) fn from_inode(inner: INode) -> Self {
        Self { inner }
    }
}
