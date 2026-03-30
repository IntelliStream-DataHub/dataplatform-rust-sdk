mod async_service;

use pyo3::pyclass;
use dataplatform_rust_sdk::generic::INode;


#[pyclass(module="datahub_python_sdk",name="INode",from_py_object)]
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
