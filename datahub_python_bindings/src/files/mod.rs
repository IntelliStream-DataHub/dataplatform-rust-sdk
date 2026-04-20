mod async_service;
mod sync_service;

use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::FileUpload;
use dataplatform_rust_sdk::generic::{INode, IdAndExtId};
use pyo3::prelude::*;
use pyo3::types::PyType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[pyclass(module = "datahub_python_sdk", name = "INode", from_py_object)]
#[derive(Clone)]
pub struct PyINode {
    inner: INode,
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

#[pymethods]
impl PyINode {
    #[new]
    #[pyo3(signature = (
    name,
    external_id,
    path,
    size,
    id=None,
    description = None,
    checksum = None,
    source = None,
    r#type = None,
    mime_type = None,
    source_date_created = None,
    source_last_updated = None,
    parent_id = None,
    parent_external_id = None,
    data_set_id = None,
    metadata = None,
    related_resources = None,
    security_categories = None))]
    pub fn new(
        name: String,
        external_id: String,
        path: String,
        size: u64,
        id: Option<u64>,
        description: Option<String>,
        checksum: Option<String>,
        source: Option<String>,
        r#type: Option<String>,
        mime_type: Option<String>,
        source_date_created: Option<DateTime<Utc>>,
        source_last_updated: Option<DateTime<Utc>>,
        parent_id: Option<i64>,
        parent_external_id: Option<String>,
        data_set_id: Option<i64>,
        metadata: Option<HashMap<String, String>>,
        related_resources: Option<Vec<i64>>,
        security_categories: Option<Vec<i32>>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: INode {
                id,
                name,
                external_id,
                path,
                size,
                description,
                checksum,
                source,
                r#type,
                mime_type,
                source_date_created,
                source_last_updated,
                date_created: DateTime::default(),
                last_updated: DateTime::default(),
                parent_id,
                parent_external_id,
                data_set_id,
                metadata,
                related_resources,
                security_categories,
            },
        })
    }

    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    pub fn name(&self) -> &str {
        self.inner.name.as_str()
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        self.inner.external_id.as_str()
    }
    #[getter]
    pub fn path(&self) -> &str {
        self.inner.path.as_str()
    }
    #[getter]
    pub fn size(&self) -> u64 {
        self.inner.size
    }
    #[getter]
    pub fn checksum(&self) -> Option<&str> {
        self.inner.checksum.as_deref()
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }
    #[getter]
    pub fn r#type(&self) -> Option<&str> {
        self.inner.r#type.as_deref()
    }
    #[getter]
    pub fn mime_type(&self) -> Option<&str> {
        self.inner.mime_type.as_deref()
    }
    #[getter]
    pub fn source_date_created(&self) -> Option<DateTime<Utc>> {
        self.inner.source_date_created
    }
    #[getter]
    pub fn source_last_updated(&self) -> Option<DateTime<Utc>> {
        self.inner.source_last_updated
    }
    #[getter]
    pub fn date_created(&self) -> DateTime<Utc> {
        self.inner.date_created
    }
    #[getter]
    pub fn last_updated(&self) -> DateTime<Utc> {
        self.inner.last_updated
    }
    #[getter]
    pub fn parent_id(&self) -> Option<i64> {
        self.inner.parent_id
    }
    #[getter]
    pub fn parent_external_id(&self) -> Option<&str> {
        self.inner.parent_external_id.as_deref()
    }
    #[getter]
    pub fn data_set_id(&self) -> Option<i64> {
        self.inner.data_set_id
    }
    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }
    #[getter]
    pub fn related_resources(&self) -> Option<&Vec<i64>> {
        self.inner.related_resources.as_ref()
    }
    #[getter]
    pub fn security_categories(&self) -> Option<&Vec<i32>> {
        self.inner.security_categories.as_ref()
    }
}
#[pyclass(module = "datahub_python_sdk", name = "FileUpload", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyFileUpload {
    pub inner: FileUpload,
}

impl From<FileUpload> for PyFileUpload {
    fn from(ts: FileUpload) -> Self {
        Self { inner: ts }
    }
}
impl From<PyFileUpload> for FileUpload {
    fn from(ts: PyFileUpload) -> Self {
        ts.inner
    }
}

#[pymethods]
impl PyFileUpload {
    #[new]
    #[pyo3(signature = (
    path,
    destination_path = None,
    external_id = None,
    name = None,
    metadata = None,
    description = None,
    source = None,
    data_set_id = None,
    related_resources = None,
    ))]
    pub fn __init__(
        path: &str,
        destination_path: Option<&str>,
        external_id: Option<&str>,
        name: Option<&str>,
        metadata: Option<HashMap<String, String>>,
        description: Option<&str>,
        source: Option<&str>,
        data_set_id: Option<u64>,
        related_resources: Option<Vec<u64>>,
    ) -> PyResult<Self> {
        let mut file_upload = FileUpload::new(path);
        if let Some(external_id) = external_id {
            file_upload.external_id = external_id.to_string();
        }
        if let Some(destination_path) = destination_path {
            file_upload.set_destination_path(destination_path.to_string());
        }
        if let Some(metadata) = metadata {
            file_upload.metadata = Some(metadata);
        }
        if let Some(description) = description {
            file_upload.description = Some(description.to_string());
        }
        if let Some(source) = source {
            file_upload.source = Some(source.to_string());
        }
        if let Some(data_set_id) = data_set_id {
            file_upload.data_set_id = Some(data_set_id);
        }
        if let Some(related_resources) = related_resources {
            file_upload.related_resources = Some(related_resources.to_vec());
        }
        if let Some(name) = name {
            file_upload.name = name.to_string();
        }
        Ok(Self { inner: file_upload })
    }
    #[classmethod]
    pub fn from_path(_py: Py<PyType>, path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: FileUpload::new(path),
        })
    }
    #[classmethod]
    pub fn new_with_destination_path(
        _py: Py<PyType>,
        path: &str,
        destination_path: &str,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: FileUpload::new_with_destination_path(path, destination_path),
        })
    }
    #[getter]
    pub fn external_id(&self) -> &str {
        self.inner.external_id.as_str()
    }
    #[getter]
    pub fn file_path(&self) -> &str {
        self.inner.file_path.as_str()
    }
    #[getter]
    pub fn name(&self) -> &str {
        self.inner.name.as_str()
    }
    #[getter]
    pub fn destination_path(&self) -> Option<&str> {
        self.inner.destination_path.as_deref()
    }
    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.source.as_deref()
    }
    #[getter]
    pub fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
    #[getter]
    pub fn mime_type(&self) -> Option<&str> {
        self.inner.mime_type.as_deref()
    }
    #[getter]
    pub fn related_resources(&self) -> Option<&Vec<u64>> {
        self.inner.related_resources.as_ref()
    }
    #[getter]
    pub fn source_date_created(&self) -> Option<DateTime<Utc>> {
        self.inner.source_date_created
    }
    #[getter]
    pub fn source_last_updated(&self) -> Option<DateTime<Utc>> {
        self.inner.source_last_updated
    }
}

#[derive(Clone, FromPyObject)]
pub enum PyFileIdentifiable {
    FileUpload(PyFileUpload),
    INode(PyINode),
    ExternalId(String),
    Id(u64),
}

impl From<PyFileIdentifiable> for IdAndExtId {
    fn from(value: PyFileIdentifiable) -> Self {
        match value {
            PyFileIdentifiable::FileUpload(upload) => IdAndExtId {
                id: None,
                external_id: Some(upload.external_id().to_string()),
            },
            PyFileIdentifiable::INode(node) => IdAndExtId {
                id: node.id(),
                external_id: Some(node.external_id().to_string()),
            },
            PyFileIdentifiable::ExternalId(ext_id) => IdAndExtId {
                id: None,
                external_id: Some(ext_id),
            },
            PyFileIdentifiable::Id(id) => IdAndExtId {
                id: Some(id),
                external_id: None,
            },
        }
    }
}
