pub mod async_service;
pub mod sync_service;

use crate::datetime::opt_py_datetime_to_utc;
use crate::resources::PyResource;
use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::files::{FileDownload, FileUpdate};
use dataplatform_rust_sdk::generic::{INode, IdAndExtId};
use dataplatform_rust_sdk::{ApiService, FileUpload};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyType};
use pyo3_async_runtimes::tokio::future_into_py;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "INode", from_py_object)]
#[derive(Clone)]
pub struct PyINode {
    inner: INode,
    /// The client this object was returned by, enabling navigation (`related_resource_nodes`).
    /// `None` on locally-constructed inodes — navigation then raises.
    client: Option<Arc<ApiService>>,
}
impl From<INode> for PyINode {
    fn from(ts: INode) -> Self {
        Self {
            inner: ts,
            client: None,
        }
    }
}

impl From<PyINode> for INode {
    fn from(ts: PyINode) -> Self {
        ts.inner
    }
}

impl PyINode {
    /// Wrap an inode returned by the API, stamping the client so navigation methods work.
    pub fn with_client(inner: INode, client: Arc<ApiService>) -> Self {
        Self {
            inner,
            client: Some(client),
        }
    }

    /// This file's `related_resources` ids as resource selectors (for `resources.by_ids`).
    fn related_id_collections(&self) -> Vec<IdAndExtId> {
        self.inner
            .related_resources
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|id| IdAndExtId {
                id: Some(*id as u64),
                external_id: None,
            })
            .collect()
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
        source_date_created: Option<Bound<'_, PyAny>>,
        source_last_updated: Option<Bound<'_, PyAny>>,
        parent_id: Option<i64>,
        parent_external_id: Option<String>,
        data_set_id: Option<i64>,
        metadata: Option<HashMap<String, String>>,
        related_resources: Option<Vec<i64>>,
        security_categories: Option<Vec<i32>>,
    ) -> PyResult<Self> {
        let source_date_created = opt_py_datetime_to_utc(source_date_created.as_ref())?;
        let source_last_updated = opt_py_datetime_to_utc(source_last_updated.as_ref())?;
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
            client: None,
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

/// Object-level navigation. Available only on inodes returned by the API (which carry a client);
/// calling these on a locally-constructed `INode` raises a clear error.
#[pymethods]
impl PyINode {
    /// Fetch the resources this file references (its `related_resources` ids), resolved to
    /// `Resource` objects via the resources service. (The `related_resources` *property* returns
    /// the raw ids; this resolves them.) Blocking; see [`related_resource_nodes_async`].
    fn related_resource_nodes(&self, py: Python<'_>) -> PyResult<Vec<PyResource>> {
        let service = self.client.clone().ok_or_else(crate::missing_client_err)?;
        let ids = self.related_id_collections();
        if ids.is_empty() {
            return Ok(vec![]);
        }
        py.detach(|| {
            let result = crate::nav_runtime()
                .block_on(service.resources.by_ids(&ids))
                .map_err(crate::datahub_err)?;
            Ok(result
                .nodes()
                .unwrap_or_default()
                .into_iter()
                .map(|r| PyResource::with_client(r, service.clone()))
                .collect())
        })
    }

    /// Awaitable variant of [`related_resource_nodes`].
    fn related_resource_nodes_async<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.client.clone().ok_or_else(crate::missing_client_err)?;
        let ids = self.related_id_collections();
        future_into_py(py, async move {
            if ids.is_empty() {
                return Ok(Vec::<PyResource>::new());
            }
            let result = service
                .resources
                .by_ids(&ids)
                .await
                .map_err(crate::datahub_err)?;
            Ok(result
                .nodes()
                .unwrap_or_default()
                .into_iter()
                .map(|r| PyResource::with_client(r, service.clone()))
                .collect())
        })
    }
}
#[pyclass(module = "datahub_sdk", name = "FileUpload", from_py_object)]
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

/// A partial update for one file or folder. Identify the node with `id` or `external_id`; every
/// other argument is optional and only sent when given, so an omitted field is left unchanged.
#[pyclass(module = "datahub_sdk", name = "FileUpdate", from_py_object)]
#[derive(Clone, Debug)]
pub struct PyFileUpdate {
    pub inner: FileUpdate,
}

impl From<PyFileUpdate> for FileUpdate {
    fn from(u: PyFileUpdate) -> Self {
        u.inner
    }
}

#[pymethods]
impl PyFileUpdate {
    #[new]
    #[pyo3(signature = (
        external_id = None,
        id = None,
        name = None,
        path = None,
        data_set_id = None,
        description = None,
        source = None,
        metadata = None,
        related_resources = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn __init__(
        external_id: Option<&str>,
        id: Option<u64>,
        name: Option<&str>,
        path: Option<&str>,
        data_set_id: Option<u64>,
        description: Option<&str>,
        source: Option<&str>,
        metadata: Option<HashMap<String, String>>,
        related_resources: Option<Vec<u64>>,
    ) -> PyResult<Self> {
        let mut inner = match (external_id, id) {
            (Some(external_id), _) => FileUpdate::by_external_id(external_id),
            (None, Some(id)) => FileUpdate::by_id(id),
            (None, None) => {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "FileUpdate needs an external_id or an id",
                ));
            }
        };
        if let Some(name) = name {
            inner = inner.with_name(name);
        }
        if let Some(path) = path {
            inner = inner.with_path(path);
        }
        if let Some(data_set_id) = data_set_id {
            inner = inner.with_data_set_id(data_set_id);
        }
        if let Some(description) = description {
            inner = inner.with_description(description);
        }
        if let Some(source) = source {
            inner = inner.with_source(source);
        }
        if let Some(metadata) = metadata {
            inner = inner.with_metadata(metadata);
        }
        if let Some(related_resources) = related_resources {
            inner = inner.with_related_resources(related_resources);
        }
        Ok(Self { inner })
    }

    #[getter]
    fn external_id(&self) -> Option<String> {
        self.inner.external_id.clone()
    }

    #[getter]
    fn id(&self) -> Option<u64> {
        self.inner.id
    }
}

/// The result of a download: the file's bytes plus what the server said they are.
#[pyclass(module = "datahub_sdk", name = "FileDownload")]
#[derive(Clone, Debug)]
pub struct PyFileDownload {
    pub inner: FileDownload,
}

impl From<FileDownload> for PyFileDownload {
    fn from(d: FileDownload) -> Self {
        Self { inner: d }
    }
}

#[pymethods]
impl PyFileDownload {
    #[getter]
    fn file_name(&self) -> Option<String> {
        self.inner.file_name.clone()
    }

    #[getter]
    fn mime_type(&self) -> Option<String> {
        self.inner.mime_type.clone()
    }

    /// The file content as `bytes`.
    #[getter]
    fn content<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.bytes)
    }

    fn __len__(&self) -> usize {
        self.inner.bytes.len()
    }

    fn __repr__(&self) -> String {
        format!(
            "FileDownload(file_name={:?}, mime_type={:?}, {} bytes)",
            self.inner.file_name,
            self.inner.mime_type,
            self.inner.bytes.len()
        )
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyINode>()?;
    m.add_class::<PyFileUpload>()?;
    m.add_class::<PyFileUpdate>()?;
    m.add_class::<PyFileDownload>()?;
    m.add_class::<sync_service::PyFilesServiceSync>()?;
    m.add_class::<async_service::PyFilesServiceAsync>()?;
    Ok(())
}
