use crate::files::{PyFileDownload, PyFileIdentifiable, PyFileUpdate, PyFileUpload, PyINode};

use intellistream_datahub_sdk::files::FileUpdate;
use intellistream_datahub_sdk::generic::{DataWrapper, IdAndExtId};
use intellistream_datahub_sdk::{ApiService, FileUpload};
use pyo3::{PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

/// The blocking `/files` surface — a file and folder tree of `INode`s.
///
/// Reached as `client.files`.
///
/// Deleting is a **soft** delete — the file moves to the trash, where `list_trash` finds it and
/// `restore` brings it back.
#[pyclass(module = "intellistream_datahub_sdk", name = "FilesServiceSync")]
pub struct PyFilesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

/// Wrap every returned node so it keeps a handle on the client it came from (enabling
/// `related_resource_nodes` navigation).
fn to_py_inodes(
    result: &DataWrapper<intellistream_datahub_sdk::generic::INode>,
    service: &Arc<ApiService>,
) -> Vec<PyINode> {
    result
        .get_items()
        .iter()
        .map(|node| PyINode::with_client(node.clone(), service.clone()))
        .collect()
}

#[pymethods]
impl PyFilesServiceSync {
    /// Upload one file, described by a `FileUpload`. Returns the created `INode`s.
    ///
    /// **Local-file problems are `PanicException`, not `DataHubException`**, and `except Exception`
    /// does not catch them.
    fn upload_file<'py>(
        &self,
        py: Python<'py>,
        file_upload: PyFileUpload,
    ) -> PyResult<Vec<PyINode>> {
        let upload: FileUpload = file_upload.into();
        //let payload = DataWrapper::from_vec(events);
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.upload_file(upload))
                .map_err(|e| crate::datahub_err(e))?;

            let py_inodes: Vec<PyINode> = result
                .get_items()
                .iter()
                .map(|node| PyINode::with_client(node.clone(), service.clone()))
                .collect();
            Ok(py_inodes)
        })
    }

    /// The contents of the file tree's root.
    fn list_root_directory<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.list_root_directory())
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyINode> = result
                .get_items()
                .into_iter()
                .map(|u| PyINode::with_client(u.clone(), service.clone()))
                .collect();
            Ok(py_units)
        })
    }
    /// Move files to the trash. Returns `None`.
    ///
    /// A soft delete: `restore` brings it back. The `external_id` is freed immediately, so a
    /// trashed file can no longer be found under it.
    fn delete<'py>(&self, py: Python<'py>, input: Vec<PyFileIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids: Vec<IdAndExtId> = input.into_iter().map(|u| IdAndExtId::from(u)).collect();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.delete(&DataWrapper::from_vec(input_ids)))
                .map_err(|e| crate::datahub_err(e))?;

            Ok(())
        })
    }

    /// The contents of one directory, named by its absolute path.
    ///
    /// **`path` must begin with `/`**; without it the call silently addresses the wrong route.
    fn list_directory_by_path<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.list_directory_by_path(path))
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyINode> = result
                .get_items()
                .iter()
                .map(|ts| PyINode::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    /// Metadata for one file or folder, by numeric id.
    fn get_by_id<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.get_by_id(id))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Metadata for one file or folder, by external id.
    fn get_by_external_id<'py>(
        &self,
        py: Python<'py>,
        external_id: &str,
    ) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.get_by_external_id(external_id))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Full-text search over file and folder names and descriptions.
    fn search<'py>(&self, py: Python<'py>, query: &str) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.search(query))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// The soft-deleted files the caller can read.
    fn list_trash<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.list_trash())
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Restore soft-deleted files, identified by numeric id; the trashed external id answers 404.
    fn restore<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyFileIdentifiable>,
    ) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();
        let input_ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.restore(&DataWrapper::from_vec(input_ids)))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Rename, move, or edit the metadata of one file or folder.
    fn update<'py>(&self, py: Python<'py>, update: PyFileUpdate) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();
        let update: FileUpdate = update.into();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.update(&update))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Download a file's content into memory.
    fn download<'py>(&self, py: Python<'py>, id: u64) -> PyResult<PyFileDownload> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.download(id))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyFileDownload::from(result))
        })
    }

    /// Download a file straight to `destination`, without holding it in memory. Returns the number
    /// of bytes written.
    fn download_to_path<'py>(
        &self,
        py: Python<'py>,
        id: u64,
        destination: &str,
    ) -> PyResult<u64> {
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.files.download_to_path(id, destination))
                .map_err(|e| crate::datahub_err(e))
        })
    }
}
