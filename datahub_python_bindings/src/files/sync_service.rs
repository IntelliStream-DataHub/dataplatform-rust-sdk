use crate::files::{PyFileDownload, PyFileIdentifiable, PyFileUpdate, PyFileUpload, PyINode};

use dataplatform_rust_sdk::files::FileUpdate;
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId};
use dataplatform_rust_sdk::{ApiService, FileUpload};
use pyo3::{PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "FilesServiceSync")]
pub struct PyFilesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

/// Wrap every returned node so it keeps a handle on the client it came from (enabling
/// `related_resource_nodes` navigation).
fn to_py_inodes(
    result: &DataWrapper<dataplatform_rust_sdk::generic::INode>,
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

    /// Restore soft-deleted files. Identify each by numeric id: the trashed
    /// `DELETED_..._<epochMillis>` external id does not round-trip through the server's
    /// lowercasing hash, so that route answers 404. See `FileService::restore` in the SDK.
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
