use crate::files::{PyFileDownload, PyFileIdentifiable, PyFileUpdate, PyFileUpload, PyINode};
use dataplatform_rust_sdk::files::FileUpdate;
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId};
use dataplatform_rust_sdk::{ApiService, FileUpload};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "FilesServiceAsync")]
pub struct PyFilesServiceAsync {
    pub api_service: Arc<ApiService>,
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
impl PyFilesServiceAsync {
    fn upload_file<'py>(
        &self,
        py: Python<'py>,
        file_upload: PyFileUpload,
    ) -> PyResult<Bound<'py, PyAny>> {
        let upload: FileUpload = file_upload.into();
        //let payload = DataWrapper::from_vec(events);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .files
                .upload_file(upload)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_inodes: Vec<PyINode> = result
                .get_items()
                .iter()
                .map(|node| PyINode::with_client(node.clone(), service.clone()))
                .collect();
            Ok(py_inodes)
        })
    }

    fn list_root_directory<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .files
                .list_root_directory()
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyINode> = result
                .get_items()
                .iter()
                .map(|ts| PyINode::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }
    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyFileIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let wrapper = DataWrapper::from_vec(input_ids);
        future_into_py(py, async move {
            service
                .files
                .delete(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }

    fn list_directory_by_path<'py>(
        &self,
        py: Python<'py>,
        path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .files
                .list_directory_by_path(path.as_str())
                .await
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
    fn get_by_id<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .files
                .get_by_id(id)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Metadata for one file or folder, by external id.
    fn get_by_external_id<'py>(
        &self,
        py: Python<'py>,
        external_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .files
                .get_by_external_id(external_id.as_str())
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Full-text search over file and folder names and descriptions.
    fn search<'py>(&self, py: Python<'py>, query: String) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .files
                .search(query.as_str())
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// The soft-deleted files the caller can read.
    fn list_trash<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .files
                .list_trash()
                .await
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let wrapper = DataWrapper::from_vec(input_ids);
        future_into_py(py, async move {
            let result = service
                .files
                .restore(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Rename, move, or edit the metadata of one file or folder.
    fn update<'py>(&self, py: Python<'py>, update: PyFileUpdate) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let update: FileUpdate = update.into();
        future_into_py(py, async move {
            let result = service
                .files
                .update(&update)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(to_py_inodes(&result, &service))
        })
    }

    /// Download a file's content into memory.
    fn download<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .files
                .download(id)
                .await
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
        destination: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .files
                .download_to_path(id, destination)
                .await
                .map_err(|e| crate::datahub_err(e))
        })
    }
}
