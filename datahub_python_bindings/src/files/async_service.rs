use crate::files::{PyFileIdentifiable, PyFileUpload, PyINode};
use dataplatform_rust_sdk::generic::{IdAndExtId, IdAndExtIdCollection};
use dataplatform_rust_sdk::{ApiService, FileUpload};
use pyo3::exceptions::PyException;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass]
pub struct PyFilesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyFilesServiceSync {
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
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyFileUpload> = result
                .get_items()
                .iter()
                .map(|fu| PyFileUpload::from(fu.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    fn list_root_directory<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .files
                .list_root_directory()
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyINode> = result
                .get_items()
                .iter()
                .map(|ts| PyINode { inner: ts.clone() })
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
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);
        future_into_py(py, async move {
            service
                .files
                .delete(&wrapper)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
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
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyINode> = result
                .get_items()
                .iter()
                .map(|ts| PyINode { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }
}
