use crate::files::{PyFileIdentifiable, PyFileUpload, PyINode};

use crate::{Identifiable, PyIdCollection};
use dataplatform_rust_sdk::generic::{INode, IdAndExtId};
use dataplatform_rust_sdk::{
    ApiService, FileUpload, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection,
};
use pyo3::exceptions::PyException;
use pyo3::{PyResult, Python, pyclass, pymethods};
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
    ) -> PyResult<PyFileUpload> {
        let upload: FileUpload = file_upload.into();
        //let payload = DataWrapper::from_vec(events);
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.upload_file(upload))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            let res = result.get_items().first().unwrap().clone().into();

            Ok(res)
        })
    }

    fn list_root_directory<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.list_root_directory())
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyINode> = result
                .get_items()
                .into_iter()
                .map(|u| PyINode::from(u.clone()))
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
                .block_on(service.events.delete(&input_ids))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            Ok(())
        })
    }

    fn list_directory_by_path<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Vec<PyINode>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.files.list_directory_by_path(path))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyINode> = result
                .get_items()
                .iter()
                .map(|ts| PyINode::from(ts.clone()))
                .collect();
            Ok(py_ts)
        })
    }
}
