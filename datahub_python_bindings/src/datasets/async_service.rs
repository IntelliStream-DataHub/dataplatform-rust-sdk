use crate::datasets::{DatasetIdentifiable, PyDataset};
use crate::{DatahubIdentity, Identifiable, PyIdCollection, PySearchAndFilterForm};
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::datasets::Dataset;
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId, IdAndExtIdCollection};
use pyo3::exceptions::PyException;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass]
pub struct PyDatasetsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyDatasetsServiceAsync {
    fn list<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .datasets
                .list()
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|ts| PyDataset { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }

    fn create<'p>(&self, py: Python<'p>, input: Vec<PyDataset>) -> PyResult<Bound<'p, PyAny>> {
        let datasets: Vec<Dataset> = input.iter().cloned().map(Dataset::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .datasets
                .create(&datasets)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|ts| PyDataset { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'p>(
        &self,
        py: Python<'p>,
        input: Vec<DatasetIdentifiable>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .datasets
                .by_ids(&input_ids)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|u| PyDataset { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'p>(
        &self,
        py: Python<'p>,
        input: Vec<DatasetIdentifiable>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .datasets
                .delete(&input_ids)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|ts| PyDataset { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }
}
