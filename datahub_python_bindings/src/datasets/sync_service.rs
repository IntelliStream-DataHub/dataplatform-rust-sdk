use crate::datasets::{DatasetIdentifiable, PyDataset};
use crate::resources::PyResource;
use crate::{PyIdCollection, PySearchAndFilterForm};
use dataplatform_rust_sdk::datasets::Dataset;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::{ApiService, Resource};
use pyo3::exceptions::PyException;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

#[pyclass(module = "datahub_python_sdk", name = "DatasetsService")]
pub struct PyDatasetsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyDatasetsServiceSync {
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyDataset>) -> PyResult<Vec<PyDataset>> {
        let datasets: Vec<Dataset> = input.iter().cloned().map(Dataset::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.datasets.create(&datasets)));

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        let py_res: Vec<PyDataset> = result
            .get_items()
            .iter()
            .map(|ts| PyDataset { inner: ts.clone() })
            .collect();
        Ok(py_res)
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<DatasetIdentifiable>,
    ) -> PyResult<Vec<PyDataset>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        let result = py.detach(|| self.runtime.block_on(service.datasets.by_ids(&input_ids)));

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        let py_res: Vec<PyDataset> = result
            .get_items()
            .iter()
            .map(|ts| PyDataset { inner: ts.clone() })
            .collect();
        Ok(py_res)
    }
    fn delete<'py>(&self, py: Python<'py>, input: Vec<DatasetIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        py.detach(|| {
            self.runtime
                .block_on(service.datasets.delete(&input_ids))
                .map_err(|e| PyException::new_err(e.get_message()))
        })?;
        Ok(())
    }
}
