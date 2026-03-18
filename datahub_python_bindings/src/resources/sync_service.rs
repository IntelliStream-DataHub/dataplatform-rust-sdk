use std::sync::Arc;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::tokio::future_into_py;
use dataplatform_rust_sdk::{ApiService, Resource};
use dataplatform_rust_sdk::generic::IdAndExtId;
use crate::{PyIdCollection, PySearchAndFilterForm};
use crate::resources::async_service::PyResourcesServiceAsync;
use crate::resources::PyResource;

#[pyclass]
pub struct PyResourcesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyResourcesServiceSync {

    fn create<'py>(&self, py: Python<'py>,input: Vec<PyResource>) -> PyResult<Vec<PyResource>> {
        let resources: Vec<Resource> = input.iter().cloned().map(Resource::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| {
            self.runtime
                .block_on(service.resources.create(&resources))
        });

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        let py_res: Vec<PyResource> = result.nodes().as_ref().unwrap().iter().map(|ts| PyResource { inner: ts.clone() }).collect();
        Ok(py_res)



    }

    fn by_ids<'py>(&self, py: Python<'py>, input: Vec<PyIdCollection>) -> PyResult<Vec<PyResource>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();

        let result = py.detach(|| {
            self.runtime
                .block_on(service.resources.by_ids(&input_ids))
        });

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        let py_res: Vec<PyResource> = result.nodes().as_ref().unwrap().iter().map(|ts| PyResource { inner: ts.clone() }).collect();
        Ok(py_res)
    }
    fn delete<'py>(&self, py: Python<'py>,input: Vec<PyIdCollection>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();

        let result = py.detach(|| {
            self.runtime
                .block_on(service.resources.by_ids(&input_ids))
        });

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        let py_res: Vec<PyResource> = result.nodes().as_ref().unwrap().iter().map(|ts| PyResource { inner: ts.clone() }).collect();
        Ok(())
        
    }
    fn search<'py>(&self, py: Python<'py>, input: PySearchAndFilterForm) -> PyResult<Bound<'py, PyAny>> {

        todo!()
        /*let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service.resources.search(&input.into()).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyResource> = result.get_items().iter().map(|ts| PyResource { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    */
    }

}