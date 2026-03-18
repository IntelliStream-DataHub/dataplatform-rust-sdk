use std::sync::Arc;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::tokio::future_into_py;
use dataplatform_rust_sdk::{ApiService, Resource};
use dataplatform_rust_sdk::generic::{IdAndExtId};
use crate::{PyIdCollection, PyResource, PySearchAndFilterForm};


#[pyclass]
pub struct PyResourcesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyResourcesServiceAsync {

    fn create<'py>(&self, py: Python<'py>,input: Vec<PyResource>) -> PyResult<Bound<'py, PyAny>> {
        let resources: Vec<Resource> = input.iter().cloned().map(Resource::from).collect();
        //let payload = DataWrapper::from_vec(resources);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.resources.create(&resources).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyResource> = result.nodes().as_ref().unwrap().iter().map(|ts| PyResource { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'py>(&self, py: Python<'py>, input: Vec<PyIdCollection>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .resources
                .by_ids(&input_ids)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyResource> = result
                .nodes()
                .unwrap()
                .iter()
                .map(|u| PyResource { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'py>(&self, py: Python<'py>,input: Vec<PyIdCollection>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service.resources.delete(&input_ids).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyResource> = result.nodes()
                .unwrap()
                .into_iter().map(|res| PyResource { inner: res.clone() }).collect();
            Ok(py_ts)
        })
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