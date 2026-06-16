use crate::relations::{PyGraphResult, PyRelForm};
use crate::resources::ResourceIdentifiable;
use crate::{PyResource, PySearchAndFilterForm};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::RelForm;
use dataplatform_rust_sdk::{ApiService, Resource};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "ResourcesServiceAsync")]
pub struct PyResourcesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyResourcesServiceAsync {
    #[pyo3(signature = (nodes, relations = None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        nodes: Vec<PyResource>,
        relations: Option<Vec<PyRelForm>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let resources: Vec<Resource> = nodes.into_iter().map(Resource::from).collect();
        let rel_forms: Vec<RelForm> = relations
            .unwrap_or_default()
            .into_iter()
            .map(RelForm::from)
            .collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .create(resources, rel_forms)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyGraphResult::from_wrapper(result))
        })
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .resources
                .by_ids(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyResource> = result
                .nodes()
                .unwrap()
                .iter()
                .map(|u| PyResource { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .resources
                .delete(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyResource> = result
                .nodes()
                .unwrap()
                .into_iter()
                .map(|res| PyResource { inner: res.clone() })
                .collect();
            Ok(py_ts)
        })
    }
    fn search<'py>(
        &self,
        py: Python<'py>,
        input: PySearchAndFilterForm,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .resources
                .search(&input.into())
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_res: Vec<PyResource> = result
                .get_items()
                .iter()
                .map(|r| PyResource { inner: r.clone() })
                .collect();
            Ok(py_res)
        })
    }
}
