use crate::relations::{PyGraphResult, PyRelForm};
use crate::resources::ResourceIdentifiable;
use crate::resources::PyResource;
use crate::resources::async_service::PyResourcesServiceAsync;
use crate::PySearchAndFilterForm;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::RelForm;
use dataplatform_rust_sdk::{ApiService, Resource};
use pyo3::{PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "ResourcesServiceSync")]
pub struct PyResourcesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyResourcesServiceSync {
    #[pyo3(signature = (nodes, relations = None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        nodes: Vec<PyResource>,
        relations: Option<Vec<PyRelForm>>,
    ) -> PyResult<PyGraphResult> {
        let resources: Vec<Resource> = nodes.into_iter().map(Resource::from).collect();
        let rel_forms: Vec<RelForm> = relations
            .unwrap_or_default()
            .into_iter()
            .map(RelForm::from)
            .collect();
        let service = self.api_service.clone();
        let result = py.detach(|| {
            self.runtime
                .block_on(service.resources.create(resources, rel_forms))
        });

        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(PyGraphResult::from_wrapper(result))
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Vec<PyResource>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        let result = py.detach(|| self.runtime.block_on(service.resources.by_ids(&input_ids)));

        let result = result.map_err(|e| crate::datahub_err(e))?;

        let py_res: Vec<PyResource> = result
            .nodes()
            .as_ref()
            .unwrap()
            .iter()
            .map(|ts| PyResource { inner: ts.clone() })
            .collect();
        Ok(py_res)
    }
    fn delete<'py>(&self, py: Python<'py>, input: Vec<ResourceIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        py.detach(|| {
            self.runtime
                .block_on(service.resources.delete(&input_ids))
                .map_err(|e| crate::datahub_err(e))
        })?;

        Ok(())
    }
    fn search<'py>(
        &self,
        py: Python<'py>,
        input: PySearchAndFilterForm,
    ) -> PyResult<Vec<PyResource>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.search(&input.into()))
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
