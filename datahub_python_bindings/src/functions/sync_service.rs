use crate::functions::{FunctionIdentifyable, PyFunction};
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::functions::Function;
use dataplatform_rust_sdk::generic::IdAndExtId;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(module = "datahub_python_sdk", name = "FunctionsServiceSync")]
pub struct PyFunctionsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyFunctionsServiceSync {
    fn create(&self, py: Python<'_>, input: Vec<PyFunction>) -> PyResult<Vec<PyFunction>> {
        let fns: Vec<Function> = input.into_iter().map(Function::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.functions.create(&fns))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyFunction::from)
                .collect())
        })
    }

    /// List every function visible to the calling tenant.
    fn list(&self, py: Python<'_>) -> PyResult<Vec<PyFunction>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.functions.list())
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyFunction::from)
                .collect())
        })
    }

    fn by_ids(
        &self,
        py: Python<'_>,
        input: Vec<FunctionIdentifyable>,
    ) -> PyResult<Vec<PyFunction>> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.functions.by_ids(&ids))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyFunction::from)
                .collect())
        })
    }

    /// Convenience for the function-worker bootstrap: returns the function with the given
    /// externalId, or raises if no such function exists.
    fn by_external_id(&self, py: Python<'_>, external_id: String) -> PyResult<PyFunction> {
        let service = self.api_service.clone();
        py.detach(|| {
            let function = self
                .runtime
                .block_on(service.functions.by_external_id(&external_id))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(PyFunction::from(function))
        })
    }

    fn delete(&self, py: Python<'_>, input: Vec<FunctionIdentifyable>) -> PyResult<()> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.functions.delete(&ids))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(())
        })
    }
}
