use crate::functions::{FunctionIdentifyable, PyFunction};
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::functions::Function;
use dataplatform_rust_sdk::generic::IdAndExtId;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_python_sdk", name = "FunctionsServiceAsync")]
pub struct PyFunctionsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyFunctionsServiceAsync {
    fn create<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyFunction>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let fns: Vec<Function> = input.into_iter().map(Function::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .functions
                .create(&fns)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyFunction::from)
                .collect::<Vec<_>>())
        })
    }

    fn list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .functions
                .list()
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyFunction::from)
                .collect::<Vec<_>>())
        })
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<FunctionIdentifyable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .functions
                .by_ids(&ids)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyFunction::from)
                .collect::<Vec<_>>())
        })
    }

    fn by_external_id<'py>(
        &self,
        py: Python<'py>,
        external_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let function = service
                .functions
                .by_external_id(&external_id)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(PyFunction::from(function))
        })
    }

    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<FunctionIdentifyable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .functions
                .delete(&ids)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(())
        })
    }
}
