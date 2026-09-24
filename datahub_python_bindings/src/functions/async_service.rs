use crate::functions::{FunctionIdentifyable, PyFunction};
use intellistream_datahub_sdk::ApiService;
use intellistream_datahub_sdk::functions::Function;
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::resources::ResourceUpdate;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

/// Awaitable twin of `FunctionsServiceSync`, reached as `client.functions` on an
/// `AsyncDataHubClient`.
#[pyclass(module = "intellistream_datahub_sdk", name = "FunctionsServiceAsync")]
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
                .collect::<Vec<_>>())
        })
    }

    /// The first `limit` functions you may read, newest first. `limit` defaults to the server's
    /// 1000 and may not exceed 10000; there is no paging, so a bigger catalogue is truncated.
    #[pyo3(signature = (limit = None))]
    fn list<'py>(&self, py: Python<'py>, limit: Option<u64>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .functions
                .list(limit)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyFunction::with_client(function, service.clone()))
        })
    }

    /// One function by numeric id.
    ///
    /// Raises on 404 — and a 404 does not tell you the id is free: a function you may not read is
    /// reported as missing rather than forbidden.
    fn get_by_id<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .functions
                .get_by_id(id)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .first()
                .map(|f| PyFunction::with_client(f.clone(), service.clone())))
        })
    }

    /// Update functions in place. `.nodes` holds typed node objects — a function comes back as
    /// `Function`.
    fn update<'py>(
        &self,
        py: Python<'py>,
        input: Vec<crate::resources::PyResourceUpdate>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .functions
                .update(&updates)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(crate::relations::PyGraphResult::from_wrapper(
                result,
                service.clone(),
            ))
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }
}
