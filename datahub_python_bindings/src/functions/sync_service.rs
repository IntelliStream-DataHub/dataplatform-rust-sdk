use crate::functions::{FunctionIdentifyable, PyFunction};
use intellistream_datahub_sdk::ApiService;
use intellistream_datahub_sdk::functions::Function;
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::resources::ResourceUpdate;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "FunctionsServiceSync")]
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
                .collect())
        })
    }

    /// The first `limit` functions you may read, newest first. `limit` defaults to the server's
    /// 1000 and may not exceed 10000; there is no paging, so a bigger catalogue is truncated.
    #[pyo3(signature = (limit = None))]
    fn list(&self, py: Python<'_>, limit: Option<u64>) -> PyResult<Vec<PyFunction>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.functions.list(limit))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyFunction::with_client(function, service.clone()))
        })
    }

    /// One function by numeric id.
    ///
    /// Raises on 404 — and a 404 does not tell you the id is free: a function you may not read is
    /// reported as missing rather than forbidden. Prefer this to `by_ids` when you have the id:
    /// `by_ids` has no endpoint behind it and pages the whole listing to filter client-side.
    fn get_by_id(&self, py: Python<'_>, id: u64) -> PyResult<Option<PyFunction>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.functions.get_by_id(id)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(result
            .get_items()
            .first()
            .map(|f| PyFunction::with_client(f.clone(), service.clone())))
    }

    /// Update functions in place. Each `ResourceUpdate` targets one function and carries only the
    /// fields it changes; `geolocation` is ignored, being an asset-only field.
    ///
    /// `.nodes` holds typed node objects — a function comes back as `Function`.
    fn update(
        &self,
        py: Python<'_>,
        input: Vec<crate::resources::PyResourceUpdate>,
    ) -> PyResult<crate::relations::PyGraphResult> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.functions.update(&updates)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(crate::relations::PyGraphResult::from_wrapper(
            result,
            service.clone(),
        ))
    }

    fn delete(&self, py: Python<'_>, input: Vec<FunctionIdentifyable>) -> PyResult<()> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.functions.delete(&ids))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }
}
