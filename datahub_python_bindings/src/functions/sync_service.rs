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

    /// Functions matching every criterion, newest created first unless `sort_by` says otherwise.
    /// Returns a `Page`; send its `next_cursor` back as `cursor`, with the same sort, for the next.
    ///
    /// Pass either `filter=` or the individual keywords, not both.
    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, source=None,
                        labels=None, metadata=None, created_time=None, last_updated_time=None,
                        data_set_id=None, limit=None, sort_by=None, sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter(
        &self,
        py: Python<'_>,
        filter: Option<crate::functions::PyFunctionFilter>,
        id: Option<Vec<u64>>,
        external_id: Option<crate::StringOrList>,
        name: Option<crate::StringOrList>,
        source: Option<crate::StringOrList>,
        labels: Option<crate::StringOrList>,
        metadata: Option<std::collections::HashMap<String, Option<String>>>,
        created_time: Option<crate::events::PyTimeFilter>,
        last_updated_time: Option<crate::events::PyTimeFilter>,
        data_set_id: Option<Vec<crate::DataSetRef>>,
        limit: Option<u64>,
        sort_by: Option<crate::StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<crate::PyPage> {
        let form = crate::functions::build_function_filter_form(
            filter, id, external_id, name, source, labels, metadata, created_time,
            last_updated_time, data_set_id, limit, sort_by, sort_order, cursor,
        )?;
        let service = self.api_service.clone();
        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.functions.filter(&form))
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyFunction> = result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
                .collect();
            Ok::<_, pyo3::PyErr>((items, next_cursor))
        })?;
        crate::PyPage::new(py, items, next_cursor)
    }

    /// Free-text search over functions, best match first. The phrase selects and `filter` only
    /// removes. `query` is required at 3–140 characters; `limit` defaults to 100 and caps at 1000.
    #[pyo3(signature = (query, filter = None, limit = None))]
    fn search(
        &self,
        py: Python<'_>,
        query: String,
        filter: Option<crate::functions::PyFunctionFilter>,
        limit: Option<u64>,
    ) -> PyResult<Vec<PyFunction>> {
        let form = crate::search_form(query, filter.map(|f| f.inner), limit);
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.functions.search(&form))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|f| PyFunction::with_client(f, service.clone()))
                .collect())
        })
    }

    /// Look up functions by id or external id. What does not exist, or you may not read, is
    /// left out rather than raising.
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
    /// reported as missing rather than forbidden.
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
