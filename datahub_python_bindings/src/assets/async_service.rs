use crate::nodes::PyAsset;
use crate::relations::PyGraphResult;
use crate::resources::sync_service::build_resource_filter_form;
use crate::resources::{PyResourceFilter, PyResourceUpdate, ResourceIdentifiable};
use crate::{DataSetRef, StringOrList};
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::nodes::Asset;
use intellistream_datahub_sdk::resources::ResourceUpdate;
use intellistream_datahub_sdk::ApiService;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;

/// Awaitable twin of `AssetsServiceSync`, reached as `client.assets` on an
/// `AsyncDataHubClient`.
///
/// Same methods, same arguments, same semantics — each returns an awaitable instead of
/// blocking. `AssetsServiceSync` carries the per-method documentation.
#[pyclass(module = "intellistream_datahub_sdk", name = "AssetsServiceAsync")]
pub struct PyAssetsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyAssetsServiceAsync {
    /// Create one or more assets. Each needs an `external_id` and a `name`.
    ///
    /// Unlike `resources.create`, the `ASSET` label does not have to be set by hand — this
    /// endpoint builds assets by definition.
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyAsset>) -> PyResult<Bound<'py, PyAny>> {
        let assets: Vec<Asset> = input.into_iter().map(Asset::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .assets
                .create(&assets)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect::<Vec<_>>())
        })
    }

    /// One asset by numeric id.
    ///
    /// Raises on 404 — and a 404 does not tell you the id is free: a node that exists but is not
    /// an asset, and an asset you may not read, are both reported as missing.
    fn get_by_id<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .assets
                .get_by_id(id)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .first()
                .map(|a| PyAsset::with_client(a.clone(), service.clone())))
        })
    }

    /// Assets by id or external id. Ids that match nothing — or name a node of another type — are
    /// silently omitted rather than raising.
    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .assets
                .by_ids(&ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect::<Vec<_>>())
        })
    }

    /// The first `limit` assets you may read, newest created first.
    ///
    /// `limit` defaults to the server's 1000 and may not exceed 10000. A plain list rather than a
    /// `Page`: there is no cursor to continue with, so narrow with `filter` instead.
    #[pyo3(signature = (limit = None))]
    fn list<'py>(&self, py: Python<'py>, limit: Option<u64>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .assets
                .list(limit)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect::<Vec<_>>())
        })
    }

    /// Assets matching every criterion, newest first. Returns a `Page` — `.next_cursor` continues
    /// it.
    ///
    /// Pass either `filter=` or the individual keywords, not both. There is no `node_type`
    /// keyword on purpose: this endpoint answers with assets whatever it is given, so the server
    /// replaces it.
    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, source=None,
                        labels=None, metadata=None, created_time=None, last_updated_time=None,
                        is_root=None, data_set_id=None, limit=None, sort_by=None,
                        sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter<'py>(
        &self,
        py: Python<'py>,
        filter: Option<PyResourceFilter>,
        id: Option<Vec<u64>>,
        external_id: Option<StringOrList>,
        name: Option<StringOrList>,
        source: Option<StringOrList>,
        labels: Option<StringOrList>,
        metadata: Option<HashMap<String, Option<String>>>,
        created_time: Option<crate::events::PyTimeFilter>,
        last_updated_time: Option<crate::events::PyTimeFilter>,
        is_root: Option<bool>,
        data_set_id: Option<Vec<DataSetRef>>,
        limit: Option<u64>,
        sort_by: Option<StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = build_resource_filter_form(
            filter,
            id,
            external_id,
            name,
            source,
            labels,
            metadata,
            created_time,
            last_updated_time,
            None,
            is_root,
            data_set_id,
            limit,
            sort_by,
            sort_order,
            cursor,
        )?;
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .assets
                .filter(&form)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyAsset> = result
                .get_items()
                .iter()
                .map(|a| PyAsset::with_client(a.clone(), service.clone()))
                .collect();
            Python::attach(|py| crate::PyPage::new(py, items, next_cursor))
        })
    }

    /// Free-text search over assets, best match first.
    ///
    /// The phrase selects and `filter` only removes. `query` is required at 3–140 characters;
    /// `limit` defaults to 100 and caps at 1000.
    #[pyo3(signature = (query, filter = None, limit = None))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        query: String,
        filter: Option<PyResourceFilter>,
        limit: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = crate::search_form(query, filter.map(|f| f.inner), limit);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .assets
                .search(&form)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect::<Vec<_>>())
        })
    }

    /// Update assets in place. `geolocation` is the one field that means anything here and
    /// nowhere else.
    ///
    /// `.nodes` holds typed node objects, not necessarily all assets — an update may touch
    /// relations whose other end is something else.
    fn update<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyResourceUpdate>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .assets
                .update(&updates)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyGraphResult::from_wrapper(result, service.clone()))
        })
    }

    /// Delete assets by id or external id. A delete that would strand a surviving node raises a
    /// 409 `would-strand`, naming the blockers on the exception's `problem`.
    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .assets
                .delete(&ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }
}
