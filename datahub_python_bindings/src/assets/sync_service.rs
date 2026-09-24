use crate::nodes::PyAsset;
use crate::relations::PyGraphResult;
use crate::resources::sync_service::build_resource_filter_form;
use crate::resources::{PyResourceFilter, PyResourceUpdate, ResourceIdentifiable};
use crate::{DataSetRef, StringOrList};
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::nodes::Asset;
use intellistream_datahub_sdk::resources::ResourceUpdate;
use intellistream_datahub_sdk::ApiService;
use pyo3::{pyclass, pymethods, PyResult, Python};
use std::collections::HashMap;
use std::sync::Arc;

/// The blocking `/assets` surface — the `ASSET`-labelled corner of the resource graph.
///
/// Reached as `client.assets`. The `/resources` pipeline with the type pinned, answering `Asset`.
#[pyclass(module = "intellistream_datahub_sdk", name = "AssetsServiceSync")]
pub struct PyAssetsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyAssetsServiceSync {
    /// Create one or more assets. Each needs an `external_id` and a `name`.
    ///
    /// Unlike `resources.create`, the `ASSET` label does not have to be set by hand — this
    /// endpoint builds assets by definition. Domain labels you do set are kept alongside it.
    ///
    /// Relations are not creatable here; use `resources.create` to build assets and the edges
    /// between them in one call.
    fn create(&self, py: Python<'_>, input: Vec<PyAsset>) -> PyResult<Vec<PyAsset>> {
        let assets: Vec<Asset> = input.into_iter().map(Asset::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.assets.create(&assets))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect())
        })
    }

    /// One asset by numeric id.
    ///
    /// Raises on 404 — and a 404 does not tell you the id is free: a node that exists but is not
    /// an asset, and an asset you may not read, are both reported as missing.
    fn get_by_id(&self, py: Python<'_>, id: u64) -> PyResult<Option<PyAsset>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.assets.get_by_id(id)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(result
            .get_items()
            .first()
            .map(|a| PyAsset::with_client(a.clone(), service.clone())))
    }

    /// Assets by id or external id. Ids that match nothing — or name a node of another type — are
    /// silently omitted rather than raising.
    fn by_ids(&self, py: Python<'_>, input: Vec<ResourceIdentifiable>) -> PyResult<Vec<PyAsset>> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.assets.by_ids(&ids))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect())
        })
    }

    /// The first `limit` assets you may read, newest created first.
    ///
    /// `limit` defaults to the server's 1000 and may not exceed 10000. No paging; narrow with
    /// `filter`.
    #[pyo3(signature = (limit = None))]
    fn list(&self, py: Python<'_>, limit: Option<u64>) -> PyResult<Vec<PyAsset>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.assets.list(limit))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect())
        })
    }

    /// Assets matching every criterion, newest first. Returns a `Page` — `.next_cursor` continues
    /// it.
    ///
    /// The criteria are the resource ones: `external_id`, `name` and `source` are pattern lists
    /// (`*` and `%` are wildcards, `_` is literal, matching is case-insensitive), `labels` must
    /// **all** be present, a `None` `metadata` value matches the key alone, and `data_set_id`
    /// expands down the dataset hierarchy — where `None` places no restriction but `[]` matches
    /// nothing.
    ///
    /// Pass either `filter=` or the individual keywords, not both.
    ///
    /// There is no `node_type` keyword on purpose: this endpoint answers with assets whatever it
    /// is given, so the server replaces it. A `node_type` set on a `filter=` object is discarded
    /// the same way.
    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, source=None,
                        labels=None, metadata=None, created_time=None, last_updated_time=None,
                        is_root=None, data_set_id=None, limit=None, sort_by=None,
                        sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter(
        &self,
        py: Python<'_>,
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
    ) -> PyResult<crate::PyPage> {
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
        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.assets.filter(&form))
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyAsset> = result
                .get_items()
                .iter()
                .map(|a| PyAsset::with_client(a.clone(), service.clone()))
                .collect();
            Ok::<_, pyo3::PyErr>((items, next_cursor))
        })?;
        crate::PyPage::new(py, items, next_cursor)
    }

    /// Free-text search over assets, best match first.
    ///
    /// The phrase selects and `filter` only removes, so a filter can never widen a search.
    /// `query` is required at 3–140 characters; `limit` defaults to 100 and caps at 1000 — both
    /// different from `filter`, which defaults to 1000 and caps at 10000.
    #[pyo3(signature = (query, filter = None, limit = None))]
    fn search(
        &self,
        py: Python<'_>,
        query: String,
        filter: Option<PyResourceFilter>,
        limit: Option<u64>,
    ) -> PyResult<Vec<PyAsset>> {
        let form = crate::search_form(query, filter.map(|f| f.inner), limit);
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.assets.search(&form))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(|a| PyAsset::with_client(a, service.clone()))
                .collect())
        })
    }

    /// Update assets in place. Each `ResourceUpdate` targets one asset and carries only the fields
    /// it changes; `geolocation` is the one that means anything here and nowhere else.
    ///
    /// `.nodes` holds typed node objects, not necessarily all assets — an update may touch
    /// relations whose other end is something else.
    fn update(&self, py: Python<'_>, input: Vec<PyResourceUpdate>) -> PyResult<PyGraphResult> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.assets.update(&updates)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(PyGraphResult::from_wrapper(result, service.clone()))
    }

    /// Delete assets by id or external id. Deleting an asset removes all of its relationships.
    ///
    /// A delete that would disconnect a surviving node from the graph root raises a 409
    /// `would-strand`, naming the blockers on the exception's `problem`.
    fn delete(&self, py: Python<'_>, input: Vec<ResourceIdentifiable>) -> PyResult<()> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.assets.delete(&ids))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }
}
