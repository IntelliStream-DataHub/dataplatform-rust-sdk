use intellistream_datahub_sdk::nodes::Node;
use crate::relations::{PyGraphResult, PyRelForm};
use crate::resources::{PyResourceFilter, ResourceIdentifiable};
use crate::resources::{PyResourceNetwork, PyResourceUpdate};
use intellistream_datahub_sdk::resources::ResourceUpdate;
use crate::resources::async_service::PyResourcesServiceAsync;
use crate::{DataSetRef, StringOrList, opt_data_set_refs, opt_patterns};
use intellistream_datahub_sdk::filters::NodeFilter;
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::relations::RelForm;
use intellistream_datahub_sdk::resources::{
    FetchNearestResourcesForm, RelatedResourcesForm, ResourceFilter, ResourceFilterForm,
};
use intellistream_datahub_sdk::ApiService;
use pyo3::{PyResult, Python, pyclass, pymethods};
use std::collections::HashMap;
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "ResourcesServiceSync")]
pub struct PyResourcesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyResourcesServiceSync {
    /// The first `limit` nodes in the tenant, newest created first — the cheap "what have I got"
    /// read, with no criteria and no paging.
    ///
    /// Spans every node type and answers each row as its own class, exactly as `filter` does, so
    /// `isinstance(node, TimeSeries)` works on what comes back. `limit` defaults to the server's
    /// 1000 and may not exceed 10000; a `Page` is not returned because there is no cursor to
    /// continue with — narrow with `filter` instead of raising the number.
    #[pyo3(signature = (limit = None))]
    fn list(&self, py: Python<'_>, limit: Option<u64>) -> PyResult<Vec<crate::nodes::PyNode>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.list(limit))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .map(|r| crate::nodes::PyNode::with_client(r.clone(), service.clone()))
                .collect())
        })
    }

    #[pyo3(signature = (nodes, relations = None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        nodes: Vec<crate::nodes::NodeInput>,
        relations: Option<Vec<PyRelForm>>,
    ) -> PyResult<PyGraphResult> {
        let nodes: Vec<Node> = nodes.into_iter().map(Node::from).collect();
        let rel_forms: Vec<RelForm> = relations
            .unwrap_or_default()
            .into_iter()
            .map(RelForm::from)
            .collect();
        let service = self.api_service.clone();
        let result = py.detach(|| {
            self.runtime
                .block_on(service.resources.create(nodes, rel_forms))
        });

        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(PyGraphResult::from_wrapper(result, service.clone()))
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Vec<crate::nodes::PyNode>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        let result = py.detach(|| self.runtime.block_on(service.resources.by_ids(&input_ids)));

        let result = result.map_err(|e| crate::datahub_err(e))?;

        let py_res: Vec<crate::nodes::PyNode> = result
            .nodes()
            .as_ref()
            .unwrap()
            .iter()
            .map(|ts| crate::nodes::PyNode::with_client(ts.clone(), service.clone()))
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
    #[pyo3(signature = (query, filter = None, limit = None))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        query: String,
        filter: Option<PyResourceFilter>,
        limit: Option<u64>,
    ) -> PyResult<Vec<crate::nodes::PyNode>> {
        let form = crate::search_form(query, filter.map(|f| f.inner), limit);
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.search(&form))
                .map_err(|e| crate::datahub_err(e))?;

            let py_res: Vec<crate::nodes::PyNode> = result
                .get_items()
                .iter()
                .map(|r| crate::nodes::PyNode::with_client(r.clone(), service.clone()))
                .collect();
            Ok(py_res)
        })
    }

    /// Update nodes in place. Each [`ResourceUpdate`] targets one node and carries only the
    /// fields to change; every field it can set is shared by all node types, so one update form
    /// covers them all.
    ///
    /// **The echo is flat.** Unlike every read on this service, the api answers here with each
    /// node shaped as a plain `Resource` whatever its real type, so `.nodes` holds `Resource`
    /// objects even for a timeseries. Re-read the node if you need its typed form. The `labels`
    /// do reflect what the server stored, intrinsic type-label included.
    fn update<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyResourceUpdate>,
    ) -> PyResult<PyGraphResult> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.resources.update(&updates)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(PyGraphResult::from_resource_wrapper(result, service.clone()))
    }


    /// `GET /resources/{id}` — one resource by numeric id. Raises when it does not exist,
    /// unlike `by_ids`, which silently omits what it cannot find.
    fn get_by_id<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Option<crate::nodes::PyNode>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.resources.get_by_id(id)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(result
            .get_items()
            .first()
            .map(|r| crate::nodes::PyNode::with_client(r.clone(), service.clone())))
    }

    /// `POST /resources/filter` — structured lookup; every criterion is combined with AND.
    ///
    /// `external_id`, `name` and `source` are **pattern** lists: `*` and `%` are wildcards, `_`
    /// is literal, matching is case-insensitive, and an entry with no wildcard matches exactly.
    /// Entries OR within a list; the fields AND. Each also accepts a bare string. The singular
    /// `id`/`external_id`/`name` these replaced are gone — a one-element list is the old
    /// behaviour, without the two forms ANDing against each other.
    ///
    /// `labels` must **all** be present; a `None` `metadata` value matches the key alone.
    ///
    /// `data_set_id` takes numeric ids, external ids, or `IdCollection`s — it used to take ids
    /// only — and expands down the dataset hierarchy. **`None` and `[]` differ**: `None` places no
    /// restriction, `[]` narrows to no datasets and matches nothing.
    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, source=None,
                        labels=None, metadata=None, created_time=None, last_updated_time=None,
                        node_type=None, is_root=None, data_set_id=None, limit=None, sort_by=None,
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
        node_type: Option<StringOrList>,
        is_root: Option<bool>,
        data_set_id: Option<Vec<DataSetRef>>,
        limit: Option<u64>,
        sort_by: Option<StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<crate::PyPage> {
        let form = build_resource_filter_form(
            filter, id, external_id, name, source, labels, metadata, created_time,
            last_updated_time, node_type, is_root, data_set_id, limit, sort_by, sort_order,
            cursor,
        )?;
        let service = self.api_service.clone();
        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.filter(&form))
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<crate::nodes::PyNode> = result
                .get_items()
                .iter()
                .map(|r| crate::nodes::PyNode::with_client(r.clone(), service.clone()))
                .collect();
            Ok::<_, pyo3::PyErr>((items, next_cursor))
        })?;
        crate::PyPage::new(py, items, next_cursor)
    }

    /// `POST /resources/fetch-nearest` — the closest `limit` nodes carrying one of `end_labels`,
    /// plus the sub-graph connecting them back to the start.
    ///
    /// Caps on matching end-nodes rather than hop depth, so "the 10 nearest TIMESERIES" is exactly
    /// ten however many nodes lie between. Starts from a numeric `id` only.
    #[pyo3(signature = (id, end_labels=None, limit=None, relationship_types=None, excluded_labels=None))]
    fn fetch_nearest<'py>(
        &self,
        py: Python<'py>,
        id: u64,
        end_labels: Option<Vec<String>>,
        limit: Option<u64>,
        relationship_types: Option<Vec<String>>,
        excluded_labels: Option<Vec<String>>,
    ) -> PyResult<PyResourceNetwork> {
        let form = build_nearest_form(id, end_labels, limit, relationship_types, excluded_labels);
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.fetch_nearest(&form))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyResourceNetwork::from_network(result, service.clone()))
        })
    }

    /// Walk the graph from a starting resource and return the connected sub-graph.
    #[pyo3(signature = (external_id=None, id=None, depth=-1, relationship_types=None, limit=5000))]
    fn fetch_related<'py>(
        &self,
        py: Python<'py>,
        external_id: Option<String>,
        id: Option<u64>,
        depth: i32,
        relationship_types: Option<Vec<String>>,
        limit: i32,
    ) -> PyResult<PyResourceNetwork> {
        let form = RelatedResourcesForm {
            id,
            external_id,
            depth,
            relationship_types,
            limit,
            excluded_labels: vec![],
        };
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.fetch_related(&form))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyResourceNetwork::from_network(result, service.clone()))
        })
    }
}

/// The one place Python filter kwargs become a `ResourceFilter` — shared by `resources.filter`,
/// `resources.search`'s `filter` argument, and the `ResourceFilter` class itself, so the three
/// cannot drift into accepting different things.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_resource_filter(
    id: Option<Vec<u64>>,
    external_id: Option<StringOrList>,
    name: Option<StringOrList>,
    source: Option<StringOrList>,
    labels: Option<StringOrList>,
    metadata: Option<HashMap<String, Option<String>>>,
    created_time: Option<crate::events::PyTimeFilter>,
    last_updated_time: Option<crate::events::PyTimeFilter>,
    node_type: Option<StringOrList>,
    is_root: Option<bool>,
    data_set_id: Option<Vec<DataSetRef>>,
) -> ResourceFilter {
    ResourceFilter {
        node: NodeFilter {
            id,
            external_id: opt_patterns(external_id),
            name: opt_patterns(name),
            source: opt_patterns(source),
            labels: opt_patterns(labels),
            metadata,
            created_time: created_time.map(Into::into),
            last_updated_time: last_updated_time.map(Into::into),
        },
        node_type: opt_patterns(node_type),
        is_root,
        data_set_id: opt_data_set_refs(data_set_id),
    }
}

/// Shared by the sync and async `filter` bindings: turn Python kwargs into a `ResourceFilterForm`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_resource_filter_form(
    filter: Option<crate::resources::PyResourceFilter>,
    id: Option<Vec<u64>>,
    external_id: Option<StringOrList>,
    name: Option<StringOrList>,
    source: Option<StringOrList>,
    labels: Option<StringOrList>,
    metadata: Option<HashMap<String, Option<String>>>,
    created_time: Option<crate::events::PyTimeFilter>,
    last_updated_time: Option<crate::events::PyTimeFilter>,
    node_type: Option<StringOrList>,
    is_root: Option<bool>,
    data_set_id: Option<Vec<DataSetRef>>,
    limit: Option<u64>,
    sort_by: Option<StringOrList>,
    sort_order: Option<String>,
    cursor: Option<String>,
) -> PyResult<ResourceFilterForm> {
    let any_keyword = id.is_some()
        || external_id.is_some()
        || name.is_some()
        || source.is_some()
        || labels.is_some()
        || metadata.is_some()
        || created_time.is_some()
        || last_updated_time.is_some()
        || node_type.is_some()
        || is_root.is_some()
        || data_set_id.is_some();
    let from_keywords = build_resource_filter(
        id, external_id, name, source, labels, metadata, created_time, last_updated_time,
        node_type, is_root, data_set_id,
    );
    let filter = crate::resolve_filter(filter.map(|f| f.inner), from_keywords, any_keyword)?;
    let mut form = ResourceFilterForm::new(filter);
    if let Some(limit) = limit {
        form = form.with_limit(limit);
    }
    Ok(form.with_paging(crate::build_page_request(sort_by, sort_order, cursor)))
}

/// Shared by the sync and async `fetch_nearest` bindings.
pub(crate) fn build_nearest_form(
    id: u64,
    end_labels: Option<Vec<String>>,
    limit: Option<u64>,
    relationship_types: Option<Vec<String>>,
    excluded_labels: Option<Vec<String>>,
) -> FetchNearestResourcesForm {
    FetchNearestResourcesForm {
        id: Some(id),
        end_labels,
        limit,
        relationship_types,
        excluded_labels,
    }
}
