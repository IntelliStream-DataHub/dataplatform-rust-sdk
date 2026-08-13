use crate::relations::{PyGraphResult, PyRelForm};
use crate::resources::{PyResourceFilter, ResourceIdentifiable};
use crate::resources::{PyResource, PyResourceNetwork, PyResourceUpdate};
use dataplatform_rust_sdk::resources::ResourceUpdate;
use crate::resources::async_service::PyResourcesServiceAsync;
use crate::{DataSetRef, PySearchAndFilterForm, StringOrList, opt_data_set_refs, opt_patterns};
use dataplatform_rust_sdk::filters::NodeFilter;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::RelForm;
use dataplatform_rust_sdk::resources::{
    FetchNearestResourcesForm, RelatedResourcesForm, ResourceFilter, ResourceRetreiver,
};
use dataplatform_rust_sdk::{ApiService, Resource};
use pyo3::{PyResult, Python, pyclass, pymethods};
use std::collections::HashMap;
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
        Ok(PyGraphResult::from_wrapper(result, service.clone()))
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
            .map(|ts| PyResource::with_client(ts.clone(), service.clone()))
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
    #[pyo3(signature = (input, filter = None))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        input: PySearchAndFilterForm,
        filter: Option<PyResourceFilter>,
    ) -> PyResult<Vec<PyResource>> {
        let form = input.into_form(filter.map(|f| f.inner));
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.search(&form))
                .map_err(|e| crate::datahub_err(e))?;

            let py_res: Vec<PyResource> = result
                .get_items()
                .iter()
                .map(|r| PyResource::with_client(r.clone(), service.clone()))
                .collect();
            Ok(py_res)
        })
    }

    /// Update resources in place. Each [`ResourceUpdate`] targets one resource and carries only
    /// the fields to change. Returns the updated graph, whose node labels reflect what the server
    /// stored (the intrinsic type-label is always kept).
    fn update<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyResourceUpdate>,
    ) -> PyResult<PyGraphResult> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.resources.update(&updates)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(PyGraphResult::from_wrapper(result, service.clone()))
    }


    /// `GET /resources/{id}` — one resource by numeric id. Raises when it does not exist,
    /// unlike `by_ids`, which silently omits what it cannot find.
    fn get_by_id<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Option<PyResource>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.resources.get_by_id(id)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(result
            .get_items()
            .first()
            .map(|r| PyResource::with_client(r.clone(), service.clone())))
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
    #[pyo3(signature = (id=None, external_id=None, name=None, source=None, labels=None,
                        metadata=None, created_time=None, last_updated_time=None, node_type=None,
                        is_root=None, data_set_id=None, limit=None, sort_by=None, sort_order=None,
                        cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter<'py>(
        &self,
        py: Python<'py>,
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
        let retriever = build_resource_retriever(
            id, external_id, name, source, labels, metadata, created_time,
            last_updated_time, node_type, is_root, data_set_id, limit, sort_by, sort_order,
            cursor,
        );
        let service = self.api_service.clone();
        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.filter(&retriever))
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyResource> = result
                .get_items()
                .iter()
                .map(|r| PyResource::with_client(r.clone(), service.clone()))
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

/// Shared by the sync and async `filter` bindings: turn Python kwargs into a `ResourceRetreiver`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_resource_retriever(
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
) -> ResourceRetreiver {
    let filter = build_resource_filter(
        id, external_id, name, source, labels, metadata, created_time, last_updated_time,
        node_type, is_root, data_set_id,
    );
    let mut retriever = ResourceRetreiver::new(filter);
    if let Some(limit) = limit {
        retriever = retriever.with_limit(limit);
    }
    retriever.with_paging(crate::build_page_request(sort_by, sort_order, cursor))
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
