use crate::relations::{PyGraphResult, PyRelForm};
use crate::resources::ResourceIdentifiable;
use crate::resources::{PyResource, PyResourceNetwork, PyResourceUpdate};
use dataplatform_rust_sdk::resources::ResourceUpdate;
use crate::resources::async_service::PyResourcesServiceAsync;
use crate::PySearchAndFilterForm;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::RelForm;
use dataplatform_rust_sdk::resources::{
    FetchNearestResourcesForm, IdObject, RelatedResourcesForm, ResourceFilter, ResourceRetreiver,
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
    /// `name` and `source` are case-insensitive substring matches accepting `%` as a wildcard;
    /// `external_id` and `id` are exact. `data_set_ids` takes numeric ids only.
    #[pyo3(signature = (id=None, external_id=None, name=None, source=None, is_root=None,
                        data_set_ids=None, metadata=None, limit=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter<'py>(
        &self,
        py: Python<'py>,
        id: Option<u64>,
        external_id: Option<String>,
        name: Option<String>,
        source: Option<String>,
        is_root: Option<bool>,
        data_set_ids: Option<Vec<u64>>,
        metadata: Option<HashMap<String, String>>,
        limit: Option<u64>,
    ) -> PyResult<Vec<PyResource>> {
        let retriever = build_resource_retriever(
            id, external_id, name, source, is_root, data_set_ids, metadata, limit,
        );
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.resources.filter(&retriever))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .map(|r| PyResource::with_client(r.clone(), service.clone()))
                .collect())
        })
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

/// Shared by the sync and async `filter` bindings: turn Python kwargs into a `ResourceRetreiver`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_resource_retriever(
    id: Option<u64>,
    external_id: Option<String>,
    name: Option<String>,
    source: Option<String>,
    is_root: Option<bool>,
    data_set_ids: Option<Vec<u64>>,
    metadata: Option<HashMap<String, String>>,
    limit: Option<u64>,
) -> ResourceRetreiver {
    let filter = ResourceFilter {
        id,
        external_id,
        name,
        source,
        is_root,
        data_set_ids: data_set_ids
            .map(|ids| ids.into_iter().map(IdObject::new).collect()),
        metadata,
        created_time: None,
        last_updated_time: None,
    };
    let mut retriever = ResourceRetreiver::new(filter);
    if let Some(limit) = limit {
        retriever = retriever.with_limit(limit);
    }
    retriever
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
