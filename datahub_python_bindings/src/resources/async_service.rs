use crate::relations::{PyGraphResult, PyRelForm};
use crate::resources::{PyResourceFilter, PyResourceNetwork, PyResourceUpdate, ResourceIdentifiable};
use dataplatform_rust_sdk::resources::ResourceUpdate;
use crate::{DataSetRef, PyResource, PySearchAndFilterForm, StringOrList};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::RelForm;
use dataplatform_rust_sdk::resources::RelatedResourcesForm;
use dataplatform_rust_sdk::{ApiService, Resource};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "ResourcesServiceAsync")]
pub struct PyResourcesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyResourcesServiceAsync {
    #[pyo3(signature = (nodes, relations = None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        nodes: Vec<PyResource>,
        relations: Option<Vec<PyRelForm>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let resources: Vec<Resource> = nodes.into_iter().map(Resource::from).collect();
        let rel_forms: Vec<RelForm> = relations
            .unwrap_or_default()
            .into_iter()
            .map(RelForm::from)
            .collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .create(resources, rel_forms)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyGraphResult::from_wrapper(result, service.clone()))
        })
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .resources
                .by_ids(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyResource> = result
                .nodes()
                .unwrap()
                .iter()
                .map(|u| PyResource::with_client(u.clone(), service.clone()))
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<ResourceIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .resources
                .delete(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyResource> = result
                .nodes().unwrap_or_default()
                .into_iter()
                .map(|res| PyResource::with_client(res.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }
    #[pyo3(signature = (input, filter = None))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        input: PySearchAndFilterForm,
        filter: Option<PyResourceFilter>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = input.into_form(filter.map(|f| f.inner));
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .resources
                .search(&form)
                .await
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .update(&updates)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyGraphResult::from_wrapper(result, service.clone()))
        })
    }

    /// `GET /resources/{id}` — one resource by numeric id. Raises when it does not exist,
    /// unlike `by_ids`, which silently omits what it cannot find.
    fn get_by_id<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .get_by_id(id)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .first()
                .map(|r| PyResource::with_client(r.clone(), service.clone())))
        })
    }

    /// `POST /resources/filter` — structured lookup; every criterion is combined with AND.
    /// See the sync twin for the pattern, label and data-set-scope rules.
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let retriever = crate::resources::sync_service::build_resource_retriever(
            id, external_id, name, source, labels, metadata, created_time,
            last_updated_time, node_type, is_root, data_set_id, limit, sort_by, sort_order,
            cursor,
        );
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .filter(&retriever)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyResource> = result
                .get_items()
                .iter()
                .map(|r| PyResource::with_client(r.clone(), service.clone()))
                .collect();
            Python::attach(|py| crate::PyPage::new(py, items, next_cursor))
        })
    }

    /// `POST /resources/fetch-nearest` — the closest `limit` nodes carrying one of `end_labels`,
    /// plus the sub-graph connecting them back to the start. Starts from a numeric `id` only.
    #[pyo3(signature = (id, end_labels=None, limit=None, relationship_types=None, excluded_labels=None))]
    fn fetch_nearest<'py>(
        &self,
        py: Python<'py>,
        id: u64,
        end_labels: Option<Vec<String>>,
        limit: Option<u64>,
        relationship_types: Option<Vec<String>>,
        excluded_labels: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = crate::resources::sync_service::build_nearest_form(
            id, end_labels, limit, relationship_types, excluded_labels,
        );
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .fetch_nearest(&form)
                .await
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = RelatedResourcesForm {
            id,
            external_id,
            depth,
            relationship_types,
            limit,
            excluded_labels: vec![],
        };
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .fetch_related(&form)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyResourceNetwork::from_network(result, service.clone()))
        })
    }
}
