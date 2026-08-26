use intellistream_datahub_sdk::nodes::Node;
use crate::relations::{PyGraphResult, PyRelForm};
use crate::resources::{PyResourceFilter, PyResourceNetwork, PyResourceUpdate, ResourceIdentifiable};
use intellistream_datahub_sdk::resources::ResourceUpdate;
use crate::{DataSetRef, StringOrList};
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::relations::RelForm;
use intellistream_datahub_sdk::resources::RelatedResourcesForm;
use intellistream_datahub_sdk::ApiService;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::collections::HashMap;
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "ResourcesServiceAsync")]
pub struct PyResourcesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyResourcesServiceAsync {
    #[pyo3(signature = (nodes, relations = None))]
    fn create<'py>(
        &self,
        py: Python<'py>,
        nodes: Vec<crate::nodes::NodeInput>,
        relations: Option<Vec<PyRelForm>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let nodes: Vec<Node> = nodes.into_iter().map(Node::from).collect();
        let rel_forms: Vec<RelForm> = relations
            .unwrap_or_default()
            .into_iter()
            .map(RelForm::from)
            .collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .create(nodes, rel_forms)
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

            let py_units: Vec<crate::nodes::PyNode> = result
                .nodes()
                .unwrap()
                .iter()
                .map(|u| crate::nodes::PyNode::with_client(u.clone(), service.clone()))
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

            // `delete` answers 204 with no body, so this list is always empty; the api types
            // the echo as flat resources, so that is what it is wrapped as.
            let py_ts: Vec<crate::nodes::PyNode> = result
                .nodes().unwrap_or_default()
                .into_iter()
                .map(|res| crate::nodes::PyNode::with_client(Node::Resource(res), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }
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
                .resources
                .search(&form)
                .await
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let updates: Vec<ResourceUpdate> = input.into_iter().map(ResourceUpdate::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .update(&updates)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(PyGraphResult::from_resource_wrapper(result, service.clone()))
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
                .map(|r| crate::nodes::PyNode::with_client(r.clone(), service.clone())))
        })
    }

    /// `POST /resources/filter` — structured lookup; every criterion is combined with AND.
    /// See the sync twin for the pattern, label and data-set-scope rules.
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = crate::resources::sync_service::build_resource_filter_form(
            filter, id, external_id, name, source, labels, metadata, created_time,
            last_updated_time, node_type, is_root, data_set_id, limit, sort_by, sort_order,
            cursor,
        )?;
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .resources
                .filter(&form)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<crate::nodes::PyNode> = result
                .get_items()
                .iter()
                .map(|r| crate::nodes::PyNode::with_client(r.clone(), service.clone()))
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
