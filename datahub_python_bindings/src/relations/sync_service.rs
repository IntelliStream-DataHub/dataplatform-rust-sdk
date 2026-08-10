use crate::relations::{
    EdgeIdentifiable, PyEdgeProxy, PyGraphResult, PyRelForm, PyRelTypeForm, PyRelationshipType,
};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::{RelForm, RelTypeForm};
use dataplatform_rust_sdk::ApiService;
use pyo3::{pyclass, pymethods, PyResult, Python};
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "EdgesServiceSync")]
pub struct PyEdgesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyEdgesServiceSync {
    /// One relationship by numeric id, or `None` if no edge has that id.
    ///
    /// The server answers an unknown id with 404; that is absorbed into `None` here, matching the
    /// other `get()` methods in these bindings. Any other error still raises.
    fn get(&self, py: Python<'_>, id: u64) -> PyResult<Option<PyEdgeProxy>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let r = crate::none_on_404(self.runtime.block_on(service.edges.get(id)))?;
            Ok(r.and_then(|w| w.get_items().first().cloned().map(PyEdgeProxy::from)))
        })
    }

    /// Several relationships plus the resources they connect, as a `GraphResult` — `nodes` holds
    /// both endpoints of each edge and `relations` the edges, so no follow-up call is needed.
    fn by_ids(&self, py: Python<'_>, input: Vec<EdgeIdentifiable>) -> PyResult<PyGraphResult> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let wrapper = py.detach(|| {
            self.runtime
                .block_on(service.edges.by_ids(&ids))
                .map_err(crate::datahub_err)
        })?;
        Ok(PyGraphResult::from_wrapper(wrapper, service))
    }

    /// Link resources that already exist. To create the resources *and* their links together, use
    /// `resources.create(nodes, relations)` instead.
    ///
    /// All-or-nothing: if any relation in the batch fails, none are created. A relation targeting
    /// a dataset must use `BELONGS_TO`; a timeseries cannot be linked to a second dataset; you
    /// need write access to the datasets of both endpoints. Re-creating an existing edge between
    /// the same two resources conflicts with status 409.
    fn create(&self, py: Python<'_>, input: Vec<PyRelForm>) -> PyResult<Vec<PyEdgeProxy>> {
        let service = self.api_service.clone();
        let forms: Vec<RelForm> = input.into_iter().map(RelForm::from).collect();
        py.detach(|| {
            let r = self
                .runtime
                .block_on(service.edges.create(&forms))
                .map_err(crate::datahub_err)?;
            Ok(r.get_items()
                .iter()
                .cloned()
                .map(PyEdgeProxy::from)
                .collect())
        })
    }

    /// Delete relationships by `EdgeProxy` or numeric id. Deletes the link only — the resources at
    /// each end stay intact. Idempotent: unknown ids are silently skipped.
    fn delete(&self, py: Python<'_>, input: Vec<EdgeIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        py.detach(|| {
            self.runtime
                .block_on(service.edges.delete(&ids))
                .map_err(crate::datahub_err)
        })?;
        Ok(())
    }

    /// Every relationship type the tenant has defined.
    fn types(&self, py: Python<'_>) -> PyResult<Vec<PyRelationshipType>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let r = self
                .runtime
                .block_on(service.edges.types())
                .map_err(crate::datahub_err)?;
            Ok(r.get_items()
                .iter()
                .cloned()
                .map(PyRelationshipType::from)
                .collect())
        })
    }

    /// Register relationship type names up front. Names normalise to uppercase snake case.
    ///
    /// A name that already exists currently makes the server fail silently — it answers 200 with
    /// an empty body, and in a batch the valid new types are rolled back alongside the duplicate.
    /// Treat an empty result as "something already existed and nothing was created", and use
    /// `types()` to read the real state.
    fn create_types(
        &self,
        py: Python<'_>,
        input: Vec<PyRelTypeForm>,
    ) -> PyResult<Vec<PyRelationshipType>> {
        let service = self.api_service.clone();
        let forms: Vec<RelTypeForm> = input.into_iter().map(RelTypeForm::from).collect();
        py.detach(|| {
            let r = self
                .runtime
                .block_on(service.edges.create_types(&forms))
                .map_err(crate::datahub_err)?;
            Ok(r.get_items()
                .iter()
                .cloned()
                .map(PyRelationshipType::from)
                .collect())
        })
    }
}
