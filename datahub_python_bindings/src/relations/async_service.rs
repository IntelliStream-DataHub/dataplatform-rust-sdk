use crate::relations::{
    EdgeIdentifiable, PyEdgeProxy, PyGraphResult, PyRelForm, PyRelTypeForm, PyRelationshipType,
};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::relations::{RelForm, RelTypeForm};
use dataplatform_rust_sdk::ApiService;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "EdgesServiceAsync")]
pub struct PyEdgesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyEdgesServiceAsync {
    /// One relationship by numeric id, or `None` if no edge has that id. An unknown id is a 404
    /// server-side; that is absorbed into `None`, matching the other `get()` methods here.
    fn get<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let r = crate::none_on_404(service.edges.get(id).await)?;
            Ok(r.and_then(|w| w.get_items().first().cloned().map(PyEdgeProxy::from)))
        })
    }

    /// Several relationships plus the resources they connect, as a `GraphResult`.
    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<EdgeIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        future_into_py(py, async move {
            let wrapper = service
                .edges
                .by_ids(&ids)
                .await
                .map_err(crate::datahub_err)?;
            Ok(PyGraphResult::from_wrapper(wrapper, service))
        })
    }

    /// Link resources that already exist. All-or-nothing; a duplicate edge conflicts with 409.
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyRelForm>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let forms: Vec<RelForm> = input.into_iter().map(RelForm::from).collect();
        future_into_py(py, async move {
            let r = service
                .edges
                .create(&forms)
                .await
                .map_err(crate::datahub_err)?;
            Ok(r.get_items()
                .iter()
                .cloned()
                .map(PyEdgeProxy::from)
                .collect::<Vec<_>>())
        })
    }

    /// Delete relationships by `EdgeProxy` or numeric id. The endpoint resources stay intact.
    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<EdgeIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        future_into_py(py, async move {
            service
                .edges
                .delete(&ids)
                .await
                .map_err(crate::datahub_err)?;
            Ok(())
        })
    }

    /// Every relationship type the tenant has defined.
    fn types<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let r = service.edges.types().await.map_err(crate::datahub_err)?;
            Ok(r.get_items()
                .iter()
                .cloned()
                .map(PyRelationshipType::from)
                .collect::<Vec<_>>())
        })
    }

    /// Register relationship type names up front. See the sync service for the duplicate-name
    /// caveat.
    fn create_types<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyRelTypeForm>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let forms: Vec<RelTypeForm> = input.into_iter().map(RelTypeForm::from).collect();
        future_into_py(py, async move {
            let r = service
                .edges
                .create_types(&forms)
                .await
                .map_err(crate::datahub_err)?;
            Ok(r.get_items()
                .iter()
                .cloned()
                .map(PyRelationshipType::from)
                .collect::<Vec<_>>())
        })
    }
}
