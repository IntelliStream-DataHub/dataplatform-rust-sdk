use crate::policies::{
    require_single, PolicyIdentifiable, PyNamingCheck, PyNamingCheckForm, PyPolicy,
};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::policies::{NamingCheckForm, Policy};
use dataplatform_rust_sdk::ApiService;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "PoliciesServiceAsync")]
pub struct PyPoliciesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyPoliciesServiceAsync {
    /// Every policy node in the tenant.
    fn list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.policies.list().await.map_err(crate::datahub_err)?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyPolicy::from)
                .collect::<Vec<_>>())
        })
    }

    /// The policy templates available to instantiate.
    fn types<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.policies.types().await.map_err(crate::datahub_err)?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyPolicy::from)
                .collect::<Vec<_>>())
        })
    }

    /// A single policy by numeric id, or `None` if no policy has that id.
    fn get<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.policies.get(id).await.map_err(crate::datahub_err)?;
            Ok(result.get_items().first().cloned().map(PyPolicy::from))
        })
    }

    /// Create policies. Every type except `NAMING_CONVENTION` must name a `data_set_id`.
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyPolicy>) -> PyResult<Bound<'py, PyAny>> {
        let policies: Vec<Policy> = input.into_iter().map(Policy::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .policies
                .create(&policies)
                .await
                .map_err(crate::datahub_err)?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyPolicy::from)
                .collect::<Vec<_>>())
        })
    }

    /// Update one policy — the whole policy, not a set-of-changes block. One per call.
    fn update<'py>(&self, py: Python<'py>, input: Vec<PyPolicy>) -> PyResult<Bound<'py, PyAny>> {
        require_single(&input)?;
        let policies: Vec<Policy> = input.into_iter().map(Policy::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .policies
                .update(&policies)
                .await
                .map_err(crate::datahub_err)?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PyPolicy::from)
                .collect::<Vec<_>>())
        })
    }

    /// Delete policies by `Policy`, numeric id, or external id.
    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PolicyIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        future_into_py(py, async move {
            service
                .policies
                .delete(&ids)
                .await
                .map_err(crate::datahub_err)?;
            Ok(())
        })
    }

    /// Preflight candidate external ids against the naming policy. Nothing is written.
    fn check_naming<'py>(
        &self,
        py: Python<'py>,
        form: PyNamingCheckForm,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form: NamingCheckForm = form.into();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .policies
                .check_naming(&form)
                .await
                .map_err(crate::datahub_err)?;
            Ok(PyNamingCheck::from(result))
        })
    }
}
