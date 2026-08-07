use crate::policies::{
    require_single, PolicyIdentifiable, PyNamingCheck, PyNamingCheckForm, PyPolicy,
};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::policies::{NamingCheckForm, Policy};
use dataplatform_rust_sdk::ApiService;
use pyo3::{pyclass, pymethods, PyResult, Python};
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "PoliciesServiceSync")]
pub struct PyPoliciesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyPoliciesServiceSync {
    /// Every policy node in the tenant.
    fn list<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyPolicy>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.policies.list()));
        let result = result.map_err(crate::datahub_err)?;
        Ok(result
            .get_items()
            .iter()
            .cloned()
            .map(PyPolicy::from)
            .collect())
    }

    /// The policy templates available to instantiate. These are synthesised from the server's
    /// policy-type enum, so they carry a `name`, a `type` and a description but no `id`.
    fn types<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyPolicy>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.policies.types()));
        let result = result.map_err(crate::datahub_err)?;
        Ok(result
            .get_items()
            .iter()
            .cloned()
            .map(PyPolicy::from)
            .collect())
    }

    /// A single policy by numeric id, or `None` if no policy has that id. (The server answers an
    /// unknown id with 200 and no items rather than 404.)
    fn get<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Option<PyPolicy>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.policies.get(id)));
        let result = result.map_err(crate::datahub_err)?;
        Ok(result.get_items().first().cloned().map(PyPolicy::from))
    }

    /// Create policies. Every type except `NAMING_CONVENTION` must name a `data_set_id`, or the
    /// server refuses the whole batch with 400.
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyPolicy>) -> PyResult<Vec<PyPolicy>> {
        let policies: Vec<Policy> = input.into_iter().map(Policy::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.policies.create(&policies)));
        let result = result.map_err(crate::datahub_err)?;
        Ok(result
            .get_items()
            .iter()
            .cloned()
            .map(PyPolicy::from)
            .collect())
    }

    /// Update one policy. Send the whole policy with the fields as they should end up — this is
    /// not a set-of-changes block. One per call: the server reads only the first item.
    fn update<'py>(&self, py: Python<'py>, input: Vec<PyPolicy>) -> PyResult<Vec<PyPolicy>> {
        require_single(&input)?;
        let policies: Vec<Policy> = input.into_iter().map(Policy::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.policies.update(&policies)));
        let result = result.map_err(crate::datahub_err)?;
        Ok(result
            .get_items()
            .iter()
            .cloned()
            .map(PyPolicy::from)
            .collect())
    }

    /// Delete policies by `Policy`, numeric id, or external id.
    fn delete<'py>(&self, py: Python<'py>, input: Vec<PolicyIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        py.detach(|| {
            self.runtime
                .block_on(service.policies.delete(&ids))
                .map_err(crate::datahub_err)
        })?;
        Ok(())
    }

    /// Preflight candidate external ids against the naming policy. Nothing is written; only
    /// non-conforming ids come back.
    fn check_naming<'py>(
        &self,
        py: Python<'py>,
        form: PyNamingCheckForm,
    ) -> PyResult<PyNamingCheck> {
        let form: NamingCheckForm = form.into();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.policies.check_naming(&form)));
        Ok(PyNamingCheck::from(result.map_err(crate::datahub_err)?))
    }
}
