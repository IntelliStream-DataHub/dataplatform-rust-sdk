use crate::labels::{require_named, LabelIdentifiable, PyLabel};
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::labels::Label;
use intellistream_datahub_sdk::ApiService;
use pyo3::{PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "LabelsServiceSync")]
pub struct PyLabelsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyLabelsServiceSync {
    /// Every label in the tenant.
    fn list<'py>(&self, py: Python<'py>) -> PyResult<Vec<PyLabel>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.labels.list()));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(result.get_items().iter().map(|l| PyLabel { inner: l.clone() }).collect())
    }

    /// A single label by numeric id, or `None` if it doesn't exist (the server answers an
    /// unknown id with 404; that is absorbed into `None`).
    fn get<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Option<PyLabel>> {
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.labels.get(id)));
        let result = crate::none_on_404(result)?;
        Ok(result.and_then(|w| w.get_items().first().map(|l| PyLabel { inner: l.clone() })))
    }

    /// Create labels (each needs a unique `name`). A duplicate name raises with status 409.
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyLabel>) -> PyResult<Vec<PyLabel>> {
        require_named(&input)?;
        let labels: Vec<Label> = input.into_iter().map(Label::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.labels.create(&labels)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(result.get_items().iter().map(|l| PyLabel { inner: l.clone() }).collect())
    }

    /// Update labels (identify each by `id`); only the fields you set are applied.
    fn update<'py>(&self, py: Python<'py>, input: Vec<PyLabel>) -> PyResult<Vec<PyLabel>> {
        require_named(&input)?;
        let labels: Vec<Label> = input.into_iter().map(Label::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.labels.update(&labels)));
        let result = result.map_err(|e| crate::datahub_err(e))?;
        Ok(result.get_items().iter().map(|l| PyLabel { inner: l.clone() }).collect())
    }

    /// Delete labels by `Label`, numeric id, or name. Rejected with status 400 if a label is
    /// still referenced by a resource.
    fn delete<'py>(&self, py: Python<'py>, input: Vec<LabelIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        py.detach(|| {
            self.runtime
                .block_on(service.labels.delete(&ids))
                .map_err(|e| crate::datahub_err(e))
        })?;
        Ok(())
    }
}
