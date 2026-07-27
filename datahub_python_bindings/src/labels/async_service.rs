use crate::labels::{require_named, LabelIdentifiable, PyLabel};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::labels::Label;
use dataplatform_rust_sdk::ApiService;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "LabelsServiceAsync")]
pub struct PyLabelsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyLabelsServiceAsync {
    /// Every label in the tenant.
    fn list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.labels.list().await.map_err(|e| crate::datahub_err(e))?;
            let py_labels: Vec<PyLabel> =
                result.get_items().iter().map(|l| PyLabel { inner: l.clone() }).collect();
            Ok(py_labels)
        })
    }

    /// A single label by numeric id, or `None` if it doesn't exist.
    fn get<'py>(&self, py: Python<'py>, id: u64) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.labels.get(id).await.map_err(|e| crate::datahub_err(e))?;
            Ok(result.get_items().first().map(|l| PyLabel { inner: l.clone() }))
        })
    }

    /// Create labels (each needs a unique `name`). A duplicate name raises with status 409.
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyLabel>) -> PyResult<Bound<'py, PyAny>> {
        require_named(&input)?;
        let labels: Vec<Label> = input.into_iter().map(Label::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.labels.create(&labels).await.map_err(|e| crate::datahub_err(e))?;
            let py_labels: Vec<PyLabel> =
                result.get_items().iter().map(|l| PyLabel { inner: l.clone() }).collect();
            Ok(py_labels)
        })
    }

    /// Update labels (identify each by `id`); only the fields you set are applied.
    fn update<'py>(&self, py: Python<'py>, input: Vec<PyLabel>) -> PyResult<Bound<'py, PyAny>> {
        require_named(&input)?;
        let labels: Vec<Label> = input.into_iter().map(Label::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.labels.update(&labels).await.map_err(|e| crate::datahub_err(e))?;
            let py_labels: Vec<PyLabel> =
                result.get_items().iter().map(|l| PyLabel { inner: l.clone() }).collect();
            Ok(py_labels)
        })
    }

    /// Delete labels by `Label`, numeric id, or name. Rejected with status 400 if a label is
    /// still referenced by a resource.
    fn delete<'py>(&self, py: Python<'py>, input: Vec<LabelIdentifiable>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        future_into_py(py, async move {
            service.labels.delete(&ids).await.map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }
}
