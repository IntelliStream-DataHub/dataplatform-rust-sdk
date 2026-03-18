use pyo3::pymethods;
use super::PyTimeSeries;
#[pymethods]
impl PyTimeSeries {
    #[getter]
    fn external_id(&self) -> String {
        self.inner.external_id.clone()
    }

    #[getter]
    fn name(&self) -> Option<String> {
        Some(self.inner.name.clone())
    }

    #[getter]
    fn description(&self) -> Option<String> {
        self.inner.description.clone()
    }
}
