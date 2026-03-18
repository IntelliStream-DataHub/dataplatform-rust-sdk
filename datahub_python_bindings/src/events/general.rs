use pyo3::pymethods;
use crate::PyEvent;

#[pymethods]
impl PyEvent {
    #[getter]
    fn external_id(&self) -> String {
        self.inner.get_external_id().to_string()
    }
}