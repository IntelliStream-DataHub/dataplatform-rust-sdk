use std::sync::Arc;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::tokio::future_into_py;
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::generic::{IdAndExtId, IdAndExtIdCollection};
use crate::PyIdCollection;
use crate::unit::PyUnit;

#[pyclass(module = "datahub_python_sdk")]
pub(crate) struct PyUnitServiceAsync {
    pub(crate) api_service: Arc<ApiService>,
}

#[pymethods]
impl PyUnitServiceAsync {
    fn list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .units
                .list()
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyUnit> = result
                .get_items()
                .iter()
                .map(|u| PyUnit { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn by_ids<'p>(&self, py: Python<'p>, input: Vec<PyIdCollection>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        future_into_py(py, async move {
            let result = service
                .units
                .by_ids(&wrapper)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyUnit> = result
                .get_items()
                .iter()
                .map(|u| PyUnit { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn by_external_id<'p>(&self, py: Python<'p>, input: String) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .units
                .by_external_id(&input)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyUnit> = result
                .get_items()
                .iter()
                .map(|u| PyUnit { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
}