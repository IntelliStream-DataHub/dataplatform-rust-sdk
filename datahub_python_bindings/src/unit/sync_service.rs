use std::sync::Arc;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use tokio::runtime::Runtime;
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::generic::{IdAndExtId, IdAndExtIdCollection};
use crate::PyIdCollection;
use crate::unit::PyUnit;

#[pyclass(module = "datahub_python_sdk")]
pub(crate) struct PyUnitServiceSync {
    pub(crate) api_service: Arc<ApiService>,
    pub(crate) runtime: Arc<Runtime>,
}

#[pymethods]
impl PyUnitServiceSync {
    fn list(&self, py: Python<'_>) -> PyResult<Vec<PyUnit>> {
        let service = self.api_service.clone();

        // 1. Only do the non-Python work inside allow_threads
        let result = py.detach(|| {
            self.runtime
                .block_on(service.units.list())
                // Map the error to a String or a thread-safe Send error here
                .map_err(|e| e.get_message())
        }).map_err(|e_msg| PyException::new_err(e_msg))?; // 2. Convert to PyException once back in GIL

        // 3. Now that we are back in the GIL-protected zone,
        // we can safely create PyUnit objects.
        let py_units: Vec<PyUnit> = result
            .get_items()
            .iter()
            .map(|u| PyUnit { inner: u.clone() })
            .collect();

        Ok(py_units)
    }

    fn by_ids<'py>(&self, py: Python<'py>, input: Vec<PyIdCollection>) -> PyResult<Vec<PyUnit>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.units.by_ids(&wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyUnit> = result
                .get_items()
                .iter()
                .map(|u| PyUnit { inner: u.clone() })
                .collect();

            Ok(py_units)
        })
    }
    fn by_external_ids<'py>(&self, py: Python<'py>, input: &str) -> PyResult<Vec<PyUnit>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.units.by_external_id(input))
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
