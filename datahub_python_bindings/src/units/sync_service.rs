use crate::PyIdCollection;
use crate::units::PyUnit;
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

#[pyclass(module = "datahub_sdk", name = "UnitServiceSync")]
pub(crate) struct PyUnitServiceSync {
    pub(crate) api_service: Arc<ApiService>,
    pub(crate) runtime: Arc<Runtime>,
}

#[pymethods]
impl PyUnitServiceSync {
    fn list(&self, py: Python<'_>) -> PyResult<Vec<PyUnit>> {
        let service = self.api_service.clone();

        // 1. Only do the non-Python work inside allow_threads
        let result = py
            .detach(|| self.runtime.block_on(service.units.list()))
            // 2. Back under the GIL, raise the same DataHubException every sibling method and
            //    the async twin raise. Mapping to a bare PyException here used to drop the
            //    status code, which is the only thing that makes a 401 or 403 diagnosable —
            //    and for those the message is empty, so the exception carried nothing at all.
            .map_err(crate::datahub_err)?;

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
        let wrapper = DataWrapper::from_vec(input_ids);

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.units.by_ids(&wrapper))
                .map_err(|e| crate::datahub_err(e))?;

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
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyUnit> = result
                .get_items()
                .iter()
                .map(|u| PyUnit { inner: u.clone() })
                .collect();

            Ok(py_units)
        })
    }
}
