use crate::PyIdCollection;
use crate::units::PyUnit;
use intellistream_datahub_sdk::ApiService;
use intellistream_datahub_sdk::generic::{DataWrapper, IdAndExtId};
use pyo3::prelude::*;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// The blocking `/units` surface — read-only access to the tenant's unit catalogue.
///
/// Reached as `client.units`. The catalogue is seeded server-side, so there is no create, update
/// or delete here. A unit's `external_id` is the stable handle you put in
/// `TimeSeries.unit_external_id`.
#[pyclass(module = "intellistream_datahub_sdk", name = "UnitServiceSync")]
pub(crate) struct PyUnitServiceSync {
    pub(crate) api_service: Arc<ApiService>,
    pub(crate) runtime: Arc<Runtime>,
}

#[pymethods]
impl PyUnitServiceSync {
    /// The whole catalogue, in one call.
    ///
    /// No `limit`, no filter, no paging — this is the only way to enumerate units, and the way
    /// to find the `external_id` for a unit you want to reference.
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

    /// Units by id or external id. Missing entries are omitted rather than raising.
    ///
    /// **Takes `IdCollection` objects only** — unlike every other `by_ids` in these bindings,
    /// a bare `str` or `int` is a `TypeError`. Write
    /// `units.by_ids([IdCollection(external_id="pressure_bar")])`.
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
    /// One unit by external id.
    ///
    /// **Singular despite the name** — it takes one string, not a list, and answers with a list
    /// of zero or one. A unit that does not exist is an empty list rather than an exception.
    ///
    /// The awaitable twin is spelled `by_external_id`, without the `s`.
    fn by_external_ids<'py>(&self, py: Python<'py>, input: &str) -> PyResult<Vec<PyUnit>> {
        let service = self.api_service.clone();
        py.detach(|| {
            // A unit that does not exist is a 404; report it as an empty list rather than
            // raising, matching what this returned before the API standardised on 404.
            let result =
                crate::none_on_404(self.runtime.block_on(service.units.by_external_id(input)))?;

            let py_units: Vec<PyUnit> = result
                .iter()
                .flat_map(|w| w.get_items())
                .map(|u| PyUnit { inner: u.clone() })
                .collect();

            Ok(py_units)
        })
    }
}
