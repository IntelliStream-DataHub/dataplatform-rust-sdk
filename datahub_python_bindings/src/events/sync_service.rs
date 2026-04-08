use crate::events::{EventIdentifyable, PyEvent, PyEventFilter};
use crate::{PyIdCollection, PySearchAndFilterForm};
use dataplatform_rust_sdk::filters::EventFilter;
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId, IdAndExtIdCollection};
use dataplatform_rust_sdk::{
    ApiService, Event, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection,
};
use pyo3::exceptions::PyException;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::runtime;

#[pyclass]
pub struct PyEventsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyEventsServiceSync {
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyEvent>) -> PyResult<Vec<PyEvent>> {
        let events: Vec<Event> = input.iter().cloned().map(Event::from).collect();
        //let payload = DataWrapper::from_vec(events);
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.create(&events))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|ts| PyEvent { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<EventIdentifyable>,
    ) -> PyResult<Vec<PyEvent>> {
        let service = self.api_service.clone();
        let input_ids: Vec<IdAndExtId> =
            input.iter().map(|u| IdAndExtId::from(u.clone())).collect();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.by_ids(&input_ids))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|u| PyEvent { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'py>(&self, py: Python<'py>, input: Vec<EventIdentifyable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids: Vec<IdAndExtId> =
            input.iter().map(|u| IdAndExtId::from(u.clone())).collect();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.delete(&input_ids))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            Ok(())
        })
    }

    fn filter<'py>(&self, py: Python<'py>, input: PyEventFilter) -> PyResult<Vec<PyEvent>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.filter(&input.into()))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_ts: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|ts| PyEvent { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }
}
