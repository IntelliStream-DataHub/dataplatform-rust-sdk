use std::sync::Arc;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::tokio::future_into_py;
use dataplatform_rust_sdk::{ApiService, Event, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection};
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId, IdAndExtIdCollection};
use crate::{PyIdCollection, PySearchAndFilterForm};
use crate::events::{PyEvent, PyEventFilter};
use crate::timeseries::async_service::PyTimeSeriesServiceAsync;
use crate::timeseries::{PyTimeSeries, PyTimeSeriesUpdate};

#[pyclass]
pub struct PyEventsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyEventsServiceAsync {

    fn create<'py>(&self, py: Python<'py>,input: Vec<PyEvent>) -> PyResult<Bound<'py, PyAny>> {
        let events: Vec<Event> = input.iter().cloned().map(Event::from).collect();
        //let payload = DataWrapper::from_vec(events);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.events.create(&events).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyEvent> = result.get_items().iter().map(|ts| PyEvent { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'py>(&self, py: Python<'py>, input: Vec<PyIdCollection>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .events
                .by_ids(&input_ids)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|u| PyEvent { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'py>(&self, py: Python<'py>,input: Vec<PyIdCollection>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service.events.delete(&input_ids).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyEvent> = result.get_items().into_iter().map(|ev| PyEvent { inner: ev.clone() }).collect();
            Ok(py_ts)
        })
    }

    fn filter<'py>(&self, py: Python<'py>, input: PyEventFilter) -> PyResult<Bound<'py, PyAny>> {


        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service.events.filter(&input.into()).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyEvent> = result.get_items().iter().map(|ts| PyEvent { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }

}