use crate::events::{EventIdentifyable, PyEvent, PyEventFilter, PyEventSearch, PyEventUpdate};
use crate::{PyIdCollection, PySearchAndFilterForm};
use dataplatform_rust_sdk::events::{EventIdCollection, EventUpdate};
use dataplatform_rust_sdk::filters::EventFilter;
use dataplatform_rust_sdk::generic::DataWrapper;
use dataplatform_rust_sdk::{
    ApiService, Event, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection,
};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::runtime;
use uuid::Uuid;

#[pyclass(module = "datahub_sdk", name = "EventsServiceSync")]
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
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|ts| PyEvent::with_client(ts.clone(), service.clone()))
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
        let input_ids: Vec<EventIdCollection> =
            input.iter().map(|u| EventIdCollection::from(u.clone())).collect();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.by_ids(&input_ids))
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|u| PyEvent::with_client(u.clone(), service.clone()))
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'py>(&self, py: Python<'py>, input: Vec<EventIdentifyable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids: Vec<EventIdCollection> =
            input.iter().map(|u| EventIdCollection::from(u.clone())).collect();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.delete(&input_ids))
                .map_err(|e| crate::datahub_err(e))?;

            Ok(())
        })
    }

    fn filter<'py>(&self, py: Python<'py>, input: PyEventFilter) -> PyResult<Vec<PyEvent>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.filter(&input.into()))
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|ts| PyEvent::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    /// Look up a single event by its UUID. Returns `None` if no such event exists.
    fn get(&self, py: Python<'_>, id: Uuid) -> PyResult<Option<PyEvent>> {
        let service = self.api_service.clone();
        py.detach(|| match self.runtime.block_on(service.events.get(&id)) {
            Ok(dw) => Ok(dw
                .get_items()
                .first()
                .map(|e| PyEvent::with_client(e.clone(), service.clone()))),
            // The backend 404s an unknown id; surface that as `None`, not an exception.
            Err(e) if e.get_status().as_u16() == 404 => Ok(None),
            Err(e) => Err(crate::datahub_err(e)),
        })
    }

    /// Update events in place. Each `EventUpdate` targets one event and carries only the fields to
    /// change; returns the events after the update.
    fn update(&self, py: Python<'_>, input: Vec<PyEventUpdate>) -> PyResult<Vec<PyEvent>> {
        let service = self.api_service.clone();
        let updates: Vec<EventUpdate> = input.into_iter().map(EventUpdate::from).collect();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.update(&updates))
                .map_err(crate::datahub_err)?;
            let py_ts: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|ev| PyEvent::with_client(ev.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    /// Free-text search over event descriptions, ranked by relevance.
    fn search(&self, py: Python<'_>, input: PyEventSearch) -> PyResult<Vec<PyEvent>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.search(&input.into()))
                .map_err(crate::datahub_err)?;
            let py_ts: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|ev| PyEvent::with_client(ev.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    /// Total number of events in the tenant.
    fn count(&self, py: Python<'_>) -> PyResult<u64> {
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.events.count())
                .map_err(crate::datahub_err)
        })
    }

    /// Distinct `type` values across readable events, sorted alphabetically.
    #[pyo3(signature = (limit = 1000))]
    fn list_types(&self, py: Python<'_>, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.list_types(limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }

    /// Distinct `subType` values across readable events, sorted alphabetically.
    #[pyo3(signature = (limit = 1000))]
    fn list_sub_types(&self, py: Python<'_>, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.list_sub_types(limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }

    /// Distinct `status` values across readable events, sorted alphabetically.
    #[pyo3(signature = (limit = 1000))]
    fn list_statuses(&self, py: Python<'_>, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.list_statuses(limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }

    /// Distinct `source` values across readable events, sorted alphabetically.
    #[pyo3(signature = (limit = 1000))]
    fn list_sources(&self, py: Python<'_>, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.list_sources(limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }

    /// Distinct `type` values containing `q` (case-insensitive substring), for type-ahead.
    #[pyo3(signature = (q, limit = 1000))]
    fn search_types(&self, py: Python<'_>, q: String, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.search_types(&q, limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }

    /// Distinct `subType` values containing `q` (case-insensitive substring), for type-ahead.
    #[pyo3(signature = (q, limit = 1000))]
    fn search_sub_types(&self, py: Python<'_>, q: String, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.search_sub_types(&q, limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }

    /// Distinct `status` values containing `q` (case-insensitive substring), for type-ahead.
    #[pyo3(signature = (q, limit = 1000))]
    fn search_statuses(&self, py: Python<'_>, q: String, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.search_statuses(&q, limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }

    /// Distinct `source` values containing `q` (case-insensitive substring), for type-ahead.
    #[pyo3(signature = (q, limit = 1000))]
    fn search_sources(&self, py: Python<'_>, q: String, limit: u64) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.events.search_sources(&q, limit))
                .map_err(crate::datahub_err)?;
            Ok(result.get_items().clone())
        })
    }
}
