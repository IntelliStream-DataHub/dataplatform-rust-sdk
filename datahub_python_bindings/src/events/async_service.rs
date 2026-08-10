use crate::events::{
    EventIdentifyable, PyEvent, PyEventDimension, PyEventFilter, PyEventSearch, PyEventUpdate,
};
use crate::timeseries::async_service::PyTimeSeriesServiceAsync;
use crate::timeseries::{PyTimeSeries, PyTimeSeriesUpdate};
use crate::{PyIdCollection, PySearchAndFilterForm};
use dataplatform_rust_sdk::events::{EventDimension, EventIdCollection, EventUpdate};
use dataplatform_rust_sdk::generic::DataWrapper;
use dataplatform_rust_sdk::{
    ApiService, Event, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection,
};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use uuid::Uuid;

#[pyclass(module = "datahub_sdk", name = "EventsServiceAsync")]
pub struct PyEventsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyEventsServiceAsync {
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyEvent>) -> PyResult<Bound<'py, PyAny>> {
        let events: Vec<Event> = input.iter().cloned().map(Event::from).collect();
        //let payload = DataWrapper::from_vec(events);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .events
                .create(&events)
                .await
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| EventIdCollection::from(u.clone()))
            .collect::<Vec<EventIdCollection>>();

        future_into_py(py, async move {
            let result = service
                .events
                .by_ids(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|u| PyEvent::with_client(u.clone(), service.clone()))
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<EventIdentifyable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| EventIdCollection::from(u.clone()))
            .collect::<Vec<EventIdCollection>>();

        future_into_py(py, async move {
            let result = service
                .events
                .delete(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyEvent> = result
                .get_items()
                .into_iter()
                .map(|ev| PyEvent::with_client(ev.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    fn filter<'py>(&self, py: Python<'py>, input: PyEventFilter) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .events
                .filter(&input.into())
                .await
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
    fn get<'py>(&self, py: Python<'py>, id: Uuid) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            match service.events.get(&id).await {
                Ok(dw) => Ok(dw
                    .get_items()
                    .first()
                    .map(|e| PyEvent::with_client(e.clone(), service.clone()))),
                // The backend 404s an unknown id; surface that as `None`, not an exception.
                Err(e) if e.get_status().as_u16() == 404 => Ok(None),
                Err(e) => Err(crate::datahub_err(e)),
            }
        })
    }

    /// Update events in place. Each `EventUpdate` targets one event and carries only the fields to
    /// change; returns the events after the update.
    fn update<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyEventUpdate>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let updates: Vec<EventUpdate> = input.into_iter().map(EventUpdate::from).collect();
        future_into_py(py, async move {
            let result = service
                .events
                .update(&updates)
                .await
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
    fn search<'py>(&self, py: Python<'py>, input: PyEventSearch) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .events
                .search(&input.into())
                .await
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
    fn count<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let count = service.events.count().await.map_err(crate::datahub_err)?;
            Ok(count)
        })
    }
    /// Distinct values an event field takes in this tenant. `query` filters by case-insensitive
    /// substring; omit it to list everything. `limit` defaults to 1000 server-side and is clamped
    /// to 1..=10000. Alphabetical, and restricted to your readable datasets.
    #[pyo3(signature = (dimension, query = None, limit = None))]
    fn list_dimension<'py>(
        &self,
        py: Python<'py>,
        dimension: PyEventDimension,
        query: Option<String>,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let dim: EventDimension = dimension.into();
        future_into_py(py, async move {
            let r = service
                .events
                .list_dimension(dim, query.as_deref(), limit)
                .await
                .map_err(crate::datahub_err)?;
            Ok(r.get_items().to_vec())
        })
    }

    /// Every distinct `type` on events you can read.
    #[pyo3(signature = (limit = None))]
    fn list_types<'py>(&self, py: Python<'py>, limit: Option<u32>) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::TYPE, None, limit)
    }

    /// Distinct `type` values containing `query` (case-insensitive substring).
    #[pyo3(signature = (query, limit = None))]
    fn search_types<'py>(
        &self,
        py: Python<'py>,
        query: String,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::TYPE, Some(query), limit)
    }

    /// Every distinct `subType` on events you can read.
    #[pyo3(signature = (limit = None))]
    fn list_sub_types<'py>(&self, py: Python<'py>, limit: Option<u32>) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::SUB_TYPE, None, limit)
    }

    /// Distinct `subType` values containing `query` (case-insensitive substring).
    #[pyo3(signature = (query, limit = None))]
    fn search_sub_types<'py>(
        &self,
        py: Python<'py>,
        query: String,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::SUB_TYPE, Some(query), limit)
    }

    /// Every distinct `status` on events you can read.
    #[pyo3(signature = (limit = None))]
    fn list_statuses<'py>(&self, py: Python<'py>, limit: Option<u32>) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::STATUS, None, limit)
    }

    /// Distinct `status` values containing `query` (case-insensitive substring).
    #[pyo3(signature = (query, limit = None))]
    fn search_statuses<'py>(
        &self,
        py: Python<'py>,
        query: String,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::STATUS, Some(query), limit)
    }

    /// Every distinct `source` on events you can read.
    #[pyo3(signature = (limit = None))]
    fn list_sources<'py>(&self, py: Python<'py>, limit: Option<u32>) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::SOURCE, None, limit)
    }

    /// Distinct `source` values containing `query` (case-insensitive substring).
    #[pyo3(signature = (query, limit = None))]
    fn search_sources<'py>(
        &self,
        py: Python<'py>,
        query: String,
        limit: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.list_dimension(py, PyEventDimension::SOURCE, Some(query), limit)
    }
}
