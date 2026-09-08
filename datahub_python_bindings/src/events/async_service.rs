use crate::events::{
    EventIdentifyable, PyEventFilter, PyEvent, PyEventDimension, PyEventUpdate,
};
use crate::timeseries::async_service::PyTimeSeriesServiceAsync;
use crate::timeseries::{PyTimeSeries, PyTimeSeriesUpdate};
use crate::{PyIdCollection};
use intellistream_datahub_sdk::events::{EventDimension, EventIdCollection, EventUpdate};
use intellistream_datahub_sdk::generic::DataWrapper;
use intellistream_datahub_sdk::{
    ApiService, Event, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection,
};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use uuid::Uuid;

#[pyclass(module = "intellistream_datahub_sdk", name = "EventsServiceAsync")]
pub struct PyEventsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyEventsServiceAsync {
    /// A criteria-free page of the tenant's events.
    ///
    /// **The oldest `limit` events, not the newest.** It runs the event filter with an empty body,
    /// whose default sort is `eventTime` ascending — the order the cursor pages in. The node
    /// listings beside it (`resources.list`, `timeseries.list`, `datasets.list`) really are
    /// newest-first; events are the one member of the family that reads the other way round. For
    /// "what just happened", use `filter(sort_by="eventTime", sort_order="desc")`.
    ///
    /// `limit` defaults to the server's 1000 and may not exceed 10000. A plain list is returned
    /// rather than a `Page`: there is no cursor to continue with.
    #[pyo3(signature = (limit = None))]
    fn list<'py>(&self, py: Python<'py>, limit: Option<u64>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .events
                .list(limit)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .map(|e| PyEvent::with_client(e.clone(), service.clone()))
                .collect::<Vec<_>>())
        })
    }

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

    #[pyo3(signature = (filter=None, external_id=None, source=None, r#type=None, sub_type=None,
                        status=None, data_set_id=None, event_time=None, metadata=None,
                        related_resources=None, created_time=None, last_updated_time=None,
                        limit=None, sort_by=None, sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter<'py>(
        &self,
        py: Python<'py>,
        filter: Option<PyEventFilter>,
        external_id: Option<crate::StringOrList>,
        source: Option<crate::StringOrList>,
        r#type: Option<crate::StringOrList>,
        sub_type: Option<crate::StringOrList>,
        status: Option<crate::StringOrList>,
        data_set_id: Option<Vec<crate::DataSetRef>>,
        event_time: Option<crate::events::PyTimeFilter>,
        metadata: Option<std::collections::HashMap<String, Option<String>>>,
        related_resources: Option<Vec<crate::PyIdCollection>>,
        created_time: Option<crate::events::PyTimeFilter>,
        last_updated_time: Option<crate::events::PyTimeFilter>,
        limit: Option<u64>,
        sort_by: Option<crate::StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = crate::events::event_filter_form(
            filter, external_id, source, r#type, sub_type, status, data_set_id, event_time,
            metadata, related_resources, created_time, last_updated_time, limit, sort_by,
            sort_order, cursor,
        )?;
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .events
                .filter(&form)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyEvent> = result
                .get_items()
                .iter()
                .map(|ts| PyEvent::with_client(ts.clone(), service.clone()))
                .collect();
            Python::attach(|py| crate::PyPage::new(py, items, next_cursor))
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
    #[pyo3(signature = (query, filter = None, limit = None))]
    fn search<'py>(
        &self,
        py: Python<'py>,
        query: String,
        filter: Option<PyEventFilter>,
        limit: Option<u64>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = crate::search_form(query, filter.map(Into::into), limit);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .events
                .search(&form)
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
