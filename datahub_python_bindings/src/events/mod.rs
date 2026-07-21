use crate::PyIdCollection;
use crate::datetime::opt_py_datetime_to_utc;
use crate::timeseries::datapoints::{
    PyDatapointString, PyDatapointsCollectionDatapoints, PyDatapointsCollectionString,
    PyRetrieveFilter,
};
use crate::timeseries::{PyDeleteFilter, PyTimeSeries, PyTimeSeriesUpdate};
use dataplatform_rust_sdk::filters::{BasicEventFilter, EventFilter, TimeFilter};
use dataplatform_rust_sdk::events::EventIdCollection;
use dataplatform_rust_sdk::{Event, TimeSeries};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{Bound, PyErr, PyResult, pyclass, pymethods};
use std::collections::HashMap;
use uuid::Uuid;

pub mod async_service;
pub mod general;
pub mod sync_service;

#[pyclass(module = "datahub_sdk", name = "Event", from_py_object)]
#[derive(Clone)]
pub struct PyEvent {
    pub inner: Event,
}

impl From<Event> for PyEvent {
    fn from(ts: Event) -> Self {
        Self { inner: ts }
    }
}
impl From<PyEvent> for Event {
    fn from(ts: PyEvent) -> Self {
        ts.inner
    }
}

impl PyEvent {
    pub fn uuid(&self) -> Option<&Uuid> {
        self.inner.id.as_ref()
    }
}

#[pyclass(module = "datahub_sdk", name = "EventFilter", from_py_object)]
#[derive(Clone)]
pub struct PyEventFilter {
    pub inner: EventFilter,
}
impl From<EventFilter> for PyEventFilter {
    fn from(ts: EventFilter) -> Self {
        Self { inner: ts }
    }
}
impl From<PyEventFilter> for EventFilter {
    fn from(ts: PyEventFilter) -> Self {
        ts.inner
    }
}

#[pymethods]
impl PyEventFilter {
    #[new]
    #[pyo3(signature=(basic_filter,limit=None))]
    fn new(basic_filter: PyBasicEventFilter, limit: Option<u64>) -> Self {
        let mut filter = EventFilter::default();
        filter.set_filter(basic_filter.into());
        filter.set_limit(limit.unwrap_or(100));
        Self {
            inner: filter.build(),
        }
    }
    #[getter]
    fn filter(&self) -> Option<PyBasicEventFilter> {
        self.inner.filter().cloned().map(|f| f.into())
    }
    #[getter]
    pub fn limit(&self) -> u64 {
        self.inner.limit
    }
    #[setter]
    pub fn set_limit(&mut self, limit: u64) {
        self.inner.limit = limit;
    }
    #[getter]
    pub fn cursor(&self) -> Option<&str> {
        self.inner.cursor()
    }
}

#[pyclass(
    module = "datahub_sdk",
    name = "BasicEventFilter",
    from_py_object
)]
#[derive(Clone)]
pub struct PyBasicEventFilter {
    inner: BasicEventFilter,
}
impl From<BasicEventFilter> for PyBasicEventFilter {
    fn from(ts: BasicEventFilter) -> Self {
        Self { inner: ts }
    }
}
impl From<PyBasicEventFilter> for BasicEventFilter {
    fn from(ts: PyBasicEventFilter) -> Self {
        ts.inner
    }
}
#[pymethods]
impl PyBasicEventFilter {
    #[new]
    #[pyo3(signature=(
        id=None,
        external_id_prefix=None,
        description=None,
        source=None,
        r#type=None,
        sub_type=None,
        data_set_ids=None,
        event_time=None,
        metadata=None,
        related_resource_ids=None,
        related_resource_external_ids=None,
        created_time=None,
        last_updated_time=None,
    ))]
    fn new(
        id: Option<u64>,
        external_id_prefix: Option<String>,
        description: Option<String>,
        source: Option<String>,
        r#type: Option<String>,
        sub_type: Option<String>,
        data_set_ids: Option<Vec<u64>>,
        event_time: Option<PyTimeFilter>,
        metadata: Option<HashMap<String, String>>,
        related_resource_ids: Option<Vec<u64>>,
        related_resource_external_ids: Option<Vec<String>>,
        created_time: Option<PyTimeFilter>,
        last_updated_time: Option<PyTimeFilter>,
    ) -> Self {
        Self {
            inner: BasicEventFilter::new(
                id,
                external_id_prefix,
                description,
                source,
                r#type,
                sub_type,
                data_set_ids,
                event_time.map(|f| f.inner),
                metadata,
                related_resource_ids,
                related_resource_external_ids,
                created_time.map(|f| f.inner),
                last_updated_time.map(|f| f.inner),
            ),
        }
    }
}
#[pyclass(module = "datahub_sdk", name = "TimeFilter", from_py_object)]
#[derive(Clone)]
pub struct PyTimeFilter {
    inner: TimeFilter,
}

impl From<TimeFilter> for PyTimeFilter {
    fn from(ts: TimeFilter) -> Self {
        Self { inner: ts }
    }
}
impl From<PyTimeFilter> for TimeFilter {
    fn from(ts: PyTimeFilter) -> Self {
        ts.inner
    }
}
#[pymethods]
impl PyTimeFilter {
    #[new]
    #[pyo3(signature=(start=None,end=None))]
    fn new(
        start: Option<Bound<'_, PyAny>>,
        end: Option<Bound<'_, PyAny>>,
    ) -> Result<Self, PyErr> {
        let start = opt_py_datetime_to_utc(start.as_ref())?;
        let end = opt_py_datetime_to_utc(end.as_ref())?;
        // Returning Option because if both are None, we can't create a filter
        match (start, end) {
            (Some(start), Some(end)) => {
                if start > end {
                    Err(PyErr::new::<PyValueError, _>(
                        "start_time cannot be after end_time",
                    ))
                } else {
                    Ok(Self {
                        inner: TimeFilter::Between {
                            min: start,
                            max: end,
                        },
                    })
                }
            }
            (Some(start), None) => Ok(Self {
                inner: TimeFilter::After { min: start },
            }),
            (None, Some(end)) => Ok(Self {
                inner: TimeFilter::Before { max: end },
            }),
            (None, None) => Err(PyErr::new::<PyValueError, _>(
                "Both start and end cannot be None",
            )),
        }
    }
}
/// Event id selector exposed to Python. Events are keyed by a client-generated UUID v7, so this
/// carries the `id` (UUID) and/or the `external_id`. Construct with either or both:
/// `EventIdCollection(id=my_uuid)` or `EventIdCollection(external_id="...")`.
#[pyclass(
    module = "datahub_sdk",
    name = "EventIdCollection",
    from_py_object
)]
#[derive(Clone)]
pub struct PyEventIdCollection {
    pub id: Option<Uuid>,
    pub external_id: Option<String>,
}
#[pymethods]
impl PyEventIdCollection {
    #[new]
    #[pyo3(signature = (id = None, external_id = None))]
    fn new(id: Option<Uuid>, external_id: Option<String>) -> PyResult<Self> {
        if id.is_none() && external_id.is_none() {
            return Err(PyErr::new::<PyValueError, _>(
                "EventIdCollection needs an id (UUID) or an external_id",
            ));
        }
        Ok(Self { id, external_id })
    }
    #[getter]
    fn id(&self) -> Option<Uuid> {
        self.id
    }
    #[getter]
    fn external_id(&self) -> Option<String> {
        self.external_id.clone()
    }
}

#[derive(Clone, FromPyObject)]
pub enum EventIdentifyable {
    Event(PyEvent),
    EventId(PyEventIdCollection),
    // A bare `uuid.UUID` selects an event by its id; a bare `str` selects by external id. UUID is
    // tried before the string catch-all so a UUID doesn't get swallowed as an external id.
    Uuid(Uuid),
    ExternalId(String),
}

impl From<PyEvent> for EventIdentifyable {
    fn from(event: PyEvent) -> Self {
        EventIdentifyable::Event(event)
    }
}
impl From<PyEventIdCollection> for EventIdentifyable {
    fn from(event: PyEventIdCollection) -> Self {
        EventIdentifyable::EventId(event)
    }
}
// Convert to the Rust event selector, *preserving the UUID*. The previous impl dropped the id and
// always sent the external id, so delete/by_ids by UUID silently didn't work. Prefer the id when we
// have one; fall back to the external id otherwise.
impl From<EventIdentifyable> for EventIdCollection {
    fn from(value: EventIdentifyable) -> Self {
        match value {
            EventIdentifyable::EventId(c) => EventIdCollection {
                id: c.id,
                external_id: c.external_id,
            },
            EventIdentifyable::Event(event) => match event.uuid().copied() {
                Some(id) => EventIdCollection::from_uuid(id),
                None => EventIdCollection::from_external_id(event.external_id()),
            },
            EventIdentifyable::Uuid(id) => EventIdCollection::from_uuid(id),
            EventIdentifyable::ExternalId(external_id) => {
                EventIdCollection::from_external_id(&external_id)
            }
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEvent>()?;
    m.add_class::<PyEventIdCollection>()?;
    m.add_class::<PyEventFilter>()?;
    m.add_class::<PyBasicEventFilter>()?;
    m.add_class::<PyTimeFilter>()?;
    Ok(())
}
