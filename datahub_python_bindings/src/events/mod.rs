use std::collections::HashMap;
use chrono::{DateTime, Utc};
use pyo3::{pyclass, pymethods, PyErr};
use pyo3::exceptions::PyValueError;
use dataplatform_rust_sdk::{Event, TimeSeries};
use dataplatform_rust_sdk::filters::{BasicEventFilter, EventFilter, TimeFilter};

pub mod general;
pub mod async_service;
pub mod sync_service;

#[pyclass(module="datahub_python_sdk")]
#[derive(Clone)]
pub struct PyEvent{
    pub inner: Event
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
    pub(crate) fn from_event(inner: Event) -> Self {
        Self { inner }
    }
}

#[pyclass(module="datahub_python_sdk")]
#[derive(Clone)]
pub struct PyEventFilter{
    pub inner: EventFilter
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
impl PyEventFilter{
    #[new]
    fn new(basic_filter: PyBasicEventFilter) -> Self {
        let mut filter = EventFilter::new();
        filter.set_filter(basic_filter.into());
        Self { inner: EventFilter::new() }
    }
}


#[pyclass(module="datahub_python_sdk")]
#[derive(Clone)]
pub struct PyBasicEventFilter{
    inner: BasicEventFilter
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

        Self { inner: BasicEventFilter::new(
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
        ) }
    }
}
#[pyclass(from_py_object,module="datahub_python_sdk")]
#[derive(Clone)]
pub struct PyTimeFilter {
    inner: TimeFilter
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
    fn new(
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<Self,PyErr> { // Returning Option because if both are None, we can't create a filter
        match (start_time, end_time) {
            (Some(start), Some(end)) => {
                if start > end {
                    Err(PyErr::new::<PyValueError, _>("start_time cannot be after end_time"))
                }
                else {
                    Ok(Self {inner:TimeFilter::Between { min: start, max: end }})}
                },
            (Some(start), None) => Ok(Self {inner:TimeFilter::After { min: start }}),
            (None, Some(end)) => Ok(Self {inner:TimeFilter::Before { max: end }}),
            (None, None) =>  Err(PyErr::new::<PyValueError, _>("Both start_time and end_time cannot be None")),
        }
    }
}