use crate::{DataSetRef, PyIdCollection, StringOrList, opt_data_set_refs, opt_patterns};
use crate::datetime::opt_py_datetime_to_utc;
use crate::timeseries::datapoints::{
    PyDatapointString, PyDatapointsCollectionDatapoints, PyDatapointsCollectionString,
    PyRetrieveFilter,
};
use crate::timeseries::{PyDeleteFilter, PyTimeSeries, PyTimeSeriesUpdate};
use crate::{PyFieldStr, PyFieldU64, PyListFieldIdCollection, PyMapField};
use intellistream_datahub_sdk::filters::{EventFilter, DataSort, EventFilterForm, TimeFilter};
use intellistream_datahub_sdk::events::{
    EventDimension, EventIdCollection, EventUpdate, EventUpdateFields,
};
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::{ApiService, Event, TimeSeries};
use std::sync::Arc;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{Bound, PyErr, PyResult, pyclass, pymethods};
use std::collections::HashMap;
use uuid::Uuid;

pub mod async_service;
pub mod general;
pub mod sync_service;

#[pyclass(module = "intellistream_datahub_sdk", name = "Event", from_py_object)]
#[derive(Clone)]
pub struct PyEvent {
    pub inner: Event,
    /// The client this object was returned by, enabling navigation methods
    /// (`related_resources`). `None` on locally-constructed events — navigation then raises.
    pub client: Option<Arc<ApiService>>,
}

impl From<Event> for PyEvent {
    fn from(ts: Event) -> Self {
        Self {
            inner: ts,
            client: None,
        }
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

    /// Wrap an event returned by the API, stamping the client so navigation methods work.
    pub fn with_client(inner: Event, client: Arc<ApiService>) -> Self {
        Self {
            inner,
            client: Some(client),
        }
    }

    /// The event's related-resource references as id selectors, in the order they were attached.
    pub(crate) fn related_id_collections(&self) -> Vec<IdAndExtId> {
        self.inner.get_related_resources().clone()
    }
}

/// Build the request body for `events.filter` from either form of its arguments.
///
/// Shared by the sync and async services so the accepted keywords cannot drift apart between them.
#[allow(clippy::too_many_arguments)]
pub fn event_filter_form(
    filter: Option<PyEventFilter>,
    external_id: Option<StringOrList>,
    source: Option<StringOrList>,
    r#type: Option<StringOrList>,
    sub_type: Option<StringOrList>,
    status: Option<StringOrList>,
    data_set_id: Option<Vec<DataSetRef>>,
    event_time: Option<PyTimeFilter>,
    metadata: Option<HashMap<String, Option<String>>>,
    related_resources: Option<Vec<PyIdCollection>>,
    created_time: Option<PyTimeFilter>,
    last_updated_time: Option<PyTimeFilter>,
    limit: Option<u64>,
    sort_by: Option<StringOrList>,
    sort_order: Option<String>,
    cursor: Option<String>,
) -> PyResult<EventFilterForm> {
    let any_keyword = external_id.is_some()
        || source.is_some()
        || r#type.is_some()
        || sub_type.is_some()
        || status.is_some()
        || data_set_id.is_some()
        || event_time.is_some()
        || metadata.is_some()
        || related_resources.is_some()
        || created_time.is_some()
        || last_updated_time.is_some();
    let from_keywords = PyEventFilter::new(
        external_id,
        source,
        r#type,
        sub_type,
        status,
        data_set_id,
        event_time,
        metadata,
        related_resources,
        created_time,
        last_updated_time,
    )
    .inner;

    let mut form = EventFilterForm::default();
    form.set_filter(crate::resolve_filter(
        filter.map(Into::into),
        from_keywords,
        any_keyword,
    )?);
    form.set_limit(limit.unwrap_or(100));
    // A bare string is a one-element list here as it is on every other filter field; only one
    // property is used either way.
    if let Some(property) = sort_by {
        form.set_sort(DataSort {
            property: property.into(),
            order: sort_order,
        });
    }
    if let Some(cursor) = cursor {
        form.set_cursor(cursor);
    }
    Ok(form.build())
}

#[pyclass(
    module = "intellistream_datahub_sdk",
    name = "EventFilter",
    from_py_object
)]
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
    /// AND-combined criteria for `events.filter`.
    ///
    /// `external_id`, `source`, `type`, `sub_type` and `status` are **pattern** lists: `*` and `%`
    /// are wildcards, `_` is literal, matching is case-insensitive, and an entry with no wildcard
    /// matches exactly. Entries within a list OR together, so `type=["alarm", "warning"]` is one
    /// call where the old scalar `type` needed two. Each is named in the singular because each also
    /// accepts a bare string, which is what most calls pass.
    ///
    /// `metadata` entries must all be present, and a `None` value matches the key alone.
    /// `related_resources` keeps its plural — its entries **AND**, so all of them must be attached
    /// to the event.
    ///
    /// `data_set_id` takes numeric ids, external ids, or `IdCollection`s, and expands down the
    /// dataset hierarchy, so naming a parent covers its children. **`None` and `[]` differ here**:
    /// `None` places no restriction, `[]` narrows to no datasets and matches nothing.
    ///
    /// There is no `id`: events are keyed by UUID, and the field the api used to declare was typed
    /// as a long that nothing read. Use `events.by_ids` to look one up.
    #[new]
    #[pyo3(signature=(
        external_id=None,
        source=None,
        r#type=None,
        sub_type=None,
        status=None,
        data_set_id=None,
        event_time=None,
        metadata=None,
        related_resources=None,
        created_time=None,
        last_updated_time=None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        external_id: Option<StringOrList>,
        source: Option<StringOrList>,
        r#type: Option<StringOrList>,
        sub_type: Option<StringOrList>,
        status: Option<StringOrList>,
        data_set_id: Option<Vec<DataSetRef>>,
        event_time: Option<PyTimeFilter>,
        metadata: Option<HashMap<String, Option<String>>>,
        related_resources: Option<Vec<PyIdCollection>>,
        created_time: Option<PyTimeFilter>,
        last_updated_time: Option<PyTimeFilter>,
    ) -> Self {
        Self {
            inner: EventFilter {
                external_id: opt_patterns(external_id),
                source: opt_patterns(source),
                r#type: opt_patterns(r#type),
                sub_type: opt_patterns(sub_type),
                status: opt_patterns(status),
                data_set_id: opt_data_set_refs(data_set_id),
                event_time: event_time.map(|f| f.inner),
                metadata,
                related_resources: related_resources
                    .map(|rr| rr.into_iter().map(IdAndExtId::from).collect())
                    .unwrap_or_default(),
                created_time: created_time.map(|f| f.inner),
                last_updated_time: last_updated_time.map(|f| f.inner),
            },
        }
    }
}
#[pyclass(module = "intellistream_datahub_sdk", name = "TimeFilter", from_py_object)]
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
    module = "intellistream_datahub_sdk",
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

/// One event's update for `events.update`. Target the event by an `Event`, its UUID `id`, or its
/// `external_id`; every field is optional and uses the same wrappers as the other services
/// (`FieldStr`/`FieldU64` for scalars, `ListFieldIdCollection` for the related-resource list,
/// `MapField` for metadata). Mirrors `ResourceUpdate`.
#[pyclass(module = "intellistream_datahub_sdk", name = "EventUpdate")]
#[derive(Clone)]
pub struct PyEventUpdate {
    pub inner: EventUpdate,
}

impl From<PyEventUpdate> for EventUpdate {
    fn from(v: PyEventUpdate) -> Self {
        v.inner
    }
}

#[pymethods]
impl PyEventUpdate {
    #[new]
    #[pyo3(signature = (
        event,
        external_id = None,
        description = None,
        r#type = None,
        sub_type = None,
        status = None,
        data_set_id = None,
        metadata = None,
        source = None,
        related_resources = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn __init__(
        event: EventIdentifyable,
        external_id: Option<PyFieldStr>,
        description: Option<PyFieldStr>,
        r#type: Option<PyFieldStr>,
        sub_type: Option<PyFieldStr>,
        status: Option<PyFieldStr>,
        data_set_id: Option<PyFieldU64>,
        metadata: Option<PyMapField>,
        source: Option<PyFieldStr>,
        related_resources: Option<PyListFieldIdCollection>,
    ) -> Self {
        let ident = EventIdCollection::from(event);
        Self {
            inner: EventUpdate {
                id: ident.id,
                external_id: ident.external_id,
                update: EventUpdateFields {
                    external_id: external_id.map(Into::into),
                    description: description.map(Into::into),
                    r#type: r#type.map(Into::into),
                    sub_type: sub_type.map(Into::into),
                    status: status.map(Into::into),
                    data_set_id: data_set_id.map(Into::into),
                    metadata: metadata.map(Into::into),
                    source: source.map(Into::into),
                    related_resources: related_resources.map(Into::into),
                },
            },
        }
    }

    #[getter]
    fn target_id(&self) -> Option<Uuid> {
        self.inner.id
    }
    #[getter]
    fn target_external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
}

/// The categorical event fields that have a queryable vocabulary.
///
/// Served from dimension tables the write path maintains, not from a scan of the events — cheap
/// enough for a typeahead, but *eventually consistent*: a new value appears once the write path
/// records it, and a value no event carries any more lingers until the server's reconcile.
#[pyclass(module = "intellistream_datahub_sdk", name = "EventDimension", eq, eq_int, frozen, hash)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum PyEventDimension {
    TYPE,
    SUB_TYPE,
    STATUS,
    SOURCE,
}

impl From<PyEventDimension> for EventDimension {
    fn from(d: PyEventDimension) -> Self {
        match d {
            PyEventDimension::TYPE => EventDimension::Type,
            PyEventDimension::SUB_TYPE => EventDimension::SubType,
            PyEventDimension::STATUS => EventDimension::Status,
            PyEventDimension::SOURCE => EventDimension::Source,
        }
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyEventDimension>()?;
    m.add_class::<PyEvent>()?;
    m.add_class::<PyEventIdCollection>()?;
    m.add_class::<PyEventFilter>()?;
    m.add_class::<PyTimeFilter>()?;
    m.add_class::<PyEventUpdate>()?;
    Ok(())
}
