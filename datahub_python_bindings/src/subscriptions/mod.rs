use crate::PyIdCollection;
use crate::timeseries::PyTimeSeries;
use crate::StringOrList;
use crate::events::PyTimeFilter;
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::subscriptions::{
    DataCollectionString, DataWrapperMessage, EventAction, EventObject, Subscription,
    SubscriptionFilter, SubscriptionMessage, SubscriptionFilterForm, WsDatapoint,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::{Bound, PyResult, pyclass, pymethods};
use std::str::FromStr;
use strum::{Display, EnumString};

pub mod async_service;
pub mod general;
pub mod listener;
pub mod sync_service;

#[pyclass(module = "intellistream_datahub_sdk", name = "Subscription")]
#[derive(Clone)]
pub struct PySubscription {
    pub inner: Subscription,
}

impl From<Subscription> for PySubscription {
    fn from(s: Subscription) -> Self {
        Self { inner: s }
    }
}
impl From<PySubscription> for Subscription {
    fn from(s: PySubscription) -> Self {
        s.inner
    }
}

/// Criteria for `subscriptions.filter`. Every field is optional; the fields AND together and the
/// entries within a list OR.
#[pyclass(module = "intellistream_datahub_sdk", name = "SubscriptionFilter", from_py_object)]
#[derive(Clone, Default)]
pub struct PySubscriptionFilter {
    pub inner: SubscriptionFilter,
}

impl From<SubscriptionFilter> for PySubscriptionFilter {
    fn from(s: SubscriptionFilter) -> Self {
        Self { inner: s }
    }
}
impl From<PySubscriptionFilter> for SubscriptionFilter {
    fn from(s: PySubscriptionFilter) -> Self {
        s.inner
    }
}

#[pymethods]
impl PySubscriptionFilter {
    /// `external_id` and `name` are **pattern** lists: `*` and `%` are wildcards, `_` is literal,
    /// matching is case-insensitive, and an entry with no wildcard matches exactly. Each also
    /// accepts a bare string. `timeseries` matches subscriptions bound to at least one of them.
    #[new]
    #[pyo3(signature = (
        id = None,
        external_id = None,
        name = None,
        timeseries = None,
        created_time = None,
        last_updated_time = None,
    ))]
    pub fn new(
        id: Option<Vec<u64>>,
        external_id: Option<StringOrList>,
        name: Option<StringOrList>,
        timeseries: Option<Vec<SubscriptionTimeseriesId>>,
        created_time: Option<PyTimeFilter>,
        last_updated_time: Option<PyTimeFilter>,
    ) -> Self {
        Self {
            inner: SubscriptionFilter {
                id,
                external_id: external_id.map(Into::into),
                name: name.map(Into::into),
                timeseries: timeseries
                    .unwrap_or_default()
                    .into_iter()
                    .map(IdAndExtId::from)
                    .collect(),
                created_time: created_time.map(Into::into),
                last_updated_time: last_updated_time.map(Into::into),
            },
        }
    }

    #[getter]
    fn id(&self) -> Option<Vec<u64>> {
        self.inner.id.clone()
    }
    #[getter]
    fn external_id(&self) -> Option<Vec<String>> {
        self.inner.external_id.clone()
    }
    #[getter]
    fn name(&self) -> Option<Vec<String>> {
        self.inner.name.clone()
    }
    #[getter]
    fn timeseries(&self) -> Vec<PyIdCollection> {
        self.inner
            .timeseries
            .iter()
            .cloned()
            .map(PyIdCollection::from)
            .collect()
    }
}

/// Build the request body for `subscriptions.filter` from either form of its arguments.
///
/// Shared by the sync and async services so the accepted keywords cannot drift apart between them.
#[allow(clippy::too_many_arguments)]
pub fn subscription_filter_form(
    filter: Option<PySubscriptionFilter>,
    id: Option<Vec<u64>>,
    external_id: Option<StringOrList>,
    name: Option<StringOrList>,
    timeseries: Option<Vec<SubscriptionTimeseriesId>>,
    created_time: Option<PyTimeFilter>,
    last_updated_time: Option<PyTimeFilter>,
    limit: Option<u64>,
    sort_by: Option<StringOrList>,
    sort_order: Option<String>,
    cursor: Option<String>,
) -> PyResult<SubscriptionFilterForm> {
    let any_keyword = id.is_some()
        || external_id.is_some()
        || name.is_some()
        || timeseries.is_some()
        || created_time.is_some()
        || last_updated_time.is_some();
    let from_keywords = PySubscriptionFilter::new(
        id,
        external_id,
        name,
        timeseries,
        created_time,
        last_updated_time,
    )
    .inner;
    Ok(SubscriptionFilterForm {
        filter: crate::resolve_filter(filter.map(Into::into), from_keywords, any_keyword)?,
        limit,
        paging: crate::build_page_request(sort_by, sort_order, cursor),
    })
}

/// Things accepted as a subscription identifier when deleting.
#[derive(Clone, FromPyObject)]
pub enum SubscriptionIdentifyable {
    Subscription(PySubscription),
    Collection(PyIdCollection),
    ExternalId(String),
    Id(u64),
}

impl From<SubscriptionIdentifyable> for IdAndExtId {
    fn from(value: SubscriptionIdentifyable) -> Self {
        match value {
            SubscriptionIdentifyable::Subscription(s) => Self {
                id: s.inner.id,
                external_id: Some(s.inner.external_id.clone()),
            },
            SubscriptionIdentifyable::Collection(c) => c.into(),
            SubscriptionIdentifyable::ExternalId(ext) => Self {
                id: None,
                external_id: Some(ext),
            },
            SubscriptionIdentifyable::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}

/// Things accepted as a timeseries identifier when constructing a SubscriptionFilter or
/// Subscription. Mirrors `PyTimeseriesIdentifiable` but lives here to avoid a hard dep on the
/// timeseries module's enum (which would force importing it everywhere).
#[derive(Clone, FromPyObject)]
pub enum SubscriptionTimeseriesId {
    TimeSeries(PyTimeSeries),
    Collection(PyIdCollection),
    ExternalId(String),
    Id(u64),
}

impl From<SubscriptionTimeseriesId> for IdAndExtId {
    fn from(value: SubscriptionTimeseriesId) -> Self {
        match value {
            SubscriptionTimeseriesId::TimeSeries(ts) => Self {
                id: ts.inner.id,
                external_id: Some(ts.inner.external_id.clone()),
            },
            SubscriptionTimeseriesId::Collection(c) => c.into(),
            SubscriptionTimeseriesId::ExternalId(ext) => Self {
                id: None,
                external_id: Some(ext),
            },
            SubscriptionTimeseriesId::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}

// -- WebSocket message wrappers ------------------------------------------------------------

#[pyclass(module = "intellistream_datahub_sdk", name = "EventAction")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum PyEventAction {
    #[strum(ascii_case_insensitive)]
    Create,
    #[strum(ascii_case_insensitive)]
    Update,
    #[strum(ascii_case_insensitive)]
    Delete,
    #[strum(ascii_case_insensitive)]
    Rename,
}

impl From<EventAction> for PyEventAction {
    fn from(value: EventAction) -> Self {
        match value {
            EventAction::Create => PyEventAction::Create,
            EventAction::Update => PyEventAction::Update,
            EventAction::Delete => PyEventAction::Delete,
            EventAction::Rename => PyEventAction::Rename,
        }
    }
}

#[pymethods]
impl PyEventAction {
    fn __repr__(&self) -> String {
        self.to_string()
    }
    fn __str__(&self) -> String {
        self.to_string()
    }
}

#[pyclass(module = "intellistream_datahub_sdk", name = "EventObject")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, Display)]
#[strum(serialize_all = "UPPERCASE")]
pub enum PyEventObject {
    #[strum(ascii_case_insensitive)]
    Label,
    #[strum(ascii_case_insensitive)]
    Relation,
    #[strum(ascii_case_insensitive)]
    Resource,
    #[strum(ascii_case_insensitive)]
    Timeseries,
    #[strum(ascii_case_insensitive)]
    Function,
    #[strum(ascii_case_insensitive)]
    Event,
    #[strum(ascii_case_insensitive)]
    Datapoints,
    #[strum(serialize = "RESOURCE_AND_RELATION", ascii_case_insensitive)]
    ResourceAndRelation,
}

impl From<EventObject> for PyEventObject {
    fn from(value: EventObject) -> Self {
        match value {
            EventObject::Label => PyEventObject::Label,
            EventObject::Relation => PyEventObject::Relation,
            EventObject::Resource => PyEventObject::Resource,
            EventObject::Timeseries => PyEventObject::Timeseries,
            EventObject::Function => PyEventObject::Function,
            EventObject::Event => PyEventObject::Event,
            EventObject::Datapoints => PyEventObject::Datapoints,
            EventObject::ResourceAndRelation => PyEventObject::ResourceAndRelation,
        }
    }
}

#[pymethods]
impl PyEventObject {
    fn __repr__(&self) -> String {
        self.to_string()
    }
    fn __str__(&self) -> String {
        self.to_string()
    }
}

#[pyclass(module = "intellistream_datahub_sdk", name = "WsDatapoint")]
#[derive(Clone)]
pub struct PyWsDatapoint {
    pub inner: WsDatapoint,
}

impl From<WsDatapoint> for PyWsDatapoint {
    fn from(value: WsDatapoint) -> Self {
        Self { inner: value }
    }
}

#[pymethods]
impl PyWsDatapoint {
    #[getter]
    fn timestamp(&self) -> &str {
        &self.inner.timestamp
    }
    #[getter]
    fn value(&self) -> &str {
        &self.inner.value
    }
    /// Parse the value as a float. Raises ValueError if the value isn't numeric (e.g. for
    /// string-typed timeseries that share this delivery channel).
    fn as_float(&self) -> PyResult<f64> {
        f64::from_str(&self.inner.value).map_err(|e| {
            PyValueError::new_err(format!(
                "datapoint value '{}' is not a float: {}",
                self.inner.value, e
            ))
        })
    }
}

#[pyclass(module = "intellistream_datahub_sdk", name = "DataCollectionString")]
#[derive(Clone)]
pub struct PyDataCollectionString {
    pub inner: DataCollectionString,
}

impl From<DataCollectionString> for PyDataCollectionString {
    fn from(value: DataCollectionString) -> Self {
        Self { inner: value }
    }
}

#[pymethods]
impl PyDataCollectionString {
    #[getter]
    fn id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    fn external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
    #[getter]
    fn value_type(&self) -> Option<&str> {
        self.inner.value_type.as_deref()
    }
    #[getter]
    fn inclusive_begin(&self) -> Option<&str> {
        self.inner.inclusive_begin.as_deref()
    }
    #[getter]
    fn exclusive_end(&self) -> Option<&str> {
        self.inner.exclusive_end.as_deref()
    }
    #[getter]
    fn datapoints(&self) -> Vec<PyWsDatapoint> {
        self.inner
            .datapoints
            .iter()
            .cloned()
            .map(PyWsDatapoint::from)
            .collect()
    }
}

#[pyclass(module = "intellistream_datahub_sdk", name = "DataWrapperMessage")]
#[derive(Clone)]
pub struct PyDataWrapperMessage {
    pub inner: DataWrapperMessage,
}

impl From<DataWrapperMessage> for PyDataWrapperMessage {
    fn from(value: DataWrapperMessage) -> Self {
        Self { inner: value }
    }
}

#[pymethods]
impl PyDataWrapperMessage {
    #[getter]
    fn event_action(&self) -> PyEventAction {
        self.inner.event_action.clone().into()
    }
    #[getter]
    fn event_object(&self) -> PyEventObject {
        self.inner.event_object.clone().into()
    }
    #[getter]
    fn tenant_id(&self) -> Option<&str> {
        self.inner.tenant_id.as_deref()
    }
    #[getter]
    fn items(&self) -> Vec<PyDataCollectionString> {
        self.inner
            .items
            .iter()
            .cloned()
            .map(PyDataCollectionString::from)
            .collect()
    }
}

#[pyclass(module = "intellistream_datahub_sdk", name = "SubscriptionMessage")]
#[derive(Clone)]
pub struct PySubscriptionMessage {
    pub inner: SubscriptionMessage,
}

impl From<SubscriptionMessage> for PySubscriptionMessage {
    fn from(value: SubscriptionMessage) -> Self {
        Self { inner: value }
    }
}

#[pymethods]
impl PySubscriptionMessage {
    /// The subscription this message was delivered for (useful when one listener multiplexes
    /// several subscriptions).
    #[getter]
    fn subscription_external_id(&self) -> &str {
        &self.inner.subscription_external_id
    }
    #[getter]
    fn message_id(&self) -> &str {
        &self.inner.message_id
    }
    #[getter]
    fn payload(&self) -> PyDataWrapperMessage {
        self.inner.payload.clone().into()
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySubscription>()?;
    m.add_class::<PySubscriptionFilter>()?;
    m.add_class::<PySubscriptionMessage>()?;
    m.add_class::<PyDataWrapperMessage>()?;
    m.add_class::<PyDataCollectionString>()?;
    m.add_class::<PyWsDatapoint>()?;
    m.add_class::<PyEventAction>()?;
    m.add_class::<PyEventObject>()?;
    m.add_class::<listener::PySubscriptionListener>()?;
    m.add_class::<listener::PySubscriptionListenerAsync>()?;
    m.add_class::<sync_service::PySubscriptionsServiceSync>()?;
    m.add_class::<async_service::PySubscriptionsServiceAsync>()?;
    Ok(())
}
