use crate::PyIdCollection;
use crate::timeseries::PyTimeSeries;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::subscriptions::{
    DataCollectionString, DataSort, DataWrapperMessage, EventAction, EventObject, Subscription,
    SubscriptionFilter, SubscriptionMessage, SubscriptionRetriever, WsDatapoint,
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

#[pyclass(module = "datahub_python_sdk", name = "Subscription")]
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

#[pyclass(module = "datahub_python_sdk", name = "SubscriptionFilter")]
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
    #[new]
    #[pyo3(signature=(timeseries=None))]
    fn new(timeseries: Option<Vec<SubscriptionTimeseriesId>>) -> Self {
        let timeseries = timeseries
            .unwrap_or_default()
            .into_iter()
            .map(IdAndExtId::from)
            .collect();
        Self {
            inner: SubscriptionFilter { timeseries },
        }
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

#[pyclass(module = "datahub_python_sdk", name = "DataSort")]
#[derive(Clone, Default)]
pub struct PyDataSort {
    pub inner: DataSort,
}

impl From<DataSort> for PyDataSort {
    fn from(s: DataSort) -> Self {
        Self { inner: s }
    }
}
impl From<PyDataSort> for DataSort {
    fn from(s: PyDataSort) -> Self {
        s.inner
    }
}

#[pymethods]
impl PyDataSort {
    #[new]
    #[pyo3(signature=(property=None, order=None, nulls=None))]
    fn new(
        property: Option<Vec<String>>,
        order: Option<String>,
        nulls: Option<String>,
    ) -> Self {
        Self {
            inner: DataSort {
                property,
                order,
                nulls,
            },
        }
    }

    #[getter]
    fn property(&self) -> Option<Vec<String>> {
        self.inner.property.clone()
    }
    #[getter]
    fn order(&self) -> Option<String> {
        self.inner.order.clone()
    }
    #[getter]
    fn nulls(&self) -> Option<String> {
        self.inner.nulls.clone()
    }
}

#[pyclass(module = "datahub_python_sdk", name = "SubscriptionRetriever")]
#[derive(Clone)]
pub struct PySubscriptionRetriever {
    pub inner: SubscriptionRetriever,
}

impl From<SubscriptionRetriever> for PySubscriptionRetriever {
    fn from(s: SubscriptionRetriever) -> Self {
        Self { inner: s }
    }
}
impl From<PySubscriptionRetriever> for SubscriptionRetriever {
    fn from(s: PySubscriptionRetriever) -> Self {
        s.inner
    }
}

#[pymethods]
impl PySubscriptionRetriever {
    #[new]
    #[pyo3(signature=(filter=None, limit=None, sort=None))]
    fn new(
        filter: Option<PySubscriptionFilter>,
        limit: Option<u32>,
        sort: Option<PyDataSort>,
    ) -> Self {
        let mut inner = SubscriptionRetriever::default();
        if let Some(f) = filter {
            inner.filter = f.into();
        }
        if let Some(l) = limit {
            inner.limit = l;
        }
        if let Some(s) = sort {
            inner.sort = s.into();
        }
        Self { inner }
    }

    #[getter]
    fn filter(&self) -> PySubscriptionFilter {
        self.inner.filter.clone().into()
    }
    #[getter]
    fn limit(&self) -> u32 {
        self.inner.limit
    }
    #[getter]
    fn sort(&self) -> PyDataSort {
        self.inner.sort.clone().into()
    }
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

#[pyclass(module = "datahub_python_sdk", name = "EventAction")]
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

#[pyclass(module = "datahub_python_sdk", name = "EventObject")]
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

#[pyclass(module = "datahub_python_sdk", name = "WsDatapoint")]
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

#[pyclass(module = "datahub_python_sdk", name = "DataCollectionString")]
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

#[pyclass(module = "datahub_python_sdk", name = "DataWrapperMessage")]
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

#[pyclass(module = "datahub_python_sdk", name = "SubscriptionMessage")]
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
    m.add_class::<PyDataSort>()?;
    m.add_class::<PySubscriptionRetriever>()?;
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
