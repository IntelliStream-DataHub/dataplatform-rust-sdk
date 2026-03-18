use chrono::{DateTime, TimeZone, Utc};
use pyo3::pyclass;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};
use dataplatform_rust_sdk::{TimeSeries, TimeSeriesUpdate};
use dataplatform_rust_sdk::generic::{Datapoint, DatapointString, DatapointsCollection, DeleteFilter, RetrieveFilter};

pub mod general;
pub mod async_service;
pub mod sync_service;
mod construction;

#[pyclass]
#[derive(Clone)]
pub struct PyTimeSeries{
    inner: TimeSeries
}

impl From<TimeSeries> for PyTimeSeries {
    fn from(ts: TimeSeries) -> Self {
        Self { inner: ts }
    }
}
impl From<PyTimeSeries> for TimeSeries {
    fn from(ts: PyTimeSeries) -> Self {
        ts.inner
    }
}

impl PyTimeSeries {
    pub(crate) fn fromTimeseries(inner: TimeSeries) -> Self {
        Self { inner }
    }
}

#[pyclass(module="datahub_python_sdk")]
#[derive(Clone,Debug)]
pub struct PyTimeSeriesUpdate{
    inner: TimeSeriesUpdate
}
impl From<TimeSeriesUpdate> for PyTimeSeriesUpdate {
    fn from(ts: TimeSeriesUpdate) -> Self {
        Self { inner: ts }
    }
}
impl From<PyTimeSeriesUpdate> for TimeSeriesUpdate {
    fn from(ts: PyTimeSeriesUpdate) -> Self {
        ts.inner
    }
}

impl PyTimeSeriesUpdate {
    pub(crate) fn new(inner: TimeSeriesUpdate) -> Self {
        Self { inner }
    }

}

#[pyclass(module="datahub_python_sdk")]
#[derive(Clone,Debug)]
pub struct PyDatapointsCollectionString{
    inner: DatapointsCollection<DatapointString>
}
impl From<DatapointsCollection<DatapointString>> for PyDatapointsCollectionString {
    fn from(ts: DatapointsCollection<DatapointString>) -> Self {
        Self { inner: ts }
    }
}
impl From<PyDatapointsCollectionString> for DatapointsCollection<DatapointString> {
    fn from(ts: PyDatapointsCollectionString) -> Self {
        ts.inner
    }
}

impl PyDatapointsCollectionString {
    pub(crate) fn from_datapoints_collection(inner: DatapointsCollection<DatapointString>) -> Self {
        Self { inner }
    }
}
#[pyclass(module="datahub_python_sdk")]
#[derive(Clone,Debug)]
struct PyDatapointsCollectionDatapoints{
    inner: DatapointsCollection<Datapoint>
}
impl From<DatapointsCollection<Datapoint>> for PyDatapointsCollectionDatapoints {
    fn from(ts: DatapointsCollection<Datapoint>) -> Self {
        Self { inner: ts }
    }
}
impl From<PyDatapointsCollectionDatapoints> for DatapointsCollection<Datapoint> {
    fn from(ts: PyDatapointsCollectionDatapoints) -> Self {
        ts.inner
    }
}
impl PyDatapointsCollectionDatapoints {
    pub(crate) fn from_datapoints_collection(inner: DatapointsCollection<Datapoint>) -> Self {
        Self { inner }
    }
}


#[pyclass(module="datahub_python_sdk")]
#[derive(Clone)]
pub(crate) struct PyDatapointString{
    inner: DatapointString
}
impl From<DatapointString> for PyDatapointString {
    fn from(form: DatapointString) -> Self {
        Self { inner: form }
    }
}
impl From<PyDatapointString> for DatapointString {
    fn from(value: PyDatapointString) -> Self {
        value.inner
    }
}
#[pymethods]
impl PyDatapointString {
    #[new]
    pub fn new(ts: u64, value: f64) -> Self {
        ;
        Self { inner: DatapointString{timestamp: ts.to_string(),value: value.to_string()} }
    }
    #[getter]
    pub fn ts(&self) -> &String {
        &self.inner.timestamp
    }
    #[getter]
    pub fn value(&self) -> f64 {
        self.inner.value.parse::<f64>().unwrap()
    }
}
#[pyclass(module="datahub_python_sdk")]
#[derive(Clone,Debug)]
struct PyDeleteFilter{
    inner: DeleteFilter
}
impl From<DeleteFilter> for PyDeleteFilter {
    fn from(ts: DeleteFilter) -> Self {
        Self { inner: ts }
    }
}
impl From<PyDeleteFilter> for DeleteFilter {
    fn from(ts: PyDeleteFilter) -> Self {
        ts.inner
    }
}

impl PyDeleteFilter {
    pub(crate) fn from_rust(inner: DeleteFilter) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyDeleteFilter {
    #[new]
    pub fn new(id: Option<u64>, external_id: Option<String>, inclusive_begin: Option<String>, exclusive_end: Option<String>) -> Self {
        // 1. Map the Option<String>, parse it, and convert FixedOffset to Utc
        let inclusive_begin = inclusive_begin.map(|s| {
            DateTime::parse_from_rfc3339(&s)
                .expect("Invalid ISO format")
                .with_timezone(&Utc)
        });

        // 2. Assuming exclusive_end is also a timestamp string based on your context
        let exclusive_end = exclusive_end.map(|s| {
            DateTime::parse_from_rfc3339(&s)
                .expect("Invalid ISO format")
                .with_timezone(&Utc)
        });
        Self { inner: DeleteFilter{id,external_id,inclusive_begin,exclusive_end} }
    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }
}