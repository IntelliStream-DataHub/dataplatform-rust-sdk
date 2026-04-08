use crate::events::EventIdentifyable;
use crate::timeseries::async_service::PyTimeSeriesServiceAsync;
use crate::timeseries::datapoints::{
    PyDatapointString, PyDatapointsCollectionDatapoints, PyDatapointsCollectionString,
    PyRetrieveFilter,
};
use crate::timeseries::sync_service::PyTimeSeriesServiceSync;
use crate::{
    DatahubIdentity, Identifiable, PyFieldStr, PyFieldU64, PyIdCollection, PyListFieldU64,
    PyMapField,
};
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use dataplatform_rust_sdk::fields::{Field, ListField, MapField};
use dataplatform_rust_sdk::generic::{
    Datapoint, DatapointString, DatapointsCollection, DeleteFilter, IdAndExtId, RelationForm,
    RetrieveFilter,
};
use dataplatform_rust_sdk::{TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateFields};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::pyclass;
use pyo3::types::{PyDict, PyList, PyTuple};
use serde::de::Unexpected::Map;
use std::collections::HashMap;
use std::str::FromStr;
use strum::{Display, EnumString};

pub mod async_service;
mod construction;
pub mod datapoints;
pub mod general;
pub mod sync_service;

/// Python wrapper for Timeseries objects, represents contextualization data for timeseries
///
/// A timeseries is a univariate series of datapoints of the form (timestamp, value)
///
/// Parameters
/// ----------
/// units: str
///     units of the timeseries, e.g. "a.u" or "mW"
/// name: str | None
///     user defined name of the timeseries, if not provided external_id will be used
///     external id must be between 3 and 512 characters long.
/// external_id: str | None
///     unique user defined id.
///     external id must be between 3 and 512 characters long.
///     external ids must be unique for each timeseries
///     the same external id can be used for other entities.
///     if not provided a snakecase of the name will be used.
/// id: int, default None
///     internal id set by datahub
/// value_type: {"bigint","decimal","text"}
///     String enumerator for the datatype of the timeseries
/// metadata: dict, default None
///     a dict[str,str] of user defined metadata for semi-structured data storage
/// description: str, default None
///     User defined description of the timeseries
/// units: str, default None
///     units of the timeseries, e.g. "a.u", "mW", "Liter/min this can be anything
/// unit_external_id: str, default None
///     External id for the units of the timeseries this is used to connecto to datahub units system.
///     The units system will allow you to convert between units and easily convert between units-systems for unified storage.
///
/// security_categories: list[int], default None
///     Currenty not supported used.
///
/// data_set_id: int
///     the id of the datasets this timeseries belongs to
/// relations_from: list[PyRelationFrom]
///     A list of other Datahub entities that are connected to this timeseries
///     PyRelationForm
///         relationship_type: str
///             the type of relation, e.g. "derived_from" or "measures pressure of"
///         entity: Identifyable
///             a Datahub entity connected to this timeseries can be a Timeseries, Dataset, Asset or Policy
///
///
#[pyclass(module = "datahub_python_sdk", name = "TimeSeries")]
#[derive(Clone)]
pub struct PyTimeSeries {
    pub inner: TimeSeries,
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
impl From<PyTimeSeries> for PyIdCollection {
    fn from(value: PyTimeSeries) -> Self {
        Self {
            inner: IdAndExtId {
                id: value.inner.id,
                external_id: Some(value.inner.external_id.clone()),
            },
        }
    }
}
#[pyclass(module = "datahub_python_sdk", name = "RelationFrom")]
#[derive(Clone, Debug)]
pub struct PyRelationFrom {
    pub inner: RelationForm,
}
impl From<RelationForm> for PyRelationFrom {
    fn from(ts: RelationForm) -> Self {
        Self { inner: ts }
    }
}
impl From<PyRelationFrom> for RelationForm {
    fn from(ts: PyRelationFrom) -> Self {
        ts.inner
    }
}

#[pymethods]
impl PyRelationFrom {
    /// constructor for PyRelationFrom
    #[new]
    #[pyo3(signature=(entity, relationship_type))]
    pub fn new(entity: Identifiable, relationship_type: String) -> Self {
        let id_collection = entity.id_collection();
        Self {
            inner: RelationForm {
                id: id_collection.id,
                external_id: id_collection.external_id,
                relationship_type,
            },
        }
    }
}

#[derive(FromPyObject)]
pub enum PyTimeseriesIdentifiable {
    #[pyo3(transparent)]
    TimeSeries(PyTimeSeries),
    #[pyo3(transparent)]
    Collection(PyIdCollection),
    ExternalId(String),
    Id(u64),
}
impl From<PyTimeseriesIdentifiable> for IdAndExtId {
    fn from(value: PyTimeseriesIdentifiable) -> Self {
        match value {
            PyTimeseriesIdentifiable::Collection(idcoll) => idcoll.into(),
            PyTimeseriesIdentifiable::TimeSeries(ts) => Self {
                id: ts.id(),
                external_id: Some(ts.external_id().to_string()),
            },
            PyTimeseriesIdentifiable::ExternalId(extid) => Self {
                id: None,
                external_id: Some(extid.to_string()),
            },
            PyTimeseriesIdentifiable::Id(id) => Self {
                id: Some(id),
                external_id: None,
            },
        }
    }
}
/// Python wrapper for TimeseriesUpdate, represents a request for change to a timeseries
///
/// Parameters
/// ----------
/// ts: Timeseries
#[pyclass(
    module = "datahub_python_sdk",
    name = "TimeSeriesUpdate",
    from_py_object
)]
#[derive(Clone, Debug)]
pub struct PyTimeSeriesUpdate {
    inner: TimeSeriesUpdate,
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
#[pymethods]
impl PyTimeSeriesUpdate {
    #[new]
    #[pyo3(signature=(
        ts,
        external_id=None,
        name=None,
        metadata=None,
        unit=None,
        description=None,
        unit_external_id=None,
        security_categories=None,
        data_set_id=None,
        relations_from=None,
        value_type=None,
    ))]
    pub fn __init__(
        ts: Identifiable, // todo make TimeseriesIdentifyable
        external_id: Option<PyFieldStr>,
        name: Option<PyFieldStr>,
        metadata: Option<PyMapField>,
        unit: Option<PyFieldStr>,
        description: Option<PyFieldStr>,
        unit_external_id: Option<PyFieldStr>,
        security_categories: Option<PyListFieldU64>,
        data_set_id: Option<PyFieldU64>,
        relations_from: Option<PyListFieldU64>,
        value_type: Option<ValueType>,
    ) -> PyResult<Self> {
        let id_collection = ts.id_collection();
        let update = TimeSeriesUpdateFields {
            external_id: external_id.map(|s| s.0).unwrap_or_default(),
            name: name.map(|s| s.0).unwrap_or_default(),
            metadata: metadata.map(|s| s.0).unwrap_or_default(),
            unit: unit.map(|s| s.0).unwrap_or_default(),
            description: description.map(|s| s.0).unwrap_or_default(),
            unit_external_id: unit_external_id.map(|s| s.0).unwrap_or_default(),
            security_categories: security_categories.map(|s| s.0).unwrap_or_default(),
            data_set_id: data_set_id.map(|s| s.0).unwrap_or_default(),
            relations_from: relations_from.map(|s| s.0).unwrap_or_default(),
            value_type: Field::new(value_type.map(|s| s.to_string()), false),
        };
        Ok(Self {
            inner: TimeSeriesUpdate {
                id: id_collection.id,
                external_id: id_collection.external_id,
                update,
            },
        })
    }
    #[getter]
    fn target_external_id(&self) -> Option<&str> {
        self.inner.external_id.as_deref()
    }
    #[getter]
    fn target_id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    fn external_id(&self) -> PyFieldStr {
        self.inner.update.external_id.clone().into()
    }
    #[getter]
    fn name(&self) -> PyFieldStr {
        self.inner.update.name.clone().into()
    }
    #[getter]
    fn metadata(&self) -> PyMapField {
        self.inner.update.metadata.clone().into()
    }
    #[getter]
    fn unit(&self) -> PyFieldStr {
        self.inner.update.unit.clone().into()
    }
    #[getter]
    fn description(&self) -> PyFieldStr {
        self.inner.update.description.clone().into()
    }
    #[getter]
    fn unit_external_id(&self) -> PyFieldStr {
        self.inner.update.unit_external_id.clone().into()
    }
    #[getter]
    fn security_categories(&self) -> PyListFieldU64 {
        self.inner.update.security_categories.clone().into()
    }
    #[getter]
    fn data_set_id(&self) -> PyFieldU64 {
        self.inner.update.data_set_id.clone().into()
    }
    #[getter]
    fn relations_from(&self) -> PyListFieldU64 {
        self.inner.update.relations_from.clone().into()
    }
    #[getter]
    fn value_type(&self) -> Option<&String> {
        self.inner.update.value_type.set.as_ref()
    }
}

/// Python wrapper for DeleteFilter, represents a request for deleting datapoints
///
/// Parameters
/// ----------
///
///
#[pyclass(module = "datahub_python_sdk", name = "DeleteFilter")]
#[derive(Clone, Debug)]
pub struct PyDeleteFilter {
    inner: DeleteFilter,
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

#[pymethods]
impl PyDeleteFilter {
    #[new]
    #[pyo3(signature=(ts, inclusive_begin=None, exclusive_end=None))]
    pub fn new(
        ts: Identifiable,
        inclusive_begin: Option<DateTime<FixedOffset>>,
        exclusive_end: Option<DateTime<FixedOffset>>,
    ) -> Self {
        let id_collection = ts.id_collection();
        Self {
            inner: DeleteFilter {
                id: id_collection.id,
                external_id: id_collection.external_id,
                inclusive_begin: inclusive_begin.map(|d| d.with_timezone(&Utc)),
                exclusive_end: exclusive_end.map(|d| d.with_timezone(&Utc)),
            },
        }
    }
    #[getter]
    pub fn target_id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    pub fn target_external_id(&self) -> Option<u64> {
        self.inner.id
    }
    #[getter]
    pub fn inclusive_begin(&self) -> Option<DateTime<Utc>> {
        self.inner.inclusive_begin.map(|d| d.with_timezone(&Utc))
    }
    #[getter]
    pub fn exclusive_end(&self) -> Option<DateTime<Utc>> {
        self.inner.exclusive_end.map(|d| d.with_timezone(&Utc))
    }
}

/// Enumerator for the datatype of a timeseries.
///
/// 3 options are available: BigInt, Decimal, Text
///
/// from pyhton these can be passed directly as case-insensitive literal strings
#[pyclass(module = "datahub_python_sdk", skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, Display)]
#[strum(serialize_all = "camelCase")] // Ensures internal string representation is lowercase
pub enum ValueType {
    #[strum(ascii_case_insensitive)]
    BigInt,
    #[strum(ascii_case_insensitive)]
    Decimal,
    #[strum(ascii_case_insensitive)]
    Text,
}

#[pymethods]
impl ValueType {
    #[new]
    fn new(value: &str) -> PyResult<Self> {
        // FromStr is provided by EnumString, handling the case-insensitive logic
        ValueType::from_str(value)
            .map_err(|_| PyValueError::new_err(format!("Invalid data type: {}", value)))
    }

    fn __repr__(&self) -> String {
        self.to_string()
    }
}
// This allows PyO3 to accept a string from Python and turn it into ValueType
impl<'py> FromPyObject<'_, 'py> for ValueType {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        let s = obj.extract::<String>()?;
        ValueType::from_str(&s).map_err(|_| {
            PyValueError::new_err(format!(
                "ValueType '{s}'. Must be 'bigint', 'decimal', or 'text'."
            ))
        })
    }
}

pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PyDeleteFilter>()?;
    m.add_class::<PyRetrieveFilter>()?;
    m.add_class::<PyTimeSeriesUpdate>()?;
    m.add_class::<PyTimeSeries>()?;
    m.add_class::<PyDatapointString>()?;
    m.add_class::<PyDatapointsCollectionDatapoints>()?;
    m.add_class::<PyDatapointsCollectionString>()?;
    Ok(())
}
