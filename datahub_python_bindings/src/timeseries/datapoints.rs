use crate::datetime::{opt_py_datetime_to_utc, py_datetime_to_utc};
use crate::timeseries::PyTimeseriesIdentifiable;
use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::generic::{
    Datapoint, DatapointString, DatapointsCollection, IdAndExtId, Identifiable, RetrieveFilter,
};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyType};
use pyo3::{Bound, Py, Python, pyclass, pymethods};

#[pyclass(module = "datahub_sdk", name = "DatapointsCollectionString")]
#[derive(Clone, Debug)]
pub struct PyDatapointsCollectionString {
    pub inner: DatapointsCollection<DatapointString>,
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

#[pyclass(module = "datahub_sdk", name = "DatapointsCollectionDatapoints")]
#[derive(Clone, Debug)]
pub struct PyDatapointsCollectionDatapoints {
    pub inner: DatapointsCollection<Datapoint>,
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

#[pymethods]
impl PyDatapointsCollectionDatapoints {
    //#[getter]
    //pub fn datapoints(&self) -> Vec<PyDatapoint> {
    //    self.inner.datapoints.iter().map(|dp| PyDatapoint { inner: dp.clone() }).collect()
    //}
    pub fn get_datapoints(&self) -> Vec<PyDatapoint> {
        self.inner
            .datapoints
            .iter()
            .map(|dp| PyDatapoint { inner: dp.clone() })
            .collect()
    }

    pub fn __len__(&self) -> usize {
        self.inner.datapoints.len()
    }
    #[getter]
    pub fn next_cursor(&self) -> Option<String> {
        self.inner.next_cursor.clone()
    }
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }

    fn as_dict<'py>(&self, py: Python<'py>) -> PyResult<pyo3::Bound<'py, PyDict>> {
        let timestamps: Vec<DateTime<chrono::Utc>> = self
            .inner
            .datapoints
            .iter()
            .map(|dp| dp.timestamp())
            .collect();

        let values: Vec<Option<f64>> = self.inner.datapoints.iter().map(|dp| dp.value()).collect();

        let dict = PyDict::new(py); // Create a bound dict
        dict.set_item("timestamps", timestamps)?;
        dict.set_item("values", values)?;

        Ok(dict)
    }
}

#[pyclass(module = "datahub_sdk", name = "DatapointString")]
#[derive(Clone)]
pub struct PyDatapointString {
    pub inner: DatapointString,
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
    pub fn new(ts: &Bound<'_, PyAny>, value: &str) -> PyResult<Self> {
        Ok(Self {
            inner: DatapointString::from_datetime(py_datetime_to_utc(ts)?, value),
        })
    }
    #[getter]
    pub fn timestamp(&self) -> &String {
        &self.inner.timestamp
    }
    #[getter]
    pub fn value(&self) -> &str {
        self.inner.value.as_str()
    }
    #[setter]
    pub fn set_timestamp(&mut self, value: &str) {
        self.inner.timestamp = value.to_string();
    }
    #[setter]
    pub fn set_value(&mut self, value: &str) {
        self.inner.value = value.to_string();
    }
    #[classmethod]
    pub fn from_int(_cls: &Bound<'_, PyType>, ts: &Bound<'_, PyAny>, value: i64) -> PyResult<Self> {
        Ok(Self {
            inner: DatapointString::from_datetime(py_datetime_to_utc(ts)?, &value.to_string()),
        })
    }
    #[classmethod]
    pub fn from_float(
        _cls: &Bound<'_, PyType>,
        ts: &Bound<'_, PyAny>,
        value: f64,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: DatapointString::from_datetime(py_datetime_to_utc(ts)?, &value.to_string()),
        })
    }
}

#[pyclass(module = "datahub_sdk", name = "RetrieveFilter")]
#[derive(Clone)]
pub struct PyRetrieveFilter {
    inner: RetrieveFilter,
}
impl From<RetrieveFilter> for PyRetrieveFilter {
    fn from(form: RetrieveFilter) -> Self {
        Self { inner: form }
    }
}
impl From<PyRetrieveFilter> for RetrieveFilter {
    fn from(value: PyRetrieveFilter) -> Self {
        value.inner
    }
}

#[pymethods]
impl PyRetrieveFilter {
    #[new]
    #[pyo3(signature = (
    ts,
    start = None,
    end = None,
    limit = None,
    aggregates = None,
    granularity = None,
    cursor = None,
))]
    pub fn new(
        ts: PyTimeseriesIdentifiable,
        start: Option<Bound<'_, PyAny>>,
        end: Option<Bound<'_, PyAny>>,
        limit: Option<u64>,
        aggregates: Option<Vec<String>>,
        granularity: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<Self> {
        let start = opt_py_datetime_to_utc(start.as_ref())?;
        let end = opt_py_datetime_to_utc(end.as_ref())?;
        let id_coll: IdAndExtId = ts.into();
        Ok(Self {
            inner: RetrieveFilter {
                start,
                end,
                limit,
                aggregates,
                granularity,
                cursor,
                id: id_coll.id,
                external_id: id_coll.external_id.clone(),
            },
        })
    }
    #[getter]
    pub fn start(&self) -> Option<DateTime<chrono::Utc>> {
        self.inner.start
    }
    #[getter]
    pub fn end(&self) -> Option<DateTime<chrono::Utc>> {
        self.inner.end
    }
    #[getter]
    pub fn limit(&self) -> Option<u64> {
        self.inner.limit
    }
    #[getter]
    pub fn aggregates(&self) -> Option<&Vec<String>> {
        self.inner.aggregates.as_ref()
    }
    #[getter]
    pub fn granularity(&self) -> Option<&String> {
        self.inner.granularity.as_ref()
    }
    #[getter]
    pub fn cursor(&self) -> Option<&String> {
        self.inner.cursor.as_ref()
    }
}
#[pyclass(module = "datahub_sdk", name = "Datapoint")]
#[derive(Clone)]
pub struct PyDatapoint {
    inner: Datapoint,
}

impl From<Datapoint> for PyDatapoint {
    fn from(value: Datapoint) -> Self {
        Self { inner: value }
    }
}
impl From<PyDatapoint> for Datapoint {
    fn from(value: PyDatapoint) -> Self {
        value.inner
    }
}

#[pymethods]
impl PyDatapoint {
    #[new]
    pub fn new(
        timestamp: &Bound<'_, PyAny>,
        value: Option<f64>,
        min: Option<f64>,
        max: Option<f64>,
        average: Option<f64>,
        sum: Option<f64>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: Datapoint {
                timestamp: py_datetime_to_utc(timestamp)?,
                value,
                min,
                max,
                average,
                sum,
            },
        })
    }
    #[getter]
    pub fn timestamp(&self) -> DateTime<Utc> {
        self.inner.timestamp
    }
    pub fn __str__(&self) -> String {
        self.inner.to_string()
    }
    #[getter]
    pub fn value(&self) -> Option<f64> {
        self.inner.value
    }
    #[getter]
    pub fn min(&self) -> Option<f64> {
        self.inner.min
    }
    #[getter]
    pub fn max(&self) -> Option<f64> {
        self.inner.max
    }
    #[getter]
    pub fn average(&self) -> Option<f64> {
        self.inner.average
    }
    #[getter]
    pub fn sum(&self) -> Option<f64> {
        self.inner.sum
    }
}
