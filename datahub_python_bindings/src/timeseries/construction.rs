use std::collections::HashMap;
use pyo3::pymethods;
use dataplatform_rust_sdk::generic::{DatapointString, DatapointsCollection};
use dataplatform_rust_sdk::TimeSeries;
use crate::timeseries::{PyDatapointString, PyDatapointsCollectionString, PyTimeSeries};

#[pymethods]
impl PyTimeSeries {
    #[staticmethod]
    pub fn from_dict(dict: HashMap<String, String>) -> PyTimeSeries {
        PyTimeSeries{inner: TimeSeries::from_dict(dict)}
    }
    #[new]
    pub fn new(
        id: u64,
        external_id: String,
        name: String,
        value_type: String,
        unit: Option<String>,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        unit_external_id: Option<String>,
        security_categories: Option<Vec<u64>>,
        data_set_id: Option<u64>) -> PyTimeSeries {
        let inner = TimeSeries{id,external_id,name,metadata,unit,description,unit_external_id,security_categories,data_set_id,value_type, created_time: None, last_updated_time: None, relations_from: vec![] };
        PyTimeSeries{inner}
    }
}

#[pymethods]
impl PyDatapointsCollectionString{
    #[new]
    pub fn new(pydatapoints: Vec<PyDatapointString>,id: Option<u64>,external_id: Option<String> ,next_cursor: Option<String>) -> PyDatapointsCollectionString {
        let datapoints: Vec<DatapointString> = pydatapoints.into_iter().map(|datapoint| datapoint.into()).collect();
        let inner: DatapointsCollection<DatapointString> = DatapointsCollection{datapoints,next_cursor,id,external_id,unit:None,unit_external_id:None};
        PyDatapointsCollectionString{inner}
    }
}