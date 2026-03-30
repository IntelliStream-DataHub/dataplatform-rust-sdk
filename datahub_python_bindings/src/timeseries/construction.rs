use crate::timeseries::datapoints::{PyDatapointString, PyDatapointsCollectionString};
use crate::timeseries::{PyRelationFrom, PyTimeSeries, ValueType};
use crate::{DatahubIdentity, Identifyable};
use dataplatform_rust_sdk::TimeSeries;
use dataplatform_rust_sdk::datahub::to_snake_lower_cased_allow_start_with_digits;
use dataplatform_rust_sdk::generic::{DatapointString, DatapointsCollection, RelationForm};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::HashMap;

#[pymethods]
impl PyTimeSeries {
    #[staticmethod]
    pub fn from_dict(dict: HashMap<String, String>) -> PyTimeSeries {
        PyTimeSeries {
            inner: TimeSeries::from_dict(dict),
        }
    }
    /// Constructor for PyTimeSeries
    ///
    /// Either name or external_id must be provided
    /// if name is provided external_id will be set to snakecase of provided name
    /// if external_id is provided name will be equal to external_id
    /// providing neither will raise a ValueError
    ///
    #[new]
    #[pyo3(signature = (
    name = None,
    external_id = None,
    value_type = ValueType::BigInt,
    metadata = None,
    description = None,
    unit = None,
    unit_external_id = None,
    security_categories = None,
    data_set_id = None,
    relations_from = None
))]
    pub fn new(
        name: Option<String>,
        external_id: Option<String>,
        value_type: ValueType,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        unit: Option<String>,
        unit_external_id: Option<String>,
        security_categories: Option<Vec<u64>>,
        data_set_id: Option<u64>,
        relations_from: Option<Vec<PyRelationFrom>>,
    ) -> PyResult<PyTimeSeries> {
        let (final_name, final_ext_id) = match (name, external_id) {
            (Some(name), Some(external_id)) => (name, external_id),
            (None, Some(external_id)) => (external_id.clone(), external_id),
            (Some(name), None) => (
                name.clone(),
                to_snake_lower_cased_allow_start_with_digits(&name),
            ),
            (None, None) => {
                return Err(PyValueError::new_err(
                    "name or external_id must be provided",
                ));
            }
        };
        let inner = TimeSeries {
            id: None,
            external_id: final_ext_id, // external_id is required and is either set or generated from name
            name: final_name,
            metadata,
            unit,
            description,
            unit_external_id,
            security_categories,
            data_set_id,
            value_type: value_type.to_string(),
            created_time: None,
            last_updated_time: None,
            relations_from: relations_from
                .map(|r| r.into_iter().map(|s| RelationForm::from(s)).collect())
                .unwrap_or_default(),
        };
        Ok(PyTimeSeries { inner })
    }
}

#[pymethods]
impl PyDatapointsCollectionString {
    #[new]
    #[pyo3(signature=(datapoints,ts))]
    pub fn new(
        datapoints: Vec<PyDatapointString>,
        ts: Identifyable,
    ) -> PyDatapointsCollectionString {
        let datapoints: Vec<DatapointString> = datapoints
            .into_iter()
            .map(|datapoint| datapoint.into())
            .collect();

        if let Identifyable::TimeSeries(ts) = ts {
            let inner: DatapointsCollection<DatapointString> = DatapointsCollection {
                datapoints,
                next_cursor: None,
                id: ts.id(),
                external_id: Some(ts.external_id().to_string()),
                unit: ts.unit().map(|u| u.to_string()),
                unit_external_id: ts.unit_external_id().map(|u| u.to_string()),
            };
            PyDatapointsCollectionString { inner }
        } else {
            let inner: DatapointsCollection<DatapointString> = DatapointsCollection {
                datapoints,
                next_cursor: None,
                id: ts.id_collection().id,
                external_id: ts.id_collection().external_id,
                unit: None,
                unit_external_id: None,
            };
            PyDatapointsCollectionString { inner }
        }
    }
}
