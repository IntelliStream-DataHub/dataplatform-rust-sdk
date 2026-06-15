use super::{PyTimeSeries, ValueType};
use chrono::{DateTime, Utc};
use pyo3::pymethods;
use std::collections::HashMap;
#[pymethods]
impl PyTimeSeries {
    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }

    #[getter]
    pub fn external_id(&self) -> &str {
        &self.inner.external_id.as_str()
    }
    #[setter]
    pub fn set_external_id(&mut self, value: String) {
        self.inner.external_id = value;
    }

    #[getter]
    pub fn name(&self) -> &String {
        &self.inner.name
    }
    #[setter]
    pub fn set_name(&mut self, value: String) {
        self.inner.name = value;
    }

    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }
    #[setter]
    pub fn set_metadata(&mut self, value: Option<HashMap<String, String>>) {
        self.inner.metadata = value;
    }

    #[getter]
    pub fn unit(&self) -> Option<&str> {
        self.inner.unit.as_deref()
    }
    #[setter]
    pub fn set_unit(&mut self, value: Option<String>) {
        self.inner.unit = value;
    }

    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.description.as_deref()
    }
    #[setter]
    pub fn set_description(&mut self, value: Option<String>) {
        self.inner.description = value;
    }
    #[getter]
    pub fn unit_external_id(&self) -> Option<&str> {
        self.inner.unit_external_id.as_deref()
    }
    #[setter]
    pub fn set_unit_external_id(&mut self, value: Option<String>) {
        self.inner.unit_external_id = value;
    }
    #[getter]
    pub fn security_categories(&self) -> Option<&Vec<u64>> {
        self.inner.security_categories.as_ref()
    }
    #[setter]
    pub fn set_security_categories(&mut self, value: Option<Vec<u64>>) {
        self.inner.security_categories = value;
    }
    #[getter]
    pub fn data_set_id(&self) -> Option<u64> {
        self.inner.data_set_id
    }
    #[setter]
    pub fn set_data_set_id(&mut self, value: Option<u64>) {
        self.inner.data_set_id = value;
    }
    #[getter]
    pub fn value_type(&self) -> &str {
        &self.inner.value_type.as_str()
    }
    #[setter]
    pub fn set_value_type(&mut self, value: ValueType) {
        self.inner.value_type = value.to_string();
    }
    #[getter]
    pub fn created_time(&self) -> Option<DateTime<Utc>> {
        self.inner.created_time
    }
    #[getter]
    pub fn last_updated_time(&self) -> Option<DateTime<Utc>> {
        self.inner.last_updated_time
    }
}
