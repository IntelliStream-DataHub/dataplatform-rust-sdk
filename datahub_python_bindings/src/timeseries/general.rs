use std::collections::HashMap;
use chrono::{DateTime, Utc};
use pyo3::pymethods;
use super::PyTimeSeries;
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

    #[getter]
    pub fn name(&self) -> &String {
        &self.inner.name
    }


    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.metadata.as_ref()
    }


    #[getter]
    pub fn unit(&self) -> Option<&str> {
        self.inner.unit.as_deref()
    }

    #[getter]
    pub fn description(&self) -> Option<String> {
        self.inner.description.clone()
    }

    #[getter]
    pub fn unit_external_id(&self)-> Option<&String> {
        self.inner.unit_external_id.as_ref()
    }
    #[getter]
    pub fn security_categories(&self)-> Option<&Vec<u64>> {
        self.inner.security_categories.as_ref()
    }
    #[getter]
    pub fn data_set_id(&self)-> Option<u64> {
        self.inner.data_set_id
    }
    #[getter]
    pub fn value_type(&self)-> &str {
        &self.inner.value_type.as_str()
    }
    #[getter]
    pub fn created_time(&self)-> Option<DateTime<Utc>> {
        self.inner.created_time
    }
    #[getter]
    pub fn last_updated_time(&self)-> Option<DateTime<Utc>> {
        self.inner.last_updated_time
    }
}
