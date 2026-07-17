use crate::PyEvent;
use chrono::{DateTime, Utc};
use pyo3::pymethods;
use std::collections::HashMap;
use uuid::Uuid;

#[pymethods]
impl PyEvent {
    #[new]
    #[pyo3(signature=(
    external_id,
    event_time,
    metadata=None,
    description=None,
    r#type=None,
    sub_type=None,
    status=None,
    data_set_id=None,
    related_resource_ids=None,
    related_resource_external_ids=None,
    source=None,
    ))]
    pub fn __init__(
        external_id: String,
        event_time: DateTime<Utc>,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        r#type: Option<String>,
        sub_type: Option<String>,
        status: Option<String>,
        data_set_id: Option<u64>,
        related_resource_ids: Option<Vec<u64>>,
        related_resource_external_ids: Option<Vec<String>>,
        source: Option<String>,
    ) -> Self {
        let mut ev = dataplatform_rust_sdk::Event::new(external_id, event_time);
        ev.metadata = metadata;
        ev.description = description;
        ev.r#type = r#type;
        ev.sub_type = sub_type;
        ev.status = status;
        ev.data_set_id = data_set_id;
        ev.related_resource_ids = related_resource_ids.unwrap_or_default();
        ev.related_resource_external_ids = related_resource_external_ids.unwrap_or_default();
        ev.source = source;
        Self { inner: ev }
    }
    #[getter]
    pub fn id(&self) -> Option<Uuid> {
        // The Rust SDK already holds a Uuid (serde parses the wire string); pyo3's `uuid` feature
        // converts it to a Python `uuid.UUID` so callers get a real UUID, matching the type stub.
        self.inner.get_id().copied()
    }

    #[getter]
    pub fn external_id(&self) -> &str {
        self.inner.get_external_id()
    }
    #[setter]
    pub fn set_external_id(&mut self, value: String) {
        self.inner.external_id = value;
    }
    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.get_metadata()
    }
    #[setter]
    pub fn set_metadata(&mut self, value: Option<HashMap<String, String>>) {
        self.inner.metadata = value;
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.get_description()
    }
    #[setter]
    pub fn set_description(&mut self, value: Option<String>) {
        self.inner.description = value;
    }
    #[getter]
    pub fn r#type(&self) -> Option<&str> {
        self.inner.get_type()
    }
    #[setter]
    pub fn set_type(&mut self, value: Option<String>) {
        self.inner.r#type = value;
    }
    #[getter]
    pub fn sub_type(&self) -> Option<&str> {
        self.inner.get_sub_type()
    }
    #[setter]
    pub fn set_sub_type(&mut self, value: Option<String>) {
        self.inner.sub_type = value;
    }
    #[getter]
    pub fn status(&self) -> Option<&str> {
        self.inner.get_status()
    }
    #[setter]
    pub fn set_status(&mut self, value: Option<String>) {
        self.inner.status = value;
    }
    #[getter]
    pub fn created_time(&self) -> Option<&DateTime<Utc>> {
        self.inner.get_created_time()
    }
    #[getter]
    pub fn last_updated_time(&self) -> Option<&DateTime<Utc>> {
        self.inner.get_last_updated_time()
    }
    #[getter]
    pub fn data_set_id(&self) -> Option<u64> {
        self.inner.get_data_set_id()
    }
    #[setter]
    pub fn set_data_set_id(&mut self, value: Option<u64>) {
        self.inner.data_set_id = value;
    }
    #[getter]
    pub fn related_resource_ids(&self) -> &Vec<u64> {
        self.inner.get_related_resource_ids()
    }
    #[setter]
    pub fn set_related_resource_ids(&mut self, value: Vec<u64>) {
        self.inner.related_resource_ids = value;
    }
    #[getter]
    pub fn related_resource_external_ids(&self) -> &Vec<String> {
        self.inner.get_related_resource_external_ids()
    }
    #[setter]
    pub fn set_related_resource_external_ids(&mut self, value: Vec<String>) {
        self.inner.related_resource_external_ids = value;
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.get_source()
    }
    #[setter]
    pub fn set_source(&mut self, value: Option<String>) {
        self.inner.source = value;
    }
    #[getter]
    pub fn event_time(&self) -> &DateTime<Utc> {
        self.inner.get_event_time()
    }
    #[setter]
    pub fn set_event_time(&mut self, value: DateTime<Utc>) {
        self.inner.event_time = value;
    }
}
