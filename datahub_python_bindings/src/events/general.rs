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
    metadata=None,
    description=None,
    r#type=None,
    sub_type=None,
    status=None,
    data_set_id=None,
    related_resource_ids=None,
    related_resource_external_ids=None,
    source=None,
    event_time=None,
    ))]
    pub fn __init__(
        external_id: String,
        metadata: Option<HashMap<String, String>>,
        description: Option<String>,
        r#type: Option<String>,
        sub_type: Option<String>,
        status: Option<String>,
        data_set_id: Option<u64>,
        related_resource_ids: Option<Vec<u64>>,
        related_resource_external_ids: Option<Vec<String>>,
        source: Option<String>,
        event_time: Option<DateTime<Utc>>,
    ) -> Self {
        let mut ev = dataplatform_rust_sdk::Event::new(external_id);
        ev.metadata = metadata;
        ev.description = description;
        ev.r#type = r#type;
        ev.sub_type = sub_type;
        ev.status = status;
        ev.data_set_id = data_set_id;

        ev.data_set_id = data_set_id;
        ev.related_resource_ids = related_resource_ids.unwrap_or_default();
        ev.related_resource_external_ids = related_resource_external_ids.unwrap_or_default();
        ev.source = source;
        ev.event_time = event_time;
        Self { inner: ev }
    }
    #[getter]
    pub fn id(&self) -> Option<String> {
        self.inner.get_id().map(ToString::to_string)
    }

    #[getter]
    pub fn external_id(&self) -> &str {
        self.inner.get_external_id()
    }
    #[getter]
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        self.inner.get_metadata()
    }
    #[getter]
    pub fn description(&self) -> Option<&str> {
        self.inner.get_description()
    }
    #[getter]
    pub fn r#type(&self) -> Option<&str> {
        self.inner.get_type()
    }
    #[getter]
    pub fn sub_type(&self) -> Option<&str> {
        self.inner.get_sub_type()
    }
    #[getter]
    pub fn status(&self) -> Option<&str> {
        self.inner.get_status()
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
    #[getter]
    pub fn related_resource_ids(&self) -> &Vec<u64> {
        self.inner.get_related_resource_ids()
    }
    #[getter]
    pub fn related_resource_external_ids(&self) -> &Vec<String> {
        self.inner.get_related_resource_external_ids()
    }
    #[getter]
    pub fn source(&self) -> Option<&str> {
        self.inner.get_source()
    }
    #[getter]
    pub fn event_time(&self) -> Option<&DateTime<Utc>> {
        self.inner.get_event_time()
    }
}
