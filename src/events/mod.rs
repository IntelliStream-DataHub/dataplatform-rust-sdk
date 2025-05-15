use std::collections::HashMap;
use std::rc::{Weak};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::ApiService;
use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtIdCollection, RelationForm};
use crate::http::ResponseError;
use crate::timeseries::{TimeSeries};

pub struct EventsService<'a>{
    pub(crate) api_service: Weak<ApiService<'a>>,
    base_url: String
}

impl<'a> EventsService<'a>{

    pub fn new(api_service: Weak<ApiService<'a>>, base_url: &String) -> Self {
        let base_url = format!("{}/events", base_url);
        EventsService {api_service, base_url}
    }

    pub fn create_events(&self, data: &DataWrapper<TimeSeries>)
        -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub fn delete_events(&self, data: &DataWrapper<IdAndExtIdCollection>)
        -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub fn filters_events(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub fn list_events(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub async fn get_event_by_id(&self, id: String) 
        -> Result<DataWrapper<Event>, ResponseError> {
        self.execute_get_request(&self.base_url).await
    }

    pub fn retrieve_events(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub fn search_events(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub fn update_events(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Event {
    pub id: Uuid,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub sub_type: Option<String>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "valueType")]
    pub value_type: String,
    #[serde(rename = "createdTime")]
    pub created_time: Option<u64>,
    #[serde(rename = "lastUpdatedTime")]
    pub last_updated_time: Option<u64>,
    #[serde(rename = "relatedResourceIds")]
    pub related_resource_ids: Vec<u64>,
    #[serde(rename = "relatedResourceExternalIds")]
    pub related_resource_external_ids: Vec<String>,
    pub source: Option<String>,
    #[serde(rename = "startTime")]
    pub start_time: Option<DateTime<Utc>>,
    #[serde(rename = "endTime")]
    pub end_time: Option<DateTime<Utc>>,
}

impl Event {

}