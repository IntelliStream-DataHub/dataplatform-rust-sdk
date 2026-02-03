mod filters;
#[cfg(test)]
mod tests;

use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::filters::Filters;
use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtIdCollection};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, Utc};
use filters::EventFilter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Weak;
use uuid::Uuid;

pub struct EventsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl EventsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/events", base_url);
        EventsService {
            api_service,
            base_url,
        }
    }

    pub async fn create(&self, data: &Vec<Event>) -> Result<DataWrapper<Event>, ResponseError> {
        let mut dw = DataWrapper::new();
        dw.set_items(data.clone());
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<Event>, _>(path, &dw)
            .await
    }

    pub async fn create_one(&self, event: &Event) -> Result<DataWrapper<Event>, ResponseError> {
        let events = vec![event.clone()];
        self.create(&events).await
    }

    pub async fn delete_by_external_ids(
        &self,
        data: Vec<&str>,
    ) -> Result<DataWrapper<Event>, ResponseError> {
        let data = IdAndExtIdCollection::from_external_id_vec(data);
        self.delete(&data).await
    }

    pub async fn delete_by_ids(&self, data: Vec<u64>) -> Result<DataWrapper<Event>, ResponseError> {
        let data = IdAndExtIdCollection::from_id_vec(data);
        self.delete(&data).await
    }

    pub async fn delete(
        &self,
        json: &IdAndExtIdCollection,
    ) -> Result<DataWrapper<Event>, ResponseError> {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, json).await
    }

    pub async fn filter(&self, filter: &EventFilter) -> Result<DataWrapper<Event>, ResponseError> {
        let path = &format!("{}/filter", self.base_url);
        self.execute_post_request(path, &filter).await
    }

    pub async fn get_event_by_id(&self, id: String) -> Result<DataWrapper<Event>, ResponseError> {
        self.execute_get_request(&self.base_url,None::<&str>).await
    }

    pub async fn by_ids(
        &self,
        id_collection: &IdAndExtIdCollection,
    ) -> Result<DataWrapper<Event>, ResponseError> {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<Event>, _>(path, id_collection)
            .await
    }

    pub fn retrieve(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub fn search(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }

    pub fn update(&self) -> Result<(), ResponseError> {
        unimplemented!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Event {
    pub id: Option<Uuid>,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    #[serde(rename = "subType")]
    pub sub_type: Option<String>,
    pub status: Option<String>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Option<u64>,
    #[serde(skip_serializing)]
    #[serde(rename = "createdTime")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing)]
    #[serde(rename = "lastUpdatedTime")]
    pub last_updated_time: Option<DateTime<Utc>>,
    #[serde(rename = "relatedResourceIds")]
    pub related_resource_ids: Vec<u64>,
    #[serde(rename = "relatedResourceExternalIds")]
    pub related_resource_external_ids: Vec<String>,
    pub source: Option<String>,
    #[serde(rename = "eventTime")]
    pub event_time: Option<DateTime<Utc>>,
}

impl Event {
    pub fn new(external_id: String) -> Self {
        Event {
            id: None,
            external_id,
            metadata: None,
            description: None,
            r#type: None,
            sub_type: None,
            status: None,
            data_set_id: None,
            created_time: None,
            last_updated_time: None,
            related_resource_ids: vec![],
            related_resource_external_ids: vec![],
            source: None,
            event_time: None,
        }
    }

    pub fn add_metadata(&mut self, key: String, value: String) {
        if self.metadata.is_none() {
            self.metadata = Some(HashMap::new());
        }
        self.metadata.as_mut().unwrap().insert(key, value);
    }

    pub fn remove_metadata(&mut self, key: String) {
        if self.metadata.is_some() {
            self.metadata.as_mut().unwrap().remove(&key);
        }
    }

    pub fn add_related_resource_id(&mut self, id: u64) {
        self.related_resource_ids.push(id);
    }

    pub fn remove_related_resource_id(&mut self, id: u64) {
        self.related_resource_ids.retain(|&x| x != id);
    }

    pub fn add_related_resource_external_id(&mut self, external_id: String) {
        self.related_resource_external_ids.push(external_id);
    }

    pub fn remove_related_resource_external_id(&mut self, external_id: String) {
        self.related_resource_external_ids
            .retain(|x| x != &external_id);
    }

    pub fn get_id(&self) -> Option<&Uuid> {
        self.id.as_ref()
    }

    pub fn get_external_id(&self) -> &str {
        self.external_id.as_str()
    }

    pub fn set_external_id(&mut self, external_id: String) {
        self.external_id = external_id;
    }

    pub fn get_metadata(&self) -> Option<&HashMap<String, String>> {
        self.metadata.as_ref()
    }

    pub fn get_type(&self) -> Option<&str> {
        self.r#type.as_deref()
    }

    pub fn set_type(&mut self, r#type: String) {
        self.r#type = Some(r#type);
    }

    pub fn get_sub_type(&self) -> Option<&str> {
        self.sub_type.as_deref()
    }

    pub fn set_sub_type(&mut self, sub_type: String) {
        self.sub_type = Some(sub_type);
    }

    pub fn get_data_set_id(&self) -> Option<u64> {
        self.data_set_id
    }

    pub fn get_data_set_id_as_ref(&self) -> Option<&u64> {
        self.data_set_id.as_ref()
    }

    pub fn set_data_set_id(&mut self, data_set_id: u64) {
        self.data_set_id = Some(data_set_id);
    }

    pub fn get_description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    pub fn set_description(&mut self, description: String) {
        self.description = Some(description);
    }

    pub fn get_source(&self) -> Option<&str> {
        self.source.as_deref()
    }

    pub fn set_source(&mut self, source: String) {
        self.source = Some(source);
    }

    pub fn get_status(&self) -> Option<&str> {
        self.status.as_deref()
    }

    pub fn set_status(&mut self, status: &str) {
        self.status = Some(status.to_string());
    }

    pub fn get_event_time(&self) -> Option<&DateTime<Utc>> {
        self.event_time.as_ref()
    }

    pub fn set_event_time(&mut self, event_time: DateTime<Utc>) {
        self.event_time = Some(event_time);
    }

    pub fn get_related_resource_ids(&self) -> &Vec<u64> {
        &self.related_resource_ids
    }

    pub fn get_related_resource_external_ids(&self) -> &Vec<String> {
        &self.related_resource_external_ids
    }

    pub fn get_metadata_keys(&self) -> Option<Vec<&str>> {
        self.metadata
            .as_ref()
            .map(|m| m.keys().map(|k| k.as_str()).collect())
    }

    pub fn get_metadata_value(&self, key: &str) -> Option<&str> {
        self.metadata
            .as_ref()
            .and_then(|m| m.get(key))
            .map(|v| v.as_str())
    }

    pub fn get_created_time(&self) -> Option<&DateTime<Utc>> {
        self.created_time.as_ref()
    }

    pub fn get_last_updated_time(&self) -> Option<&DateTime<Utc>> {
        self.last_updated_time.as_ref()
    }
}
