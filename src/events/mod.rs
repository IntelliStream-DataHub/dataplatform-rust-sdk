mod test;

use std::collections::HashMap;
use std::rc::{Weak};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::{ApiService};
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::filters::Filters;
use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtIdCollection};
use crate::http::ResponseError;

pub struct EventsService<'a>{
    pub(crate) api_service: Weak<ApiService<'a>>,
    base_url: String
}

impl<'a> EventsService<'a>{

    pub fn new(api_service: Weak<ApiService<'a>>, base_url: &String) -> Self {
        let base_url = format!("{}/events", base_url);
        EventsService {api_service, base_url}
    }

    pub async fn create(&self, data: &Vec<Event>)
        -> Result<DataWrapper<Event>, ResponseError>
    {
        let mut dw = DataWrapper::new();
        dw.set_items(data.clone());
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<Event>, _>(path, &dw).await
    }

    pub async fn create_one(&self, event: &Event)
                            -> Result<DataWrapper<Event>, ResponseError>
    {
        let events = vec![event.clone()];
        self.create(&events).await
    }

    pub async fn delete_by_external_ids(&self, data: Vec<&str>)
                        -> Result<DataWrapper<Event>, ResponseError>
    {
        let data = IdAndExtIdCollection::from_external_id_vec(data);
        self.delete(&data).await
    }

    pub async fn delete_by_ids(&self, data: Vec<u64>)
                                        -> Result<DataWrapper<Event>, ResponseError>
    {
        let data = IdAndExtIdCollection::from_id_vec(data);
        self.delete(&data).await
    }

    pub async fn delete(&self, json: &IdAndExtIdCollection)
                        -> Result<DataWrapper<Event>, ResponseError>
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, json).await
    }

    pub async fn filter(&self, filters: &Filters) -> Result<DataWrapper<Event>, ResponseError> {
        let filter_request = RetrieveEventsFilter::new();
        let path = &format!("{}/list", self.base_url);
        self.execute_post_request(path, &filter_request).await
    }

    pub async fn get_event_by_id(&self, id: String) -> Result<DataWrapper<Event>, ResponseError> {
        self.execute_get_request(&self.base_url).await
    }

    pub async fn by_ids(&self, id_collection: &IdAndExtIdCollection) -> Result<DataWrapper<Event>, ResponseError> {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<Event>, _>(path, id_collection).await
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

#[derive(Debug, Serialize, Deserialize, Clone)]
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
        Event{
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
        self.related_resource_external_ids.retain(|x| x != &external_id);
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
        self.metadata.as_ref().map(|m| m.keys().map(|k| k.as_str()).collect())
    }

    pub fn get_metadata_value(&self, key: &str) -> Option<&str> {
        self.metadata.as_ref().and_then(|m| m.get(key)).map(|v| v.as_str())
    }

    pub fn get_created_time(&self) -> Option<&DateTime<Utc>> {
        self.created_time.as_ref()
    }

    pub fn get_last_updated_time(&self) -> Option<&DateTime<Utc>> {
        self.last_updated_time.as_ref()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct EventsFilter {
    id: Option<u64>,
    external_id_prefix: Option<String>,
    source: Option<String>,
    r#type: Option<String>,
    sub_type: Option<String>,
    data_set_ids: Option<Vec<u64>>,
    event_time: Option<DateTime<Utc>>,
    metadata: Option<HashMap<String, String>>,
    related_resource_ids: Option<Vec<u64>>,
    related_resource_external_ids: Option<Vec<String>>,
}

impl EventsFilter {
    
    pub fn new() -> Self {
        Self{
            id: None,
            external_id_prefix: None,
            source: None,
            r#type: None,
            sub_type: None,
            data_set_ids: None,
            event_time: None,
            metadata: None,
            related_resource_ids: None,
            related_resource_external_ids: None,
        }
    }

    fn set_property(&mut self, property_name: &str, value: &str) {
        match property_name {
            "id" => {
                // You'll need to parse the string value into the correct type (u64 in this case)
                if let Ok(id) = value.parse::<u64>() {
                    self.id = Some(id);
                } else {
                    // Handle parsing error
                    eprintln!("Error: Could not parse '{}' as u64 for property 'id'", value);
                }
            }
            "external_id_prefix" => {
                self.external_id_prefix = Some(value.to_string());
            }
            "source" => {
                self.source = Some(value.to_string());
            }
            "type" => { // Using r#type for the keyword
                self.r#type = Some(value.to_string());
            }
            "sub_type" => {
                self.sub_type = Some(value.to_string());
            }
            "data_set_ids" => {
                eprintln!("Warning: Use set_data_set_ids( Vec<u64> )!");
            }
            // Add cases for other properties as needed
            _ => {
                // Handle unknown property names
                eprintln!("Warning: Unknown property name '{}'", property_name);
            }
        }
    }
    
    fn set_data_set_ids(&mut self, data_set_ids: Vec<u64>) {
        self.data_set_ids = Some(data_set_ids.clone());
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AdvancedEventFilter {

}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RetrieveEventsFilter {
    filter: Option<EventsFilter>,
    limit: Option<u64>,
    cursor: Option<String>,
    sort: Option<String>,
    advanced_filter: Option<AdvancedEventFilter>,
    #[serde(skip)]
    http_status_code: Option<u16>,
}

impl RetrieveEventsFilter {
    pub fn set_http_status_code(&mut self, http_status_code: u16) {
        self.http_status_code = Some(http_status_code);
    }
}

impl RetrieveEventsFilter {
    
    pub fn new() -> Self {
        RetrieveEventsFilter {
            filter: None,
            limit: None,
            cursor: None,
            sort: None,
            advanced_filter: None,
            http_status_code: None,
        }
    }
    
    pub fn new_with_prefix(property: &str, value: &str) -> Self {
        let new_value = to_snake_lower_cased_allow_start_with_digits(value);
        let mut filter = EventsFilter::new();
        filter.set_property(property, &new_value);
        Self {
            filter: Some(filter),
            limit: None,
            cursor: None,
            sort: None,
            advanced_filter: None,
            http_status_code: None,
        }
    }
}