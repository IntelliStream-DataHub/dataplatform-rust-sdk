#[cfg(test)]
mod tests;

use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId, RelationForm, SearchAndFilterForm, SearchForm};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, FixedOffset, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use maplit::hashmap;
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::fields::{Field, ListField, MapField};
use crate::filters::{AdvancedEventFilter, BasicEventFilter, TimeFilter};
use crate::graph_data_wrapper::{GraphDataWrapper, GraphNode};
use crate::resources::ResourceUpdateFields;

pub struct DatasetsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}
impl ApiServiceProvider for DatasetsService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl DatasetsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/datasets", base_url);
        DatasetsService {
            api_service,
            base_url,
        }
    }

    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<Dataset>, ResponseError>
    where
            for<'a> &'a I: Into<DataWrapper<Dataset>>,
    {
        let dw = data.into();
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<Dataset>, _>(path, &dw)
            .await
    }

    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Dataset>, ResponseError>
    where
            for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }

    pub async fn filter(&self, filter: &DatasetFilter) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/filter", self.base_url);
        self.execute_post_request(path, &filter).await
    }

    pub async fn by_ids<I>(&self, id_collection: &I) -> Result<DataWrapper<Dataset>, ResponseError>
    where
            for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<Dataset>, _>(path, &id_collection.into())
            .await
    }

    pub async fn search(&self, search: &DatasetSearch) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/filter", self.base_url);
        self.execute_post_request(path, &search).await
    }
    pub async fn list(&self) -> Result<DataWrapper<Dataset>, ResponseError> {
        todo!()
    }
    pub async fn update(&self) -> Result<(), ResponseError> {
        todo!()
    }
    pub async fn policies(&self) -> Result<DataWrapper<Dataset>, ResponseError> {
        todo!()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    pub id: Option<u64>,
    //@NotNull
    //@Size(min= 3, max = 256)
    pub  external_id:String,
    //@NotNull
    //3, max = 512)
    pub name: String,
    pub description: Option<String>,
    pub policies :Option<Vec<String>>,
    pub metadata: HashMap<String, String>,
    pub connected_data_sets:Vec<u64>,
    pub created_time: Option<DateTime<FixedOffset>>,
    pub last_updated_time: Option<DateTime<FixedOffset>>,
}
impl DataHubEntity for Dataset {
    fn ext_id(&self) -> &String {
        &self.external_id
    }
}
impl GraphNode for Dataset {}

impl Dataset {
    pub fn new(name: String) -> Self {
        // creates an empty dataset with external id given by snake_case of name.
        Dataset {
            id: None,
            external_id: to_snake_lower_cased_allow_start_with_digits(&name),
            metadata: hashmap!{},
            description: None,
            name,
            policies: None,
            connected_data_sets: vec![],

            created_time: None,
            last_updated_time: None,
        }
    }
    pub fn add_metadata(&mut self, key: String, value: String){
        self.metadata.insert(key, value);
    }
    pub fn remove_metadata(&mut self, key: String) {
        self.metadata.remove(&key);
    }
    pub fn set_name(&mut self, name: String)-> &mut Self {
        self.name = name;
        self
    }
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) -> &mut Self{
        self.metadata = metadata;
        self
    }
    pub fn set_policies(&mut self, policies: Vec<String>) -> &mut Self{
        self.policies = Some(policies);
        self
    }
    pub fn add_connected_data_set(&mut self, id: u64) {
        self.connected_data_sets.push(id);
    }
    pub fn remove_connected_data_set(&mut self, id: u64) {
        self.connected_data_sets.retain(|&x| x != id);
    }
    pub fn id(&self) -> Option<&u64> {
        self.id.as_ref()
    }
    pub fn external_id(&self) -> &String {
        &self.external_id
    }
    pub fn set_external_id(&mut self, external_id: String)-> &mut Self {
        self.external_id = external_id;
        self
    }
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
    pub fn description(&self) -> Option<&String> { self.description.as_ref() }
    pub fn set_description(&mut self, description: String) -> &mut Self{
        self.description = Some(description);
        self
    }
    pub fn created_time(&self) -> Option<&DateTime<FixedOffset>> {
        self.created_time.as_ref()
    }
    pub fn last_updated_time(&self) -> Option<&DateTime<FixedOffset>> {
        self.last_updated_time.as_ref()
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetUpdate {
    pub external_id: String,
    id: Option<u64>,
    update: Option<DatasetUpdateFields>,
    relation_update: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DatasetUpdateFields {
    //todo!()
    external_id: String, // one should not be able to set external ID to null.
    name: Field<String>,
    description: Field<String>,
    policies: ListField<String>,
    metadata: MapField,
    labels: ListField<String>,
    connected_data_sets: Vec<u64>, // I think we should be able to connect by id or external id.
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetFilter {
    // use in /list, and search?
    advanced_filter: Option<AdvancedEventFilter>,
    filter: BasicDatasetFilter,
    cursor: Option<String>,
    limit: usize,
}

impl DatasetFilter {
    pub fn set_filter(&mut self, filter: BasicDatasetFilter) -> &mut Self{
        self.filter = filter;
        self
    }
    pub(crate) fn set_advanced_filter(&mut self, filter: BasicDatasetFilter) -> &mut Self{
        self.filter = filter;
        self
    }
    pub fn set_limit (&mut self, limit: usize) -> &mut Self {
        self.limit = limit;
        self
    }pub fn cursor(&self) -> Option<&String> { self.cursor.as_ref() }
    pub fn new() -> Self {
        Self {
            filter: BasicDatasetFilter::new(),
            cursor: None,
            limit: 100,
            advanced_filter: None,
        }
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BasicDatasetFilter {
    metadata: Option<HashMap<String, String>>,
    created_time: Option<TimeFilter>,
    last_updated_time: Option<TimeFilter>,
    external_id_prefix: Option<String>,
    id: Option<u64>,
    description: Option<String>,
    policies: Option<Vec<String>>,
    active: Option<bool>,

}

impl BasicDatasetFilter {
    pub fn new() -> Self {
        Self {
            id: None,
            external_id_prefix: None,
            description: None,
            metadata: None,
            created_time: None,
            last_updated_time: None,
            policies: None,
            active: None,
        }
    }
    pub fn set_id(&mut self, id: u64) -> &mut Self {
        self.id = Some(id);
        self
    }
    pub fn set_external_id_prefix(&mut self, external_id: String) -> &mut Self {
        self.external_id_prefix = Some(external_id);
        self
    }
    pub fn set_description(&mut self, external_id: String) -> &mut Self {
        self.description = Some(external_id);
        self
    }
    pub fn set_policies(&mut self, policies: Vec<String>) -> &mut Self {
        self.policies = Some(policies);
        self
    }
    pub fn set_active(&mut self, active: bool) -> &mut Self {
        self.active = Some(active);
        self
    }
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) -> &mut Self {
        self.metadata = Some(metadata);
        self
    }
    pub fn set_created_time(&mut self, created_time: TimeFilter) -> &mut Self {
        self.created_time = Some(created_time);
        self
    }
    pub fn set_last_updated_time(&mut self, last_updated_time: TimeFilter) -> &mut Self {
        self.created_time = Some(last_updated_time);
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]

pub struct DatasetSearch {
    filter: BasicDatasetFilter,
    search: SearchForm,
    limit: usize,
    cursor: Option<String>,
}
impl DatasetSearch {
    pub fn new() -> Self {
        Self {
            filter: BasicDatasetFilter::new(),
            search: SearchForm::new(),
            limit: 100,
            cursor: None,
        }
    }
    pub fn set_filter(&mut self, filter: BasicDatasetFilter) -> &mut Self {
        self.filter = filter;
        self
    }
    pub fn set_search(&mut self, search: SearchForm) -> &mut Self {
        self.search = search;
        self
    }
    pub fn set_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = limit;
        self
    }
    pub fn cursor(&self) -> Option<&String> { self.cursor.as_ref() }
    pub fn build(&self) -> Self {
        self.clone()
    }
}