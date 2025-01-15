
use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::collections::HashMap;
use std::rc::{Weak};
use crate::ApiService;
use crate::fields::{Field, ListField, MapField};
use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtIdCollection};
use crate::http::{process_response, ResponseError};

pub struct TimeSeriesService<'a>{
    pub(crate) api_service: Weak<ApiService<'a>>,
    base_url: String
}

impl<'a> TimeSeriesService<'a> {

    pub fn new(api_service: Weak<ApiService<'a>>, base_url: &String) -> Self {
        let base_url = format!("{}/timeseries", base_url);
        TimeSeriesService {api_service, base_url}
    }

    pub async fn list(&self)
                      -> Result<DataWrapper<TimeSeries>, ResponseError> {

        // Create and send an HTTP GET request
        let response = self.get_api_service().http_client
            .get(&self.base_url) // Correctly access `base_url`
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;

        // Process the HTTP response and deserialize it as `DataWrapper<TimeSeries>`
        process_response::<DataWrapper<TimeSeries>>(response).await
    }

    pub async fn list_with_limit(&self, query: &LimitParam)
                      -> Result<DataWrapper<TimeSeries>, ResponseError> {

        // Create and send an HTTP GET request
        let response = self.get_api_service().http_client
            .get(&self.base_url) // Correctly access `base_url`
            .query(query)
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;

        // Process the HTTP response and deserialize it as `DataWrapper<TimeSeries>`
        process_response::<DataWrapper<TimeSeries>>(response).await
    }

    pub async fn create(&self, json: &DataWrapper<TimeSeries>)
            -> Result<DataWrapper<TimeSeries>, ResponseError>
    {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<TimeSeries>, _>(path, json).await
    }

    pub async fn delete(&self, json: &IdAndExtIdCollection)
            -> Result<DataWrapper<TimeSeries>, ResponseError>
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, json).await
    }

    pub async fn update(&self, json: &TimeSeriesUpdateCollection)
            -> Result<DataWrapper<TimeSeries>, ResponseError>
    {
        let path = &format!("{}/update", self.base_url);
        self.execute_post_request::<DataWrapper<TimeSeries>, _>(path, json).await
    }

    pub async fn by_ids(&self, json: &IdAndExtIdCollection)
            -> Result<DataWrapper<TimeSeries>, ResponseError>
    {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<TimeSeries>, _>(path, json).await
    }

}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeries {
    pub id: u64,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub unit: String,
    pub description: Option<String>,
    #[serde(rename = "unitExternalId")]
    pub unit_external_id: Option<String>,
    #[serde(rename = "securityCategories")]
    pub security_categories: Option<Vec<u64>>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "valueType")]
    pub value_type: String,
    #[serde(rename = "createdTime")]
    pub created_time: Option<u64>,
    #[serde(rename = "lastUpdatedTime")]
    pub last_updated_time: Option<u64>,
    #[serde(rename = "relationsFrom")]
    pub relations_from: Vec<u64>,
    #[serde(rename = "isString")]
    pub is_string: bool,
    #[serde(rename = "isStep")]
    pub is_step: bool
}

impl TimeSeries {

    pub fn new(external_id: &str, name: &str, unit: &str) -> TimeSeries{
        TimeSeries {
            id: 0,
            external_id: external_id.to_string(),
            name: name.to_string(),
            metadata: None,
            unit: unit.to_string(),
            description: None,
            unit_external_id: None,
            security_categories: None,
            data_set_id: None,
            value_type: "float".to_string(),
            created_time: None,
            last_updated_time: None,
            relations_from: vec![],
            is_string: false,
            is_step: false,
        }
    }

    pub fn builder() -> TimeSeries {
        TimeSeries::new("", "", "")
    }

    pub fn set_name(&mut self, name: &str) -> &mut TimeSeries {
        self.name = name.to_string();
        self
    }

    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) -> &mut TimeSeries {
        self.metadata = Some(metadata);
        self
    }

    pub fn set_external_id(&mut self, external_id: &str) -> &mut TimeSeries {
        self.external_id = external_id.to_string();
        self
    }

    pub fn set_unit(&mut self, unit: &str) -> &mut TimeSeries {
        self.unit = unit.to_string();
        self
    }

    pub fn set_description(&mut self, description: &str) -> &mut TimeSeries {
        self.description = Some(description.to_string());
        self
    }

    pub fn set_unit_external_id(&mut self, unit_external_id: &str) -> &mut TimeSeries {
        self.unit_external_id = Some(unit_external_id.to_string());
        self
    }

    pub fn set_security_categories(&mut self, security_categories: Vec<u64>) -> &mut TimeSeries {
        self.security_categories = Some(security_categories);
        self
    }

    pub fn set_data_set_id(&mut self, data_set_id: u64) -> &mut TimeSeries {
        self.data_set_id = Some(data_set_id);
        self
    }

    pub fn set_value_type(&mut self, value_type: &str) -> &mut TimeSeries {
        self.value_type = value_type.to_string();
        self
    }

    pub fn set_created_time(&mut self, created_time: u64) -> &mut TimeSeries {
        self.created_time = Some(created_time);
        self
    }

    pub fn set_last_updated_time(&mut self, last_updated_time: u64) -> &mut TimeSeries {
        self.last_updated_time = Some(last_updated_time);
        self
    }

    pub fn set_relations_from(&mut self, relations_from: Vec<u64>) -> &mut TimeSeries {
        self.relations_from = relations_from;
        self
    }

    pub fn set_is_string(&mut self, is_string: bool) -> &mut TimeSeries {
        self.is_string = is_string;
        self
    }

    pub fn set_is_step(&mut self, is_step: bool) -> &mut TimeSeries {
        self.is_step = is_step;
        self
    }

}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LimitParam {
    limit: i64,
}

impl LimitParam {

    pub fn new() -> Self {
        LimitParam { limit: 100 }
    }

    pub fn set_limit(&mut self, limit: i64) {
        self.limit = limit;
    }

    pub fn get_limit(&self) -> i64 {
        self.limit
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeriesUpdateFields {
    #[serde(rename = "externalId")]
    pub external_id: Field<String>,
    pub name: Field<String>,
    pub metadata: MapField,
    pub unit: Field<String>,
    pub description: Field<String>,
    #[serde(rename = "unitExternalId")]
    pub unit_external_id: Field<String>,
    #[serde(rename = "securityCategories")]
    pub security_categories: ListField<u64>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Field<u64>,
    #[serde(rename = "relationsFrom")]
    pub relations_from: ListField<u64>,
    #[serde(rename = "isString")]
    pub is_string: Field<bool>,
    #[serde(rename = "isStep")]
    pub is_step: Field<bool>,
    #[serde(rename = "valueType")]
    pub value_type: Field<String>,
}

impl TimeSeriesUpdateFields {

    pub fn new() -> TimeSeriesUpdateFields {
        TimeSeriesUpdateFields {
            external_id: Field::new(),
            name: Field::new(),
            metadata: MapField::new(),
            unit: Field::new(),
            description: Field::new(),
            unit_external_id: Field::new(),
            security_categories: ListField::new(),
            data_set_id: Field::new(),
            relations_from: ListField::new(),
            is_string: Field::new(),
            is_step: Field::new(),
            value_type: Field::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeriesUpdate {
    pub id: Option<u64>,
    #[serde(rename = "externalId")]
    pub external_id: Option<String>,
    pub update: TimeSeriesUpdateFields
}

impl TimeSeriesUpdate {

}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TimeSeriesUpdateCollection {
    items: Vec<TimeSeriesUpdate>
}

impl TimeSeriesUpdateCollection {

    pub fn new() -> Self {
        TimeSeriesUpdateCollection {
            items: vec![]
        }
    }

    pub fn get_items(&self) -> Vec<TimeSeriesUpdate> {
        self.items.clone()
    }

    pub fn set_items(&mut self, items: Vec<TimeSeriesUpdate>) {
        self.items = items;
    }

    pub fn add_item(&mut self, item: TimeSeriesUpdate) {
        self.items.push(item);
    }
}