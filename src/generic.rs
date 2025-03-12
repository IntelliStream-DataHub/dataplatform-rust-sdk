use std::rc::{Rc, Weak};
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use chrono::{DateTime, Utc, TimeZone};

use crate::{ApiService};
use crate::events::EventsService;
use crate::http::{process_response, ResponseError};
use crate::timeseries::{TimeSeries, TimeSeriesService};
use crate::unit::{Unit, UnitsService};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IdAndExtId {
    pub(crate) id: Option<u64>,
    #[serde(rename = "externalId")]
    pub(crate) external_id: Option<String>,
}

impl IdAndExtId {
    pub fn from_id(id: u64) -> Self {
        IdAndExtId { id: Some(id), external_id: None}
    }

    pub fn from_external_id(external_id: &str) -> Self {
        IdAndExtId { id: None, external_id: Some(external_id.to_string())}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatapointString {
    pub(crate) timestamp: String,
    pub(crate) value: String,
}

impl DatapointString {
    fn from(timestamp: &str, value: &str) -> Self {
        DatapointString {timestamp: timestamp.to_string(), value: value.to_string()}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Datapoint {
    // Read from "isoTime" when deserializing, but emit "timestamp" on serialization
    #[serde(rename(serialize = "timestamp", deserialize = "isoTime"))]
    pub(crate) timestamp: DateTime<Utc>,
    pub(crate) value: f64,
}

impl Datapoint {
    fn from(timestamp: DateTime<Utc>, value: f64) -> Self {
        Datapoint {timestamp, value}
    }

    fn from_epoch_millis_timestamp(epoch_millis: i64, value: f64) -> Self {
        Datapoint {timestamp: Utc.timestamp_millis_opt(epoch_millis).unwrap(), value}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatapointEpoch {
    pub(crate) timestamp: i64,
    pub(crate) value: f64,
}

impl DatapointEpoch {
    fn from(timestamp: i64, value: f64) -> Self {
        DatapointEpoch {timestamp, value}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatapointsCollection<T> {
    pub(crate) id: Option<u64>,
    #[serde(rename = "externalId")]
    pub(crate) external_id: Option<String>,
    pub datapoints: Vec<T>
}

impl<T> DatapointsCollection<T> {
    pub fn from_id(id: u64) -> Self {
        DatapointsCollection { id: Some(id), external_id: None, datapoints: vec![] }
    }

    pub fn from_external_id(external_id: &str) -> Self {
        DatapointsCollection { id: None, external_id: Some(external_id.to_string()), datapoints: vec![]}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RelationForm {
    pub(crate) id: Option<u64>,
    #[serde(rename = "externalId")]
    pub(crate) external_id: Option<String>,
    #[serde(rename = "relationshipType")]
    pub(crate) relationship_type: String,
}

impl RelationForm {
    pub fn from_id(id: u64, relationship_type: String) -> Self {
        RelationForm { id: Some(id), external_id: None, relationship_type }
    }

    pub fn from_external_id(external_id: String, relationship_type: String) -> Self {
        RelationForm { id: None, external_id: Some(external_id), relationship_type }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SearchAndFilterForm {
    pub(crate) filter: Option<u64>,
    pub(crate) search: Option<SearchForm>,
    pub(crate) limit: Option<u64>,
}

impl SearchAndFilterForm {
    pub fn new() -> Self {
        SearchAndFilterForm{filter: None, search: None, limit: None}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SearchForm {
    pub(crate) name: Option<String>,
    pub(crate) description: Option<String>,
    pub(crate) query: Option<String>,
}

impl SearchForm {
    pub fn new() -> Self {
        SearchForm{name: None, description: None, query: None}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IdAndExtIdCollection {
    items: Vec<IdAndExtId>
}

impl IdAndExtIdCollection {

    pub fn new() -> Self {
        IdAndExtIdCollection {
            items: vec![]
        }
    }

    pub fn from_id_vec(ids: Vec<u64>) -> Self {
        let mut items = vec![];
        for id in ids {
            items.push(IdAndExtId::from_id(id));
        }
        IdAndExtIdCollection { items }
    }

    pub fn from_external_id_vec(external_ids: Vec<&str>) -> Self {
        let mut items = vec![];
        for external_id in external_ids {
            items.push(IdAndExtId::from_external_id(external_id));
        }
        IdAndExtIdCollection { items }
    }

    pub fn set_items(&mut self, items: Vec<IdAndExtId>) {
        self.items = items;
    }

    pub fn add_item(&mut self, item: IdAndExtId) {
        self.items.push(item);
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RetrieveFilter {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
    pub aggregates: Option<Vec<String>>,
    pub granularity: Option<String>,
    pub cursor: Option<String>,
    pub id: Option<u64>,
    #[serde(rename = "externalId")]
    pub external_id: Option<String>,
}

impl RetrieveFilter {
    pub(crate) fn new() -> Self {
        RetrieveFilter {
            start: None,
            end: None,
            limit: None,
            aggregates: None,
            granularity: None,
            cursor: None,
            id: None,
            external_id: None,
        }
    }

    pub(crate) fn set_start(&mut self, start: DateTime<Utc>) -> &mut RetrieveFilter {
        self.start = Some(start);
        self
    }

    pub(crate) fn set_end(&mut self, end: DateTime<Utc>) -> &mut RetrieveFilter {
        self.end = Some(end);
        self
    }

    pub(crate) fn set_limit(&mut self, limit: i64) -> &mut RetrieveFilter {
        self.limit = Some(limit);
        self
    }

    pub(crate) fn set_aggregates(&mut self, aggregates: Vec<String>) -> &mut RetrieveFilter {
        self.aggregates = Some(aggregates);
        self
    }

    pub(crate) fn add_aggregate(&mut self, aggregate: &str) -> &mut RetrieveFilter {
        if self.aggregates.is_none() {
            self.aggregates = Some(vec![]);
        }
        self.aggregates.as_mut().unwrap().push(aggregate.to_string());
        self
    }

    pub(crate) fn set_granularity(&mut self, granularity: &str) -> &mut RetrieveFilter {
        self.granularity = Some(granularity.to_string());
        self
    }

    pub(crate) fn set_id(&mut self, id: u64) -> &mut RetrieveFilter {
        self.id = Some(id);
        self
    }

    pub(crate) fn set_external_id(&mut self, external_id: &str) -> &mut RetrieveFilter {
        self.external_id = Some(external_id.to_string());
        self
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataWrapper<T> {
    items: Vec<T>,
    #[serde(skip)]
    http_status_code: Option<u16>,
}

impl<T> DataWrapper<T> {
    pub fn new() -> Self {
        DataWrapper {
            items: vec![],
            http_status_code: None,
        }
    }

    pub fn get_items(&self) -> &Vec<T> {
        &self.items
    }

    pub fn get_items_mut(&mut self) -> &mut Vec<T> {
        &mut self.items
    }


    pub fn set_items(&mut self, items: Vec<T>) {
        self.items = items;
    }

    pub fn add_item(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn length(&self) -> u64 {
        self.items.len() as u64
    }

    pub fn get_http_status_code(&self) -> Option<u16> {
        self.http_status_code
    }

    pub fn set_http_status_code(&mut self, http_status_code: u16) {
        self.http_status_code = Some(http_status_code);
    }

}

pub trait ApiServiceProvider<'a> {
    fn api_service(&self) -> &Weak<ApiService<'a>>;

    fn get_api_service(&self) -> Rc<ApiService<'a>> {
        self.api_service().upgrade().unwrap()
    }

    async fn execute_get_request<T: DeserializeOwned + DataWrapperDeserialization>(
        &self,
        path: &str
    ) -> Result<T, ResponseError> {
        let response = self.get_api_service().http_client
            .get(path)
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;
        process_response::<T>(response, path).await
    }

    async fn execute_post_request<T: DeserializeOwned + DataWrapperDeserialization, J: Serialize>(
        &self,
        path: &str,
        json: &J,
    ) -> Result<T, ResponseError> {
        let response = self.get_api_service().http_client
            .post(path)
            .json(json)
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;
        if response.status() == 204 {
            // Return deserialized `T` with an empty body and the HTTP status code
            T::deserialize_and_set_status("", response.status().as_u16())
                .map_err(|err| {
                    eprintln!("Failed to create object from empty response: {}", err);
                    ResponseError {
                        status: response.status(),
                        message: err.to_string(),
                    }
                })
        } else {
            process_response::<T>(response, path).await
        }
    }

}

impl<'a> ApiServiceProvider<'a> for TimeSeriesService<'a> {
    fn api_service(&self) -> &Weak<ApiService<'a>> {
        &self.api_service
    }
}

impl<'a> ApiServiceProvider<'a> for UnitsService<'a> {
    fn api_service(&self) -> &Weak<ApiService<'a>> {
        &self.api_service
    }
}
impl<'a> ApiServiceProvider<'a> for EventsService<'a> {
    fn api_service(&self) -> &Weak<ApiService<'a>> {
        &self.api_service
    }
}

// A marker trait
pub trait DataWrapperDeserialization {
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error>
    where Self: Sized;
}

// Implement the custom logic for specific types
impl DataWrapperDeserialization for DataWrapper<Unit> {
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error>
    where Self: Sized,
    {
        // Deserialize from JSON
        serde_json::from_str(body).map(|mut wrapper: DataWrapper<Unit>| {
            wrapper.set_http_status_code(status_code); // Set the HTTP status code
            wrapper // Return the modified wrapper
        })
    }
}

impl DataWrapperDeserialization for DataWrapper<TimeSeries> {
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error>
    where Self: Sized,
    {
        if status_code == 204 {
            let mut wrapper: DataWrapper<TimeSeries> = DataWrapper::new();
            wrapper.set_http_status_code(status_code);
            return Ok(wrapper)
        }
        // Deserialize from JSON
        serde_json::from_str(body).map(|mut wrapper: DataWrapper<TimeSeries>| {
            wrapper.set_http_status_code(status_code); // Set the HTTP status code
            wrapper // Return the modified wrapper
        })
    }
}

impl DataWrapperDeserialization for DataWrapper<DatapointsCollection<Datapoint>> {
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error>
    where Self: Sized,
    {
        // Deserialize from JSON
        serde_json::from_str(body).map(|mut wrapper: DataWrapper<DatapointsCollection<Datapoint>>| {
            wrapper.set_http_status_code(status_code); // Set the HTTP status code
            wrapper // Return the modified wrapper
        })
    }
}

impl DataWrapperDeserialization for DataWrapper<String> {
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error>
    where Self: Sized,
    {
        if status_code >= 200 && status_code < 300 {
            let mut wrapper: DataWrapper<String> = DataWrapper::new();
            wrapper.set_http_status_code(status_code);
            Ok(wrapper)
        } else {
            eprintln!("Insert datapoints HTTP request failed: {}", body);
            let mut wrapper: DataWrapper<String> = DataWrapper::new();
            wrapper.items.push(body.to_string());
            wrapper.set_http_status_code(status_code);
            Ok(wrapper)
        }
    }
}

impl DataWrapperDeserialization for String {
    fn deserialize_and_set_status(body: &str, _status_code: u16) -> Result<Self, serde_json::Error>
    where Self: Sized,
    {
        Ok(body.to_string())
    }
}
