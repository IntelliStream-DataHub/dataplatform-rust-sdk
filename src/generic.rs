use std::rc::{Rc, Weak};
use std::hash::Hasher;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
use chrono::{DateTime, Utc, TimeZone};
use reqwest::multipart::Form;
use crate::{ApiService};
use crate::events::{Event, EventsService};
use crate::files::{FileService, FileUpload};
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
    pub fn from(timestamp: &str, value: &str) -> Self {
        DatapointString {timestamp: timestamp.to_string(), value: value.to_string()}
    }

    pub fn from_datetime(timestamp: DateTime<Utc>, value: &str) -> Self {
        DatapointString {timestamp: timestamp.timestamp_millis().to_string(), value: value.to_string()}
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Datapoint {
    // Read from "isoTime" when deserializing, but emit "timestamp" on serialization
    #[serde(rename(serialize = "timestamp", deserialize = "isoTime"))]
    pub(crate) timestamp: DateTime<Utc>,
    #[serde(default)]
    pub(crate) value: Option<f64>,
    #[serde(default)]
    pub(crate) min: Option<f64>,
    #[serde(default)]
    pub(crate) max: Option<f64>,
    #[serde(default)]
    pub(crate) average: Option<f64>,
    #[serde(default)]
    pub(crate) sum: Option<f64>
}

impl Datapoint {
    pub fn from(timestamp: DateTime<Utc>, value: f64) -> Self {
        Datapoint {timestamp, value: Some(value), min: None, max: None, average: None, sum: None}
    }

    pub fn from_epoch_millis_timestamp(epoch_millis: i64, value: f64) -> Self {
        Datapoint {
            timestamp: Utc.timestamp_millis_opt(epoch_millis).unwrap(),
            value: Some(value),
            min: None,
            max: None,
            average: None,
            sum: None
        }
    }

    pub fn get_timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }

    pub fn get_average(&self) -> Option<f64> {
        self.average
    }

    pub fn get_value(&self) -> Option<f64> {
        self.value
    }

    pub fn get_min(&self) -> Option<f64> {
        self.min
    }

    pub fn get_max(&self) -> Option<f64> {
        self.max
    }

    pub fn get_sum(&self) -> Option<f64> {
        self.sum
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
    pub datapoints: Vec<T>,
    #[serde(rename = "nextCursor")]
    pub next_cursor: Option<String>,
    pub unit: Option<String>,
    #[serde(rename = "unitExternalId")]
    pub unit_external_id: Option<String>,
    #[serde(rename = "isStep")]
    pub is_step: bool,
    #[serde(rename = "isString")]
    pub is_string: bool,
}

impl<T> DatapointsCollection<T> {
    pub fn from_id(id: u64) -> Self {
        DatapointsCollection {
            id: Some(id),
            external_id: None,
            datapoints: vec![],
            next_cursor: None,
            unit: None,
            unit_external_id: None,
            is_step: false,
            is_string: false,
        }
    }

    pub fn from_external_id(external_id: &str) -> Self {
        DatapointsCollection {
            id: None,
            external_id: Some(external_id.to_string()),
            datapoints: vec![],
            next_cursor: None,
            unit: None,
            unit_external_id: None,
            is_step: false,
            is_string: false,
        }
    }

    pub fn from(id: Option<u64>, external_id: Option<String>) -> Self {
        if let Some(id) = id {
            DatapointsCollection::from_id(id)
        } else if let Some(external_id) = external_id {
            DatapointsCollection::from_external_id(&external_id)
        } else {
            panic!("Either id or external_id must be provided")
        }
    }

    pub fn to_string(&self) -> String {
        format!("DatapointsCollection {{ id: {:?}, external_id: {:?}, datapoints: {:?} }}",
                self.id,
                self.external_id,
                self.datapoints.len(),
        )
    }

    // Calculate hash based on id and external_id
    pub fn hash(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        if let Some(id_value) = self.id {
            hasher.write_u64(id_value);
        }
        if let Some(ref external_id_value) = self.external_id {
            hasher.write(external_id_value.as_bytes());
        }

        hasher.finish()
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
pub struct DeleteFilter {
    pub(crate) id: Option<u64>,
    #[serde(rename = "externalId")]
    pub(crate) external_id: Option<String>,
    #[serde(rename = "inclusiveBegin")]
    pub(crate) inclusive_begin: Option<DateTime<Utc>>,
    #[serde(rename = "exclusiveEnd")]
    pub(crate) exclusive_end: Option<DateTime<Utc>>,
}

impl DeleteFilter {
    pub(crate) fn new() -> Self {
        DeleteFilter {
            id: None,
            external_id: None,
            inclusive_begin: None,
            exclusive_end: None
        }
    }

    pub(crate) fn from_external_id(
        external_id: String,
        inclusive_begin: Option<DateTime<Utc>>,
        exclusive_end: Option<DateTime<Utc>>
    ) -> Self {
        DeleteFilter {
            id: None,
            external_id: Some(external_id),
            inclusive_begin,
            exclusive_end
        }
    }

    pub(crate) fn from_id(
        id: u64,
        inclusive_begin: Option<DateTime<Utc>>,
        exclusive_end: Option<DateTime<Utc>>
    ) -> Self {
        DeleteFilter {
            id: Some(id),
            external_id: None,
            inclusive_begin,
            exclusive_end
        }
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

    pub fn to_string(&self) -> String {
        format!("RetrieveFilter {{ start: {:?}, end: {:?}, limit: {:?}, aggregates: {:?}, granularity: {:?}, cursor: {:?}, id: {:?}, external_id: {:?} }}",
                self.start,
                self.end,
                self.limit,
                self.aggregates,
                self.granularity,
                self.cursor,
                self.id,
                self.external_id,
        )
    }
}

pub trait Identifiable {
    fn id(&self) -> u64;
    fn external_id(&self) -> &str;
}


#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataWrapper<T> {
    items: Vec<T>,
    #[serde(skip)]
    http_status_code: Option<u16>,
    error_body: Option<String>,
}

impl<T> DataWrapper<T> {
    pub fn new() -> Self {
        DataWrapper {
            items: vec![],
            http_status_code: None,
            error_body: None,
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

    pub fn to_string(&self) -> String {
        format!("DataWrapper {{ items: {:?}, http_status_code: {:?} }}",
                self.items.len(),
                self.http_status_code,
        )
    }
}

// Constrain T by requiring it implement the Identifiable trait.
impl<T: Identifiable> DataWrapper<T> {
    pub fn remove_item(&mut self, id_to_remove: Option<u64>, external_id_to_remove: Option<String>) {
        self.items.retain(|item| {
            // Filter by ID if provided
            if let Some(id_val) = id_to_remove {
                if item.id() == id_val {
                    return false;
                }
            }
            // Filter by external ID if provided
            if let Some(ext_val) = &external_id_to_remove {
                if item.external_id() == ext_val {
                    return false;
                }
            }

            // Keep item if it fails neither check
            true
        });
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

    async fn execute_file_upload_request<T: DeserializeOwned + DataWrapperDeserialization>(
        &self,
        path: &str,
        multipart_form: Form,
    ) -> Result<T, ResponseError> {
        let response = self.get_api_service().http_client
            .put(path)
            .multipart(multipart_form)
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP file upload request failed: {}", err);
                ResponseError::from_err(err)
            })?;
        if response.status() == 201 {
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

impl<'a> ApiServiceProvider<'a> for FileService<'a> {
    fn api_service(&self) -> &Weak<ApiService<'a>> {
        &self.api_service
    }
}

// A marker trait
pub trait DataWrapperDeserialization
where
    Self: Sized,
{
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error>;
}

impl<T> DataWrapperDeserialization for DataWrapper<T>
where
    T: DeserializeOwned,
    DataWrapper<T>: Sized,
{
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error> {
        if status_code >= 200 && status_code < 300 {
            if status_code == 204 || body.is_empty() { // HTTP No content doesnt return anything
                let mut wrapper: DataWrapper<T> = DataWrapper::new();
                wrapper.set_http_status_code(status_code);
                return Ok(wrapper)
            }
            // For 2xx responses, we expect the body to be a valid DataWrapper<T>
            // If body is empty, it's fine for `from_str` to fail and return an error
            // Or, if you specifically want an empty wrapper for 2xx with empty body:
            // let mut wrapper = DataWrapper::new();
            // wrapper.set_http_status_code(status_code);
            // return Ok(wrapper);
            // However, typically a successful response with a body should be parsed.
            serde_json::from_str(body).map(|mut wrapper: DataWrapper<T>| {
                wrapper.set_http_status_code(status_code);
                wrapper
            })
        } else {
            // For non-2xx responses (errors)
            eprintln!("HTTP request failed with status code {}: {}", status_code, body);

            // Attempt to deserialize the body into DataWrapper<T>
            // This is useful if the error response *itself* is a structured JSON,
            // for example, containing an error object.
            match serde_json::from_str(body).map(|mut wrapper: DataWrapper<T>| {
                wrapper.set_http_status_code(status_code); // Set the HTTP status code
                wrapper // Return the modified wrapper
            }) {
                Ok(result) => {
                    Ok(result)
                },
                Err(e) => {
                    eprintln!("Error parsing HTTP response body: {}", body);
                    let mut wrapper: DataWrapper<T> = DataWrapper::new();
                    wrapper.error_body = Some(body.to_string());
                    wrapper.set_http_status_code(status_code);
                    Ok(wrapper)
                }
            }
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
