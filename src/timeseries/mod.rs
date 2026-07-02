mod test;

use crate::buffer::DurableSpool;
use crate::datahub::DataHubApi;
use crate::fields::{Field, ListField, MapField};
use crate::generic::{
    ApiServiceProvider, DataWrapper, Datapoint, DatapointString, DatapointsCollection,
    DeleteFilter, IdAndExtId, RelationForm, RetrieveFilter, SearchAndFilterForm,
    SearchForm,
};
use crate::http::{process_response, ResponseError};
use crate::serde_helper::is_zero;
use crate::ApiService;
use chrono::{DateTime, Utc};
use futures::{future::join_all, FutureExt};
use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::collections::HashMap;
use std::sync::{Mutex, Weak};

/// A single spooled datapoint (flattened from a `DatapointsCollection`), used by durable buffering.
#[derive(Debug, Serialize, Deserialize, Clone)]
struct SpoolDatapoint {
    #[serde(default, with = "crate::serde_helper::opt_string_id", skip_serializing_if = "Option::is_none")]
    id: Option<u64>,
    #[serde(rename = "externalId", skip_serializing_if = "Option::is_none")]
    external_id: Option<String>,
    timestamp: String,
    value: String,
}

pub struct TimeSeriesService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
    // Durable spool for datapoint ingestion (lazily opened on first buffered send; None if off).
    spool: Mutex<Option<DurableSpool>>,
}

impl TimeSeriesService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/timeseries", base_url);
        TimeSeriesService {
            api_service,
            base_url,
            spool: Mutex::new(None),
        }
    }

    pub async fn list(&self) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        self.execute_get_request(&self.base_url, None::<&str>).await
    }

    pub async fn list_with_limit(
        &self,
        limit: Option<u64>,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let query = [("limit", limit.unwrap_or(100))];
        self.execute_get_request::<DataWrapper<TimeSeries>, _>(&self.base_url, Some(&query))
            .await
    }

    pub async fn create(
        &self,
        json: &DataWrapper<TimeSeries>,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<TimeSeries>, _>(path, json)
            .await
    }

    pub async fn create_one(
        &self,
        ts: &TimeSeries,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let mut dw = DataWrapper::new();
        dw.add_item(ts.clone());
        self.create(&dw).await
    }

    pub async fn create_from_list(
        &self,
        ts_list: &Vec<TimeSeries>,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let mut dw = DataWrapper::new();
        ts_list.iter().for_each(|ts| {
            dw.add_item(ts.clone());
        });
        self.create(&dw).await
    }

    pub async fn delete(
        &self,
        json: &DataWrapper<IdAndExtId>,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, json).await
    }

    pub async fn update(
        &self,
        json: &TimeSeriesUpdateCollection,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let path = &format!("{}/update", self.base_url);
        self.execute_post_request::<DataWrapper<TimeSeries>, _>(path, json)
            .await
    }

    pub async fn by_ids(
        &self,
        json: &DataWrapper<IdAndExtId>,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<TimeSeries>, _>(path, json)
            .await
    }

    pub async fn search(
        &self,
        form: &SearchAndFilterForm,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let path = &format!("{}/search", self.base_url);
        self.execute_post_request::<DataWrapper<TimeSeries>, _>(path, form)
            .await
    }

    pub async fn search_by_name(
        &self,
        name: &str,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let mut search_form = SearchForm::new();
        search_form.name = Some(name.to_string());
        let mut search_and_filter_form = SearchAndFilterForm::new();
        search_and_filter_form.search = Some(search_form);
        self.search(&search_and_filter_form).await
    }

    pub async fn search_by_query(
        &self,
        query: &str,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let mut search_form = SearchForm::new();
        search_form.query = Some(query.to_string());
        let mut search_and_filter_form = SearchAndFilterForm::new();
        search_and_filter_form.search = Some(search_form);
        self.search(&search_and_filter_form).await
    }

    pub async fn search_by_description(
        &self,
        query: &str,
    ) -> Result<DataWrapper<TimeSeries>, ResponseError> {
        let mut search_form = SearchForm::new();
        search_form.description = Some(query.to_string());
        let mut search_and_filter_form = SearchAndFilterForm::new();
        search_and_filter_form.search = Some(search_form);
        self.search(&search_and_filter_form).await
    }

    pub async fn insert_datapoint(
        &self,
        id: Option<u64>,
        external_id: Option<String>,
        timestamp: DateTime<Utc>,
        value: String,
    ) -> Result<DataWrapper<String>, ResponseError> {
        let mut data_request: DataWrapper<DatapointsCollection<DatapointString>> =
            DataWrapper::new();

        let mut dp_collection = if let Some(id_value) = id {
            DatapointsCollection::from_id(id_value)
        } else if let Some(external_id_value) = external_id {
            DatapointsCollection::from_external_id(&external_id_value) // Assuming from_id can handle both id types based on your selection
        } else {
            panic!("Neither id nor external_id provided.");
        };

        let dp = DatapointString::from_datetime(timestamp, value.as_str());
        dp_collection.datapoints.push(dp);
        data_request.add_item(dp_collection);
        self.insert_datapoints(&mut data_request).await
    }

    /// Insert datapoints. With durable buffering enabled on the client this flushes any on-disk
    /// backlog first, sends in <=100k-datapoint chunks, and spools to disk on a transient failure
    /// (e.g. the server is unreachable); otherwise it behaves exactly as before. Retries are safe:
    /// datapoints dedup on `(series, timestamp)` in the backend's ReplacingMergeTree.
    pub async fn insert_datapoints(
        &self,
        json: &mut DataWrapper<DatapointsCollection<DatapointString>>,
    ) -> Result<DataWrapper<String>, ResponseError> {
        let svc = self.get_api_service();
        if !svc.config.buffering_enabled() {
            drop(svc);
            return self.insert_datapoints_unbuffered(json).await;
        }
        self.ensure_spool(&svc.config);
        drop(svc); // don't hold the ApiService Arc across awaits

        let path = format!("{}/data", self.base_url);
        let now = Utc::now().timestamp_millis();
        let new_dps = flatten_collections(json.get_items());

        if !self.drain_spool(&path, now).await {
            self.append_to_spool(&new_dps, now);
            return Ok(buffered_string_wrapper());
        }
        match self.post_datapoint_chunks(&path, &new_dps).await {
            Ok(()) => {
                let mut w = DataWrapper::new();
                w.set_http_status_code(204);
                Ok(w)
            }
            Err(e) if e.is_bufferable() => {
                self.append_to_spool(&new_dps, now);
                Ok(buffered_string_wrapper())
            }
            Err(e) => Err(e),
        }
    }

    /// Records currently held in the durable datapoint spool (0 when buffering is off).
    pub fn buffered_count(&self) -> u64 {
        self.spool.lock().unwrap().as_ref().map_or(0, |s| s.size())
    }

    fn ensure_spool(&self, config: &DataHubApi) {
        let mut guard = self.spool.lock().unwrap();
        if guard.is_none() {
            let dir = config.buffer_directory().join("datapoints");
            if let Ok(spool) = DurableSpool::open(
                dir,
                config.effective_buffer_retention_ms(),
                config.effective_buffer_max_bytes(),
            ) {
                *guard = Some(spool);
            }
        }
    }

    fn append_to_spool(&self, dps: &[SpoolDatapoint], now: i64) {
        let records: Vec<(i64, String)> = dps
            .iter()
            .filter_map(|dp| {
                let ts = dp.timestamp.parse::<i64>().unwrap_or(now);
                serde_json::to_string(dp).ok().map(|json| (ts, json))
            })
            .collect();
        if let Some(spool) = self.spool.lock().unwrap().as_mut() {
            let _ = spool.append(&records, now);
        }
    }

    async fn post_datapoint_chunks(
        &self,
        path: &str,
        dps: &[SpoolDatapoint],
    ) -> Result<(), ResponseError> {
        for chunk in dps.chunks(100_000) {
            let dw = regroup_datapoints(chunk);
            self.execute_post_request::<DataWrapper<String>, _>(path, &dw)
                .await?;
        }
        Ok(())
    }

    /// Drain the datapoint spool, oldest segment first. Returns false on a transient failure (server
    /// still down), leaving the rest buffered; terminal failures drop the offending segment.
    async fn drain_spool(&self, path: &str, now: i64) -> bool {
        if let Some(spool) = self.spool.lock().unwrap().as_mut() {
            let _ = spool.roll(now);
        }
        loop {
            let seq = self
                .spool
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|s| s.oldest_sealed_seq());
            let Some(seq) = seq else {
                return true;
            };
            let lines = self
                .spool
                .lock()
                .unwrap()
                .as_ref()
                .map(|s| s.read_segment(seq, now).unwrap_or_default())
                .unwrap_or_default();
            if lines.is_empty() {
                if let Some(s) = self.spool.lock().unwrap().as_mut() {
                    let _ = s.delete_segment(seq);
                }
                continue;
            }
            let dps: Vec<SpoolDatapoint> = lines
                .iter()
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect();
            match self.post_datapoint_chunks(path, &dps).await {
                Err(e) if e.is_bufferable() => return false,
                _ => {
                    if let Some(s) = self.spool.lock().unwrap().as_mut() {
                        let _ = s.delete_segment(seq);
                    }
                }
            }
        }
    }

    async fn insert_datapoints_unbuffered(
        &self,
        json: &mut DataWrapper<DatapointsCollection<DatapointString>>,
    ) -> Result<DataWrapper<String>, ResponseError> {
        let path = &format!("{}/data", self.base_url);
        let mut new_request_bodies = vec![];
        let mut futures = vec![];
        const MAX_DATAPOINTS_PER_REQUEST: usize = 100000;
        // Count data points
        let mut active_timeseries_with_datapoints = vec![];
        let mut total_datapoints: usize = 0;
        for dp_collection in json.get_items().iter() {
            total_datapoints += dp_collection.datapoints.len();
            active_timeseries_with_datapoints.push(dp_collection.hash());
        }

        if total_datapoints > MAX_DATAPOINTS_PER_REQUEST {
            while total_datapoints > MAX_DATAPOINTS_PER_REQUEST {
                println!("Total datapoints left: {}", total_datapoints);
                // Divide the request into multiple batch requests
                let mut new_json: DataWrapper<DatapointsCollection<DatapointString>> =
                    DataWrapper::new();
                for orig_dp_collection in json.get_items_mut() {
                    let mut new_dp_collection = DatapointsCollection::from(
                        orig_dp_collection.id,
                        orig_dp_collection.external_id.clone(),
                    );
                    if Some(orig_dp_collection.id) != None {
                        new_dp_collection.id = orig_dp_collection.id;
                    } else if Some(orig_dp_collection.external_id.clone()) != None {
                        new_dp_collection.external_id = orig_dp_collection.external_id.clone();
                    }

                    let batch_size: usize =
                        MAX_DATAPOINTS_PER_REQUEST / active_timeseries_with_datapoints.len();
                    println!("Current Batch size: {}", batch_size);
                    if orig_dp_collection.datapoints.len() > batch_size {
                        let chunk: Vec<DatapointString> =
                            orig_dp_collection.datapoints.drain(..batch_size).collect();
                        new_dp_collection.datapoints.extend(chunk);
                    } else if orig_dp_collection.datapoints.len() == 0 {
                        // Find the hash for active timeseries, and remove it from the vec
                        if let Some(pos) = active_timeseries_with_datapoints
                            .iter()
                            .position(|&x| x == orig_dp_collection.hash())
                        {
                            println!("Remove datacollection: {}", orig_dp_collection.to_string());
                            active_timeseries_with_datapoints.remove(pos);
                        }
                    } else {
                        new_dp_collection
                            .datapoints
                            .extend(orig_dp_collection.datapoints.clone());
                    }
                    new_json.add_item(new_dp_collection.clone());
                    total_datapoints = total_datapoints - new_dp_collection.datapoints.len();
                    println!("Total datapoints left: {}", total_datapoints);
                }

                let mut new_total_datapoints: usize = 0;
                for dp_collection in new_json.get_items().iter() {
                    new_total_datapoints += dp_collection.datapoints.len();
                }
                println!(
                    "Sending insert datapoints request with {} datapoints.",
                    new_total_datapoints
                );

                let new_json_clone = new_json.clone();
                new_request_bodies.push(new_json_clone);
            }
        }
        // Now create futures after all request bodies are created
        for request_body in &new_request_bodies {
            let f = self
                .execute_post_request::<DataWrapper<String>, _>(path, request_body)
                .map(|result| match result {
                    Ok(ref r) => {
                        // The backend acknowledges a successful insert with 204 No Content.
                        assert_eq!(r.get_http_status_code().unwrap(), 204);
                        println!("Successfully inserted datapoints.");
                    }
                    Err(e) => {
                        eprintln!("{}", e.message);
                        panic!("Error inserting datapoints: {:?}", e.get_message());
                    }
                });
            futures.push(f);
        }
        join_all(futures).await;

        total_datapoints = 0;
        for dp_collection in json.get_items().iter() {
            total_datapoints += dp_collection.datapoints.len();
        }
        println!("Final request: Total datapoints left: {}", total_datapoints);
        self.execute_post_request::<DataWrapper<String>, _>(path, json)
            .await
    }

    pub async fn retrieve_datapoints(
        &self,
        json: &DataWrapper<RetrieveFilter>,
    ) -> Result<DataWrapper<DatapointsCollection<Datapoint>>, ResponseError> {
        let path = &format!("{}/data/list", self.base_url);
        self.execute_post_request::<DataWrapper<DatapointsCollection<Datapoint>>, _>(path, json)
            .await
    }

    pub async fn delete_datapoints(
        &self,
        json: &DataWrapper<DeleteFilter>,
    ) -> Result<DataWrapper<String>, ResponseError> {
        let path = &format!("{}/data/delete", self.base_url);
        self.execute_post_request::<DataWrapper<String>, _>(path, json)
            .await
    }

    pub async fn retrieve_latest_datapoint(
        &self,
        json: &DataWrapper<IdAndExtId>,
    ) -> Result<DataWrapper<DatapointsCollection<Datapoint>>, ResponseError> {
        let path = &format!("{}/data/latest", self.base_url);
        self.execute_post_request::<DataWrapper<DatapointsCollection<Datapoint>>, _>(path, json)
            .await
    }
}

/// Flatten datapoint collections into individual spool records (one timestamp each, for retention).
fn flatten_collections(
    collections: &[DatapointsCollection<DatapointString>],
) -> Vec<SpoolDatapoint> {
    let mut out = Vec::new();
    for c in collections {
        for dp in &c.datapoints {
            out.push(SpoolDatapoint {
                id: c.id,
                external_id: c.external_id.clone(),
                timestamp: dp.timestamp.clone(),
                value: dp.value.clone(),
            });
        }
    }
    out
}

/// Regroup flattened datapoints back into collections (by id or external id) for a single request.
fn regroup_datapoints(
    dps: &[SpoolDatapoint],
) -> DataWrapper<DatapointsCollection<DatapointString>> {
    let mut groups: HashMap<String, DatapointsCollection<DatapointString>> = HashMap::new();
    for dp in dps {
        let key = match (&dp.id, &dp.external_id) {
            (Some(id), _) => format!("id:{}", id),
            (None, Some(ext)) => format!("ext:{}", ext),
            (None, None) => "none".to_string(),
        };
        let coll = groups.entry(key).or_insert_with(|| DatapointsCollection {
            id: dp.id,
            external_id: dp.external_id.clone(),
            datapoints: vec![],
            next_cursor: None,
            unit: None,
            unit_external_id: None,
        });
        coll.datapoints.push(DatapointString {
            timestamp: dp.timestamp.clone(),
            value: dp.value.clone(),
        });
    }
    let mut dw = DataWrapper::new();
    dw.set_items(groups.into_values().collect());
    dw
}

/// A result for a buffered (not-yet-confirmed) datapoint insert: HTTP 202 with no items.
fn buffered_string_wrapper() -> DataWrapper<String> {
    let mut w = DataWrapper::new();
    w.set_http_status_code(202);
    w
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeries {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub unit: Option<String>,
    pub description: Option<String>,
    #[serde(rename = "unitExternalId")]
    pub unit_external_id: Option<String>,
    #[serde(rename = "securityCategories")]
    pub security_categories: Option<Vec<u64>>,
    #[serde(rename = "dataSetId")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "valueType")]
    pub value_type: String,
    #[serde(rename = "createdTime")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(rename = "lastUpdatedTime")]
    pub last_updated_time: Option<DateTime<Utc>>,
    #[serde(rename = "relationsFrom")]
    pub relations_from: Vec<RelationForm>,
}

impl TimeSeries {
    pub fn new(external_id: &str, name: &str) -> TimeSeries {
        TimeSeries {
            id: None,
            external_id: external_id.to_string(),
            name: name.to_string(),
            metadata: None,
            unit: None,
            description: None,
            unit_external_id: None,
            security_categories: None,
            data_set_id: None,
            value_type: "float".to_string(),
            created_time: None,
            last_updated_time: None,
            relations_from: vec![],
        }
    }
    pub fn from_dict(dict: HashMap<String, String>) -> Self {
        Self {
            id: dict.get("id").map(|v| v.parse::<u64>().unwrap()),
            external_id: dict.get("externalId").unwrap().to_string(),
            name: dict.get("name").unwrap().to_string(),
            metadata: dict
                .get("metadata")
                .map(|v| serde_json::from_str(v).unwrap()),
            unit: dict.get("units").map(|v| v.to_string()),
            description: dict.get("description").map(|v| v.to_string()),
            unit_external_id: dict.get("unitExternalId").map(|v| v.to_string()),
            security_categories: dict
                .get("securityCategories")
                .map(|v| serde_json::from_str(v).unwrap()),
            data_set_id: dict.get("dataSetId").map(|v| v.parse::<u64>().unwrap()),
            value_type: dict.get("valueType").unwrap().to_string(),
            created_time: None,
            last_updated_time: None,
            relations_from: vec![],
        }
    }

    pub fn builder() -> TimeSeries {
        TimeSeries::new("", "")
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
        self.unit = Option::from(unit.to_string());
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

    pub fn set_created_time(&mut self, created_time: DateTime<Utc>) -> &mut TimeSeries {
        self.created_time = Some(created_time);
        self
    }

    pub fn set_last_updated_time(&mut self, last_updated_time: DateTime<Utc>) -> &mut TimeSeries {
        self.last_updated_time = Some(last_updated_time);
        self
    }

    pub fn set_relations_from(&mut self, relations_from: Vec<RelationForm>) -> &mut TimeSeries {
        self.relations_from = relations_from;
        self
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
    #[serde(rename = "valueType")]
    pub value_type: Field<String>,
}

impl TimeSeriesUpdateFields {
    pub fn new() -> TimeSeriesUpdateFields {
        TimeSeriesUpdateFields {
            external_id: Field::default(),
            name: Field::default(),
            metadata: MapField::default(),
            unit: Field::default(),
            description: Field::default(),
            unit_external_id: Field::default(),
            security_categories: ListField::default(),
            data_set_id: Field::default(),
            relations_from: ListField::default(),
            value_type: Field::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeriesUpdate {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(rename = "externalId")]
    pub external_id: Option<String>,
    pub update: TimeSeriesUpdateFields,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TimeSeriesUpdateCollection {
    items: Vec<TimeSeriesUpdate>,
}

impl TimeSeriesUpdateCollection {
    pub fn new() -> Self {
        TimeSeriesUpdateCollection { items: vec![] }
    }
    #[must_use]
    pub fn from_vec(items: Vec<TimeSeriesUpdate>) -> Self {
        TimeSeriesUpdateCollection { items }
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
