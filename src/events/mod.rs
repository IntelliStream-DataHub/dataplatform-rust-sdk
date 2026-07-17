#[cfg(test)]
mod tests;

use crate::buffer::DurableSpool;
use crate::datahub::{to_snake_lower_cased_allow_start_with_digits, DataHubApi};
use crate::filters::EventFilter;
use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Mutex, Weak};
use uuid::Uuid;

pub struct EventsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
    // Durable spool for event ingestion (lazily opened on first buffered send; None if buffering off).
    spool: Mutex<Option<DurableSpool>>,
}

impl EventsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/events", base_url);
        EventsService {
            api_service,
            base_url,
            spool: Mutex::new(None),
        }
    }

    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<Event>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Event>>,
    {
        let mut dw = data.into();
        // Stamp a stable, time-ordered UUID v7 on each event that lacks an id, before the first send.
        // The server honors a client-supplied id, so a retry (e.g. from the durable buffer) carries the
        // same id and the events ReplacingMergeTree (ORDER BY id) collapses the duplicate instead of
        // creating a new row. v7 is time-ordered, which keeps that id sort key well clustered.
        for event in dw.get_items_mut() {
            if event.id.is_none() {
                event.id = Some(Uuid::now_v7());
            }
        }
        let path = format!("{}/create", self.base_url);

        let svc = self.get_api_service();
        if !svc.config.buffering_enabled() {
            return self
                .execute_post_request::<DataWrapper<Event>, _>(&path, &dw)
                .await;
        }
        self.ensure_spool(&svc.config);
        drop(svc); // don't hold the ApiService Arc across awaits

        let now = Utc::now().timestamp_millis();
        // Flush any on-disk backlog first; if it's still stuck, buffer the new events too.
        if !self.drain_spool(&path, now).await {
            self.append_to_spool(dw.get_items(), now);
            return Ok(buffered_wrapper());
        }
        match self
            .execute_post_request::<DataWrapper<Event>, _>(&path, &dw)
            .await
        {
            Ok(r) => Ok(r),
            Err(e) if e.is_bufferable() => {
                self.append_to_spool(dw.get_items(), now);
                Ok(buffered_wrapper())
            }
            Err(e) => Err(e), // terminal error: surface it
        }
    }

    /// Records currently held in the durable event spool (0 when buffering is off).
    pub fn buffered_count(&self) -> u64 {
        self.spool.lock().unwrap().as_ref().map_or(0, |s| s.size())
    }

    fn ensure_spool(&self, config: &DataHubApi) {
        let mut guard = self.spool.lock().unwrap();
        if guard.is_none() {
            let dir = config.buffer_directory().join("events");
            if let Ok(spool) = DurableSpool::open(
                dir,
                config.effective_buffer_retention_ms(),
                config.effective_buffer_max_bytes(),
            ) {
                *guard = Some(spool);
            }
        }
    }

    fn append_to_spool(&self, events: &[Event], now: i64) {
        let records: Vec<(i64, String)> = events
            .iter()
            .filter_map(|e| {
                let ts = e.event_time.timestamp_millis();
                serde_json::to_string(e).ok().map(|json| (ts, json))
            })
            .collect();
        if let Some(spool) = self.spool.lock().unwrap().as_mut() {
            let _ = spool.append(&records, now);
        }
    }

    /// Drain the spool to the server, oldest segment first. Returns false if the server is still down
    /// (a transient failure), leaving the rest buffered; terminal failures drop the offending segment.
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
            let events: Vec<Event> = lines
                .iter()
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect();
            let mut batch = DataWrapper::new();
            batch.set_items(events);
            match self
                .execute_post_request::<DataWrapper<Event>, _>(path, &batch)
                .await
            {
                Err(e) if e.is_bufferable() => return false, // server down or auth not yet restored: keep the rest
                _ => {
                    // success or terminal error: drop the segment (terminal would never succeed)
                    if let Some(s) = self.spool.lock().unwrap().as_mut() {
                        let _ = s.delete_segment(seq);
                    }
                }
            }
        }
    }

    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Event>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }

    pub async fn filter(&self, filter: &EventFilter) -> Result<DataWrapper<Event>, ResponseError> {
        let path = &format!("{}/filter", self.base_url);
        self.execute_post_request(path, &filter).await
    }

    pub async fn by_ids<I>(&self, id_collection: &I) -> Result<DataWrapper<Event>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<Event>, _>(path, &id_collection.into())
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

/// A result for a buffered (not-yet-confirmed) ingest: HTTP 202 with no items. Callers can detect
/// buffering via `get_http_status_code() == Some(202)` and `buffered_count()`.
fn buffered_wrapper() -> DataWrapper<Event> {
    let mut w = DataWrapper::new();
    w.set_http_status_code(202);
    w
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub id: Option<Uuid>,
    pub external_id: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    pub r#type: Option<String>,
    pub sub_type: Option<String>,
    pub status: Option<String>,
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
    #[serde(skip_serializing)]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing)]
    pub last_updated_time: Option<DateTime<Utc>>,
    pub related_resource_ids: Vec<u64>,
    pub related_resource_external_ids: Vec<String>,
    pub source: Option<String>,
    pub event_time: DateTime<Utc>,
}
impl DataHubEntity for Event {
    fn ext_id(&self) -> &String {
        &self.external_id
    }
}

impl Event {
    /// `event_time` is when the event actually occurred, as recorded by the source system or sensor.
    /// It is required: there is no sensible default, and "now" is usually wrong — the server records
    /// the ingestion time separately as `created_time`.
    pub fn new(external_id: String, event_time: DateTime<Utc>) -> Self {
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
            event_time,
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
        &self.external_id.as_str()
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

    pub fn get_event_time(&self) -> &DateTime<Utc> {
        &self.event_time
    }

    pub fn set_event_time(&mut self, event_time: DateTime<Utc>) {
        self.event_time = event_time;
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
