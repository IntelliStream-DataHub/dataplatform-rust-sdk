use crate::events::EventsService;
use crate::files::FileService;
use crate::http::{process_response, ResponseError};
use crate::timeseries::TimeSeriesService;
use crate::unit::UnitsService;
use crate::ApiService;
use chrono::{DateTime, TimeZone, Utc};
use oauth2::http;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::hash::Hasher;
use std::sync::{Arc, Weak};
use crate::functions::FunctionsService;
use crate::subscriptions::SubscriptionsService;

// Deliberately NOT PartialEq: an IdAndExtId is an identity *selector*, and the same backend
// object can be named three ways — {id, externalId}, {id, None}, {None, externalId}. A derived
// (structural) equality would call those unequal, and a correct semantic equality is impossible
// here without resolving against the backend. So the type simply isn't comparable.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IdAndExtId {
    // todo Implement this as an enum, would allow for better validation
    // and make it impossible to not provide any id
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "externalId")]
    pub external_id: Option<String>,
}

impl IdAndExtId {
    pub fn from_id(id: u64) -> Self {
        IdAndExtId {
            id: Some(id),
            external_id: None,
        }
    }

    pub fn from_external_id(external_id: &str) -> Self {
        IdAndExtId {
            id: None,
            external_id: Some(external_id.to_string()),
        }
    }
}
impl From<&Vec<IdAndExtId>> for DataWrapper<IdAndExtId> {
    fn from(value: &Vec<IdAndExtId>) -> Self {
        DataWrapper {
            items: value.clone(),
            ..DataWrapper::new()
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatapointString {
    pub timestamp: String,
    pub value: String,
}

impl DatapointString {
    pub fn new(timestamp: &str, value: &str) -> Self {
        DatapointString {
            timestamp: timestamp.to_string(),
            value: value.to_string(),
        }
    }

    pub fn from_datetime(timestamp: DateTime<Utc>, value: &str) -> Self {
        DatapointString {
            timestamp: timestamp.timestamp_millis().to_string(),
            value: value.to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Datapoint {
    // Read from "isoTime" when deserializing, but emit "timestamp" on serialization
    #[serde(rename(serialize = "timestamp", deserialize = "timestamp"))]
    pub timestamp: DateTime<Utc>,
    #[serde(default)]
    pub value: Option<f64>,
    #[serde(default)]
    pub min: Option<f64>,
    #[serde(default)]
    pub max: Option<f64>,
    #[serde(default)]
    pub average: Option<f64>,
    #[serde(default)]
    pub sum: Option<f64>,
}

impl Display for Datapoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut parts = Vec::new();

        parts.push(format!("timestamp: {}", self.timestamp));

        if let Some(v) = self.value {
            parts.push(format!("value: {}", v));
        }
        if let Some(v) = self.min {
            parts.push(format!("min: {}", v));
        }
        if let Some(v) = self.max {
            parts.push(format!("max: {}", v));
        }
        if let Some(v) = self.average {
            parts.push(format!("average: {}", v));
        }
        if let Some(v) = self.sum {
            parts.push(format!("sum: {}", v));
        }

        write!(f, "Datapoint {{ {} }}", parts.join(", "))
    }
}
impl Datapoint {
    pub fn from(timestamp: DateTime<Utc>, value: f64) -> Self {
        Datapoint {
            timestamp,
            value: Some(value),
            min: None,
            max: None,
            average: None,
            sum: None,
        }
    }

    pub fn from_epoch_millis_timestamp(epoch_millis: i64, value: f64) -> Self {
        Datapoint {
            timestamp: Utc.timestamp_millis_opt(epoch_millis).unwrap(),
            value: Some(value),
            min: None,
            max: None,
            average: None,
            sum: None,
        }
    }

    pub fn timestamp(&self) -> DateTime<Utc> {
        self.timestamp
    }
    pub fn average(&self) -> Option<f64> {
        self.average
    }
    pub fn value(&self) -> Option<f64> {
        self.value
    }
    pub fn min(&self) -> Option<f64> {
        self.min
    }
    pub fn max(&self) -> Option<f64> {
        self.max
    }
    pub fn sum(&self) -> Option<f64> {
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
        DatapointEpoch { timestamp, value }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DatapointsCollection<T> {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(rename = "externalId")]
    pub external_id: Option<String>,
    pub datapoints: Vec<T>,
    #[serde(rename = "nextCursor")]
    pub next_cursor: Option<String>,
    pub unit: Option<String>,
    #[serde(rename = "unitExternalId")]
    pub unit_external_id: Option<String>,
}

// Manual impl: derived Default would demand T: Default, which the fields don't need.
impl<T> Default for DatapointsCollection<T> {
    fn default() -> Self {
        DatapointsCollection {
            id: None,
            external_id: None,
            datapoints: vec![],
            next_cursor: None,
            unit: None,
            unit_external_id: None,
        }
    }
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
        format!(
            "DatapointsCollection {{ id: {:?}, external_id: {:?}, datapoints: {:?} }}",
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

/// Body of the `POST /{entity}/search` endpoints: the free-text query, an optional structured
/// filter, and a cap.
///
/// All four searches share this shape. Generic only over the filter, because that is the one part
/// that differs: each entity's search declares its *own* filter type — the same one its `/filter`
/// endpoint takes.
///
/// The phrase decides which rows are candidates, the filter only ever removes some of them, and
/// `limit` caps what survives — so a filter is never a way to *widen* a search. Omit it (`None`)
/// for no narrowing. `limit` defaults to 100, caps at 1000, and a value `<= 0` falls back to the
/// default rather than returning nothing.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SearchAndFilterForm<F> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<F>,
    pub search: SearchForm,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}
// `FilterForm` used to live here: thirteen fields, none of them read by any endpoint, sent under
// `filter` on every search. It has been removed rather than ported — the entity filters are what
// the search endpoints actually declare, and `SearchAndFilterForm` is generic over them now.

impl<F> SearchAndFilterForm<F> {
    /// A search for `query`, narrowing nothing, at the server's default limit.
    pub fn new(query: impl Into<String>) -> Self {
        SearchAndFilterForm {
            filter: None,
            search: SearchForm::new(query),
            limit: None,
        }
    }

    pub fn with_filter(mut self, filter: F) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// The free-text half of all four `/{entity}/search` endpoints.
///
/// One field, and not optional: the api declares it `@NotBlank` at 3–140 characters, so a missing
/// or null query is a 400 rather than an unnarrowed search. Listing rows with no phrase is what the
/// `/filter` endpoints are for.
///
/// `name` and `description` used to sit here as well, honoured only by the timeseries search and
/// only one of the three at a time — and `name` matched by *exact equality* under an endpoint
/// documented as full-text. Both are gone from the api: `query` already covers the description
/// column, and the filter's `name` is a case-insensitive pattern list, which is what `name` was
/// reached for and more than it could do.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SearchForm {
    pub query: String,
}

impl SearchForm {
    pub fn new(query: impl Into<String>) -> Self {
        SearchForm {
            query: query.into(),
        }
    }
}


/// One series, and the window of datapoints to remove from it, for
/// [`delete_datapoints`](crate::TimeSeriesService::delete_datapoints).
///
/// Name the series with either [`id`](Self::id) or [`external_id`](Self::external_id). The window
/// is half-open and **both** bounds are optional, so there are four calls to be made:
///
/// | Bounds set | What the api deletes |
/// |---|---|
/// | begin and end | The half-open window between them |
/// | begin only | Everything from that instant onward |
/// | end only | Everything before that instant |
/// | neither | Every datapoint of the series, leaving its definition, edges and subscriptions |
///
/// The last one is how a series is emptied and refilled, a backfill that went in wrong being the
/// usual reason. To delete the series itself instead, use
/// [`delete`](crate::TimeSeriesService::delete), which takes its datapoints with it.
///
/// A 204 means the request was accepted, not that the rows are gone: the purge is asynchronous, so
/// a read straight afterwards can still see them. None of it can be undone.
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct DeleteFilter {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(rename = "externalId")]
    pub external_id: Option<String>,
    #[serde(rename = "inclusiveBegin")]
    pub inclusive_begin: Option<DateTime<Utc>>,
    #[serde(rename = "exclusiveEnd")]
    pub exclusive_end: Option<DateTime<Utc>>,
}

impl DeleteFilter {
    /// A filter naming no series and no window. Set [`id`](Self::id) or
    /// [`external_id`](Self::external_id) before sending it: an item naming neither is a 400.
    #[must_use]
    pub fn new() -> Self {
        DeleteFilter::default()
    }

    /// The window to clear from the series with this external id. `None` leaves that side of the
    /// window open, and `None` for both clears the whole series.
    ///
    /// ```no_run
    /// # use chrono::{TimeZone, Utc};
    /// # use intellistream_datahub_sdk::generic::DeleteFilter;
    /// // Everything recorded before 2026 goes; the series itself stays.
    /// let old = DeleteFilter::from_external_id(
    ///     "engine_temperature".to_string(),
    ///     None,
    ///     Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()));
    /// ```
    #[must_use]
    pub fn from_external_id(
        external_id: String,
        inclusive_begin: Option<DateTime<Utc>>,
        exclusive_end: Option<DateTime<Utc>>,
    ) -> Self {
        DeleteFilter {
            id: None,
            external_id: Some(external_id),
            inclusive_begin,
            exclusive_end,
        }
    }

    /// As [`from_external_id`](Self::from_external_id), naming the series by its id.
    #[must_use]
    pub fn from_id(
        id: u64,
        inclusive_begin: Option<DateTime<Utc>>,
        exclusive_end: Option<DateTime<Utc>>,
    ) -> Self {
        DeleteFilter {
            id: Some(id),
            external_id: None,
            inclusive_begin,
            exclusive_end,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct RetrieveFilter {
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub limit: Option<u64>,
    pub aggregates: Option<Vec<String>>,
    pub granularity: Option<String>,
    pub cursor: Option<String>,
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
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

    pub(crate) fn set_limit(&mut self, limit: u64) -> &mut RetrieveFilter {
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
        self.aggregates
            .as_mut()
            .unwrap()
            .push(aggregate.to_string());
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

impl From<IdAndExtId> for DataWrapper<IdAndExtId> {
    fn from(value: IdAndExtId) -> Self {
        DataWrapper {
            items: vec![value],
            ..DataWrapper::new()
        }
    }
}
impl From<&IdAndExtId> for DataWrapper<IdAndExtId> {
    fn from(value: &IdAndExtId) -> Self {
        DataWrapper {
            items: vec![value.clone()],
            ..DataWrapper::new()
        }
    }
}
impl From<Vec<IdAndExtId>> for DataWrapper<IdAndExtId> {
    fn from(value: Vec<IdAndExtId>) -> Self {
        DataWrapper {
            items: value,
            ..DataWrapper::new()
        }
    }
}

impl From<Vec<RetrieveFilter>> for DataWrapper<RetrieveFilter> {
    fn from(value: Vec<RetrieveFilter>) -> Self {
        DataWrapper {
            items: value,
            ..DataWrapper::new()
        }
    }
}
pub(crate) trait DataHubEntity: Clone + Serialize {
    fn ext_id(&self) -> &String;
}
impl<T: DataHubEntity> From<T> for DataWrapper<T> {
    fn from(value: T) -> Self {
        DataWrapper {
            items: vec![value],
            ..DataWrapper::new()
        }
    }
}
impl<T: DataHubEntity> From<Vec<T>> for DataWrapper<T> {
    fn from(vector: Vec<T>) -> Self {
        DataWrapper {
            items: vector,
            ..DataWrapper::new()
        }
    }
}

impl<T: DataHubEntity> From<&Vec<T>> for DataWrapper<T> {
    fn from(vector: &Vec<T>) -> Self {
        DataWrapper {
            items: vector.clone(),
            ..DataWrapper::new()
        }
    }
}
impl<T: DataHubEntity> From<&T> for DataWrapper<T> {
    fn from(val: &T) -> Self {
        DataWrapper {
            items: vec![val.clone()],
            ..DataWrapper::new()
        }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    error_body: Option<String>,
    /// Where a paged read stopped; send it back as the request's `cursor` for the next page.
    ///
    /// Absent when there is no next page, so "keep going while `next_cursor` is `Some`" is the
    /// whole loop, with no separate end-of-data signal to get wrong. Note a *full* page may still
    /// be the last one — the server does not count the rows twice — so the walk ends with one
    /// request that comes back empty.
    ///
    /// Response-only: skipped when serializing, because `DataWrapper` doubles as a request body
    /// for the create/delete endpoints and this field is not part of theirs.
    // Renamed explicitly: this struct has no `rename_all`, so the field would otherwise be read
    // from `next_cursor` and never match the `nextCursor` the api sends — a paged read would look
    // like it had reached the end after one page.
    #[serde(default, rename = "nextCursor", skip_serializing)]
    next_cursor: Option<String>,
}

impl<T> DataWrapper<T> {
    pub fn new() -> Self {
        DataWrapper {
            items: vec![],
            http_status_code: None,
            error_body: None,
            next_cursor: None,
        }
    }

    #[must_use]
    pub fn from_vec(vec: Vec<T>) -> Self {
        DataWrapper {
            items: vec,
            http_status_code: None,
            error_body: None,
            next_cursor: None,
        }
    }

    /// The cursor for the page after this one, if there is one. See [`next_cursor`](Self::next_cursor).
    #[must_use]
    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }

    pub fn set_next_cursor(&mut self, next_cursor: Option<String>) {
        self.next_cursor = next_cursor;
    }

    #[must_use]
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

    #[must_use]
    pub fn length(&self) -> u64 {
        self.items.len() as u64
    }

    #[must_use]
    pub fn get_http_status_code(&self) -> Option<u16> {
        self.http_status_code
    }

    pub fn set_http_status_code(&mut self, http_status_code: u16) {
        self.http_status_code = Some(http_status_code);
    }

    pub fn to_string(&self) -> String {
        format!(
            "DataWrapper {{ items: {:?}, http_status_code: {:?} }}",
            self.items.len(),
            self.http_status_code,
        )
    }
}

// Constrain T by requiring it implement the Identifiable trait.
impl<T: Identifiable> DataWrapper<T> {
    pub fn remove_item(
        &mut self,
        id_to_remove: Option<u64>,
        external_id_to_remove: Option<String>,
    ) {
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

pub trait ApiServiceProvider {
    fn api_service(&self) -> &Weak<ApiService>;

    fn get_api_service(&self) -> Arc<ApiService> {
        self.api_service().upgrade().unwrap()
    }

    /// Post-process a failed request: drop a rejected token, then explain the failure.
    ///
    /// A 401 means the token just sent is not usable, and expiry is not the only way that
    /// happens — an identity provider still finishing its setup can issue one the API refuses
    /// for its whole lifetime. Clearing it here means the next call mints a fresh one, so a
    /// client that started a few seconds too early recovers on its next attempt instead of
    /// re-sending the same rejected credential until it expires.
    async fn on_request_error(&self, error: ResponseError, token: &str) -> ResponseError {
        if error.get_status() == http::StatusCode::UNAUTHORIZED {
            self.get_api_service().config.invalidate_token().await;
        }
        explain_auth_failure(error, token)
    }

    async fn get_token(&self) -> Result<String, ResponseError> {
        self.get_api_service()
            .config
            .get_api_token()
            .await
            .map_err(|e| ResponseError {
                status: http::StatusCode::UNAUTHORIZED,
                message: "failed to get api token: ".to_string() + &e.to_string(),
            })
    }

    async fn execute_get_request<
        T: DeserializeOwned + DataWrapperDeserialization,
        Param: Serialize + ?Sized,
    >(
        &self,
        path: &str,
        param: Option<&Param>,
    ) -> Result<T, ResponseError> {
        let token = self.get_token().await?;
        let response = if let Some(param) = param {
            self.get_api_service()
                .http_client
                .get(path)
                .bearer_auth(token.clone())
                .query(param)
                .send()
                .await
                .map_err(|err| {
                    eprintln!("HTTP request failed: {}", err);
                    ResponseError::from_err(err)
                })?
        } else {
            self.get_api_service()
                .http_client
                .get(path)
                .bearer_auth(token.clone())
                .send()
                .await
                .map_err(|err| {
                    eprintln!("HTTP request failed: {}", err);
                    ResponseError::from_err(err)
                })?
        };
                match process_response::<T>(response, path).await {
            Ok(value) => Ok(value),
            Err(e) => Err(self.on_request_error(e, &token).await),
        }
    }

    async fn execute_post_request<
        T: DeserializeOwned + DataWrapperDeserialization,
        J: Serialize,
    >(
        &self,
        path: &str,
        json: &J,
    ) -> Result<T, ResponseError> {
        let token = self.get_token().await?;
        let response = self
            .get_api_service()
            .http_client
            .post(path)
            .json(json)
            .bearer_auth(token.clone())
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;
        if response.status() == 204 {
            // Return deserialized `T` with an empty body and the HTTP status code
            T::deserialize_and_set_status("", response.status().as_u16()).map_err(|err| {
                eprintln!("Failed to create object from empty response: {}", err);
                ResponseError {
                    status: response.status(),
                    message: err.to_string(),
                }
            })
        } else {
                        match process_response::<T>(response, path).await {
                Ok(value) => Ok(value),
                Err(e) => Err(self.on_request_error(e, &token).await),
            }
        }
    }

    /// Uploads a file with a raw `PUT`: the file content is the request body and all metadata
    /// travels in headers (`X-Datahub-Path`, `X-Datahub-External-Id`, `X-Datahub-Dataset-Id`,
    /// `X-Datahub-Description`, `Content-Type`). The server validates and authorises the upload
    /// from the headers before it reads a single body byte.
    async fn execute_file_upload_request<T: DeserializeOwned + DataWrapperDeserialization>(
        &self,
        path: &str,
        body: reqwest::Body,
        headers: Vec<(&str, String)>,
    ) -> Result<T, ResponseError> {
        let token = self.get_token().await?;

        let mut request = self
            .get_api_service()
            .http_client
            .put(path)
            .body(body)
            .bearer_auth(token.clone());
        for (name, value) in headers {
            request = request.header(name, value);
        }

        let response = request.send().await.map_err(|err| {
            eprintln!("HTTP file upload request failed: {}", err);
            ResponseError::from_err(err)
        })?;
                match process_response::<T>(response, path).await {
            Ok(value) => Ok(value),
            Err(e) => Err(self.on_request_error(e, &token).await),
        }
    }

    /// `GET` an endpoint that answers with bytes rather than JSON (currently only
    /// `/files/download/{id}`).
    ///
    /// The body is *not* passed through [`process_response`] — there is no `DataWrapper` to
    /// deserialize and no reason to print a binary body to stdout. Non-2xx responses still surface
    /// as a [`ResponseError`] carrying the server's text explanation, so error handling matches the
    /// JSON helpers.
    async fn execute_get_stream_request(
        &self,
        path: &str,
    ) -> Result<reqwest::Response, ResponseError> {
        let token = self.get_token().await?;
        let response = self
            .get_api_service()
            .http_client
            .get(path)
            .bearer_auth(token.clone())
            .header(http::header::ACCEPT, "*/*")
            .send()
            .await
            .map_err(|err| {
                eprintln!("HTTP request failed: {}", err);
                ResponseError::from_err(err)
            })?;

        let status = response.status();
        if status.is_success() {
            return Ok(response);
        }
        // A 401 means the token we just sent is not usable. Drop it so the next call mints a
        // fresh one instead of re-sending the same rejected credential until it expires — the
        // difference between a client that recovers on its next attempt and one that stays
        // broken for the token's lifetime. See DataHubConfig::invalidate_token.
        if status == http::StatusCode::UNAUTHORIZED {
            self.get_api_service().config.invalidate_token().await;
        }
        eprintln!("Request failed with status: {status}");
        Err(explain_auth_failure(
            ResponseError {
                status,
                message: response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Failed to read response body".to_string()),
            },
            &token,
        ))
    }
}

/// Add a reason to a 401 that arrived without one.
///
/// The API rejects a token whose `organization` claim is missing, malformed or ambiguous, but its
/// authentication entry point sends no `error_description` and an empty body — so the caller gets
/// `401` and nothing else, which reads as a bad credential. The token the SDK just sent carries
/// enough to say which it was; see [`crate::auth_diagnostics`] for why reading it discloses
/// nothing.
///
/// Anything already explained is left alone: a non-401, or a 401 that did come with a body, keeps
/// its own message, and a well-formed claim adds nothing (the 401 then has a cause this cannot
/// see — expiry, revocation, audience, signature).
fn explain_auth_failure(error: ResponseError, token: &str) -> ResponseError {
    if error.get_status() != http::StatusCode::UNAUTHORIZED {
        return error;
    }
    let Some(hint) = crate::auth_diagnostics::organization_hint(token) else {
        return error;
    };
    let existing = error.get_message();
    let message = if existing.trim().is_empty() {
        hint
    } else {
        format!("{existing} — {hint}")
    };
    ResponseError {
        status: error.get_status(),
        message,
    }
}

impl ApiServiceProvider for TimeSeriesService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl ApiServiceProvider for UnitsService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl ApiServiceProvider for EventsService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl ApiServiceProvider for FileService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl ApiServiceProvider for SubscriptionsService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}
impl ApiServiceProvider for FunctionsService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}
impl ApiServiceProvider for crate::labels::LabelsService {
    fn api_service(&self) -> &Weak<ApiService> {
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
            if status_code == 204 || body.is_empty() {
                // HTTP No content doesnt return anything
                let mut wrapper: DataWrapper<T> = DataWrapper::new();
                wrapper.set_http_status_code(status_code);
                return Ok(wrapper);
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
            eprintln!(
                "HTTP request failed with status code {}: {}",
                status_code, body
            );

            // Attempt to deserialize the body into DataWrapper<T>
            // This is useful if the error response *itself* is a structured JSON,
            // for example, containing an error object.
            match serde_json::from_str(body).map(|mut wrapper: DataWrapper<T>| {
                wrapper.set_http_status_code(status_code); // Set the HTTP status code
                wrapper // Return the modified wrapper
            }) {
                Ok(result) => Ok(result),
                Err(_) => {
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
    where
        Self: Sized,
    {
        Ok(body.to_string())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct INode {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    pub name: String,
    pub description: Option<String>,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub path: String,
    pub size: u64,
    pub checksum: Option<String>,
    pub source: Option<String>,
    pub r#type: Option<String>,
    #[serde(rename = "mimeType")]
    pub mime_type: Option<String>,
    #[serde(rename = "sourceDateCreated")]
    pub source_date_created: Option<DateTime<Utc>>,
    #[serde(rename = "sourceLastUpdated")]
    pub source_last_updated: Option<DateTime<Utc>>,
    #[serde(rename = "dateCreated")]
    pub date_created: DateTime<Utc>,
    #[serde(rename = "lastUpdated")]
    pub last_updated: DateTime<Utc>,
    #[serde(rename = "parentId")]
    #[serde(default, with = "crate::serde_helper::opt_string_id_i64")]
    pub parent_id: Option<i64>,
    #[serde(rename = "parentExternalId")]
    pub parent_external_id: Option<String>,
    #[serde(rename = "dataSetId")]
    #[serde(default, with = "crate::serde_helper::opt_string_id_i64")]
    pub data_set_id: Option<i64>,
    pub metadata: Option<HashMap<String, String>>,
    #[serde(rename = "relatedResources")]
    pub related_resources: Option<Vec<i64>>,
    #[serde(rename = "securityCategories")]
    pub security_categories: Option<Vec<i32>>,
}

#[cfg(test)]
mod search_body_tests {
    use super::{SearchAndFilterForm, SearchForm};
    use crate::filters::NodeFilter;

    /// The shape all four `/search` endpoints share, and the two names that are no longer part of
    /// it. `name` and `description` were dropped from the api's `SearchForm`, and a body naming a
    /// field the api does not have is now a 400 rather than a silent no-op — so their absence is
    /// the thing worth pinning.
    #[test]
    fn the_search_half_carries_a_query_and_nothing_else() {
        let body = serde_json::to_value(
            SearchAndFilterForm::<NodeFilter>::new("pump alpha").with_limit(50),
        )
        .unwrap();

        assert_eq!(body["search"], serde_json::json!({ "query": "pump alpha" }));
        assert_eq!(body["limit"], 50);
        assert!(
            body["search"].get("name").is_none(),
            "`name` is matched through the filter's pattern list now: {body}"
        );
        assert!(
            body["search"].get("description").is_none(),
            "the phrase already covers the description column: {body}"
        );
    }

    /// The filter narrows and never widens, so "no filter" has to be *absent* rather than an empty
    /// object — an empty one would be a filter that restricts nothing, which is the same answer by
    /// luck rather than by contract.
    #[test]
    fn an_absent_filter_is_omitted_rather_than_sent_empty() {
        let body =
            serde_json::to_value(SearchAndFilterForm::<NodeFilter>::new("pump")).unwrap();
        assert!(body.get("filter").is_none(), "{body}");

        let narrowed = serde_json::to_value(
            SearchAndFilterForm::new("pump").with_filter(NodeFilter {
                name: Some(vec!["Pump Alpha".to_string()]),
                ..Default::default()
            }),
        )
        .unwrap();
        assert_eq!(narrowed["filter"]["name"], serde_json::json!(["Pump Alpha"]));
    }

    /// The phrase is not optional. The api declares it `@NotBlank`, so a body without one is a
    /// 400 — a `None` here would only ever have been a request the server refuses.
    #[test]
    fn a_bare_search_form_round_trips() {
        let form: SearchForm = serde_json::from_str(r#"{"query":"pump"}"#).unwrap();
        assert_eq!(form.query, "pump");

        assert!(serde_json::from_str::<SearchForm>("{}").is_err());
    }
}

#[cfg(test)]
mod auth_failure_tests {
    use super::explain_auth_failure;
    use crate::http::ResponseError;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    use oauth2::http::StatusCode;

    fn jwt(payload: &str) -> String {
        format!("aGVhZGVy.{}.c2ln", URL_SAFE_NO_PAD.encode(payload))
    }

    fn error(code: u16, message: &str) -> ResponseError {
        ResponseError {
            status: StatusCode::from_u16(code).unwrap(),
            message: message.to_string(),
        }
    }

    #[test]
    fn an_unexplained_401_gains_the_reason_the_server_withheld() {
        // What a caller in two organizations actually gets back: 401, empty body, no
        // `WWW-Authenticate` detail. Without this the only signal is "401", which reads as a bad
        // secret and sends people off to rotate credentials.
        let token = jwt(r#"{"organization":{"beta":{"id":"2"},"acme":{"id":"1"}}}"#);
        let explained = explain_auth_failure(error(401, ""), &token);

        assert_eq!(explained.get_status(), StatusCode::UNAUTHORIZED, "status is untouched");
        let message = explained.get_message();
        assert!(message.contains("names 2 organizations"), "{message}");
        assert!(message.contains("acme, beta"), "{message}");
        assert!(message.contains("SCOPE=organization:<alias>"), "{message}");
    }

    #[test]
    fn a_401_that_came_with_a_body_keeps_it() {
        // Should the API ever start explaining itself, its words win and ours are appended —
        // never replace a real server message with a guess.
        let token = jwt(r#"{"organization":{"acme":{"id":"1"},"beta":{"id":"2"}}}"#);
        let explained = explain_auth_failure(error(401, "Bearer token expired"), &token);
        let message = explained.get_message();
        assert!(message.starts_with("Bearer token expired"), "{message}");
        assert!(message.contains("names 2 organizations"), "{message}");
    }

    #[test]
    fn a_well_formed_token_is_left_alone() {
        // Exactly one organization, so the 401 has some other cause (expiry, revocation, audience,
        // signature). Volunteering an organization explanation here would misdirect the reader.
        let token = jwt(r#"{"organization":{"acme":{"id":"1"}}}"#);
        let explained = explain_auth_failure(error(401, ""), &token);
        assert_eq!(explained.get_message(), "");
    }

    #[test]
    fn an_opaque_token_is_left_alone() {
        // A user-supplied `TOKEN=` need not be a JWT at all.
        let explained = explain_auth_failure(error(401, ""), "an-opaque-api-key");
        assert_eq!(explained.get_message(), "");
    }

    #[test]
    fn only_401s_are_touched() {
        // A dataset-ACL 403 already carries a problem+json body explaining itself; appending
        // organization advice to it would be noise, and wrong.
        let token = jwt(r#"{"organization":{"acme":{"id":"1"},"beta":{"id":"2"}}}"#);
        for code in [400u16, 403, 404, 500] {
            let explained = explain_auth_failure(error(code, "original"), &token);
            assert_eq!(explained.get_message(), "original", "{code} should pass through");
        }
    }
}

#[cfg(test)]
mod delete_filter_tests {
    use super::{DataWrapper, DeleteFilter};
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    #[test]
    fn a_window_serialises_under_the_api_field_names() {
        let filter = DeleteFilter::from_external_id(
            "engine_temperature".to_string(),
            Some(Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()),
            Some(Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap()),
        );

        let body = serde_json::to_value(DataWrapper::from_vec(vec![filter])).unwrap();

        // The api rejects unknown fields outright, so the camelCase renames are load-bearing:
        // begin/end under any other name is a 400 rather than a wider delete.
        assert_eq!(
            body["items"][0],
            json!({
                "id": null,
                "externalId": "engine_temperature",
                "inclusiveBegin": "2026-01-01T00:00:00Z",
                "exclusiveEnd": "2026-02-01T00:00:00Z",
            })
        );
    }

    #[test]
    fn both_bounds_open_means_clear_the_whole_series() {
        let filter = DeleteFilter::from_external_id("engine_temperature".to_string(), None, None);

        let body = serde_json::to_value(DataWrapper::from_vec(vec![filter])).unwrap();

        // An absent bound has to reach the api as an explicit null rather than being dropped or
        // defaulted to an instant: null on both sides is what it reads as "every datapoint".
        assert_eq!(body["items"][0]["inclusiveBegin"], json!(null));
        assert_eq!(body["items"][0]["exclusiveEnd"], json!(null));
    }

    #[test]
    fn an_id_target_goes_out_as_a_string() {
        let filter = DeleteFilter::from_id(9007199254740993, None, None);

        let body = serde_json::to_value(DataWrapper::from_vec(vec![filter])).unwrap();

        // Ids are 64-bit and JSON numbers are not, so they travel as strings everywhere.
        assert_eq!(body["items"][0]["id"], json!("9007199254740993"));
        assert_eq!(body["items"][0]["externalId"], json!(null));
    }
}
