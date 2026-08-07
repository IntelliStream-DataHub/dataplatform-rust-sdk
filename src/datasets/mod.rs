#[cfg(test)]
mod tests;

use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::fields::{Field, ListField, MapField};
use crate::filters::{AdvancedEventFilter, BasicEventFilter, TimeFilter};
use crate::generic::{
    ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId, SearchAndFilterForm,
    SearchForm,
};
use crate::graph_data_wrapper::{GraphDataWrapper, GraphNode};
use crate::http::ResponseError;
use crate::resources::Resource;
use crate::ApiService;
use chrono::{DateTime, FixedOffset, Utc};
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Weak};

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

    /// `POST /datasets/list` — every dataset in the tenant.
    ///
    /// The endpoint takes a body (criteria + `limit` + `cursor`) but **the server currently
    /// ignores it**: the handler is a bare `dataSetRepository.findAll()`. This method therefore
    /// takes no criteria, and the result is not paged. Filtering has to happen client-side until
    /// the backend honours the form.
    pub async fn list(&self) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/list", self.base_url);
        self.execute_post_request(path, &DatasetFilter::new()).await
    }

    /// Previously posted to `/datasets/filter`, which does not exist — every call failed. It now
    /// goes to `/datasets/list`, but the server ignores the criteria, so this returns *every*
    /// dataset regardless of what you pass.
    #[deprecated(
        since = "0.1.0",
        note = "the server ignores the criteria and returns every dataset; use `list()` so that is explicit"
    )]
    pub async fn filter(
        &self,
        filter: &DatasetFilter,
    ) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/list", self.base_url);
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

    /// `POST /datasets/search` — free-text search across dataset names, ranked by relevance.
    ///
    /// Only `search.query` reaches the server; the form's `filter`, `limit` and `cursor` are
    /// accepted and then ignored by the handler. The query is validated at 3–140 characters, so a
    /// shorter one comes back 400. No match is an empty item list, not an error.
    ///
    /// [`search_by_query`](Self::search_by_query) is the shorthand for the common case.
    pub async fn search(
        &self,
        search: &DatasetSearch,
    ) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/search", self.base_url);
        self.execute_post_request(path, &search).await
    }

    /// [`search`](Self::search) with just a query string — the only part of the form the server
    /// reads. `query` must be 3–140 characters.
    pub async fn search_by_query(
        &self,
        query: &str,
    ) -> Result<DataWrapper<Dataset>, ResponseError> {
        self.search(&DatasetSearch::from_query(query)).await
    }

    /// `POST /datasets/update` — partial update of one or more datasets.
    ///
    /// Each [`DatasetUpdate`] targets a dataset by external id or numeric id and carries only the
    /// fields it changes; see [`DatasetUpdate`] for the builder.
    ///
    /// A dataset is the unit access is granted on, so the server treats editing one as an operator
    /// action: this requires an all-datasets write grant and answers **403** without one, even for
    /// a caller who can write the dataset's contents.
    pub async fn update<I>(&self, data: &I) -> Result<DataWrapper<Dataset>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<DatasetUpdate>>,
    {
        let path = &format!("{}/update", self.base_url);
        self.execute_post_request(path, &data.into()).await
    }

    /// `GET /datasets/policies` — the access policies a dataset can be associated with.
    ///
    /// Policies come back as graph [`Resource`]s, not datasets — the server runs them through the
    /// same resource transformer. Intended for populating a picker for [`Dataset::set_policies`].
    ///
    /// **Observed to return an empty list even when policies exist.** Against a backend whose
    /// `GET /policies` returned three, this endpoint returned none, despite both reading
    /// `PolicyRepository.findAll()`. That looks like a server-side bug rather than something the
    /// SDK can work around, so this is wired to the documented endpoint and left alone. If you
    /// need the actual policy list today, `GET /policies` has it — the SDK does not cover that
    /// endpoint yet.
    pub async fn policies(&self) -> Result<DataWrapper<Resource>, ResponseError> {
        let path = &format!("{}/policies", self.base_url);
        self.execute_get_request(path, None::<&str>).await
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Dataset {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    //@NotNull
    //@Size(min= 3, max = 256)
    pub external_id: String,
    //@NotNull
    //3, max = 512)
    pub name: String,
    pub description: Option<String>,
    pub policies: Option<Vec<String>>,
    pub metadata: HashMap<String, String>,
    pub connected_data_sets: Vec<u64>,
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
        // creates an empty datasets with external id given by snake_case of name.
        Dataset {
            id: None,
            external_id: to_snake_lower_cased_allow_start_with_digits(&name),
            metadata: hashmap! {},
            description: None,
            name,
            policies: None,
            connected_data_sets: vec![],

            created_time: None,
            last_updated_time: None,
        }
    }
    pub fn add_metadata(&mut self, key: String, value: String) {
        self.metadata.insert(key, value);
    }
    pub fn remove_metadata(&mut self, key: String) {
        self.metadata.remove(&key);
    }
    pub fn set_name(&mut self, name: String) -> &mut Self {
        self.name = name;
        self
    }
    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) -> &mut Self {
        self.metadata = metadata;
        self
    }
    pub fn set_policies(&mut self, policies: Vec<String>) -> &mut Self {
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
    pub fn set_external_id(&mut self, external_id: String) -> &mut Self {
        self.external_id = external_id;
        self
    }
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }
    pub fn set_description(&mut self, description: String) -> &mut Self {
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

/// A partial update for one dataset (`POST /datasets/update`), mirroring the server's
/// `DataSetForm`.
///
/// Target the dataset with [`by_external_id`](Self::by_external_id) or [`by_id`](Self::by_id),
/// then chain only the fields you are changing — anything left unset is omitted from the request
/// and untouched by the server.
///
/// ```no_run
/// # use dataplatform_rust_sdk::datasets::DatasetUpdate;
/// # use dataplatform_rust_sdk::fields::Field;
/// let update = DatasetUpdate::by_external_id("sap_work_orders")
///     .description(Field::value("SAP work orders — live sync"))
///     .write_protected(Field::value(true));
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DatasetUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    pub update: DatasetUpdateFields,
}

impl DatasetUpdate {
    /// Target the dataset with this external id.
    pub fn by_external_id(external_id: &str) -> Self {
        Self {
            id: None,
            external_id: Some(external_id.to_string()),
            update: DatasetUpdateFields::default(),
        }
    }

    /// Target the dataset with this numeric id.
    pub fn by_id(id: u64) -> Self {
        Self {
            id: Some(id),
            external_id: None,
            update: DatasetUpdateFields::default(),
        }
    }

    /// Change the dataset's `externalId`. A duplicate answers 409.
    pub fn external_id(mut self, field: Field<String>) -> Self {
        self.update.external_id = Some(field);
        self
    }

    pub fn name(mut self, field: Field<String>) -> Self {
        self.update.name = Some(field);
        self
    }

    pub fn description(mut self, field: Field<String>) -> Self {
        self.update.description = Some(field);
        self
    }

    /// Replace, add to, or remove from the metadata map — see [`MapField`].
    pub fn metadata(mut self, field: MapField) -> Self {
        self.update.metadata = Some(field);
        self
    }

    /// Replace, add to, or remove from the label list — see [`ListField`].
    pub fn labels(mut self, field: ListField<String>) -> Self {
        self.update.labels = Some(field);
        self
    }

    /// Mark the dataset write-protected, blocking further writes to its contents.
    pub fn write_protected(mut self, field: Field<bool>) -> Self {
        self.update.write_protected = Some(field);
        self
    }

    pub fn deactivated(mut self, field: Field<bool>) -> Self {
        self.update.deactivated = Some(field);
        self
    }
}

/// The changed fields of a [`DatasetUpdate`]. Every entry is optional: an unset field is left out
/// of the request entirely, which the server reads as "leave unchanged".
///
/// The set mirrors the server's `DataSetFields` exactly. Note there is no `policies` or
/// `connectedDataSets` here — the update endpoint does not accept them, whatever
/// [`Dataset`] can carry on create.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DatasetUpdateFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<Field<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Field<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Field<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MapField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<ListField<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub write_protected: Option<Field<bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deactivated: Option<Field<bool>>,
}

// `DatasetUpdate` identifies its target by id *or* external id, so it cannot implement
// `DataHubEntity` (whose `ext_id` returns a `&String`). These mirror what that trait's blanket
// impls would have given: pass one update, a reference, or a Vec of either.
impl From<DatasetUpdate> for DataWrapper<DatasetUpdate> {
    fn from(value: DatasetUpdate) -> Self {
        DataWrapper::from_vec(vec![value])
    }
}
impl From<&DatasetUpdate> for DataWrapper<DatasetUpdate> {
    fn from(value: &DatasetUpdate) -> Self {
        DataWrapper::from_vec(vec![value.clone()])
    }
}
impl From<Vec<DatasetUpdate>> for DataWrapper<DatasetUpdate> {
    fn from(value: Vec<DatasetUpdate>) -> Self {
        DataWrapper::from_vec(value)
    }
}
impl From<&Vec<DatasetUpdate>> for DataWrapper<DatasetUpdate> {
    fn from(value: &Vec<DatasetUpdate>) -> Self {
        DataWrapper::from_vec(value.clone())
    }
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
    pub fn set_filter(&mut self, filter: BasicDatasetFilter) -> &mut Self {
        self.filter = filter;
        self
    }
    pub(crate) fn set_advanced_filter(&mut self, filter: BasicDatasetFilter) -> &mut Self {
        self.filter = filter;
        self
    }
    pub fn set_limit(&mut self, limit: usize) -> &mut Self {
        self.limit = limit;
        self
    }
    pub fn cursor(&self) -> Option<&String> {
        self.cursor.as_ref()
    }
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

    /// A search carrying just the query — the only field `POST /datasets/search` actually reads.
    /// Must be 3–140 characters or the server answers 400.
    pub fn from_query(query: &str) -> Self {
        let mut search = SearchForm::new();
        search.query = Some(query.to_string());
        Self {
            search,
            ..Self::new()
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
    pub fn cursor(&self) -> Option<&String> {
        self.cursor.as_ref()
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}
