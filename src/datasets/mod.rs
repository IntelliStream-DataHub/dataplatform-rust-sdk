#[cfg(test)]
mod tests;

use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::fields::{Field, ListField, MapField};
use crate::filters::TimeFilter;
use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId, SearchForm};
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

    /// `POST /datasets/list` — every dataset in the tenant, newest first.
    ///
    /// `/list` now delegates to the same handler as `/filter`, and an empty body means "no
    /// restriction", so this posts `{}`. That leaves the server's default `limit` of 100 in place:
    /// for more than that, or for any criteria, use [`filter`](Self::filter).
    pub async fn list(&self) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/list", self.base_url);
        self.execute_post_request(path, &serde_json::json!({})).await
    }

    /// `POST /datasets/filter` — datasets matching [`DatasetFilter`], newest first.
    ///
    /// Every criterion on the filter is honoured server-side. Results are capped by the form's
    /// `limit` (default 100, max 10000) and there is no paging, so a filter broad enough to exceed
    /// the cap is silently truncated — narrow it rather than trying to page.
    pub async fn filter(
        &self,
        filter: &DatasetFilter,
    ) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/filter", self.base_url);
        self.execute_post_request(path, filter).await
    }

    pub async fn by_ids<I>(&self, id_collection: &I) -> Result<DataWrapper<Dataset>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<Dataset>, _>(path, &id_collection.into())
            .await
    }

    /// `POST /datasets/search` — Postgres full-text search over a dataset's name, external id and
    /// description at once, so a hit on any of the three matches. The last term is a prefix match
    /// (the query is `websearch_to_tsquery` with `:*` appended), which is what makes this usable
    /// from a search box mid-word.
    ///
    /// Results are **not ranked** — the query has no `ORDER BY`, so row order is whatever the
    /// index scan produced. Do not read the first item as the best match.
    ///
    /// The form's `search.query` and `limit` both reach the server; its `filter` is accepted and
    /// then ignored, so use [`filter`](Self::filter) for criteria. The query is validated at
    /// 3–140 characters, so a shorter one comes back 400. No match is an empty item list, not an
    /// error — the 404 the OpenAPI annotation still advertises was removed server-side.
    ///
    /// [`search_by_query`](Self::search_by_query) is the shorthand for the common case.
    pub async fn search(
        &self,
        search: &DatasetSearch,
    ) -> Result<DataWrapper<Dataset>, ResponseError> {
        let path = &format!("{}/search", self.base_url);
        self.execute_post_request(path, &search).await
    }

    /// [`search`](Self::search) with just a query string, leaving `limit` at the default 100.
    /// `query` must be 3–140 characters.
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
    /// **Observed to answer 200 with no body at all** — `Content-Length: 0` and no `Content-Type`,
    /// not even `{"items":[]}`. Against a backend whose `GET /policies` returned three policies,
    /// this endpoint returned that empty response, despite both reading `PolicyRepository
    /// .findAll()`. The empty body means callers see zero items, so treat the result as
    /// unreliable rather than authoritative. That is a server-side bug rather than something the
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

/// Criteria for `POST /datasets/filter` and `POST /datasets/list`.
///
/// Every field here is honoured by `DataSetCustomRepoImpl.filter`; nothing on it is decorative.
/// A field left unset places no restriction, and so does an **empty** list or map — an empty `IN`
/// is not valid SQL, and the backend reads "I built a list and had nothing to put in it" as
/// "no restriction" rather than "match nothing".
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct BasicDatasetFilter {
    /// Datasets with any of these numeric ids. Max 1000. Sent as strings, like every id on the wire.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id_vec"
    )]
    ids: Option<Vec<u64>>,
    /// Datasets with any of these external ids. Max 1000. Matched on the hash of the lowercased
    /// form, so this is exact but case-insensitive.
    #[serde(skip_serializing_if = "Option::is_none")]
    external_ids: Option<Vec<String>>,
    /// Datasets whose name matches any entry, as an ILIKE pattern — you place the `%`, so `"SAP%"`
    /// is a prefix match and `"SAP work orders"` an exact one. Case-insensitive; entries OR
    /// together. Max 1000.
    #[serde(skip_serializing_if = "Option::is_none")]
    names: Option<Vec<String>>,
    /// The source the dataset came from, as an ILIKE pattern. Max 128 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<String>,
    /// Metadata the dataset must carry. Several entries **AND** together ("has all of these"),
    /// each matching key and value exactly.
    #[serde(skip_serializing_if = "Option::is_none")]
    metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    created_time: Option<TimeFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_updated_time: Option<TimeFilter>,
    /// Prefix match on the external id, anchored at the start and case-insensitive. Max 255.
    #[serde(skip_serializing_if = "Option::is_none")]
    external_id_prefix: Option<String>,
    /// The flag is stored as metadata that is absent until first set, so `Some(false)` matches
    /// "no entry, or an entry saying false" — not just rows that carry the key.
    #[serde(skip_serializing_if = "Option::is_none")]
    write_protected: Option<bool>,
    /// Same absent-until-set semantics as [`write_protected`](Self::write_protected).
    #[serde(skip_serializing_if = "Option::is_none")]
    deactivated: Option<bool>,
}

impl BasicDatasetFilter {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn set_ids(&mut self, ids: Vec<u64>) -> &mut Self {
        self.ids = Some(ids);
        self
    }
    pub fn set_external_ids(&mut self, external_ids: Vec<String>) -> &mut Self {
        self.external_ids = Some(external_ids);
        self
    }
    pub fn set_names(&mut self, names: Vec<String>) -> &mut Self {
        self.names = Some(names);
        self
    }
    pub fn set_source(&mut self, source: String) -> &mut Self {
        self.source = Some(source);
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
        self.last_updated_time = Some(last_updated_time);
        self
    }
    pub fn set_external_id_prefix(&mut self, external_id_prefix: String) -> &mut Self {
        self.external_id_prefix = Some(external_id_prefix);
        self
    }
    pub fn set_write_protected(&mut self, write_protected: bool) -> &mut Self {
        self.write_protected = Some(write_protected);
        self
    }
    pub fn set_deactivated(&mut self, deactivated: bool) -> &mut Self {
        self.deactivated = Some(deactivated);
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}

/// Body of `POST /datasets/filter` and `POST /datasets/list`.
///
/// `DataSetRetreiver` also declares a `cursor`, but `DataSetService.filter` passes only the filter
/// and the limit to the repository — there is no paging, so no cursor is exposed here.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<BasicDatasetFilter>,
    /// Caps the result. Defaults to 100 server-side; above 10000 the request is rejected with 400,
    /// and a value <= 0 is silently treated as the default.
    limit: u64,
}

impl DatasetFilter {
    pub fn new() -> Self {
        Self {
            filter: None,
            limit: 100,
        }
    }
    pub fn from_filter(filter: BasicDatasetFilter) -> Self {
        Self {
            filter: Some(filter),
            ..Self::new()
        }
    }
    pub fn set_filter(&mut self, filter: BasicDatasetFilter) -> &mut Self {
        self.filter = Some(filter);
        self
    }
    /// Max 10000 — the server answers 400 above that.
    pub fn set_limit(&mut self, limit: u64) -> &mut Self {
        self.limit = limit;
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}

impl Default for DatasetFilter {
    fn default() -> Self {
        Self::new()
    }
}

/// Body of `POST /datasets/search`.
///
/// `DataSetSearch` declares a `filter` too, but `DataSetService.search` passes only
/// `form.getSearch().getQuery()` and `form.getLimit()` to the repository, so no criteria field is
/// exposed here. Use [`DatasetsService::filter`] for criteria.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DatasetSearch {
    search: SearchForm,
    /// Caps the result. Defaults to 100 server-side; unlike the filter endpoint the cap here is
    /// **1000**, and above it the request is rejected with 400.
    limit: u64,
}
impl DatasetSearch {
    pub fn new() -> Self {
        Self {
            search: SearchForm::new(),
            limit: 100,
        }
    }

    /// A search carrying just the query — the only part of the form besides `limit` that the
    /// server reads. Must be 3–140 characters or the server answers 400.
    pub fn from_query(query: &str) -> Self {
        let mut search = SearchForm::new();
        search.query = Some(query.to_string());
        Self {
            search,
            ..Self::new()
        }
    }
    pub fn set_search(&mut self, search: SearchForm) -> &mut Self {
        self.search = search;
        self
    }
    /// Max 1000 — the server answers 400 above that.
    pub fn set_limit(&mut self, limit: u64) -> &mut Self {
        self.limit = limit;
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}
