#[cfg(test)]
mod tests;
#[cfg(test)]
mod label_update_tests;

use crate::fields::{Field, ListField, MapField};
use crate::generic::{
    ApiServiceProvider, DataWrapper, DataWrapperDeserialization, IdAndExtId, Identifiable,
    SearchAndFilterForm,
};
use crate::graph_data_wrapper::{GraphDataWrapper, GraphNode};
use crate::http::{process_response, ResponseError};
use crate::relations::{EdgeProxy, RelForm, RelatedNode};
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Weak;

pub struct ResourceService {
    api_service: Weak<ApiService>,
    base_url: String,
}
impl ApiServiceProvider for ResourceService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl ResourceService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let resource_base_url = format!("{}/resources", base_url);
        Self {
            api_service,
            base_url: resource_base_url,
        }
    }

    /// Create resources, optionally with relations between them. Mirrors Java's
    /// `POST /resources/create` body shape `GraphDataWrapper<Resource, RelForm>`;
    /// the response is the graph in its post-create form, with each relation
    /// returned as an `EdgeProxy` carrying the server-assigned id. Pass an
    /// empty `Vec` for `relations` to create nodes only.
    pub async fn create(
        &self,
        nodes: Vec<Resource>,
        relations: Vec<RelForm>,
    ) -> Result<GraphDataWrapper<Resource>, ResponseError> {
        let payload: GraphDataWrapper<Resource, RelForm> =
            GraphDataWrapper::with_relations(nodes, relations);
        let url = &format!("{}/create", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Resource>, _>(&url, &payload)
            .await
    }
    pub async fn by_ids<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let payload = input.into();
        let url = &format!("{}/byids", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Resource>, _>(&url, &payload)
            .await
    }

    pub async fn delete<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let payload = input.into();
        //let token = self.get_token().await?;
        let url = &format!("{}/delete", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Resource>, _>(&url, &payload)
            .await
    }
    pub async fn search(
        &self,
        payload: &SearchAndFilterForm,
    ) -> Result<DataWrapper<Resource>, ResponseError> {
        let url = &format!("{}/search", self.base_url);
        self.execute_post_request::<DataWrapper<Resource>, _>(&url, &payload)
            .await
    }

    /// Walk the graph outward from a starting resource and return the connected
    /// sub-graph: the [`ResourceNetwork`] of nodes, the edges between them, and their
    /// labels. Mirrors `POST /resources/fetch-related`. Traversal is undirected and
    /// bounded by [`RelatedResourcesForm::depth`] (default `-1` = the whole connected
    /// component), optionally filtered to specific relationship types.
    ///
    /// Use it to reason about how things relate — e.g. whether two alarmed sensors
    /// share a common subsystem — which a flat [`by_ids`](Self::by_ids) read cannot answer.
    pub async fn fetch_related(
        &self,
        form: &RelatedResourcesForm,
    ) -> Result<ResourceNetwork, ResponseError> {
        let url = &format!("{}/fetch-related", self.base_url);
        self.execute_post_request::<ResourceNetwork, _>(&url, &form)
            .await
    }
    /// Update resources in place (`POST /resources/update`). Each [`ResourceUpdate`] targets one
    /// resource by id or external id and carries only the fields to change (PATCH semantics). The
    /// server returns the resources after the update, so the returned `labels` reflect what the
    /// backend actually stored — including the intrinsic type-label it always forces back.
    pub async fn update<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<GraphDataWrapper<ResourceUpdate>>,
    {
        let mut payload = input.into();
        // The server iterates `relations`; send an empty list rather than null when unset.
        if payload.relations.is_none() {
            payload.relations = Some(vec![]);
        }
        let url = &format!("{}/update", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Resource>, _>(&url, &payload)
            .await
    }

    /// `GET /resources/{id}` — one resource by its numeric id.
    ///
    /// Unlike [`by_ids`](Self::by_ids), which omits what it cannot find, this is a 404 when the
    /// resource does not exist.
    pub async fn get_by_id(&self, id: u64) -> Result<DataWrapper<Resource>, ResponseError> {
        let url = &format!("{}/{}", self.base_url, id);
        self.execute_get_request::<DataWrapper<Resource>, ()>(url, None)
            .await
    }

    /// `POST /resources/filter` — structured lookup. Every criterion is combined with AND.
    ///
    /// Prefer this to [`search`](Self::search) whenever the question is structured: an exact
    /// external id, a metadata value, a data set, a time range. It is faster and its results are
    /// predictable, where search ranks by fuzzy relevance.
    pub async fn filter(
        &self,
        retriever: &ResourceRetreiver,
    ) -> Result<DataWrapper<Resource>, ResponseError> {
        let url = &format!("{}/filter", self.base_url);
        self.execute_post_request::<DataWrapper<Resource>, _>(url, retriever)
            .await
    }

    /// `POST /resources/fetch-nearest` — the closest `limit` nodes carrying one of `end_labels`,
    /// plus the sub-graph connecting them back to the start.
    ///
    /// Where [`fetch_related`](Self::fetch_related) caps on hop depth and total nodes, this caps on
    /// the number of *matching* end-nodes, so "the 10 nearest TIMESERIES" is exactly ten however
    /// many intermediate nodes lie between them. You name what you want and the radius follows,
    /// rather than guessing a radius and seeing what falls inside it.
    pub async fn fetch_nearest(
        &self,
        form: &FetchNearestResourcesForm,
    ) -> Result<ResourceNetwork, ResponseError> {
        let url = &format!("{}/fetch-nearest", self.base_url);
        self.execute_post_request::<ResourceNetwork, _>(url, form)
            .await
    }
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    // used to be a serde skip if zero here. don't understand why
    // todo implement a smooth way to convert "datahub entities" to id-collections
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    pub external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    pub is_root: bool,
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
    pub source: Option<String>,
    pub labels: Option<Vec<String>>,
    /// The nodes this resource is connected to, with relationship type and direction,
    /// populated by the server on read. Empty on resources you construct locally for a
    /// create request (relations for create are passed separately as [`RelForm`]s).
    #[serde(default)]
    pub related_resources: Vec<RelatedNode>,
    /// Geographic location as a GeoJSON geometry (`Point`, `Polygon`, …). Optional;
    /// omitted from the request body when `None`. Serialized on the wire under the
    /// key `geoLocation` as a nested GeoJSON object, e.g.
    /// `{"type":"Point","coordinates":[10.75,59.91]}`. Build one with
    /// [`geojson::Geometry::new_point`] and friends (re-exported as
    /// [`crate::Geometry`]).
    #[serde(rename = "geoLocation", skip_serializing_if = "Option::is_none")]
    pub geolocation: Option<geojson::Geometry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<DateTime<Utc>>,
}
impl Resource {
    pub fn new() -> Self {
        Self {
            id: None,
            external_id: "".to_string(),
            name: "".to_string(),
            metadata: None,
            description: None,
            is_root: false,
            data_set_id: None,
            source: None,
            labels: None,
            related_resources: vec![],
            geolocation: None,
            created_time: None,
            last_updated_time: None,
        }
    }
}
impl GraphNode for Resource {}
impl Identifiable for Resource {
    //todo!()

    fn id(&self) -> u64 {
        self.id.unwrap_or(0)
    }
    fn external_id(&self) -> &str {
        &self.external_id
    }
}
/// One node's update in `POST /resources/update`. Target the resource by `id` or `external_id`,
/// then describe the changes in `update`. Build it fluently, e.g.
/// `ResourceUpdate::by_external_id("pump_a").add_labels(vec!["CRITICAL"]).remove_labels(vec!["DRAFT"])`.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceUpdate {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id"
    )]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    pub update: ResourceUpdateFields,
}

impl GraphNode for ResourceUpdate {}

impl ResourceUpdate {
    /// Target the resource by its external id.
    pub fn by_external_id(external_id: &str) -> Self {
        ResourceUpdate {
            id: None,
            external_id: Some(external_id.to_string()),
            update: ResourceUpdateFields::default(),
        }
    }

    /// Target the resource by its numeric id.
    pub fn by_id(id: u64) -> Self {
        ResourceUpdate {
            id: Some(id),
            external_id: None,
            update: ResourceUpdateFields::default(),
        }
    }

    /// Replace the whole label set (`labels.set`). Mutually exclusive with add/remove: this
    /// discards any pending `add_labels`/`remove_labels` on the same update.
    pub fn set_labels(mut self, labels: Vec<&str>) -> Self {
        self.update.labels = Some(ListField::set(labels.into_iter().map(String::from).collect()));
        self
    }

    /// Add labels (`labels.add`). Combines with a prior/subsequent `remove_labels` into one delta;
    /// a prior `set_labels` is discarded (a delta cannot also replace).
    pub fn add_labels(mut self, labels: Vec<&str>) -> Self {
        let add = labels.into_iter().map(String::from).collect();
        let remove = match self.update.labels.take() {
            Some(ListField::Delta { remove, .. }) => remove,
            _ => None,
        };
        self.update.labels = Some(ListField::Delta {
            add: Some(add),
            remove,
        });
        self
    }

    /// Remove labels (`labels.remove`). Combines with a prior/subsequent `add_labels` into one
    /// delta; a prior `set_labels` is discarded (a delta cannot also replace).
    pub fn remove_labels(mut self, labels: Vec<&str>) -> Self {
        let remove = labels.into_iter().map(String::from).collect();
        let add = match self.update.labels.take() {
            Some(ListField::Delta { add, .. }) => add,
            _ => None,
        };
        self.update.labels = Some(ListField::Delta {
            add,
            remove: Some(remove),
        });
        self
    }

    /// Rename the resource (`name.set`).
    pub fn set_name(mut self, name: &str) -> Self {
        self.update.name = Some(Field::new(Some(name.to_string()), false));
        self
    }
}

/// Field-level changes for a [`ResourceUpdate`]. Every field is optional and only the ones set are
/// sent; the server applies just those. `labels` uses the three-way [`ListField`] (set/add/remove).
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceUpdateFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<Field<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<Field<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Field<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_set_id: Option<Field<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MapField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Field<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub labels: Option<ListField<String>>,
}

/// Request body for [`ResourceService::fetch_related`] (`POST /resources/fetch-related`).
/// Identify the start node by `id` or `external_id`; `depth` bounds the traversal
/// (`-1` = the whole connected component), `relationship_types` filters which edge
/// types to follow (empty = all), and `limit` caps the returned node count.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RelatedResourcesForm {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    pub depth: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_types: Option<Vec<String>>,
    pub limit: i32,
    #[serde(default)]
    pub excluded_labels: Vec<String>,
}

impl RelatedResourcesForm {
    /// Start from the resource with this external id, with the server defaults
    /// (`depth = -1` = whole component, `limit = 5000`).
    pub fn from_external_id(external_id: &str) -> Self {
        Self {
            id: None,
            external_id: Some(external_id.to_string()),
            depth: -1,
            relationship_types: None,
            limit: 5000,
            excluded_labels: vec![],
        }
    }

    /// Start from the resource with this numeric id, with the server defaults.
    pub fn from_id(id: u64) -> Self {
        Self {
            id: Some(id),
            external_id: None,
            depth: -1,
            relationship_types: None,
            limit: 5000,
            excluded_labels: vec![],
        }
    }

    /// Bound the traversal to `depth` hops.
    pub fn with_depth(mut self, depth: i32) -> Self {
        self.depth = depth;
        self
    }

    /// Only follow these relationship types (e.g. `["PART_OF"]`).
    pub fn with_relationship_types(mut self, types: Vec<String>) -> Self {
        self.relationship_types = Some(types);
        self
    }
}

/// A graph label as returned in a [`ResourceNetwork`].
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Label {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

/// The result of a graph traversal ([`ResourceService::fetch_related`]): the connected
/// sub-graph reachable from a starting resource. `nodes` are the resources, `edges` the
/// relationships between them (directional `start` -> `end`, though traversal is
/// undirected), and `labels` the label catalogue for those nodes.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceNetwork {
    #[serde(default)]
    pub nodes: Vec<Resource>,
    #[serde(default)]
    pub edges: Vec<EdgeProxy>,
    #[serde(default)]
    pub labels: Vec<Label>,
}

impl ResourceNetwork {
    pub fn nodes(&self) -> &Vec<Resource> {
        &self.nodes
    }
    pub fn edges(&self) -> &Vec<EdgeProxy> {
        &self.edges
    }
    pub fn labels(&self) -> &Vec<Label> {
        &self.labels
    }
}

impl DataWrapperDeserialization for ResourceNetwork {
    fn deserialize_and_set_status(body: &str, _status_code: u16) -> Result<Self, serde_json::Error> {
        if body.is_empty() {
            return Ok(ResourceNetwork::default());
        }
        serde_json::from_str(body)
    }
}

/// Body of `POST /resources/filter`: the criteria, plus how many to return and in what order.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRetreiver {
    pub filter: ResourceFilter,
    /// Defaults to 1000 server-side and is capped at 10000. A zero or negative value falls back to
    /// the default rather than returning nothing.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<crate::filters::DataSort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl ResourceRetreiver {
    pub fn new(filter: ResourceFilter) -> Self {
        Self {
            filter,
            limit: None,
            sort: None,
            cursor: None,
        }
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_sort(mut self, sort: crate::filters::DataSort) -> Self {
        self.sort = Some(sort);
        self
    }

    pub fn with_cursor(mut self, cursor: &str) -> Self {
        self.cursor = Some(cursor.to_string());
        self
    }
}

/// Criteria for `POST /resources/filter`. Everything set is combined with AND.
///
/// `name` and `source` are case-insensitive **substring** matches and accept `%` as a wildcard;
/// `external_id` and `id` are exact.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ResourceFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_root: Option<bool>,
    /// Ids only — unlike the event filter, this endpoint does not accept a data set's external id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_set_ids: Option<Vec<IdObject>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<crate::filters::TimeFilter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<crate::filters::TimeFilter>,
}

/// An id-only reference, the shape `ResourceFilter::data_set_ids` expects.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct IdObject {
    #[serde(with = "crate::serde_helper::string_id")]
    pub id: u64,
}

impl IdObject {
    pub fn new(id: u64) -> Self {
        Self { id }
    }
}

/// Request body for [`ResourceService::fetch_nearest`] (`POST /resources/fetch-nearest`).
///
/// Note the endpoint reads `id` only: it does not resolve `external_id`, so start from a numeric
/// id (resolve one with [`by_ids`](ResourceService::by_ids) if that is all you have).
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct FetchNearestResourcesForm {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    /// Labels that qualify as a match, e.g. `["TIMESERIES"]`. Traversal continues past them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_labels: Option<Vec<String>>,
    /// How many matching end-nodes to return. Defaults to 10 server-side.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Relationship types the traversal may follow. `None` or empty = all types.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relationship_types: Option<Vec<String>>,
    /// Labels the traversal neither passes through nor returns, e.g. `["POLICY"]`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excluded_labels: Option<Vec<String>>,
}

impl FetchNearestResourcesForm {
    pub fn from_id(id: u64) -> Self {
        Self {
            id: Some(id),
            ..Default::default()
        }
    }

    pub fn with_end_labels(mut self, labels: Vec<String>) -> Self {
        self.end_labels = Some(labels);
        self
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn with_relationship_types(mut self, types: Vec<String>) -> Self {
        self.relationship_types = Some(types);
        self
    }

    pub fn with_excluded_labels(mut self, labels: Vec<String>) -> Self {
        self.excluded_labels = Some(labels);
        self
    }
}
