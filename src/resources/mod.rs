#[cfg(test)]
mod tests;

use crate::fields::{Field, ListField, MapField};
use crate::generic::{
    ApiServiceProvider, DataWrapper, DataWrapperDeserialization, IdAndExtId, Identifiable,
    RelationForm, SearchAndFilterForm,
};
use crate::graph_data_wrapper::{GraphDataWrapper, GraphNode};
use crate::http::{process_response, ResponseError};
use crate::relations::{EdgeProxy, RelForm};
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
    pub async fn update<I>(&self, input: &I) -> Result<DataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<GraphDataWrapper<ResourceUpdate>>,
    {
        todo!();

        let payload = input.into();
        let token = self.get_token().await?;
        let url = &format!("{}/update", self.base_url);

        let response = self
            .get_api_service()
            .http_client
            .post(url)
            .json(&payload)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| ResponseError {
                status: e.status().unwrap(),
                message: e.to_string(),
            })?;
        process_response::<DataWrapper<Resource>>(response, url).await
    }
    //
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
    /// Edges where this resource is the `start` node, populated by the server on
    /// read. Empty on resources you construct locally for a create request.
    pub relations: Option<Vec<EdgeProxy>>,
    pub geolocation: Option<HashMap<String, f64>>, // todo implement GEOJSON, not prio atm
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<DateTime<Utc>>,
    pub relations_form: Option<Vec<RelationForm>>,
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
            relations: None,
            geolocation: None,
            created_time: None,
            last_updated_time: None,
            relations_form: Some(vec![]),
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
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResourceUpdate {
    //todo!()
    pub update: Option<ResourceUpdateFields>,
    pub relation_update: Option<Vec<String>>,
}

impl GraphNode for ResourceUpdate {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResourceUpdateFields {
    //todo!()
    #[serde(rename = "externalId")]
    external_id: Field<String>,
    name: Field<String>,
    description: Field<String>,
    #[serde(rename = "dataSetId")]
    data_set_id: Field<u64>,
    metadata: MapField,
    source: Field<String>,
    labels: ListField<String>,
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
