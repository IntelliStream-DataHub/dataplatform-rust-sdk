use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use crate::generic::{IdAndExtId, RelationForm, SearchForm};
use crate::graph_data_wrapper::GraphNode;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct FilterAndSearchForm{
    pub(crate) filter: Option<FilterForm>,
    pub(crate) search: Option<SearchForm>,
    pub(crate) limit: Option<u64>,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FilterForm{
    pub(crate) name: Option<String>,
    pub(crate) parent_id: Option<u64>,
    // !todo implement external_id as a type so we get automatic validation
    pub(crate) parent_external_id: Option<String>,
    pub(crate) asset_subtree_ids: Option<IdAndExtId>,
    pub(crate) data_set_id: Option<u64>,
    pub(crate) metadata: Option<HashMap<String, String>>,
    pub(crate) source: Option<String>,
    pub(crate) created_time: Option<DateTime<chrono::Utc>>,
    pub(crate) last_updated: Option<DateTime<chrono::Utc>>,
    pub(crate) root: bool,
    pub(crate) external_id_prefix: Option<String>,
    pub(crate) labels: Option<Vec<String>>,
    pub(crate) geo_location: Option<HashMap<String, f64>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResourceForm {
    // used to be a serde skip if zero here. don't understand why
    // todo implement a smooth way to convert "datahub entities" to id-collections
    pub id: Option<u64>,
    pub external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    pub is_root: bool,
    pub data_set_id: Option<u64>,
    pub source: Option<String>,
    pub labels: Option<Vec<HashMap<String, String>>>,
    pub relations: Option<Vec<String>>,
    pub geolocation: Option<HashMap<String, f64>>, // todo implement GEOJSON, not prio atm
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<DateTime<Utc>>,
    pub relations_form: Option<Vec<RelationForm>>,

}
impl GraphNode for ResourceForm{}




/* // todo I think an implementation like this would be better for handling either Id or external_id
// maybe we should restrict it so that Internal ID is private
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum EitherId {
    Id(u64),
    ExternalId(String),
    Both{
        id:u64,
        external_id: String
    }
}impl EitherId {
    pub fn id(v: impl Into<u64>) -> Self {
        Self::Id(v.into())
    }

    pub fn external(v: impl Into<String>) -> Self {
        Self::ExternalId(v.into())
    }
    pub fn both(id: impl Into<u64>, external_id: impl Into<String>) -> Self {
        Self::Both{id: id.into(), external_id: external_id.into()}
    }
}*/