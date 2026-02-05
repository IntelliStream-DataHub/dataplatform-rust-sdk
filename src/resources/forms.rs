use crate::generic::RelationForm;
use crate::graph_data_wrapper::GraphNode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ResourceForm {
    // we need both a resource form and a resource as the resource endpoint doesnt accept
    // the same input as it creates. the input for labels is a hashmap with {"name": "label"},
    // the output for labels is a vec<string>
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
impl GraphNode for ResourceForm {}
