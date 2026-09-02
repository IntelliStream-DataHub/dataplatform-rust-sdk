//! The polymorphic node type returned by the `/resources` endpoints.
//!
//! `/resources` is the *generic* node query: assets, timeseries, functions, resources, data sets
//! and policies share one table and one set of criteria, and a single result page can hold any
//! mixture of them. [`Node`] is that mixture in the type system — one variant per node type, each
//! carrying the concrete struct for its type.
//!
//! # The discriminator is a label, not a field
//!
//! There is no `nodeType` key on the wire. A node's type is the *intrinsic type-label* the api
//! forces into its `labels` array on every read — `ASSET`, `TIMESERIES`, `FUNCTION`, `DATASET`,
//! `POLICY` — and a plain resource carries **none of them**, so absence is the `RESOURCE` signal.
//! [`Node`]'s `Deserialize` follows the api's own `NodeModelDeserializer`: the same label
//! canonicalization ([`to_snake_upper_cased`]), the same fallback to `RESOURCE`, and the same
//! refusal to guess when a node carries more than one type-label.
//!
//! # What a node actually carries depends on where you read it
//!
//! - **Flat reads** (`get_by_id`, `by_ids`, `filter`, `search`) are fully populated *except*
//!   `related_resources`, which is always empty — the api does not join the edges in.
//! - **Graph reads** ([`ResourceService::fetch_related`](crate::resources::ResourceService::fetch_related),
//!   `fetch_nearest`) are **typed but sparse**: Neo4j stores only a subset of the columns, so a
//!   [`TimeSeries`] from there carries **none** of its type-specific fields — no `unit`,
//!   `unit_external_id`, `value_type`, `table_engine` or `security_categories`. They are absent
//!   from the payload, not defaulted, which is why every one of them is `Option`. `metadata` is
//!   silently empty rather than absent, and `related_resources` *is* populated there.
//! - **Policies** never carry `value`, `template_id` or `data_set_id` on any read — the api's
//!   transformer does not set them.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::datasets::Dataset;
use crate::functions::Function;
use crate::generic::Identifiable;
use crate::graph_data_wrapper::GraphNode;
use crate::relations::RelatedNode;
use crate::resources::Resource;
use crate::timeseries::TimeSeries;

/// The six node types `/resources` spans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeType {
    Asset,
    TimeSeries,
    Function,
    Resource,
    Dataset,
    Policy,
}

impl NodeType {
    /// The intrinsic type-label a node of this type carries.
    ///
    /// `None` for [`NodeType::Resource`], and that is the whole design: a plain resource carries
    /// no type-label at all, so an empty match is what identifies one. Mirrors the api's
    /// `TypeLabels.forEntity` returning an empty `Optional`.
    pub const fn type_label(self) -> Option<&'static str> {
        match self {
            NodeType::Asset => Some("ASSET"),
            NodeType::TimeSeries => Some("TIMESERIES"),
            NodeType::Function => Some("FUNCTION"),
            NodeType::Dataset => Some("DATASET"),
            NodeType::Policy => Some("POLICY"),
            NodeType::Resource => None,
        }
    }

    /// The name this type answers to in [`ResourceFilter::node_type`](crate::resources::ResourceFilter).
    pub const fn filter_name(self) -> &'static str {
        match self {
            NodeType::Asset => "asset",
            NodeType::TimeSeries => "timeseries",
            NodeType::Function => "function",
            NodeType::Resource => "resource",
            NodeType::Dataset => "dataset",
            NodeType::Policy => "policy",
        }
    }

    /// The type this label names, or `None` when it is an ordinary domain label.
    /// The label is canonicalized first, so `"dataset"`, `"DataSet"` and `"data-set"` all resolve.
    pub fn from_type_label(label: &str) -> Option<Self> {
        match to_snake_upper_cased(label).as_str() {
            "ASSET" => Some(NodeType::Asset),
            "TIMESERIES" => Some(NodeType::TimeSeries),
            "FUNCTION" => Some(NodeType::Function),
            "DATASET" => Some(NodeType::Dataset),
            "POLICY" => Some(NodeType::Policy),
            _ => None,
        }
    }

    /// Which type a label set names. Empty ⇒ [`NodeType::Resource`]; more than one distinct
    /// type-label is an error, as it is on the api.
    pub fn from_labels<S: AsRef<str>>(labels: &[S]) -> Result<Self, AmbiguousNodeType> {
        let mut found: Vec<NodeType> = Vec::new();
        for label in labels {
            if let Some(t) = NodeType::from_type_label(label.as_ref()) {
                if !found.contains(&t) {
                    found.push(t);
                }
            }
        }
        match found.len() {
            0 => Ok(NodeType::Resource),
            1 => Ok(found[0]),
            _ => Err(AmbiguousNodeType(found)),
        }
    }
}

/// A node carrying more than one type-label, which names no single type.
///
/// The api rejects this on every `NodeModel` request body and at create time, so it cannot arise
/// from a healthy tenant — but note that the api's *graph* mapper silently takes the first
/// type-label instead, so the two server paths disagree about a row that has somehow acquired two.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AmbiguousNodeType(pub Vec<NodeType>);

impl std::fmt::Display for AmbiguousNodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self
            .0
            .iter()
            .filter_map(|t| t.type_label())
            .collect();
        write!(
            f,
            "a node may have at most one type-label; got {}",
            names.join(", ")
        )
    }
}

impl std::error::Error for AmbiguousNodeType {}

/// Port of the api's `TextValidator.toSnakeUpperCased`, which is what canonicalizes every label
/// name server-side before it is stored or compared.
///
/// Leading digits are dropped, the rest is upper-cased, and each whitespace character — or each
/// run of other non-word characters — becomes a single `_`. Blank input passes through unchanged.
pub fn to_snake_upper_cased(s: &str) -> String {
    if s.trim().is_empty() {
        return s.to_string();
    }
    let without_leading_digits: String = {
        let mut chars = s.chars().peekable();
        while chars.peek().is_some_and(|c| c.is_numeric()) {
            chars.next();
        }
        chars.collect()
    };

    let is_word = |c: char| c.is_alphanumeric() || c == '_';
    let upper = without_leading_digits.to_uppercase();
    let mut out = String::with_capacity(upper.len());
    let mut chars = upper.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            // The api's pattern tries a single whitespace before a run of non-word characters,
            // so "a  b" keeps both separators where "a--b" collapses to one.
            out.push('_');
        } else if !is_word(c) {
            out.push('_');
            while chars.peek().is_some_and(|n| !is_word(*n)) {
                chars.next();
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// An asset — a resource that can carry a geographic location.
///
/// Field-identical to [`Resource`] plus a meaningful `geolocation`: the api models both on the
/// same node base and distinguishes them only by the `ASSET` type-label. `geolocation` is the one
/// practical difference, since a plain resource accepts it on write but never echoes it back.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Asset {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    pub external_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub is_root: bool,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id"
    )]
    pub data_set_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    /// Populated on graph reads and the create echo; always empty on the flat reads.
    #[serde(default)]
    pub related_resources: Vec<RelatedNode>,
    /// GeoJSON geometry, as a nested object under `geoLocation`.
    ///
    /// A node reached through the graph has this reconstructed from Neo4j's native WGS-84 point,
    /// which is lossy for anything that is not a `Point` — read the asset flatly if the geometry
    /// matters.
    #[serde(
        rename = "geoLocation",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub geolocation: Option<geojson::Geometry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<DateTime<Utc>>,
}

impl Asset {
    pub fn new(external_id: &str, name: &str) -> Self {
        Self {
            id: None,
            external_id: external_id.to_string(),
            name: name.to_string(),
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

impl GraphNode for Asset {}

/// An access policy, as a node.
///
/// Reads are sparse by construction: the api's policy transformer never sets `value`,
/// `template_id` or `data_set_id`, so those are `None` on anything that came back from the server
/// regardless of what is stored.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    pub external_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The policy kind, e.g. `IS_WRITE_PROTECTED`. Named `type` on the wire.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub policy_type: Option<String>,
    /// The policy's value. The api declares it `Object`, so it is any JSON scalar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<serde_json::Value>,
    /// Wire key is `deactivated`, not `isDeactivated` — the api's getter naming decides this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deactivated: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id"
    )]
    pub template_id: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id"
    )]
    pub data_set_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    #[serde(default)]
    pub related_resources: Vec<RelatedNode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<DateTime<Utc>>,
}

impl Policy {
    pub fn new(external_id: &str, name: &str) -> Self {
        Self {
            id: None,
            external_id: external_id.to_string(),
            name: name.to_string(),
            description: None,
            policy_type: None,
            value: None,
            deactivated: None,
            template_id: None,
            data_set_id: None,
            source: None,
            metadata: None,
            labels: None,
            related_resources: vec![],
            created_time: None,
            last_updated_time: None,
        }
    }
}

impl GraphNode for Policy {}

/// One node of any type, as `/resources` returns them.
///
/// `#[non_exhaustive]`: a seventh node type on the api is an additive change, so `match` on this
/// needs a `_` arm. Where you only want one field, prefer the accessors ([`Node::external_id`],
/// [`Node::labels`], …) over matching.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum Node {
    Asset(Asset),
    TimeSeries(TimeSeries),
    Function(Function),
    Resource(Resource),
    Dataset(Dataset),
    Policy(Policy),
}

macro_rules! on_node {
    ($self:expr, $inner:ident => $body:expr) => {
        match $self {
            Node::Asset($inner) => $body,
            Node::TimeSeries($inner) => $body,
            Node::Function($inner) => $body,
            Node::Resource($inner) => $body,
            Node::Dataset($inner) => $body,
            Node::Policy($inner) => $body,
        }
    };
}

impl Node {
    /// Which of the six types this is.
    pub fn kind(&self) -> NodeType {
        match self {
            Node::Asset(_) => NodeType::Asset,
            Node::TimeSeries(_) => NodeType::TimeSeries,
            Node::Function(_) => NodeType::Function,
            Node::Resource(_) => NodeType::Resource,
            Node::Dataset(_) => NodeType::Dataset,
            Node::Policy(_) => NodeType::Policy,
        }
    }

    /// The server-assigned numeric id, absent on a node you built locally.
    pub fn id(&self) -> Option<u64> {
        on_node!(self, n => n.id)
    }

    pub fn external_id(&self) -> &str {
        on_node!(self, n => &n.external_id)
    }

    /// The node's name. `None` only on a [`Function`], the one type whose name is optional.
    pub fn name(&self) -> Option<&str> {
        match self {
            Node::Asset(n) => Some(&n.name),
            Node::TimeSeries(n) => Some(&n.name),
            Node::Function(n) => n.name.as_deref(),
            Node::Resource(n) => Some(&n.name),
            Node::Dataset(n) => Some(&n.name),
            Node::Policy(n) => Some(&n.name),
        }
    }

    /// Every label on the node, including the intrinsic type-label.
    pub fn labels(&self) -> &[String] {
        match self {
            Node::Asset(n) => n.labels.as_deref().unwrap_or(&[]),
            Node::TimeSeries(n) => n.labels.as_deref().unwrap_or(&[]),
            Node::Function(n) => &n.labels,
            Node::Resource(n) => n.labels.as_deref().unwrap_or(&[]),
            Node::Dataset(n) => n.labels.as_deref().unwrap_or(&[]),
            Node::Policy(n) => n.labels.as_deref().unwrap_or(&[]),
        }
    }

    pub fn description(&self) -> Option<&str> {
        on_node!(self, n => n.description.as_deref())
    }

    /// The name of the system this node's primary information comes from.
    pub fn source(&self) -> Option<&str> {
        on_node!(self, n => n.source.as_deref())
    }

    /// The node's metadata map. Empty rather than absent on a node read through the graph — that
    /// path does not carry the column at all.
    pub fn metadata(&self) -> Option<&HashMap<String, String>> {
        match self {
            Node::Asset(n) => n.metadata.as_ref(),
            Node::TimeSeries(n) => n.metadata.as_ref(),
            Node::Function(n) => Some(&n.metadata),
            Node::Resource(n) => n.metadata.as_ref(),
            Node::Dataset(n) => Some(&n.metadata),
            Node::Policy(n) => n.metadata.as_ref(),
        }
    }

    /// Whether this node is a root of the graph. `None` on the types that have no such column —
    /// only assets and plain resources carry it.
    pub fn is_root(&self) -> Option<bool> {
        match self {
            Node::Asset(n) => Some(n.is_root),
            Node::Resource(n) => Some(n.is_root),
            _ => None,
        }
    }

    pub fn data_set_id(&self) -> Option<u64> {
        on_node!(self, n => n.data_set_id)
    }

    /// The nodes this one is connected to. **Empty on the flat reads** — only graph reads and the
    /// `/resources/create` echo populate it.
    pub fn related_resources(&self) -> &[RelatedNode] {
        on_node!(self, n => &n.related_resources)
    }

    pub fn created_time(&self) -> Option<DateTime<Utc>> {
        on_node!(self, n => n.created_time)
    }

    pub fn last_updated_time(&self) -> Option<DateTime<Utc>> {
        on_node!(self, n => n.last_updated_time)
    }

    pub fn as_asset(&self) -> Option<&Asset> {
        match self {
            Node::Asset(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_time_series(&self) -> Option<&TimeSeries> {
        match self {
            Node::TimeSeries(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_function(&self) -> Option<&Function> {
        match self {
            Node::Function(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_resource(&self) -> Option<&Resource> {
        match self {
            Node::Resource(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_dataset(&self) -> Option<&Dataset> {
        match self {
            Node::Dataset(n) => Some(n),
            _ => None,
        }
    }

    pub fn as_policy(&self) -> Option<&Policy> {
        match self {
            Node::Policy(n) => Some(n),
            _ => None,
        }
    }

    pub fn into_asset(self) -> Option<Asset> {
        match self {
            Node::Asset(n) => Some(n),
            _ => None,
        }
    }

    pub fn into_time_series(self) -> Option<TimeSeries> {
        match self {
            Node::TimeSeries(n) => Some(n),
            _ => None,
        }
    }

    pub fn into_function(self) -> Option<Function> {
        match self {
            Node::Function(n) => Some(n),
            _ => None,
        }
    }

    pub fn into_resource(self) -> Option<Resource> {
        match self {
            Node::Resource(n) => Some(n),
            _ => None,
        }
    }

    pub fn into_dataset(self) -> Option<Dataset> {
        match self {
            Node::Dataset(n) => Some(n),
            _ => None,
        }
    }

    pub fn into_policy(self) -> Option<Policy> {
        match self {
            Node::Policy(n) => Some(n),
            _ => None,
        }
    }
}

impl From<Asset> for Node {
    fn from(v: Asset) -> Self {
        Node::Asset(v)
    }
}
impl From<TimeSeries> for Node {
    fn from(v: TimeSeries) -> Self {
        Node::TimeSeries(v)
    }
}
impl From<Function> for Node {
    fn from(v: Function) -> Self {
        Node::Function(v)
    }
}
impl From<Resource> for Node {
    fn from(v: Resource) -> Self {
        Node::Resource(v)
    }
}
impl From<Dataset> for Node {
    fn from(v: Dataset) -> Self {
        Node::Dataset(v)
    }
}
impl From<Policy> for Node {
    fn from(v: Policy) -> Self {
        Node::Policy(v)
    }
}

fn ensure_label_opt(labels: &mut Option<Vec<String>>, label: &str) {
    let entries = labels.get_or_insert_with(Vec::new);
    if !entries.iter().any(|l| l.eq_ignore_ascii_case(label)) {
        entries.push(label.to_string());
    }
}

fn ensure_label(labels: &mut Vec<String>, label: &str) {
    if !labels.iter().any(|l| l.eq_ignore_ascii_case(label)) {
        labels.push(label.to_string());
    }
}

/// Serializes as the variant's own shape, with the type-label added to `labels` when the caller
/// has not already put it there.
///
/// `/resources/create` dispatches each element of `nodes` by its own labels, so this is what makes
/// `create(vec![Dataset::new(…)])` build a data set. Two deliberate non-behaviours:
///
/// - a conflicting type-label is **not** stripped. A body labelled both `ASSET` and `POLICY` earns
///   the api's 400 naming both, which is the accurate answer to an ambiguous intent; quietly
///   picking one for the caller is not.
/// - [`Node::Resource`] is passed through verbatim, so the long-standing idiom of creating a typed
///   node by putting its label on a bare [`Resource`] keeps working unchanged.
impl Serialize for Node {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Node::Resource(r) => r.serialize(serializer),
            Node::Asset(a) => {
                let mut a = a.clone();
                ensure_label_opt(&mut a.labels, "ASSET");
                a.serialize(serializer)
            }
            Node::TimeSeries(t) => {
                let mut t = t.clone();
                ensure_label_opt(&mut t.labels, "TIMESERIES");
                t.serialize(serializer)
            }
            Node::Function(f) => {
                let mut f = f.clone();
                ensure_label(&mut f.labels, "FUNCTION");
                f.serialize(serializer)
            }
            Node::Dataset(d) => {
                let mut d = d.clone();
                ensure_label_opt(&mut d.labels, "DATASET");
                d.serialize(serializer)
            }
            Node::Policy(p) => {
                let mut p = p.clone();
                ensure_label_opt(&mut p.labels, "POLICY");
                p.serialize(serializer)
            }
        }
    }
}

/// Dispatches on the intrinsic type-label in `labels`, mirroring the api's `NodeModelDeserializer`.
///
/// The buffer-then-dispatch shape is deliberate: serde has no mode for a tag that lives *inside an
/// array field*, so the tag has to be read before the body can be bound. `serde_json::Value` is
/// the right buffer here rather than serde's private `Content` (an unstable internal API) because
/// this SDK only ever speaks JSON — the whole deserialization path starts from a `&str`.
impl<'de> Deserialize<'de> for Node {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let value = serde_json::Value::deserialize(deserializer)?;
        let labels: Vec<&str> = value
            .get("labels")
            .and_then(|l| l.as_array())
            .map(|entries| entries.iter().filter_map(|e| e.as_str()).collect())
            .unwrap_or_default();

        let kind = NodeType::from_labels(&labels).map_err(D::Error::custom)?;

        macro_rules! bind {
            ($variant:path) => {
                serde_json::from_value(value)
                    .map($variant)
                    .map_err(D::Error::custom)
            };
        }

        match kind {
            NodeType::Asset => bind!(Node::Asset),
            NodeType::TimeSeries => bind!(Node::TimeSeries),
            NodeType::Function => bind!(Node::Function),
            NodeType::Resource => bind!(Node::Resource),
            NodeType::Dataset => bind!(Node::Dataset),
            NodeType::Policy => bind!(Node::Policy),
        }
    }
}

impl GraphNode for Node {}

impl Identifiable for Node {
    fn id(&self) -> u64 {
        // The inherent `Node::id` returns `Option<u64>` and shadows this one at call sites; name
        // it explicitly so this is not an infinite recursion.
        Node::id(self).unwrap_or(0)
    }

    fn external_id(&self) -> &str {
        Node::external_id(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The full response shape for a node of each type, transcribed from the api's wire-contract
    /// tests (`AssetWireContractTest`, `ResourceWireContractTest`, `TimeseriesWireContractTest`,
    /// `DataSetModelWireContractTest`, `PolicyWireContractTest`). Ids arrive as JSON *strings*;
    /// `securityCategories` as raw numbers.
    fn asset_json() -> serde_json::Value {
        json!({
            "id": "34", "externalId": "pump_a", "name": "Pump A", "isRoot": true,
            "geoLocation": { "type": "Point", "coordinates": [10.75, 59.91] },
            "relatedResources": [], "metadata": { "k": "v" }, "description": "d",
            "dataSetId": "12", "source": "sap", "labels": ["ASSET", "PLANT"],
            "createdTime": "2024-06-17T12:34:56Z", "lastUpdatedTime": "2024-06-17T12:34:56Z"
        })
    }

    fn timeseries_json() -> serde_json::Value {
        json!({
            "id": "7", "externalId": "Engine.Temp", "name": "Engine temp",
            "metadata": {}, "unit": "deg C", "unitExternalId": "deg_c",
            "relatedResources": [], "description": null, "securityCategories": [1, 2],
            "dataSetId": "21", "source": null, "labels": ["TIMESERIES"],
            "tableEngine": "MERGETREE", "valueType": "float",
            "createdTime": "2024-06-17T12:34:56Z", "lastUpdatedTime": "2024-06-17T12:34:56Z"
        })
    }

    fn dataset_json() -> serde_json::Value {
        json!({
            "id": "3", "externalId": "raw_data", "name": "Raw data", "description": null,
            "policies": ["policy_a"], "metadata": {}, "connectedDataSets": ["5"],
            "labels": ["DATASET"], "relatedResources": [], "source": null,
            "createdTime": "2024-06-17T12:34:56Z", "lastUpdatedTime": "2024-06-17T12:34:56Z"
        })
    }

    fn policy_json() -> serde_json::Value {
        json!({
            "id": "9", "name": "Write protect", "description": null,
            "type": "IS_WRITE_PROTECTED", "value": "TRUE", "deactivated": false,
            "externalId": "write_protect", "metadata": null, "templateId": "3",
            "dataSetId": null, "source": null, "relatedResources": [], "labels": ["POLICY"],
            "createdTime": "2024-06-17T12:34:56Z", "lastUpdatedTime": "2024-06-17T12:34:56Z"
        })
    }

    fn function_json() -> serde_json::Value {
        json!({
            "id": "11", "externalId": "nifi_ingest", "name": "Ingest",
            "labels": ["FUNCTION"], "metadata": {}, "description": null, "source": null,
            "relatedResources": [],
            "createdTime": "2024-06-17T12:34:56Z", "lastUpdatedTime": "2024-06-17T12:34:56Z"
        })
    }

    fn resource_json() -> serde_json::Value {
        json!({
            "id": "1", "externalId": "plain", "name": "Plain", "isRoot": false,
            "relatedResources": [], "metadata": {}, "description": "d", "dataSetId": "12",
            "source": "sap", "labels": ["PIPE"],
            "createdTime": "2024-06-17T12:34:56Z", "lastUpdatedTime": "2024-06-17T12:34:56Z"
        })
    }

    #[test]
    fn dispatches_on_the_intrinsic_type_label() {
        let nodes: Vec<Node> = serde_json::from_value(json!([
            asset_json(), timeseries_json(), function_json(),
            resource_json(), dataset_json(), policy_json()
        ]))
        .unwrap();

        let kinds: Vec<NodeType> = nodes.iter().map(Node::kind).collect();
        assert_eq!(
            kinds,
            vec![
                NodeType::Asset,
                NodeType::TimeSeries,
                NodeType::Function,
                NodeType::Resource,
                NodeType::Dataset,
                NodeType::Policy
            ]
        );
    }

    #[test]
    fn a_node_with_no_type_label_is_a_resource() {
        // `PIPE` is an ordinary domain label, so it names no type.
        let node: Node = serde_json::from_value(resource_json()).unwrap();
        assert_eq!(node.kind(), NodeType::Resource);
        assert_eq!(node.labels(), ["PIPE"]);
    }

    #[test]
    fn absent_labels_is_a_resource() {
        let node: Node =
            serde_json::from_value(json!({ "externalId": "x", "name": "X", "isRoot": false }))
                .unwrap();
        assert_eq!(node.kind(), NodeType::Resource);
        assert!(node.labels().is_empty());
    }

    #[test]
    fn the_type_label_is_matched_case_insensitively() {
        let mut v = dataset_json();
        v["labels"] = json!(["dataset"]);
        let node: Node = serde_json::from_value(v).unwrap();
        assert_eq!(node.kind(), NodeType::Dataset);
    }

    #[test]
    fn two_type_labels_are_refused_rather_than_guessed() {
        let mut v = asset_json();
        v["labels"] = json!(["ASSET", "POLICY"]);
        let err = serde_json::from_value::<Node>(v).unwrap_err().to_string();
        assert!(
            err.contains("at most one type-label"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn an_asset_carries_its_geometry_and_root_flag() {
        let asset = serde_json::from_value::<Node>(asset_json())
            .unwrap()
            .into_asset()
            .expect("asset");
        assert_eq!(asset.id, Some(34));
        assert_eq!(asset.data_set_id, Some(12));
        assert!(asset.is_root);
        assert!(asset.geolocation.is_some());
    }

    #[test]
    fn a_timeseries_read_through_resources_carries_its_own_fields() {
        let ts = serde_json::from_value::<Node>(timeseries_json())
            .unwrap()
            .into_time_series()
            .expect("timeseries");
        assert_eq!(ts.unit.as_deref(), Some("deg C"));
        assert_eq!(ts.unit_external_id.as_deref(), Some("deg_c"));
        assert_eq!(ts.value_type.as_deref(), Some("float"));
        assert_eq!(ts.table_engine.as_deref(), Some("MERGETREE"));
        // Raw numbers on the wire, unlike every id in the family.
        assert_eq!(ts.security_categories, Some(vec![1, 2]));
        assert_eq!(ts.data_set_id, Some(21));
        assert_eq!(ts.labels.as_deref(), Some(&["TIMESERIES".to_string()][..]));
    }

    #[test]
    fn connected_data_sets_arrive_as_strings() {
        let ds = serde_json::from_value::<Node>(dataset_json())
            .unwrap()
            .into_dataset()
            .expect("dataset");
        assert_eq!(ds.connected_data_sets, vec![5]);
        assert_eq!(ds.policies, Some(vec!["policy_a".to_string()]));
    }

    #[test]
    fn a_policy_binds_the_deactivated_key_not_is_deactivated() {
        let p = serde_json::from_value::<Node>(policy_json())
            .unwrap()
            .into_policy()
            .expect("policy");
        assert_eq!(p.deactivated, Some(false));
        assert_eq!(p.policy_type.as_deref(), Some("IS_WRITE_PROTECTED"));
        assert_eq!(p.value, Some(json!("TRUE")));
        assert_eq!(p.template_id, Some(3));
        assert_eq!(p.data_set_id, None);
    }

    #[test]
    fn a_graph_sourced_timeseries_has_none_of_its_type_specific_fields() {
        // What `/resources/fetch-related` actually sends for a TIMESERIES: the shared node
        // fields and nothing else. Neo4j does not store the rest, and the api omits them rather
        // than emitting defaults — so every one of these must be optional, `valueType` included.
        // A required `valueType` made any graph traversal over a timeseries a hard error.
        let node: Node = serde_json::from_value(json!({
            "id": "793", "externalId": "well_qgl", "name": "Well QGL",
            "createdTime": "2024-06-17T12:34:56Z", "lastUpdatedTime": "2024-06-17T12:34:56Z",
            "dataSetId": "731", "labels": ["TIMESERIES"], "metadata": {}, "relatedResources": []
        }))
        .unwrap();
        let ts = node.into_time_series().expect("timeseries");
        assert_eq!(ts.value_type, None, "not told, rather than a wrong default");
        assert_eq!(ts.unit, None);
        assert_eq!(ts.table_engine, None);
        assert_eq!(ts.security_categories, None);
    }

    #[test]
    fn an_unknown_key_is_tolerated_on_a_read() {
        // `/resources/update` echoes every node as a flat resource, so `isRoot` turns up on types
        // that have no such field. A read must not fail on it.
        let mut v = timeseries_json();
        v["isRoot"] = json!(false);
        let node: Node = serde_json::from_value(v).unwrap();
        assert_eq!(node.kind(), NodeType::TimeSeries);
    }

    #[test]
    fn serializing_injects_the_type_label() {
        for (node, expected) in [
            (Node::Asset(Asset::new("a", "A")), "ASSET"),
            (Node::TimeSeries(TimeSeries::new("t", "T")), "TIMESERIES"),
            (Node::Function(Function::new("f".into())), "FUNCTION"),
            (Node::Dataset(Dataset::new("D".into())), "DATASET"),
            (Node::Policy(Policy::new("p", "P")), "POLICY"),
        ] {
            let v = serde_json::to_value(&node).unwrap();
            let labels = v["labels"].as_array().expect("labels");
            assert!(
                labels.iter().any(|l| l == expected),
                "{expected} missing from {v}"
            );
        }
    }

    #[test]
    fn serializing_a_resource_leaves_its_labels_alone() {
        // Creating a typed node by putting its label on a bare `Resource` is a long-standing
        // idiom; a plain resource has no type-label of its own to add.
        let mut r = Resource::new();
        r.external_id = "a".into();
        r.labels = Some(vec!["ASSET".into()]);
        let v = serde_json::to_value(Node::Resource(r)).unwrap();
        assert_eq!(v["labels"], json!(["ASSET"]));
    }

    #[test]
    fn serializing_never_emits_another_types_fields() {
        // The api's request-body mapper rejects a body naming a field the target type does not
        // have, so a node must serialize as its own shape and nothing wider.
        let foreign = [
            "unit",
            "unitExternalId",
            "securityCategories",
            "tableEngine",
            "valueType",
            "policies",
            "connectedDataSets",
            "type",
            "value",
            "deactivated",
            "templateId",
            "geoLocation",
            "isRoot",
        ];
        let own: HashMap<&str, &[&str]> = HashMap::from([
            ("ASSET", &["geoLocation", "isRoot"][..]),
            (
                "TIMESERIES",
                &["unit", "unitExternalId", "securityCategories", "tableEngine", "valueType"][..],
            ),
            ("FUNCTION", &[][..]),
            ("DATASET", &["policies", "connectedDataSets"][..]),
            ("POLICY", &["type", "value", "deactivated", "templateId"][..]),
        ]);

        for node in [
            Node::Asset(Asset::new("a", "A")),
            Node::TimeSeries(TimeSeries::new("t", "T")),
            Node::Function(Function::new("f".into())),
            Node::Dataset(Dataset::new("D".into())),
            Node::Policy(Policy::new("p", "P")),
        ] {
            let label = node.kind().type_label().unwrap();
            let v = serde_json::to_value(&node).unwrap();
            let obj = v.as_object().unwrap();
            for key in foreign {
                if own[label].contains(&key) {
                    continue;
                }
                assert!(
                    !obj.contains_key(key),
                    "a {label} body must not name `{key}`: {v}"
                );
            }
        }
    }

    #[test]
    fn every_typed_variant_round_trips_through_its_own_serialization() {
        for node in [
            Node::Asset(Asset::new("a", "A")),
            Node::TimeSeries(TimeSeries::new("t", "T")),
            Node::Function(Function::new("f".into())),
            Node::Dataset(Dataset::new("D".into())),
            Node::Policy(Policy::new("p", "P")),
        ] {
            let kind = node.kind();
            let round_tripped: Node =
                serde_json::from_value(serde_json::to_value(&node).unwrap()).unwrap();
            assert_eq!(round_tripped.kind(), kind);
        }
    }

    #[test]
    fn a_data_wrapper_carries_a_mixed_page() {
        use crate::generic::{DataWrapper, DataWrapperDeserialization};

        let body = json!({
            "items": [asset_json(), timeseries_json(), dataset_json()],
            "nextCursor": "abc"
        })
        .to_string();
        let wrapper = DataWrapper::<Node>::deserialize_and_set_status(&body, 200).unwrap();
        assert_eq!(wrapper.length(), 3);
        assert_eq!(wrapper.next_cursor(), Some("abc"));
        assert_eq!(wrapper.get_items()[1].kind(), NodeType::TimeSeries);
    }

    #[test]
    fn a_graph_wrapper_carries_a_mixed_node_list_under_either_key() {
        use crate::generic::DataWrapperDeserialization;
        use crate::graph_data_wrapper::GraphDataWrapper;

        for key in ["nodes", "items"] {
            let body = json!({ key: [policy_json(), function_json()] }).to_string();
            let wrapper =
                GraphDataWrapper::<Node>::deserialize_and_set_status(&body, 200).unwrap();
            let nodes = wrapper.nodes.expect("nodes");
            assert_eq!(nodes[0].kind(), NodeType::Policy);
            assert_eq!(nodes[1].kind(), NodeType::Function);
        }
    }

    #[test]
    fn accessors_read_the_shared_node_fields_off_any_variant() {
        let node: Node = serde_json::from_value(timeseries_json()).unwrap();
        assert_eq!(node.id(), Some(7));
        assert_eq!(node.external_id(), "Engine.Temp");
        assert_eq!(node.name(), Some("Engine temp"));
        assert_eq!(node.data_set_id(), Some(21));
        assert!(node.created_time().is_some());
        assert!(node.related_resources().is_empty());
    }

    #[test]
    fn the_identifiable_impl_does_not_shadow_itself_into_recursion() {
        // `Identifiable::id` and the inherent `Node::id` share a name and differ in return type;
        // the impl has to name the inherent one explicitly or it calls itself forever.
        use crate::generic::Identifiable;
        let node: Node = serde_json::from_value(asset_json()).unwrap();
        assert_eq!(Identifiable::id(&node), 34);
        assert_eq!(Identifiable::external_id(&node), "pump_a");

        let local = Node::Asset(Asset::new("x", "X"));
        assert_eq!(Identifiable::id(&local), 0, "an unsaved node has no id");
    }

    #[test]
    fn labels_canonicalize_the_way_the_api_does() {
        assert_eq!(to_snake_upper_cased("dataset"), "DATASET");
        assert_eq!(to_snake_upper_cased("data-set"), "DATA_SET");
        assert_eq!(to_snake_upper_cased("data set"), "DATA_SET");
        assert_eq!(to_snake_upper_cased("12timeseries"), "TIMESERIES");
        assert_eq!(to_snake_upper_cased("  "), "  ");
        // "data set" is not a type-label, and must not be mistaken for one.
        assert_eq!(NodeType::from_type_label("data set"), None);
        assert_eq!(NodeType::from_type_label("policy"), Some(NodeType::Policy));
    }
}
