#[cfg(test)]
mod tests;

mod service;

pub use service::EdgesService;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Server-assigned edge between two resources. Returned from `/resources/create`,
/// `/resources/update`, `/resources/fetch-related`, `/functions/list`, etc. — always
/// the response shape for graph operations.
///
/// `relationship_type` is renamed from `type` because `type` is a Rust keyword.
/// The wire field stays `"type"`.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EdgeProxy {
    #[serde(default)]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(default)]
    pub start: Option<u64>,
    #[serde(default)]
    pub end: Option<u64>,
    #[serde(rename = "type", default)]
    pub relationship_type: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub relationship_type_id: Option<u64>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Request-side edge form, mirroring server-side `RelForm`. Used when creating
/// resources with relations. The server snake-uppercases `relationship_type`
/// (e.g. `"flows_to"` -> `"FLOWS_TO"`) and snake-lowercases external ids before
/// persisting, so callers can pass either case.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RelForm {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_external_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to_external_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub from_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub to_id: Option<u64>,
    pub relationship_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub relationship_type_id: Option<u64>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Direction of a relationship relative to the node it is attached to. A node-centric
/// relation list mixes both directions of the underlying (directed) edges: `OUTBOUND`
/// means this node is the edge `start`, `INBOUND` means this node is the edge `end`.
/// Populated on outbound reads; absent on input payloads.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum RelationDirection {
    Outbound,
    Inbound,
}

/// The unified node-centric relation encoding, mirroring server-side `RelatedNode`.
/// A single node related to some other node: the target's `id`/`external_id` plus the
/// `relationship_type` and [`RelationDirection`]. Replaces the divergent legacy
/// encodings (`Resource.relations` as `EdgeProxy`, `Timeseries.relationsFrom`).
///
/// It serves both directions. On **reads** all fields are populated: `direction` and
/// `edge_id` describe how the nodes relate and which edge realizes the relation (look
/// the edge up by `edge_id` for its metadata). On **input** (e.g. creating a timeseries
/// with relations) only `id`/`external_id` + `relationship_type` are sent; `direction`
/// and `edge_id` are server-assigned and omitted.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RelatedNode {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relationship_type: Option<String>,
    /// Output-only: direction of the relation relative to the node it hangs off.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direction: Option<RelationDirection>,
    /// Output-only: id of the edge realizing this relation; look up the edge by this id
    /// for its detail/metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub edge_id: Option<u64>,
}

impl RelatedNode {
    /// Input helper: relate to the node with this numeric id via `ty`.
    pub fn from_id(id: u64, ty: impl Into<String>) -> Self {
        Self {
            id: Some(id),
            external_id: None,
            relationship_type: Some(ty.into()),
            direction: None,
            edge_id: None,
        }
    }

    /// Input helper: relate to the node with this external id via `ty`.
    pub fn from_external_id(external_id: impl Into<String>, ty: impl Into<String>) -> Self {
        Self {
            id: None,
            external_id: Some(external_id.into()),
            relationship_type: Some(ty.into()),
            direction: None,
            edge_id: None,
        }
    }
}

impl RelForm {
    pub fn by_external_ids(
        from: impl Into<String>,
        to: impl Into<String>,
        ty: impl Into<String>,
    ) -> Self {
        Self {
            id: None,
            from_external_id: Some(from.into()),
            to_external_id: Some(to.into()),
            from_id: None,
            to_id: None,
            relationship_type: ty.into(),
            relationship_type_id: None,
            metadata: HashMap::new(),
            data_set_id: None,
            description: None,
        }
    }

    pub fn by_ids(from: u64, to: u64, ty: impl Into<String>) -> Self {
        Self {
            id: None,
            from_external_id: None,
            to_external_id: None,
            from_id: Some(from),
            to_id: Some(to),
            relationship_type: ty.into(),
            relationship_type_id: None,
            metadata: HashMap::new(),
            data_set_id: None,
            description: None,
        }
    }
}

/// A relationship type in the tenant's catalogue (`GET /edges/types`).
///
/// Types are normally created on demand the first time a name is used; this is what one looks
/// like once stored. `hash` is the server's lookup key for the normalised name.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RelationshipType {
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub i18n_code: Option<String>,
    #[serde(default, with = "crate::serde_helper::opt_string_id_i64")]
    pub hash: Option<i64>,
    #[serde(default)]
    pub date_created: Option<DateTime<Utc>>,
    #[serde(default)]
    pub last_updated: Option<DateTime<Utc>>,
}

/// Request form for `POST /edges/types/create`.
///
/// `name` is required and must not be blank. The server uppercase-snake-cases it, so `"Flows To"`
/// and `"flows_to"` both land on `FLOWS_TO`; a name that normalises to nothing (only symbols) is
/// rejected with a 400.
#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RelTypeForm {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub i18n_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl RelTypeForm {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_i18n_code(mut self, i18n_code: impl Into<String>) -> Self {
        self.i18n_code = Some(i18n_code.into());
        self
    }
}

impl crate::generic::DataHubEntity for RelTypeForm {
    fn ext_id(&self) -> &String {
        &self.name
    }
}

impl crate::generic::DataHubEntity for RelForm {
    fn ext_id(&self) -> &String {
        // Edges are identified by their endpoints, not by an external id of their own. The
        // `From<T> for DataWrapper<T>` impls only need *some* borrow, and the from-side external
        // id is the closest thing an edge form has to a name.
        static EMPTY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        self.from_external_id
            .as_ref()
            .unwrap_or_else(|| EMPTY.get_or_init(String::new))
    }
}
