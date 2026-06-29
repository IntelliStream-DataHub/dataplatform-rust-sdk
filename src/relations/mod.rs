#[cfg(test)]
mod tests;

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
