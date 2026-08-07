//! Policies (`/policies`) — the rules a dataset can be governed by.
//!
//! A policy is a node like any other entity here, but it is created from a *template* rather than
//! from scratch: [`types`](PoliciesService::types) lists the available [`PolicyType`]s, and
//! [`create`](PoliciesService::create) instantiates one. Attach it to a dataset with the policy's
//! `data_set_id`, or leave that unset for a tenant-wide policy.
//!
//! [`check_naming`](PoliciesService::check_naming) is the odd one out: a read-only preflight that
//! reports what the naming policy *would* decide for a batch of candidate external ids, without
//! writing anything. Violations that were allowed through and recorded are a different thing —
//! those are events, read with `POST /events/filter` on `type = "policy_finding"`.

#[cfg(test)]
mod tests;

use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Weak;

pub struct PoliciesService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl ApiServiceProvider for PoliciesService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl PoliciesService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        PoliciesService {
            api_service,
            base_url: format!("{}/policies", base_url),
        }
    }

    /// `GET /policies` — every policy node in the tenant.
    pub async fn list(&self) -> Result<DataWrapper<Policy>, ResponseError> {
        self.execute_get_request(self.base_url.as_str(), None::<&str>)
            .await
    }

    /// `GET /policies/types` — the policy templates available to instantiate.
    ///
    /// These are synthesised from the server's `PolicyType` enum, not stored rows: each carries a
    /// `name`, a `r#type` and a description, and everything else (`id`, `external_id`,
    /// `template_id`) is `None`. Use one to fill in a [`Policy::from_type`].
    pub async fn types(&self) -> Result<DataWrapper<Policy>, ResponseError> {
        let path = &format!("{}/types", self.base_url);
        self.execute_get_request(path, None::<&str>).await
    }

    /// `GET /policies/{id}` — one policy by numeric id.
    ///
    /// An unknown id is **not** a 404: the server answers 200 with an empty `items` list, so check
    /// the item count rather than the status.
    pub async fn get(&self, id: u64) -> Result<DataWrapper<Policy>, ResponseError> {
        let path = &format!("{}/{}", self.base_url, id);
        self.execute_get_request(path, None::<&str>).await
    }

    /// `POST /policies/create` — instantiate one or more policies.
    ///
    /// Every entry is scope-validated across the whole batch before anything is written: attaching
    /// a policy where its type does not apply is a 400, as is a `NAMING_CONVENTION` policy whose
    /// regex will not compile. A policy created with `deactivated` already set stays off, so an
    /// export can be restored switched off rather than going live the moment it lands.
    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<Policy>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Policy>>,
    {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request(path, &data.into()).await
    }

    /// `POST /policies/update` — change fields on an existing policy.
    ///
    /// Unlike the event and dataset update endpoints, this takes a whole [`Policy`] rather than a
    /// set-of-changes block: send the policy with the fields you want as they should end up.
    /// **The server reads only the first item**, whatever the wrapper contains, so update one
    /// policy per call.
    ///
    /// A concurrent modification answers 409 — re-fetch and retry rather than replaying blindly.
    pub async fn update<I>(&self, data: &I) -> Result<DataWrapper<Policy>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Policy>>,
    {
        let path = &format!("{}/update", self.base_url);
        self.execute_post_request(path, &data.into()).await
    }

    /// `POST /policies/delete` — delete policy nodes by id or external id.
    ///
    /// Answers 204 with no body on success, so the returned wrapper carries the status and no
    /// items. A concurrent modification answers 409.
    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Policy>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }

    /// `POST /policies/naming/check` — preflight candidate external ids against the naming policy.
    ///
    /// Nothing is written. The same evaluator runs here as on the write path, so the answer cannot
    /// disagree with what a real write would do — which makes this the way to answer "what would
    /// enabling this policy do to the ids I already have" before turning it on.
    ///
    /// Only non-conforming ids come back: an empty [`NamingCheck::findings`] means every id is
    /// fine. Requesting a `data_set_id` the caller cannot read is a 403.
    pub async fn check_naming(&self, form: &NamingCheckForm) -> Result<NamingCheck, ResponseError> {
        let path = &format!("{}/naming/check", self.base_url);
        self.execute_post_request(path, form).await
    }
}

/// A policy node. Mirrors the server's `Policy`, which extends the shared `NodeModel` base — hence
/// the entity fields (`external_id`, `name`, `metadata`, …) alongside the policy-specific ones.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
    #[serde(default)]
    pub labels: Vec<String>,
    /// Which template this policy is an instance of. Populated on the entries returned by
    /// [`PoliciesService::types`]; on a stored policy it is often `None`, since the type is carried
    /// by `template_id` instead.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub policy_type: Option<PolicyType>,
    /// The policy's configured value. Free-form — its shape depends on [`PolicyType`] (a regex
    /// string for `NAMING_CONVENTION`, a retention count for a lifecycle policy, and so on).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<JsonValue>,
    /// Switched off without being deleted. Honoured on create as well as update.
    #[serde(default)]
    pub deactivated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub template_id: Option<u64>,
    /// The dataset this policy governs. `None` means tenant-wide.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<DateTime<Utc>>,
}

impl DataHubEntity for Policy {
    fn ext_id(&self) -> &String {
        // `Policy::external_id` is optional (the server derives one when it is omitted), but the
        // trait needs a borrow. An empty id is what the create endpoint already treats as absent.
        static EMPTY: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        self.external_id
            .as_ref()
            .unwrap_or_else(|| EMPTY.get_or_init(String::new))
    }
}

impl Policy {
    /// A new policy of the given type, named after it. Chain the `with_*` builders to fill in the
    /// rest.
    pub fn from_type(policy_type: PolicyType) -> Self {
        Self {
            name: policy_type.as_str().to_string(),
            policy_type: Some(policy_type),
            ..Default::default()
        }
    }

    /// A new policy named `name`, instantiated from the template with `template_id` — the pairing
    /// [`PoliciesService::create`] expects for a stored template.
    pub fn from_template(name: &str, template_id: u64) -> Self {
        Self {
            name: name.to_string(),
            template_id: Some(template_id),
            ..Default::default()
        }
    }

    pub fn with_external_id(mut self, external_id: &str) -> Self {
        self.external_id = Some(external_id.to_string());
        self
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// Govern one dataset rather than the whole tenant.
    pub fn with_data_set_id(mut self, data_set_id: u64) -> Self {
        self.data_set_id = Some(data_set_id);
        self
    }

    pub fn with_value(mut self, value: JsonValue) -> Self {
        self.value = Some(value);
        self
    }

    /// Create or leave the policy switched off.
    pub fn deactivated(mut self, deactivated: bool) -> Self {
        self.deactivated = deactivated;
        self
    }
}

/// The policy templates the server offers, from its `PolicyType` enum.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyType {
    /// Restricts data access or actions for security compliance.
    SecurityPolicy,
    /// Encryption requirements for stored or transmitted data.
    EncryptionPolicy,
    /// Hides or obfuscates sensitive fields.
    MaskingPolicy,
    /// Read-only dataset: blocks writes except delete.
    IsWriteProtected,
    /// Restricts read access to authorised users.
    IsReadProtected,
    /// Dataset must meet specific compliance requirements.
    HasRequirement,
    /// Enforces the external-id naming convention at write time. Tenant-wide, optionally
    /// overridden per dataset; applies to resources and datasets, events are exempt.
    NamingConvention,
}

impl PolicyType {
    /// The wire name, as the server spells it.
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyType::SecurityPolicy => "SECURITY_POLICY",
            PolicyType::EncryptionPolicy => "ENCRYPTION_POLICY",
            PolicyType::MaskingPolicy => "MASKING_POLICY",
            PolicyType::IsWriteProtected => "IS_WRITE_PROTECTED",
            PolicyType::IsReadProtected => "IS_READ_PROTECTED",
            PolicyType::HasRequirement => "HAS_REQUIREMENT",
            PolicyType::NamingConvention => "NAMING_CONVENTION",
        }
    }

    /// Where a policy of this type may be attached.
    ///
    /// Mirrors the server's `PolicyType` scope table so the mistake can be caught before the round
    /// trip — attaching a policy where its type does not apply would look configured and enforce
    /// nothing, which is why the server rejects it with a 400 rather than accepting it quietly.
    pub fn scope(&self) -> PolicyScope {
        match self {
            PolicyType::NamingConvention => PolicyScope::TenantWithDatasetOverride,
            _ => PolicyScope::DatasetOnly,
        }
    }

    /// Whether a policy of this type may name a `data_set_id`.
    pub fn can_attach_to_data_set(&self) -> bool {
        self.scope() != PolicyScope::TenantOnly
    }

    /// Whether a policy of this type may be left tenant-wide (no `data_set_id`).
    pub fn can_apply_tenant_wide(&self) -> bool {
        self.scope() != PolicyScope::DatasetOnly
    }
}

/// Where a [`PolicyType`] may be attached. See [`PolicyType::scope`].
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyScope {
    /// Tenant-wide only; naming a `data_set_id` is rejected.
    TenantOnly,
    /// Must be attached to a dataset. All types except `NAMING_CONVENTION`.
    DatasetOnly,
    /// Tenant-wide by default, optionally overridden for one dataset.
    TenantWithDatasetOverride,
}

/// Candidate ids to run the naming policy over.
///
/// `external_ids` is **required and must not be empty** — an empty one is a 400, not a trivially
/// clean answer — and is capped at 1000 entries. `names` is optional and, when given, must line up
/// one-for-one with `external_ids`; it exists only so the server can derive a more meaningful
/// `suggestion`, and is not a second list of things to check.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct NamingCheckForm {
    #[serde(default)]
    pub external_ids: Vec<String>,
    #[serde(default)]
    pub names: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
}

impl NamingCheckForm {
    /// Check these external ids against the tenant-wide policy. Must be non-empty.
    pub fn external_ids<S: AsRef<str>>(ids: &[S]) -> Self {
        Self {
            external_ids: ids.iter().map(|s| s.as_ref().to_string()).collect(),
            ..Default::default()
        }
    }

    /// Attach the human-readable name behind each external id, aligned by position, so a
    /// violation can come back with a suggestion derived from the name rather than from the
    /// malformed id. Supply exactly as many as there are external ids, or none at all.
    pub fn with_names<S: AsRef<str>>(mut self, names: &[S]) -> Self {
        self.names = names.iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Evaluate against a dataset's override rather than the tenant policy. A dataset the caller
    /// cannot read is a 403.
    pub fn with_data_set_id(mut self, data_set_id: u64) -> Self {
        self.data_set_id = Some(data_set_id);
        self
    }
}

/// The answer from [`PoliciesService::check_naming`].
///
/// The endpoint answers `{"findings": [...]}` rather than the usual `{"items": [...]}` envelope,
/// so this is its own type instead of a [`DataWrapper`].
#[derive(Debug, Default, Deserialize, Clone)]
pub struct NamingCheck {
    /// Only non-conforming ids appear. Empty means everything submitted is fine.
    #[serde(default)]
    pub findings: Vec<PolicyFinding>,
}

impl NamingCheck {
    /// Whether anything at all was flagged.
    pub fn is_clean(&self) -> bool {
        self.findings.is_empty()
    }

    /// The findings that would block a write, as opposed to merely warn.
    pub fn rejections(&self) -> impl Iterator<Item = &PolicyFinding> {
        self.findings
            .iter()
            .filter(|f| f.decision == PolicyDecision::NotOk)
    }
}

impl crate::generic::DataWrapperDeserialization for NamingCheck {
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error> {
        // 204/empty is not a shape this endpoint produces, but the helper contract allows it.
        if status_code == 204 || body.is_empty() {
            return Ok(NamingCheck::default());
        }
        serde_json::from_str(body)
    }
}

/// One item's verdict from one policy.
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PolicyFinding {
    /// Position in the submitted batch, so a rejection can name *which* of 500 ids was wrong.
    pub index: usize,
    /// The offending value, verbatim as submitted.
    pub external_id: Option<String>,
    pub decision: PolicyDecision,
    /// External id of the policy that fired — the rule, not just the symptom. Serialized as
    /// `policy` on the wire, matching the RFC 9457 `violations[]` entries.
    #[serde(rename = "policy")]
    pub policy_external_id: Option<String>,
    pub message: Option<String>,
    /// A conforming alternative where one can be derived. Offered for the caller to accept — the
    /// platform deliberately stopped rewriting external ids silently.
    pub suggestion: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyDecision {
    Ok,
    Warning,
    #[serde(rename = "NOT_OK")]
    NotOk,
}
