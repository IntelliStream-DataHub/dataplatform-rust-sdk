use crate::generic::{ApiServiceProvider, DataWrapperDeserialization};
use crate::http::ResponseError;
use crate::ApiService;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Weak;

/// Client for the `/tenant` endpoints: what the tenant your token belongs to has switched on, and
/// the settings your organization administers for itself.
///
/// Every answer here is a bare object, not an `items` envelope.
pub struct TenantService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl ApiServiceProvider for TenantService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl TenantService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        TenantService {
            api_service,
            base_url: format!("{}/tenant", base_url),
        }
    }

    /// `GET /tenant/features` — which optional features are enabled for your tenant. A disabled
    /// feature's endpoints may still exist and answer 404 or 403.
    pub async fn features(&self) -> Result<TenantFeatures, ResponseError> {
        let path = &format!("{}/features", self.base_url);
        self.execute_get_request(path, None::<&str>).await
    }

    /// `GET /tenant/settings/permissions` — what the caller may read and write, per settings scope
    /// (`"llm"`, …). Wildcard grants are already resolved, so every scope is listed by name.
    ///
    /// For gating a UI, not a security boundary: the settings endpoints enforce the same grants.
    pub async fn settings_permissions(
        &self,
    ) -> Result<HashMap<String, SettingsPermission>, ResponseError> {
        let path = &format!("{}/settings/permissions", self.base_url);
        self.execute_get_request(path, None::<&str>).await
    }

    /// `GET /tenant/settings/llm` — the model your organization's assistant runs on. The API key
    /// is never returned; [`api_key_set`](TenantLlmSettings::api_key_set) says whether one is
    /// stored. Needs the `llm` read grant (403 otherwise).
    pub async fn llm_settings(&self) -> Result<TenantLlmSettings, ResponseError> {
        let path = &format!("{}/settings/llm", self.base_url);
        self.execute_get_request(path, None::<&str>).await
    }

    /// `PUT /tenant/settings/llm` — **replace** the model configuration, and answer it as stored.
    ///
    /// A replace, not a patch: a field left `None` is cleared. The one exception is
    /// [`api_key`](TenantLlmSettingsForm::api_key), where `None` or empty keeps the stored
    /// credential, so a form can save without retyping it. Needs the `llm` write grant.
    pub async fn update_llm_settings(
        &self,
        form: &TenantLlmSettingsForm,
    ) -> Result<TenantLlmSettings, ResponseError> {
        let path = &format!("{}/settings/llm", self.base_url);
        self.execute_put_request(path, form).await
    }
}

/// Answer of [`TenantService::features`]. Each flag is `false` when the api leaves it unset.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
pub struct TenantFeatures {
    #[serde(default)]
    pub files: bool,
    #[serde(default, deserialize_with = "null_as_false")]
    pub policy: bool,
    #[serde(default, deserialize_with = "null_as_false")]
    pub streaming: bool,
    #[serde(default, deserialize_with = "null_as_false")]
    pub chat: bool,
}

fn null_as_false<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or(false))
}

/// What the caller may do with one settings scope.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, Default, PartialEq, Eq)]
pub struct SettingsPermission {
    pub read: bool,
    pub write: bool,
}

/// Answer of [`TenantService::llm_settings`] and [`TenantService::update_llm_settings`].
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TenantLlmSettings {
    /// `anthropic` or `openai-compatible`.
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    /// One of `low`, `medium`, `high`, `xhigh`, `max`.
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub turn_timeout: Option<String>,
    #[serde(default)]
    pub max_output_tokens: Option<u32>,
    #[serde(default)]
    pub max_iterations: Option<u32>,
    #[serde(default)]
    pub instructions: Option<String>,
    /// Whether a credential is stored. The credential itself is never returned.
    #[serde(default)]
    pub api_key_set: bool,
    /// Whether this amounts to a model that can actually be called. `false` means your
    /// organization has no assistant.
    #[serde(default)]
    pub configured: bool,
}

/// Body of [`TenantService::update_llm_settings`].
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TenantLlmSettingsForm {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// `None` or empty keeps the stored credential; only a non-blank value replaces it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_iterations: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}

impl From<&TenantLlmSettings> for TenantLlmSettingsForm {
    /// The stored settings as a form that saves them unchanged — `api_key` stays `None`, which
    /// keeps the stored credential. Edit the fields to change, then send.
    fn from(value: &TenantLlmSettings) -> Self {
        TenantLlmSettingsForm {
            provider: value.provider.clone(),
            model: value.model.clone(),
            api_key: None,
            base_url: value.base_url.clone(),
            reasoning_effort: value.reasoning_effort.clone(),
            effort: value.effort.clone(),
            turn_timeout: value.turn_timeout.clone(),
            max_output_tokens: value.max_output_tokens,
            max_iterations: value.max_iterations,
            instructions: value.instructions.clone(),
        }
    }
}

impl DataWrapperDeserialization for TenantFeatures {
    fn deserialize_and_set_status(body: &str, _status_code: u16) -> Result<Self, serde_json::Error> {
        serde_json::from_str(body)
    }
}

impl DataWrapperDeserialization for TenantLlmSettings {
    fn deserialize_and_set_status(body: &str, _status_code: u16) -> Result<Self, serde_json::Error> {
        serde_json::from_str(body)
    }
}

impl DataWrapperDeserialization for HashMap<String, SettingsPermission> {
    fn deserialize_and_set_status(body: &str, _status_code: u16) -> Result<Self, serde_json::Error> {
        serde_json::from_str(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::create_api_service;

    #[test]
    fn features_read_null_flags_as_off() {
        let features: TenantFeatures =
            serde_json::from_str(r#"{"files":true,"policy":null,"streaming":true}"#).unwrap();
        assert!(features.files && features.streaming);
        assert!(!features.policy && !features.chat);
    }

    #[test]
    fn the_llm_form_built_from_stored_settings_keeps_the_credential() {
        let stored = TenantLlmSettings {
            provider: Some("anthropic".into()),
            api_key_set: true,
            configured: true,
            ..Default::default()
        };
        let body = serde_json::to_value(TenantLlmSettingsForm::from(&stored)).unwrap();
        assert_eq!(body, serde_json::json!({"provider": "anthropic"}));
    }

    #[tokio::test]
    async fn features_and_settings_permissions_answer() {
        let api = create_api_service();
        api.tenant.features().await.unwrap();
        let permissions = api.tenant.settings_permissions().await.unwrap();
        assert!(permissions.contains_key("llm"), "{permissions:?}");
        if permissions["llm"].read {
            api.tenant.llm_settings().await.unwrap();
        } else {
            let err = api.tenant.llm_settings().await.expect_err("no llm read grant");
            assert_eq!(err.status.as_u16(), 403);
        }
    }

    /// Without the `llm` write grant the `PUT` is a 403. With it, an empty form is a 400 naming
    /// `provider` and `model` and writes nothing, and a configured model written back unchanged
    /// answers exactly what was stored — the credential kept because `api_key` is left `None`.
    /// The grant is checked before the form, so no branch can change the tenant's settings.
    #[tokio::test]
    async fn update_llm_settings_is_gated_validated_and_round_trips() {
        let api = create_api_service();
        let permissions = api.tenant.settings_permissions().await.unwrap();
        let empty = TenantLlmSettingsForm::default();

        if !permissions["llm"].write {
            let err = api
                .tenant
                .update_llm_settings(&empty)
                .await
                .expect_err("no llm write grant");
            assert_eq!(err.status.as_u16(), 403);
            println!("no llm write grant; the round trip is not exercised");
            return;
        }

        let err = api
            .tenant
            .update_llm_settings(&empty)
            .await
            .expect_err("provider and model are required");
        assert_eq!(err.status.as_u16(), 400);
        let fields: Vec<String> = err
            .problem()
            .map(|p| p.fields().into_iter().filter_map(|f| f.field).collect())
            .unwrap_or_default();
        assert!(fields.contains(&"provider".to_string()), "{fields:?}");
        assert!(fields.contains(&"model".to_string()), "{fields:?}");

        let stored = api.tenant.llm_settings().await.unwrap();
        if !stored.configured {
            println!("no model configured; nothing to write back unchanged");
            return;
        }
        let saved = api
            .tenant
            .update_llm_settings(&TenantLlmSettingsForm::from(&stored))
            .await
            .unwrap();
        assert_eq!(saved, stored);
    }
}
