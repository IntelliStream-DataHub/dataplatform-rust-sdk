#[cfg(test)]
mod test;

use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Weak;

/// Mirror of the server-side `EdgeProxy` fields the function worker actually needs.
/// `start` is the bound input timeseries id; the worker keys its routing map by this.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EdgeProxy {
    #[serde(default)]
    pub id: Option<u64>,
    #[serde(default)]
    pub start: Option<u64>,
    #[serde(default)]
    pub end: Option<u64>,
    #[serde(rename = "type", default)]
    pub edge_type: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

/// Client for the `/functions` endpoints. A `Function` binds a server-side model template
/// (e.g. `forecast-ema`, `anomaly-detection`) to a JSON config map. Once a `PROCESSED_BY`
/// edge attaches the function to one or more timeseries the server creates a system-managed
/// `Subscription` per binding; the function's external worker discovers those via
/// `subscriptions.list(include_system_managed=true)` and listens to each one.
pub struct FunctionsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl FunctionsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/functions", base_url);
        FunctionsService {
            api_service,
            base_url,
        }
    }

    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<Function>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Function>>,
    {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<Function>, _>(path, &data.into())
            .await
    }

    /// List every function visible to the calling tenant. The server endpoint is `GET`
    /// today — there is no filter or pagination payload, so callers send no params.
    pub async fn list(&self) -> Result<DataWrapper<Function>, ResponseError> {
        let path = &format!("{}/list", self.base_url);
        self.execute_get_request::<DataWrapper<Function>, ()>(path, None)
            .await
    }

    /// Look up functions by id or externalId. The backend has no `/byids` endpoint for
    /// functions yet; this is implemented client-side by listing and filtering, which is
    /// fine for the function-worker use case where the catalog is small.
    pub async fn by_ids(
        &self,
        ids: &[IdAndExtId],
    ) -> Result<DataWrapper<Function>, ResponseError> {
        let mut wanted_ids: Vec<u64> = vec![];
        let mut wanted_external_ids: Vec<String> = vec![];
        for id in ids {
            if let Some(numeric) = id.id {
                wanted_ids.push(numeric);
            }
            if let Some(ext) = &id.external_id {
                wanted_external_ids.push(ext.clone());
            }
        }
        let all = self.list().await?;
        let mut matched: Vec<Function> = vec![];
        for f in all.get_items() {
            let id_match = f.id.map_or(false, |i| wanted_ids.contains(&i));
            let ext_match = wanted_external_ids.contains(&f.external_id);
            if id_match || ext_match {
                matched.push(f.clone());
            }
        }
        let mut wrapper = DataWrapper::from_vec(matched);
        if let Some(code) = all.get_http_status_code() {
            wrapper.set_http_status_code(code);
        }
        Ok(wrapper)
    }

    /// Convenience for the function-worker bootstrap: `client.functions.by_external_id("...")`.
    /// Returns the first matching function or an error 404 if none exists.
    pub async fn by_external_id(&self, external_id: &str) -> Result<Function, ResponseError> {
        let dw = self
            .by_ids(&[IdAndExtId {
                id: None,
                external_id: Some(external_id.to_string()),
            }])
            .await?;
        dw.get_items().first().cloned().ok_or_else(|| ResponseError {
            status: oauth2::http::StatusCode::NOT_FOUND,
            message: format!("Function with externalId={} not found", external_id),
        })
    }

    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Function>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }
}

/// API representation of a function. Mirrors `ai.intellistream.datahub.function.Function` —
/// inherits the standard resource fields (id, external_id, name, labels, metadata,
/// created_time, last_updated_time) and adds the model-binding pair `model_name` + `config`.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Function {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    pub external_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Stable identifier of the model template (e.g. `forecast-ema`, `anomaly-detection`).
    pub model_name: String,
    /// Merged config: server-applied template defaults plus any user-supplied overrides.
    /// The worker reads parameters by key out of this map.
    #[serde(default)]
    pub config: JsonValue,
    /// Resource-shape labels. The canonical `FUNCTION` label is always present.
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    /// PROCESSED_BY edges where this function is the end (bound input timeseries on
    /// {@code start}). Populated server-side by `FunctionService.list()` so a function
    /// worker can derive its `(function, ts)` routing map without a separate edge query.
    #[serde(default, skip_serializing)]
    pub relations: Vec<EdgeProxy>,
    #[serde(skip_serializing)]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing)]
    pub last_updated_time: Option<DateTime<Utc>>,
}

impl Function {
    pub fn new(external_id: String, model_name: String) -> Self {
        Function {
            id: None,
            external_id,
            name: None,
            model_name,
            config: JsonValue::Object(serde_json::Map::new()),
            labels: vec![],
            metadata: HashMap::new(),
            relations: vec![],
            created_time: None,
            last_updated_time: None,
        }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }

    pub fn with_config(mut self, config: JsonValue) -> Self {
        self.config = config;
        self
    }
}

impl DataHubEntity for Function {
    fn ext_id(&self) -> &String {
        &self.external_id
    }
}
