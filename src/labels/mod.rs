#[cfg(test)]
mod test;

use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use serde::{Deserialize, Serialize};
use std::sync::Weak;

/// Labels categorise resources and timeseries. Every resource must carry at least one label
/// (e.g. `PIPE`, `SENSOR`, `DOCUMENT`). Labels are tenant-scoped and shared across resources —
/// creating a resource with a new label name auto-creates the label, so this service is mainly
/// for admin flows that want to pre-seed names, colors, or i18n codes, and for listing/cleanup.
///
/// Mirrors the backend `/labels` controller: [`list`](Self::list), [`get`](Self::get),
/// [`create`](Self::create), [`update`](Self::update), [`delete`](Self::delete).
pub struct LabelsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl LabelsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/labels", base_url);
        LabelsService {
            api_service,
            base_url,
        }
    }

    /// List every label in the tenant. Labels are a small, slow-changing set, so this is cheap.
    pub async fn list(&self) -> Result<DataWrapper<Label>, ResponseError> {
        self.execute_get_request(&self.base_url, None::<&str>).await
    }

    /// Look up a single label by its numeric `id`. Returns an empty `items` if it doesn't exist.
    pub async fn get(&self, id: u64) -> Result<DataWrapper<Label>, ResponseError> {
        let path = &format!("{}/{}", self.base_url, id);
        self.execute_get_request(path, None::<&str>).await
    }

    /// Create one or more labels up front. Each needs a unique `name` within the tenant (names are
    /// canonicalised to `SNAKE_UPPER_CASE` server-side). A duplicate name returns HTTP 409.
    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<Label>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Label>>,
    {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request(path, &data.into()).await
    }

    /// Update existing labels (identified by `id`). PATCH semantics: only the fields you set are
    /// applied, so build each label with [`Label::from_id`] and the `with_*` setters.
    pub async fn update<I>(&self, data: &I) -> Result<DataWrapper<Label>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Label>>,
    {
        let path = &format!("{}/update", self.base_url);
        self.execute_post_request(path, &data.into()).await
    }

    /// Delete labels by `id` or external id (the label name). Rejected with HTTP 400 if any
    /// resource still references the label — the error body lists the blocking resources. On
    /// success the server returns 204 (an empty [`DataWrapper`] with status code 204).
    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Label>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }
}

/// A DataHub label. `name` is the identifier callers set; `id`/`color` are typically assigned by
/// the server. Fields are `Option` to match the backend `LabelForm`'s PATCH semantics — unset
/// fields are omitted from the request and left untouched on update.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Label {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id"
    )]
    pub id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub i18n_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub color: Option<String>,
}

impl Label {
    /// A new label with just a name, for [`LabelsService::create`].
    pub fn new(name: &str) -> Self {
        Label {
            name: Some(name.to_string()),
            ..Default::default()
        }
    }

    /// A label carrying only an `id`, as a starting point for [`LabelsService::update`].
    pub fn from_id(id: u64) -> Self {
        Label {
            id: Some(id),
            ..Default::default()
        }
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    /// Hex color, e.g. `#3A9F2E`. Invalid values are replaced with a random one server-side.
    pub fn with_color(mut self, color: &str) -> Self {
        self.color = Some(color.to_string());
        self
    }

    pub fn with_i18n_code(mut self, i18n_code: &str) -> Self {
        self.i18n_code = Some(i18n_code.to_string());
        self
    }
}

impl From<Label> for DataWrapper<Label> {
    fn from(value: Label) -> Self {
        DataWrapper::from_vec(vec![value])
    }
}
impl From<&Label> for DataWrapper<Label> {
    fn from(value: &Label) -> Self {
        DataWrapper::from_vec(vec![value.clone()])
    }
}
impl From<Vec<Label>> for DataWrapper<Label> {
    fn from(value: Vec<Label>) -> Self {
        DataWrapper::from_vec(value)
    }
}
impl From<&Vec<Label>> for DataWrapper<Label> {
    fn from(value: &Vec<Label>) -> Self {
        DataWrapper::from_vec(value.clone())
    }
}
