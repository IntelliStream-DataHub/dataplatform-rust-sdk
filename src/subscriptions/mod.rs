pub mod listen;
mod test;

use std::sync::Weak;
pub use listen::{
    DataCollectionString, DataWrapperMessage, EventAction, EventObject, ListenError,
    SubscriptionListener, SubscriptionMessage, WsDatapoint,
};

use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub struct SubscriptionsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl SubscriptionsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/subscriptions", base_url);
        SubscriptionsService {
            api_service,
            base_url,
        }
    }

    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<Subscription>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Subscription>>,
    {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<Subscription>, _>(path, &data.into())
            .await
    }

    pub async fn list(
        &self,
        retriever: &SubscriptionRetriever,
    ) -> Result<DataWrapper<Subscription>, ResponseError> {
        let path = &format!("{}/list", self.base_url);
        self.execute_post_request::<DataWrapper<Subscription>, _>(path, retriever)
            .await
    }

    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Subscription>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }

    /// Open a WebSocket listener for the named subscription's fan-out topic. Returns a
    /// [`SubscriptionListener`] the caller drives with `next` / `ack` / `nack` / `close`.
    /// The handshake uses the bearer token currently cached in the API service.
    pub async fn listen(
        &self,
        subscription_external_id: &str,
    ) -> Result<SubscriptionListener, ListenError> {
        let token = self
            .get_token()
            .await
            .map_err(|e| ListenError::Request(format!("failed to get api token: {}", e.get_message())))?;
        let ws_url = listen::build_ws_url(&self.base_url, subscription_external_id)?;
        SubscriptionListener::connect(&ws_url, &token).await
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    pub external_id: String,
    pub name: String,
    pub timeseries: Vec<IdAndExtId>,
    #[serde(skip_serializing)]
    pub date_created: Option<DateTime<Utc>>,
    #[serde(skip_serializing)]
    pub last_updated: Option<DateTime<Utc>>,
}

impl Subscription {
    pub fn new(external_id: String, name: String, timeseries: Vec<IdAndExtId>) -> Self {
        Subscription {
            id: None,
            external_id,
            name,
            timeseries,
            date_created: None,
            last_updated: None,
        }
    }
}

impl DataHubEntity for Subscription {
    fn ext_id(&self) -> &String {
        &self.external_id
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timeseries: Vec<IdAndExtId>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct DataSort {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub property: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nulls: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionRetriever {
    pub filter: SubscriptionFilter,
    pub limit: u32,
    pub sort: DataSort,
}

impl Default for SubscriptionRetriever {
    fn default() -> Self {
        SubscriptionRetriever {
            filter: SubscriptionFilter::default(),
            limit: 100,
            sort: DataSort::default(),
        }
    }
}
