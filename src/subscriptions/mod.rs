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
    // The raw host base (without the `/subscriptions` REST suffix), used to build the WebSocket
    // listen URL which lives under `/timeseries/datapoints/subscription/listen`.
    host_base_url: String,
}

impl SubscriptionsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        SubscriptionsService {
            api_service,
            base_url: format!("{}/subscriptions", base_url),
            host_base_url: base_url.clone(),
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

    /// `GET /subscriptions?limit=N` — the first `limit` subscriptions in the tenant, newest
    /// created first.
    ///
    /// `None` sends no `limit` and leaves the server's default of 1000 in place; the maximum is
    /// 10000, above which the server answers 400 rather than clamping. There is no paging: no
    /// `nextCursor` comes back, so the cap truncates. Criteria, ordering and paging all live on
    /// [`filter`](Self::filter).
    pub async fn list(
        &self,
        limit: Option<u64>,
    ) -> Result<DataWrapper<Subscription>, ResponseError> {
        let query = limit.map(|limit| [("limit", limit)]);
        self.execute_get_request::<DataWrapper<Subscription>, _>(&self.base_url, query.as_ref())
            .await
    }

    /// `POST /subscriptions/filter` — subscriptions matching [`SubscriptionFilterForm`].
    ///
    /// This was `POST /subscriptions/list`, whose body was subscriptions-only: a `limit` that
    /// defaulted to 100 where the rest of the api defaulted to 1000, a sort property that reached
    /// the query unvalidated, and no cursor, so a tenant past one page could not reach the rest.
    /// The api moved it onto the family envelope and removed `/list` rather than aliasing it, so a
    /// client that has not moved gets a 404.
    ///
    /// [`SubscriptionFilterForm`] is a strict subset of what the endpoint now accepts: it does not
    /// yet carry the `cursor`, nor the `id`, `externalId`, `name`, `createdTime` and
    /// `lastUpdatedTime` criteria the filter grew alongside `timeseries`.
    pub async fn filter(
        &self,
        form: &SubscriptionFilterForm,
    ) -> Result<DataWrapper<Subscription>, ResponseError> {
        let path = &format!("{}/filter", self.base_url);
        self.execute_post_request::<DataWrapper<Subscription>, _>(path, form)
            .await
    }

    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Subscription>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }

    /// Open a WebSocket listener that multiplexes the named subscriptions' fan-out topics. The
    /// `subscription_external_ids` seed the initial set (may be empty — add them later with
    /// [`SubscriptionListener::subscribe`]). Returns a [`SubscriptionListener`] the caller drives
    /// with `next` / `ack` / `nack` / `close`. The handshake uses the bearer token currently
    /// cached in the API service.
    pub async fn listen<S: AsRef<str>>(
        &self,
        subscription_external_ids: &[S],
    ) -> Result<SubscriptionListener, ListenError> {
        // The listener fetches its own token and builds the URL (and re-does both on reconnect), so
        // it just needs a handle to the api service, the host base, and the initial interest set.
        let interest: Vec<String> = subscription_external_ids
            .iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        SubscriptionListener::connect(self.api_service.clone(), self.host_base_url.clone(), interest)
            .await
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Subscription {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
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
pub struct SubscriptionFilterForm {
    pub filter: SubscriptionFilter,
    pub limit: u32,
    pub sort: DataSort,
}

impl Default for SubscriptionFilterForm {
    fn default() -> Self {
        SubscriptionFilterForm {
            filter: SubscriptionFilter::default(),
            limit: 100,
            sort: DataSort::default(),
        }
    }
}
