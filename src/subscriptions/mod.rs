pub mod listen;
mod test;

use std::sync::Weak;
pub use listen::{
    DataCollectionString, DataWrapperMessage, EventAction, EventObject, ListenError,
    SubscriptionListener, SubscriptionMessage, WsDatapoint,
};

use crate::filters::{PageRequest, TimeFilter};
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

    /// `POST /subscriptions/filter` — subscriptions matching [`SubscriptionFilterForm`], newest
    /// created first.
    ///
    /// Only subscriptions whose bound timeseries the caller can *all* read are returned. A match
    /// broader than `limit` is paged, not truncated: echo the envelope's `next_cursor` back as
    /// [`PageRequest::cursor`], under the same `sort`.
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

/// Criteria for `POST /subscriptions/filter`, mirroring the api's `SubscriptionFilter`.
///
/// Deliberately **not** a [`NodeFilter`](crate::filters::NodeFilter): a subscription is not a
/// node, and has no `source`, `labels` or `metadata` to match on. What it shares with the node
/// filters it shares by name and meaning — [`external_id`](Self::external_id) and
/// [`name`](Self::name) are case-insensitive pattern lists (`*` and `%` wildcards, `_` literal),
/// and the time windows are inclusive at both ends.
///
/// Fields AND together, entries within a list OR, and `None` or an empty list places no
/// restriction.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionFilter {
    /// Max 1000. Sent as strings, like every id on the wire.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id_vec"
    )]
    pub id: Option<Vec<u64>>,
    /// Max 1000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<Vec<String>>,
    /// Max 1000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<Vec<String>>,
    /// Subscriptions bound to at least one of these timeseries. Max 1000.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timeseries: Vec<IdAndExtId>,
    /// Matched against the subscription's `dateCreated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_time: Option<TimeFilter>,
    /// Matched against the subscription's `lastUpdated`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<TimeFilter>,
}

/// Request body of `POST /subscriptions/filter`.
///
/// Sortable by `id`, `externalId`, `name`, `createdTime` and `lastUpdatedTime`; the default is
/// `createdTime` descending.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionFilterForm {
    pub filter: SubscriptionFilter,
    /// Defaults to 1000 server-side and is capped at 10000 — above that the request is rejected
    /// with 400.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
    /// Ordering and paging. Flattened, so `sort` and `cursor` sit beside `filter` and `limit`.
    #[serde(flatten)]
    pub paging: PageRequest,
}

impl SubscriptionFilterForm {
    pub fn new(filter: SubscriptionFilter) -> Self {
        Self {
            filter,
            ..Self::default()
        }
    }
}
