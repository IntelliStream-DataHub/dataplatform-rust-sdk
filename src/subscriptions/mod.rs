//! Subscriptions — a named set of timeseries to be notified about, and the WebSocket stream that
//! delivers the notifications.
//!
//! [`SubscriptionsService`] is reached as `api.subscriptions`.
//!
//! - **Managing subscriptions** — [`create`](SubscriptionsService::create),
//!   [`list`](SubscriptionsService::list), [`filter`](SubscriptionsService::filter) and
//!   [`delete`](SubscriptionsService::delete), over [`Subscription`]. There is no update or
//!   single-get. `list` is a capped, unpaged sample; criteria live on `filter` and
//!   [`SubscriptionFilterForm`], which carries only the `timeseries` criterion of the several the
//!   endpoint accepts.
//! - **Listening** — [`listen`](SubscriptionsService::listen) opens a WebSocket and returns a
//!   [`SubscriptionListener`] multiplexing the named subscriptions' streams.
//!
//! The listener has to be driven: call [`next`](SubscriptionListener::next) in a loop and
//! [`ack`](SubscriptionListener::ack) what you have processed. `next` is also what answers the
//! server's pings and transparently re-establishes a dropped connection, so a listener that is not
//! being polled is closed as idle after roughly 45 seconds — run heavy per-message work on another
//! task. Anything left unacked is redelivered to the next listener on the same subscription.

pub mod listen;
mod test;

use std::sync::Weak;
pub use listen::{
    DataCollectionString, DataWrapperMessage, EventAction, EventObject, ListenError,
    SubscriptionListener, SubscriptionMessage, WsDatapoint,
};

use crate::filters::DataSort;
use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Subscription management and WebSocket listening. Reached as `api.subscriptions`; see the
/// [module docs](self).
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

    /// `POST /subscriptions/create` — create one or more subscriptions.
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

    /// `POST /subscriptions/delete` — delete subscriptions by id or external id.
    ///
    /// Close any [`SubscriptionListener`] on the subscription first; deleting one with a live
    /// listener attached is refused.
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

/// A named standing interest in a set of timeseries, which
/// [`listen`](SubscriptionsService::listen) then streams datapoints for.
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

impl DataHubEntity for Subscription {}

/// Criteria for [`SubscriptionsService::filter`]. Currently only `timeseries`, a subset of what
/// the endpoint accepts.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionFilter {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timeseries: Vec<IdAndExtId>,
}

/// The request body of `POST /subscriptions/filter`: [`SubscriptionFilter`] criteria plus paging.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionFilterForm {
    pub filter: SubscriptionFilter,
    pub limit: u32,
    /// Absent means the endpoint's default order, `createdTime` descending.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort: Option<DataSort>,
}

impl Default for SubscriptionFilterForm {
    fn default() -> Self {
        SubscriptionFilterForm {
            filter: SubscriptionFilter::default(),
            limit: 100,
            sort: None,
        }
    }
}
