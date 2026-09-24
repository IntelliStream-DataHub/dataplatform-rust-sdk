#[cfg(test)]
mod test;

use crate::generic::{ApiServiceProvider, DataHubEntity, DataWrapper, IdAndExtId};
use crate::graph_data_wrapper::GraphDataWrapper;
use crate::http::ResponseError;
use crate::nodes::Node;
use crate::resources::ResourceUpdate;
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Weak;

pub use crate::relations::RelatedNode;

/// Client for the `/functions` endpoints. A `Function` is a plain datastore node,
/// distinguished only by the canonical `FUNCTION` type-label and otherwise carrying the shared
/// node fields; it supports the same create/read/update/delete surface a resource does.
///
/// It used to bind a server-side model template to a JSON config map, which is where the
/// `model_name`/`config` pair came from. The server dropped that feature — see its
/// "Remove functions feature. revert to simple metadata store" — and `Function` now extends the
/// node base with no fields of its own, so those two are gone from here as well.
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

    /// `GET /functions?limit=N` — the first `limit` functions you may read, newest created first.
    ///
    /// `None` sends no `limit` and leaves the server's default of 1000 in place; the maximum is
    /// 10000, above which the server answers 400 rather than clamping. The cap counts rows that
    /// survive the data-set ACL, so a `limit` is not spent on functions you cannot see.
    ///
    /// This was `GET /functions/list` — the only collection that spelled a listing that way, and
    /// uncapped where every other one is capped. A stale caller now gets a 400: `list` is not a
    /// number, and the path it lands on is `GET /functions/{id}`.
    pub async fn list(&self, limit: Option<u64>) -> Result<DataWrapper<Function>, ResponseError> {
        let query = limit.map(|limit| [("limit", limit)]);
        self.execute_get_request::<DataWrapper<Function>, _>(&self.base_url, query.as_ref())
            .await
    }

    /// `GET /functions/{id}` — one function by its numeric id.
    ///
    /// **404 does not mean the id is free.** A function the caller may not read is reported as
    /// missing rather than forbidden, so a 404 says "not a function you can read" and nothing
    /// more. A node of another type is not a function and is reported the same way.
    ///
    /// Unlike [`by_ids`](Self::by_ids), which omits what it cannot find, this is an error. Prefer
    /// it to `by_ids` when you already have the numeric id: `by_ids` does not yet call the api's
    /// `/functions/byids` and pages the whole listing to filter client-side.
    pub async fn get_by_id(&self, id: u64) -> Result<DataWrapper<Function>, ResponseError> {
        let path = &format!("{}/{}", self.base_url, id);
        self.execute_get_request::<DataWrapper<Function>, ()>(path, None)
            .await
    }

    /// `POST /functions/update` — partial update of one or more functions.
    ///
    /// Each [`ResourceUpdate`] targets a function by id or external id and carries only the fields
    /// it changes. A function is a plain node, so every field the shared update form can set
    /// applies — except
    /// [`geolocation`](crate::resources::ResourceUpdateFields::geolocation), which only an asset
    /// stores and which the server ignores here. The intrinsic `FUNCTION` type-label is immutable:
    /// an update cannot turn a function into something else.
    ///
    /// **The echo is typed**, as on
    /// [`ResourceService::update`](crate::resources::ResourceService::update), and it is a [`Node`]
    /// rather than a [`Function`]: an update may touch relations whose other end is not a function.
    /// Match on [`Node::into_function`](crate::nodes::Node::into_function) for the ones that are.
    ///
    /// This is also why the echo could not stay a flat `Resource`: `Resource` requires `isRoot`,
    /// which a function does not have, so the flat shape failed to *deserialize* here.
    pub async fn update<I>(&self, input: &I) -> Result<GraphDataWrapper<Node>, ResponseError>
    where
        for<'a> &'a I: Into<GraphDataWrapper<ResourceUpdate>>,
    {
        let mut payload = input.into();
        // The server iterates `relations`; send an empty list rather than null when unset.
        if payload.relations.is_none() {
            payload.relations = Some(vec![]);
        }
        let path = &format!("{}/update", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Node>, _>(path, &payload)
            .await
    }

    /// Look up functions by id or externalId, implemented client-side by listing and filtering.
    ///
    /// **The SDK has not wired the real endpoint yet.** The api grew `/functions/byids`,
    /// `/functions/filter` and `/functions/search` in platform #131, which is what this should
    /// call; until it does, the client-side walk stands.
    ///
    /// It asks for the largest page the api allows, because a client-side filter can only match
    /// what the listing returned — so a tenant past 10000 functions silently misses the oldest
    /// ones here. The fix is to call `/functions/byids`, not to ask for a bigger page.
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
        let all = self.list(Some(10_000)).await?;
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
            content_type: None,
        })
    }

    /// `POST /functions/delete` — delete functions by id or external id. Deleting a function
    /// removes all of its relationships.
    ///
    /// The api answers **204 with no body**, so the returned wrapper is always empty — read
    /// [`get_http_status_code`](DataWrapper::get_http_status_code), not the items. A delete that
    /// would strand a surviving node is refused with **409** `would-strand`; read it through
    /// [`ResponseError::problem`](crate::http::ResponseError::problem).
    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Function>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }
}

/// API representation of a function. Mirrors `ai.intellistream.datahub.function.Function`, which
/// extends the shared node base and adds nothing of its own — so this is exactly the node fields.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Function {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    pub external_id: String,
    /// Optional here, **required by the api** — `Function` declares `name` non-null, so a create
    /// that omits it is a 400. `Option` only because a read of a row written before the constraint
    /// can still answer without one; build one with [`Function::with_name`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Resource-shape labels. The canonical `FUNCTION` label is always present.
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The name of the system this function's primary information comes from — the `source`
    /// column shared by every node type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id"
    )]
    pub data_set_id: Option<u64>,
    /// Declared by the shared node base, but **always empty on everything this service returns**.
    ///
    /// The api maps a `FunctionEntity` through `FunctionTransformer`, which applies the shared node
    /// fields and the labels and never joins the edges in — so `list`, `get_by_id` *and* the
    /// `create` echo all answer `[]`. Unlike `/resources/create`, the create echo is no exception:
    /// it re-reads the rows from Postgres through the same transformer.
    ///
    /// Read a function's edges through
    /// [`ResourceService::fetch_related`](crate::resources::ResourceService::fetch_related) or
    /// [`EdgesService`](crate::relations::EdgesService).
    #[serde(default, skip_serializing)]
    pub related_resources: Vec<RelatedNode>,
    #[serde(skip_serializing)]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing)]
    pub last_updated_time: Option<DateTime<Utc>>,
}

impl Function {
    pub fn new(external_id: String) -> Self {
        Function {
            id: None,
            external_id,
            name: None,
            labels: vec![],
            metadata: HashMap::new(),
            description: None,
            source: None,
            data_set_id: None,
            related_resources: vec![],
            created_time: None,
            last_updated_time: None,
        }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
}

impl DataHubEntity for Function {
    fn ext_id(&self) -> &String {
        &self.external_id
    }
}
