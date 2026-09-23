#[cfg(test)]
mod tests;

use crate::generic::{
    ApiServiceProvider, DataWrapper, IdAndExtId, SearchAndFilterForm,
};
use crate::graph_data_wrapper::GraphDataWrapper;
use crate::http::ResponseError;
use crate::nodes::{Asset, Node};
use crate::resources::{ResourceFilter, ResourceFilterForm, ResourceUpdate};
use crate::ApiService;
use std::sync::Weak;

/// Client for the `/assets` endpoints — the typed view of the `ASSET`-labelled corner of the
/// resource graph.
///
/// Every call here is the generic `/resources` pipeline with the `ASSET` discriminator pinned
/// server-side, so the two paths cannot drift apart on ACLs, naming policy or status codes. The
/// difference is the shape that comes back: [`Asset`] rather than the polymorphic
/// [`Node`](crate::nodes::Node), so `geolocation` and `is_root` are reachable without a match.
///
/// The filter and search bodies are the resource ones — [`ResourceFilterForm`] and
/// [`SearchAndFilterForm<ResourceFilter>`] — because the api declares the same Java types on both
/// endpoint families. A `node_type` set on either is **replaced**, not merged: the api pins
/// `["asset"]` over whatever the caller asked for, since `node_type` entries OR together and
/// leaving a caller's `["timeseries"]` in place would widen a request to `/assets` into a mixed
/// query.
///
/// What it does *not* share with `/functions` is worth knowing: the api serves the same three
/// reads there, but the SDK's [`FunctionsService`](crate::functions::FunctionsService) has not
/// wired them yet and still filters a listing client-side.
pub struct AssetsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl ApiServiceProvider for AssetsService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl AssetsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/assets", base_url);
        AssetsService {
            api_service,
            base_url,
        }
    }

    /// `POST /assets/create` — create one or more assets. Each needs a unique `external_id` and a
    /// `name`; both are required server-side.
    ///
    /// Unlike [`ResourceService::create`](crate::resources::ResourceService::create), the `ASSET`
    /// type-label does not have to be set by the caller: the endpoint deserializes straight into
    /// the api's `Asset`, whose constructor forces the label in. [`Asset::new`] leaves `labels`
    /// unset for that reason, and any domain labels you do set are kept alongside it.
    ///
    /// Relations are not creatable here — this takes nodes only. Use
    /// [`ResourceService::create`](crate::resources::ResourceService::create) to build assets and
    /// the edges between them in one call, or [`EdgesService`](crate::relations::EdgesService) to
    /// link assets that already exist.
    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<Asset>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<Asset>>,
    {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request::<DataWrapper<Asset>, _>(path, &data.into())
            .await
    }

    /// `GET /assets/{id}` — one asset by its numeric id.
    ///
    /// **404 does not mean the id is free.** A node that exists but is not an asset, and an asset
    /// the caller may not read, are both reported as missing — the api hides existence rather than
    /// answering 403. So a 404 here says "not an asset you can read", nothing more.
    ///
    /// Unlike [`by_ids`](Self::by_ids), which omits what it cannot find, this is an error.
    pub async fn get_by_id(&self, id: u64) -> Result<DataWrapper<Asset>, ResponseError> {
        let path = &format!("{}/{}", self.base_url, id);
        self.execute_get_request::<DataWrapper<Asset>, ()>(path, None)
            .await
    }

    /// `POST /assets/byids` — a batch lookup by id or external id.
    ///
    /// Like every batch lookup in this api, it answers 200 with the found subset and silently
    /// omits the rest: an id that does not exist, names a node of another type, or is not readable
    /// is simply absent from the response rather than failing the call.
    pub async fn by_ids<I>(&self, id_collection: &I) -> Result<DataWrapper<Asset>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request::<DataWrapper<Asset>, _>(path, &id_collection.into())
            .await
    }

    /// `GET /assets?limit=N` — the first `limit` assets you may read, newest created first.
    ///
    /// `None` sends no `limit` and leaves the server's default of 1000 in place; the maximum is
    /// 10000, above which the server answers 400 rather than clamping.
    ///
    /// Assets are the bulk of a tenant, so this is a sample rather than an inventory, and there is
    /// no paging: the api nulls `next_cursor` here deliberately, because a walk needs a `sort` and
    /// a `cursor` and both live in a request body. Narrow with [`filter`](Self::filter) rather
    /// than raising the number.
    pub async fn list(&self, limit: Option<u64>) -> Result<DataWrapper<Asset>, ResponseError> {
        let query = limit.map(|limit| [("limit", limit)]);
        self.execute_get_request::<DataWrapper<Asset>, _>(&self.base_url, query.as_ref())
            .await
    }

    /// `POST /assets/filter` — the structured query, narrowed to assets.
    ///
    /// Takes the resource filter body unchanged, so [`ResourceFilter::is_root`] and
    /// [`ResourceFilter::data_set_id`] are available here and the shared
    /// [`NodeFilter`](crate::filters::NodeFilter) rules apply — wildcards, case-insensitivity, and
    /// what an empty list means. A `node_type` on the filter is overwritten with `["asset"]`.
    ///
    /// Unlike [`list`](Self::list) this pages: the form carries `sort` and `cursor`, and the
    /// response carries `next_cursor`.
    pub async fn filter(
        &self,
        form: &ResourceFilterForm,
    ) -> Result<DataWrapper<Asset>, ResponseError> {
        let path = &format!("{}/filter", self.base_url);
        self.execute_post_request::<DataWrapper<Asset>, _>(path, form)
            .await
    }

    /// `POST /assets/search` — free-text search over assets, ranked by `ts_rank` and tie-broken on
    /// id.
    ///
    /// The phrase selects and the filter only removes, so a filter can never widen a search.
    /// `query` is required at 3–140 characters; `limit` defaults to 100 and caps at 1000 — both
    /// different from the filter endpoint's 1000/10000.
    ///
    /// [`search_by_query`](Self::search_by_query) is the shorthand for the common case.
    pub async fn search(
        &self,
        form: &SearchAndFilterForm<ResourceFilter>,
    ) -> Result<DataWrapper<Asset>, ResponseError> {
        let path = &format!("{}/search", self.base_url);
        self.execute_post_request::<DataWrapper<Asset>, _>(path, form)
            .await
    }

    /// [`search`](Self::search) with just a query string, leaving `limit` at the server's default
    /// of 100. `query` must be 3–140 characters.
    pub async fn search_by_query(&self, query: &str) -> Result<DataWrapper<Asset>, ResponseError> {
        self.search(&SearchAndFilterForm::new(query)).await
    }

    /// `POST /assets/update` — partial update of one or more assets.
    ///
    /// Each [`ResourceUpdate`] targets an asset by id or external id and carries only the fields it
    /// changes. [`ResourceUpdateFields::geolocation`](crate::resources::ResourceUpdateFields::geolocation)
    /// is the one that matters here and nowhere else — an asset is the only node type that stores
    /// a geometry.
    ///
    /// **The echo is typed**, exactly as on
    /// [`ResourceService::update`](crate::resources::ResourceService::update), and it is a
    /// [`Node`] rather than an [`Asset`]: an update may touch relations whose other end is not an
    /// asset, so the response is not guaranteed to be all assets. Match on
    /// [`Node::into_asset`](crate::nodes::Node::into_asset) for the ones that are.
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

    /// `POST /assets/delete` — delete assets by id or external id. Deleting an asset removes all
    /// of its relationships.
    ///
    /// The api answers **204 with no body**, so the returned wrapper is always empty — read
    /// [`get_http_status_code`](DataWrapper::get_http_status_code), not the items.
    ///
    /// A delete that would disconnect a surviving node from the graph root is refused with **409**
    /// `would-strand`, naming the stranded resources in the problem's `blockedBy`; read it through
    /// [`ResponseError::problem`](crate::http::ResponseError::problem). The check reads the graph
    /// projection, which lags the write, so deleting an asset immediately after creating its edges
    /// can get the *wrong answer* rather than an error.
    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<Asset>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }
}
