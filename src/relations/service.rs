//! `/edges` — the relationships between resources, as first-class objects.
//!
//! Edges are usually created as a side effect of `POST /resources/create`, which makes the nodes
//! and their links in one atomic call. This service is for the rest: linking resources that
//! already exist, reading an edge back, deleting one without touching its endpoints, and managing
//! the relationship-type catalogue.

use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtId};
use crate::graph_data_wrapper::GraphDataWrapper;
use crate::http::ResponseError;
use crate::relations::{EdgeProxy, RelForm, RelTypeForm, RelationshipType};
use crate::resources::Resource;
use crate::ApiService;
use std::sync::Weak;

pub struct EdgesService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl ApiServiceProvider for EdgesService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl EdgesService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        EdgesService {
            api_service,
            base_url: format!("{}/edges", base_url),
        }
    }

    /// `GET /edges/{id}` — one relationship by numeric id.
    ///
    /// An unknown or deleted id is a **404**, in line with every other single-resource `GET` in
    /// the API. Batch lookups differ — see [`by_ids`](Self::by_ids).
    pub async fn get(&self, id: u64) -> Result<DataWrapper<EdgeProxy>, ResponseError> {
        let path = &format!("{}/{}", self.base_url, id);
        self.execute_get_request(path, None::<&str>).await
    }

    /// `POST /edges/byids` — several relationships plus the resources they connect.
    ///
    /// The response is a graph, not a list: `nodes()` holds the resources at both ends and
    /// `relations()` the edges themselves, so no follow-up call is needed to resolve endpoints.
    ///
    /// Unlike [`get`](Self::get), this does **not** 404 on unmatched ids: a batch lookup answers
    /// 200 with empty `nodes` and `relations`, the same as every other `/byids` endpoint. "Absent
    /// from the result" is the only coherent answer when some of a batch exist and some do not.
    pub async fn by_ids<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/byids", self.base_url);
        self.execute_post_request(path, &input.into()).await
    }

    /// `POST /edges/create` — link resources that already exist. Answers **201**.
    ///
    /// Identify each endpoint by numeric id or external id (see [`RelForm::by_ids`] and
    /// [`RelForm::by_external_ids`]) and name the relationship. An unknown type name is created on
    /// the fly, so [`create_types`](Self::create_types) is only needed to pre-seed the catalogue
    /// or attach a description.
    ///
    /// To create the resources *and* their links together, use `resources.create()` instead —
    /// this endpoint only connects things that are already there.
    ///
    /// **All-or-nothing**: if any relation in the batch fails, none are created. The graph rules
    /// the server enforces are worth knowing, because each is a 400:
    /// - a relation whose target is a dataset must use `BELONGS_TO`;
    /// - a timeseries belongs to exactly one dataset, so it cannot be linked to a second;
    /// - you need write access to the datasets of *both* endpoints (else 403).
    ///
    /// Re-creating an edge that already exists between the same two resources is a **409** — the
    /// `(start, end, relationship_type)` triple is unique.
    pub async fn create<I>(&self, data: &I) -> Result<DataWrapper<EdgeProxy>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<RelForm>>,
    {
        let path = &format!("{}/create", self.base_url);
        self.execute_post_request(path, &data.into()).await
    }

    /// `POST /edges/delete` — delete relationships by id. Answers **204**.
    ///
    /// Deletes the link only; the resources at each end are untouched. Idempotent: unknown ids are
    /// silently skipped, so this cannot be used to detect whether an edge existed.
    ///
    /// It will not, however, delete an edge that is an endpoint's only route to the graph root:
    /// `ResourceService.delete` refuses rather than orphan the node, and the call comes back
    /// **400** naming the resource that would be stranded:
    ///
    /// ```json
    /// {"error":{"code":400,
    ///           "fields":[{"externalId":"node_b","type":"strandedResource"}],
    ///           "message":"Deleting this selection would disconnect resource(s) [node_b] from
    ///                      the graph root. Include them in the deletion or keep a connecting path."}}
    /// ```
    ///
    /// So an edge is separately deletable only when both endpoints stay reachable without it —
    /// otherwise it goes away with the resources it connects.
    pub async fn delete<I>(&self, json: &I) -> Result<DataWrapper<EdgeProxy>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let path = &format!("{}/delete", self.base_url);
        self.execute_post_request(path, &json.into()).await
    }

    /// `GET /edges/types` — every relationship type the tenant has defined.
    pub async fn types(&self) -> Result<DataWrapper<RelationshipType>, ResponseError> {
        let path = &format!("{}/types", self.base_url);
        self.execute_get_request(path, None::<&str>).await
    }

    /// `POST /edges/types/create` — register relationship type names up front.
    ///
    /// Names are case-insensitive and normalised to uppercase snake case (`Flows To` →
    /// `FLOWS_TO`); a blank name, or one that normalises to nothing, is a 400.
    ///
    /// **A name that already exists currently fails silently.** The service has no find-or-create:
    /// it saves a fresh entity unconditionally, so a duplicate collides on the unique name hash at
    /// *commit* time — after the handler has returned — and the caller gets a **200 with an empty
    /// body** rather than the documented "existing ones returned unchanged". Worse in a batch:
    /// every form is saved in one transaction, so a single duplicate rolls back the valid new
    /// types alongside it and the response still says 200.
    ///
    /// Until that is fixed, treat a 200 with no items as "something in this batch already
    /// existed and *nothing* was created", and use [`types`](Self::types) to see the real state.
    /// The intended behaviour is a 409, matching [`create`](Self::create) on a duplicate edge;
    /// `test_duplicate_relationship_type_conflicts` encodes that and is red until then.
    pub async fn create_types<I>(
        &self,
        data: &I,
    ) -> Result<DataWrapper<RelationshipType>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<RelTypeForm>>,
    {
        let path = &format!("{}/types/create", self.base_url);
        self.execute_post_request(path, &data.into()).await
    }
}
