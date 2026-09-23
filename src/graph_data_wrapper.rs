//! [`GraphDataWrapper<T, R>`], the response shape of the graph endpoints: nodes *and* the edges
//! between them, where [`DataWrapper`](crate::generic::DataWrapper) carries rows alone.
//!
//! [`ResourceService::create`](crate::resources::ResourceService::create),
//! [`by_ids`](crate::resources::ResourceService::by_ids) and
//! [`update`](crate::resources::ResourceService::update) answer one, as do
//! [`AssetsService::update`](crate::assets::AssetsService::update),
//! [`FunctionsService::update`](crate::functions::FunctionsService::update) and
//! [`EdgesService::by_ids`](crate::relations::EdgesService::by_ids). Read it with
//! [`nodes`](GraphDataWrapper::nodes) and [`relations`](GraphDataWrapper::relations) — both are
//! `Option`, absent rather than empty when the response carried none.
//!
//! `R` defaults to [`EdgeProxy`], the response form of an edge, so
//! `GraphDataWrapper<Node>` is the shape you receive; the explicit `GraphDataWrapper<T, RelForm>`
//! is the request form. The same struct is the update request body, which is why a bare
//! `&ResourceUpdate` or `&Vec<ResourceUpdate>` can be handed directly to `update` — [`GraphNode`]
//! is the marker that unlocks those conversions.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::generic::DataWrapperDeserialization;
use crate::relations::EdgeProxy;

/// Marker for the node types a [`GraphDataWrapper`] can carry.
///
/// Implementing it is what lets `&update` and `&vec_of_updates` convert into a request body, so
/// they can be handed straight to `resources.update` and its siblings.
pub trait GraphNode: Clone + Serialize {
    fn into_wrapper(self) -> GraphDataWrapper<Self> {
        GraphDataWrapper::from(&self)
    }
}

/// Mirror of the Java `GraphDataWrapper<T, R>`: nodes of type `T` plus relations
/// of type `R`. `R` defaults to `EdgeProxy` (the response shape) so existing
/// call sites using `GraphDataWrapper<Resource>` resolve to the response form.
/// Request payloads use the explicit form `GraphDataWrapper<Resource, RelForm>`.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct GraphDataWrapper<T: GraphNode, R = EdgeProxy> {
    #[serde(alias = "items")]
    pub nodes: Option<Vec<T>>,
    pub relations: Option<Vec<R>>,
    /// Set from the raw body of a non-2xx response, never carried in one. Skipped both ways: this
    /// struct doubles as the request body for the resource and function create/update endpoints,
    /// and the api rejects a body naming a field it does not have.
    #[serde(skip)]
    pub error_body: Option<String>,
    #[serde(skip)]
    pub http_status_code: Option<u16>,
}

impl<T: GraphNode, R> GraphDataWrapper<T, R> {
    pub fn new() -> Self {
        Self {
            nodes: None,
            relations: None,
            error_body: None,
            http_status_code: None,
        }
    }

    pub fn with_relations(nodes: Vec<T>, relations: Vec<R>) -> Self {
        Self {
            nodes: Some(nodes),
            relations: Some(relations),
            error_body: None,
            http_status_code: None,
        }
    }

    pub fn nodes(&self) -> Option<Vec<T>> {
        self.nodes.clone()
    }
    pub fn relations(&self) -> Option<&Vec<R>> {
        self.relations.as_ref()
    }
    pub fn set_nodes(&mut self, nodes: Vec<T>) {
        self.nodes = Some(nodes);
    }
    pub fn set_relations(&mut self, relations: Vec<R>) {
        self.relations = Some(relations);
    }
    pub fn set_http_status_code(&mut self, _status_code: u16) {}
}

impl<T: GraphNode + DeserializeOwned, R: DeserializeOwned> DataWrapperDeserialization
    for GraphDataWrapper<T, R>
{
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error> {
        if status_code >= 200 && status_code < 300 {
            if status_code == 204 || body.is_empty() {
                return Ok(Self {
                    nodes: None,
                    relations: None,
                    error_body: None,
                    http_status_code: Some(status_code),
                });
            }
            serde_json::from_str(body).map(|mut wrapper: GraphDataWrapper<T, R>| {
                wrapper.set_http_status_code(status_code);
                wrapper
            })
        } else {
            eprintln!(
                "HTTP request failed with status code {}: {}",
                status_code, body
            );
            match serde_json::from_str(body).map(|mut wrapper: GraphDataWrapper<T, R>| {
                wrapper.set_http_status_code(status_code);
                wrapper
            }) {
                Ok(result) => Ok(result),
                Err(_) => {
                    eprintln!("Error parsing HTTP response body: {}", body);
                    Ok(GraphDataWrapper {
                        nodes: None,
                        relations: None,
                        error_body: Some(body.to_string()),
                        http_status_code: Some(status_code),
                    })
                }
            }
        }
    }
}

// `From` impls use the concrete default `R = EdgeProxy` to avoid inference
// ambiguity at call sites that write `GraphDataWrapper<T>`.
impl<T: GraphNode> From<&T> for GraphDataWrapper<T, EdgeProxy> {
    fn from(node: &T) -> Self {
        Self {
            nodes: Some(vec![node.clone()]),
            relations: None,
            error_body: None,
            http_status_code: None,
        }
    }
}
impl<T: GraphNode> From<T> for GraphDataWrapper<T, EdgeProxy> {
    fn from(node: T) -> Self {
        Self {
            nodes: Some(vec![node]),
            relations: None,
            error_body: None,
            http_status_code: None,
        }
    }
}
impl<T: GraphNode> From<Vec<T>> for GraphDataWrapper<T, EdgeProxy> {
    fn from(nodes: Vec<T>) -> Self {
        Self {
            nodes: Some(nodes),
            relations: None,
            error_body: None,
            http_status_code: None,
        }
    }
}
impl<T: GraphNode> From<&Vec<T>> for GraphDataWrapper<T, EdgeProxy> {
    fn from(nodes: &Vec<T>) -> Self {
        Self {
            nodes: Some(nodes.clone()),
            relations: None,
            error_body: None,
            http_status_code: None,
        }
    }
}
