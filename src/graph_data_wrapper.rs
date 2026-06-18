use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::generic::DataWrapperDeserialization;
use crate::relations::EdgeProxy;

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
    pub error_body: Option<String>,
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
