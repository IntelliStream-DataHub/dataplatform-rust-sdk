use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;
//todo!() crate::dataset::Dataset;
//todo! use crate::policy::Policy;
use crate::generic::{DataWrapperDeserialization, RelationForm};

pub trait GraphNode: Clone + Serialize {
    fn into_wrapper(self) -> GraphDataWrapper<Self> {
        GraphDataWrapper::from(&self)
    }
}

#[derive(Debug, Deserialize, Serialize, Clone,PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct  GraphDataWrapper<T: GraphNode> {
    #[serde(alias = "items")]
    nodes: Option<Vec<T>>,
    relations: Option<Vec<RelationForm>>,
    error_body: Option<String>,
    http_status_code: Option<u16>,
}
impl<T: GraphNode> GraphDataWrapper<T> {
    pub fn new() -> Self {
        Self {
            nodes: None,
            relations: None,
            error_body: None,
            http_status_code: None,
        }
    }
    pub fn nodes(&self) -> Option<Vec<T>> {
        self.nodes.clone()
    }
    pub fn relations(&self) -> Option<&Vec<RelationForm>> {
        self.relations.as_ref()
    }
    pub fn set_nodes(&mut self, nodes: Vec<T>) {
        self.nodes = Some(nodes);
    }
    pub fn set_relations(&mut self, relations: Vec<RelationForm>) {
        self.relations = Some(relations);
    }
    pub fn set_http_status_code(&mut self, status_code: u16) {}
}
impl<T: GraphNode + DeserializeOwned> DataWrapperDeserialization for GraphDataWrapper<T> {
    fn deserialize_and_set_status(body: &str, status_code: u16) -> Result<Self, serde_json::Error> {
        if status_code >= 200 && status_code < 300 {
            if status_code == 204 || body.is_empty() { // HTTP No content doesnt return anything
                return Ok(Self{nodes: None, relations: None, error_body: None, http_status_code: Some(status_code)})
            }
            // For 2xx responses, we expect the body to be a valid DataWrapper<T>
            // If body is empty, it's fine for `from_str` to fail and return an error
            // Or, if you specifically want an empty wrapper for 2xx with empty body:
            // let mut wrapper = DataWrapper::new();
            // wrapper.set_http_status_code(status_code);
            // return Ok(wrapper);
            // However, typically a successful response with a body should be parsed.
            serde_json::from_str(body).map(|mut wrapper: GraphDataWrapper<T>| {
                wrapper.set_http_status_code(status_code);
                wrapper
            })
        } else {
            // For non-2xx responses (errors)
            eprintln!("HTTP request failed with status code {}: {}", status_code, body);

            // Attempt to deserialize the body into DataWrapper<T>
            // This is useful if the error response *itself* is a structured JSON,
            // for example, containing an error object.
            match serde_json::from_str(body).map(|mut wrapper: GraphDataWrapper<T>| {
                wrapper.set_http_status_code(status_code); // Set the HTTP status code
                wrapper // Return the modified wrapper
            }) {
                Ok(result) => {
                    Ok(result)
                },
                Err(_) => {
                    eprintln!("Error parsing HTTP response body: {}", body);
                    Ok(GraphDataWrapper{nodes: None, relations: None, error_body: Some(body.to_string()), http_status_code: Some(status_code)})
                }
            }
        }
    }
}
impl<T: GraphNode> From<&T> for GraphDataWrapper<T> {
    fn from(node: &T) -> Self {
        Self {
            nodes: Some(vec![node.clone()]),
            relations: None,
            error_body: None,
            http_status_code: None
        }
    }
}
impl<T: GraphNode> From<T> for GraphDataWrapper<T> {
    fn from(node: T) -> Self {
        Self {
            nodes: Some(vec![node]),
            relations: None,
            error_body: None,
            http_status_code: None
        }
    }
}
impl<T: GraphNode> From<Vec<T>> for GraphDataWrapper<T> {
    fn from(nodes: Vec<T>) -> Self {
        Self {
        nodes:Some(nodes),
        relations: None,
        error_body: None,
        http_status_code: None
        }
    }
}
impl<T: GraphNode> From<&Vec<T>> for GraphDataWrapper<T> {
    fn from(nodes: &Vec<T>) -> Self {
        Self {
            nodes:Some(nodes.clone()),
            relations: None,
            error_body: None,
            http_status_code: None
        }
    }
}