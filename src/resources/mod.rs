mod forms;
#[cfg(test)]
mod tests;

use crate::fields::{Field, ListField, MapField};
use crate::generic::{
    ApiServiceProvider, DataWrapper, IdAndExtId, Identifiable, RelationForm, SearchAndFilterForm,
};
use crate::graph_data_wrapper::{GraphDataWrapper, GraphNode};
use crate::http::{process_response, ResponseError};
use crate::resources::forms::ResourceForm;
use crate::ApiService;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Weak;

pub struct ResourceService {
    api_service: Weak<ApiService>,
    base_url: String,
}
impl ApiServiceProvider for ResourceService {
    fn api_service(&self) -> &Weak<ApiService> {
        &self.api_service
    }
}

impl ResourceService {
    pub fn new(api_service: Weak<ApiService>, base_url: String) -> Self {
        Self {
            api_service,
            base_url: base_url + "/resources",
        }
    }

    pub async fn create<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<GraphDataWrapper<ResourceForm>>,
    {
        let payload = input.into();
        let url = &format!("{}/create", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Resource>, _>(&url, &payload)
            .await
    }
    pub async fn by_ids<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let payload = input.into();
        let url = &format!("{}/byids", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Resource>, _>(&url, &payload)
            .await
    }

    pub async fn delete<I>(&self, input: &I) -> Result<GraphDataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<DataWrapper<IdAndExtId>>,
    {
        let payload = input.into();
        //let token = self.get_token().await?;
        let url = &format!("{}/delete", self.base_url);
        self.execute_post_request::<GraphDataWrapper<Resource>, _>(&url, &payload)
            .await
    }
    pub async fn search(
        &self,
        payload: &SearchAndFilterForm,
    ) -> Result<DataWrapper<Resource>, ResponseError> {
        let url = &format!("{}/search", self.base_url);
        self.execute_post_request::<DataWrapper<Resource>, _>(&url, &payload)
            .await
    }
    pub async fn update<I>(&self, input: &I) -> Result<DataWrapper<Resource>, ResponseError>
    where
        for<'a> &'a I: Into<GraphDataWrapper<ResourceUpdate>>,
    {
        todo!();

        let payload = input.into();
        let token = self.get_token().await?;
        let url = &format!("{}/update", self.base_url);

        let response = self
            .get_api_service()
            .http_client
            .post(url)
            .json(&payload)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|e| ResponseError {
                status: e.status().unwrap(),
                message: e.to_string(),
            })?;
        process_response::<DataWrapper<Resource>>(response, url).await
    }
    //
}
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Resource {
    // used to be a serde skip if zero here. don't understand why
    // todo implement a smooth way to convert "datahub entities" to id-collections
    id: Option<u64>,
    external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    pub is_root: bool,
    pub data_set_id: Option<u64>,
    pub source: Option<String>,
    pub labels: Option<Vec<String>>,
    pub relations: Option<Vec<String>>,
    pub geolocation: Option<HashMap<String, f64>>, // todo implement GEOJSON, not prio atm
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<DateTime<Utc>>,
    pub relations_form: Option<Vec<RelationForm>>,
}

impl Resource {
    pub fn new() -> Self {
        Self {
            id: None,
            external_id: "".to_string(),
            name: "".to_string(),
            metadata: None,
            description: None,
            is_root: false,
            data_set_id: None,
            source: None,
            labels: None,
            relations: None,
            geolocation: None,
            created_time: None,
            last_updated_time: None,
            relations_form: Some(vec![]),
        }
    }
}
impl GraphNode for Resource {}
impl Identifiable for Resource {
    //todo!()

    fn id(&self) -> u64 {
        self.id.unwrap_or(0)
    }
    fn external_id(&self) -> &str {
        &self.external_id
    }
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResourceUpdate {
    //todo!()
    pub update: Option<ResourceUpdateFields>,
    pub relation_update: Option<Vec<String>>,
}

impl GraphNode for ResourceUpdate {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResourceUpdateFields {
    //todo!()
    #[serde(rename = "externalId")]
    external_id: Field<String>,
    name: Field<String>,
    description: Field<String>,
    #[serde(rename = "dataSetId")]
    data_set_id: Field<u64>,
    metadata: MapField,
    source: Field<String>,
    labels: ListField<String>,
}
