use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IdAndExtId {
    pub(crate) id: Option<u64>,
    #[serde(rename = "externalId")]
    pub(crate) external_id: Option<String>,
}

impl IdAndExtId {

    pub fn from_id(id: u64) -> Self {
        IdAndExtId { id: Some(id), external_id: None}
    }

    pub fn from_external_id(external_id: &str) -> Self {
        IdAndExtId { id: None, external_id: Some(external_id.to_string())}
    }

}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct IdAndExtIdCollection {
    items: Vec<IdAndExtId>
}

impl IdAndExtIdCollection {

    pub fn new() -> Self {
        IdAndExtIdCollection {
            items: vec![]
        }
    }

    pub fn from_id_vec(ids: Vec<u64>) -> Self {
        let mut items = vec![];
        for id in ids {
            items.push(IdAndExtId::from_id(id));
        }
        IdAndExtIdCollection { items }
    }

    pub fn from_external_id_vec(external_ids: Vec<&str>) -> Self {
        let mut items = vec![];
        for external_id in external_ids {
            items.push(IdAndExtId::from_external_id(external_id));
        }
        IdAndExtIdCollection { items }
    }

    pub fn set_items(&mut self, items: Vec<IdAndExtId>) {
        self.items = items;
    }

    pub fn add_item(&mut self, item: IdAndExtId) {
        self.items.push(item);
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DataWrapper<T> {
    items: Vec<T>
}

impl<T> DataWrapper<T> {
    pub fn new() -> Self {
        DataWrapper {
            items: vec![]
        }
    }

    pub fn get_items(&self) -> &Vec<T> {
        &self.items
    }

    pub fn set_items(&mut self, items: Vec<T>) {
        self.items = items;
    }

    pub fn add_item(&mut self, item: T) {
        self.items.push(item);
    }

    pub fn length(&self) -> u64 {
        self.items.len() as u64
    }
}