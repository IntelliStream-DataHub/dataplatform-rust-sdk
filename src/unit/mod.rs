
use serde::Deserialize;
use std::clone::Clone;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct Unit{
    pub id: u64,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub name: String,
    #[serde(rename = "longName")]
    pub long_name: String,
    pub symbol: String,
    pub description: String,
    #[serde(rename = "aliasNames")]
    pub alias_names: Vec<String>,
    pub quantity: String,
    pub conversion: HashMap<String, f64>,
    pub source: String,
    #[serde(rename = "sourceReference")]
    pub source_reference: String
}

#[derive(Deserialize, Debug, Clone)]
pub struct UnitResponse {
    items: Vec<Unit>,
}

impl UnitResponse {

    pub fn get_items(&self) -> Vec<Unit> {
        self.items.clone()
    }

    pub fn length(&self) -> usize {
        self.items.len()
    }

}
