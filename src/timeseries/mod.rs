
use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::collections::HashMap;
use crate::fields::Field;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeries {
    pub id: u64,
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub unit: String,
    pub description: Option<String>,
    #[serde(rename = "unitExternalId")]
    pub unit_external_id: Option<String>,
    #[serde(rename = "securityCategories")]
    pub security_categories: Option<Vec<u64>>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "valueType")]
    pub value_type: String,
    #[serde(rename = "createdTime")]
    pub created_time: Option<u64>,
    #[serde(rename = "lastUpdatedTime")]
    pub last_updated_time: Option<u64>,
    #[serde(rename = "relationsFrom")]
    pub relations_from: Vec<u64>,
    #[serde(rename = "isString")]
    pub is_string: bool,
    #[serde(rename = "isStep")]
    pub is_step: bool
}

impl TimeSeries {

    pub fn new(external_id: &str, name: &str, unit: &str) -> TimeSeries{
        TimeSeries {
            id: 0,
            external_id: "".to_string(),
            name: "".to_string(),
            metadata: None,
            unit: "".to_string(),
            description: None,
            unit_external_id: None,
            security_categories: None,
            data_set_id: None,
            value_type: "float".to_string(),
            created_time: None,
            last_updated_time: None,
            relations_from: vec![],
            is_string: false,
            is_step: false,
        }
    }

    pub fn builder() -> TimeSeries {
        TimeSeries::new("", "", "")
    }

    pub fn set_name(&mut self, name: &str) -> &mut TimeSeries {
        self.name = name.to_string();
        self
    }

    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) -> &mut TimeSeries {
        self.metadata = Some(metadata);
        self
    }

    pub fn set_external_id(&mut self, external_id: &str) -> &mut TimeSeries {
        self.external_id = external_id.to_string();
        self
    }

    pub fn set_unit(&mut self, unit: &str) -> &mut TimeSeries {
        self.unit = unit.to_string();
        self
    }

    pub fn set_description(&mut self, description: &str) -> &mut TimeSeries {
        self.description = Some(description.to_string());
        self
    }

    pub fn set_unit_external_id(&mut self, unit_external_id: &str) -> &mut TimeSeries {
        self.unit_external_id = Some(unit_external_id.to_string());
        self
    }

    pub fn set_security_categories(&mut self, security_categories: Vec<u64>) -> &mut TimeSeries {
        self.security_categories = Some(security_categories);
        self
    }

    pub fn set_data_set_id(&mut self, data_set_id: u64) -> &mut TimeSeries {
        self.data_set_id = Some(data_set_id);
        self
    }

    pub fn set_value_type(&mut self, value_type: &str) -> &mut TimeSeries {
        self.value_type = value_type.to_string();
        self
    }

    pub fn set_created_time(&mut self, created_time: u64) -> &mut TimeSeries {
        self.created_time = Some(created_time);
        self
    }

    pub fn set_last_updated_time(&mut self, last_updated_time: u64) -> &mut TimeSeries {
        self.last_updated_time = Some(last_updated_time);
        self
    }

    pub fn set_relations_from(&mut self, relations_from: Vec<u64>) -> &mut TimeSeries {
        self.relations_from = relations_from;
        self
    }

    pub fn set_is_string(&mut self, is_string: bool) -> &mut TimeSeries {
        self.is_string = is_string;
        self
    }

    pub fn set_is_step(&mut self, is_step: bool) -> &mut TimeSeries {
        self.is_step = is_step;
        self
    }

}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct LimitParam {
    limit: u64,
}

impl LimitParam {

    pub fn new() -> Self {
        LimitParam { limit: 100 }
    }

    pub fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
    }

    pub fn get_limit(&self) -> u64 {
        self.limit
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TimeSeriesCollection {
    items: Vec<TimeSeries>
}

impl TimeSeriesCollection {

    pub fn new() -> Self {
        TimeSeriesCollection {
            items: vec![]
        }
    }

    pub fn set_items(&mut self, items: Vec<TimeSeries>) {
        self.items = items;
    }

    pub fn add_item(&mut self, item: TimeSeries) {
        self.items.push(item);
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeriesUpdateFields {
    external_id: Option<Field<String>>,
    name: Option<Field<String>>,
    metadata: Option<Field<HashMap<String, String>>>,
    unit: Option<Field<String>>,
    description: Option<Field<String>>,
    unit_external_id: Option<Field<String>>,
    security_categories: Option<Field<Vec<u64>>>,
    data_set_id: Option<Field<u64>>,
    relations_from: Option<Field<Vec<u64>>>,
    is_string: Option<Field<bool>>,
    is_step: Option<Field<bool>>,
    value_type: Option<Field<String>>,
}

impl TimeSeriesUpdateFields {

    pub fn new() -> TimeSeriesUpdateFields {
        TimeSeriesUpdateFields {
            external_id: None,
            name: None,
            metadata: None,
            unit: None,
            description: None,
            unit_external_id: None,
            security_categories: None,
            data_set_id: None,
            relations_from: None,
            is_string: None,
            is_step: None,
            value_type: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TimeSeriesUpdate {
    id: Option<u64>,
    external_id: Option<String>,
    update: TimeSeriesUpdateFields
}

impl TimeSeriesUpdate {

}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TimeSeriesUpdateCollection {
    items: Vec<TimeSeries>
}

impl TimeSeriesUpdateCollection {

    pub fn new() -> Self {
        TimeSeriesUpdateCollection {
            items: vec![]
        }
    }

    pub fn get_items(&self) -> Vec<TimeSeries> {
        self.items.clone()
    }

    pub fn set_items(&mut self, items: Vec<TimeSeries>) {
        self.items = items;
    }

    pub fn add_item(&mut self, item: TimeSeries) {
        self.items.push(item);
    }
}