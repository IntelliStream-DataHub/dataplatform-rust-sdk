use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Field<T> {
    pub set: Option<T>,
    pub set_null: bool,
}

impl<T> Field<T> {
    pub fn new(value: Option<T>, set_null: bool) -> Self {
        Field {
            set: value,
            set_null,
        }
    }

    pub fn set(&mut self, value: T)
    where
        T: Clone,
    {
        self.set = Some(value);
    }

    pub fn set_null(&mut self, is_null: bool) {
        self.set_null = is_null;
    }

    pub fn get_null(&self) -> bool {
        self.set_null
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct ListField<T> {
    pub set: Option<Vec<T>>,
    pub add: Option<Vec<T>>,
    pub remove: Option<Vec<T>>,
}

impl<T> ListField<T> {
    pub fn new(set: Option<Vec<T>>, add: Option<Vec<T>>, remove: Option<Vec<T>>) -> Self {
        ListField {
            set: None,
            add: None,
            remove: None,
        }
    }
    pub fn default() -> Self {
        ListField {
            set: None,
            add: None,
            remove: None,
        }
    }

    pub fn set(&mut self, s: Vec<T>) {
        self.set = Some(s);
    }

    pub fn add(&mut self, s: Vec<T>) {
        self.add = Some(s);
    }

    pub fn remove(&mut self, s: Vec<T>) {
        self.remove = Some(s);
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct MapField {
    pub set: Option<HashMap<String, String>>,
    pub add: Option<HashMap<String, String>>,
    pub remove: Option<Vec<String>>,
}

impl MapField {
    pub fn new(
        set: Option<HashMap<String, String>>,
        add: Option<HashMap<String, String>>,
        remove: Option<Vec<String>>,
    ) -> Self {
        MapField { set, add, remove }
    }
    pub fn new_set(s: Option<HashMap<String, String>>) -> Self {
        Self {
            set: s,
            add: None,
            remove: None,
        }
    }
    pub fn new_add(s: Option<HashMap<String, String>>) -> Self {
        Self {
            set: None,
            add: s,
            remove: None,
        }
    }
    pub fn new_remove(s: Option<Vec<String>>) -> Self {
        Self {
            set: None,
            add: None,
            remove: s,
        }
    }

    pub fn set(&mut self, s: HashMap<String, String>) {
        self.set = Some(s);
    }

    pub fn add(&mut self, s: HashMap<String, String>) {
        self.add = Some(s);
    }

    pub fn remove(&mut self, s: Vec<String>) {
        self.remove = Some(s);
    }
}
