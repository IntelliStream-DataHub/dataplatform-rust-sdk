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

    /// Set the field to `value` (`{ "set": value, "setNull": false }`).
    pub fn value(value: impl Into<T>) -> Self {
        Field {
            set: Some(value.into()),
            set_null: false,
        }
    }

    /// Clear the field (`{ "setNull": true }`).
    pub fn null() -> Self {
        Field {
            set: None,
            set_null: true,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set: Option<Vec<T>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add: Option<Vec<T>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remove: Option<Vec<T>>,
}

impl<T> ListField<T> {
    pub fn new(set: Option<Vec<T>>, add: Option<Vec<T>>, remove: Option<Vec<T>>) -> Self {
        ListField { set, add, remove }
    }
    pub fn default() -> Self {
        ListField {
            set: None,
            add: None,
            remove: None,
        }
    }

    /// Replace the whole list (`{ "set": [...] }`).
    pub fn new_set(set: Vec<T>) -> Self {
        ListField {
            set: Some(set),
            add: None,
            remove: None,
        }
    }

    /// Add to the list, keeping existing entries (`{ "add": [...] }`).
    pub fn new_add(add: Vec<T>) -> Self {
        ListField {
            set: None,
            add: Some(add),
            remove: None,
        }
    }

    /// Remove entries from the list (`{ "remove": [...] }`).
    pub fn new_remove(remove: Vec<T>) -> Self {
        ListField {
            set: None,
            add: None,
            remove: Some(remove),
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
