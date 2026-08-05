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

/// A list update: **either** replace the whole list (`set`) **or** apply an `add`/`remove` delta.
/// The two are mutually exclusive by construction — there is no way to build one carrying both.
/// Create one with [`ListField::set`] or [`ListField::delta`] (or the [`add`](ListField::add) /
/// [`remove`](ListField::remove) shortcuts). Serializes as `{"set": [...]}` or
/// `{"add": [...], "remove": [...]}`.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum ListField<T> {
    /// Replace the whole list.
    Set { set: Vec<T> },
    /// Add and/or remove entries, keeping the rest.
    Delta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        add: Option<Vec<T>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remove: Option<Vec<T>>,
    },
}

impl<T> Default for ListField<T> {
    /// An empty delta — applies no change.
    fn default() -> Self {
        ListField::Delta {
            add: None,
            remove: None,
        }
    }
}

impl<T> ListField<T> {
    /// Replace the whole list with `values`.
    pub fn set(values: Vec<T>) -> Self {
        ListField::Set { set: values }
    }
    /// Add and/or remove entries, keeping the rest.
    pub fn delta(add: Option<Vec<T>>, remove: Option<Vec<T>>) -> Self {
        ListField::Delta { add, remove }
    }
    /// Add entries, keeping the rest (an add-only delta).
    pub fn add(values: Vec<T>) -> Self {
        ListField::Delta {
            add: Some(values),
            remove: None,
        }
    }
    /// Remove entries, keeping the rest (a remove-only delta).
    pub fn remove(values: Vec<T>) -> Self {
        ListField::Delta {
            add: None,
            remove: Some(values),
        }
    }

    /// The replacement list, when this is a `set`.
    pub fn get_set(&self) -> Option<&Vec<T>> {
        match self {
            ListField::Set { set } => Some(set),
            ListField::Delta { .. } => None,
        }
    }
    /// The added entries, when this is a delta carrying `add`.
    pub fn get_add(&self) -> Option<&Vec<T>> {
        match self {
            ListField::Delta { add, .. } => add.as_ref(),
            ListField::Set { .. } => None,
        }
    }
    /// The removed entries, when this is a delta carrying `remove`.
    pub fn get_remove(&self) -> Option<&Vec<T>> {
        match self {
            ListField::Delta { remove, .. } => remove.as_ref(),
            ListField::Set { .. } => None,
        }
    }
}

/// A map update: **either** replace all entries (`set`) **or** apply an `add`/`remove` delta.
/// Mutually exclusive by construction, mirroring [`ListField`]. Create one with [`MapField::set`]
/// or [`MapField::delta`] (or the [`add`](MapField::add) / [`remove`](MapField::remove) shortcuts).
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum MapField {
    /// Replace all entries.
    Set { set: HashMap<String, String> },
    /// Add and/or remove entries, keeping the rest.
    Delta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        add: Option<HashMap<String, String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remove: Option<Vec<String>>,
    },
}

impl Default for MapField {
    /// An empty delta — applies no change.
    fn default() -> Self {
        MapField::Delta {
            add: None,
            remove: None,
        }
    }
}

impl MapField {
    /// Replace all entries with `values`.
    pub fn set(values: HashMap<String, String>) -> Self {
        MapField::Set { set: values }
    }
    /// Add and/or remove entries, keeping the rest.
    pub fn delta(add: Option<HashMap<String, String>>, remove: Option<Vec<String>>) -> Self {
        MapField::Delta { add, remove }
    }
    /// Add entries, keeping the rest (an add-only delta).
    pub fn add(values: HashMap<String, String>) -> Self {
        MapField::Delta {
            add: Some(values),
            remove: None,
        }
    }
    /// Remove entries by key, keeping the rest (a remove-only delta).
    pub fn remove(keys: Vec<String>) -> Self {
        MapField::Delta {
            add: None,
            remove: Some(keys),
        }
    }

    /// The replacement entries, when this is a `set`.
    pub fn get_set(&self) -> Option<&HashMap<String, String>> {
        match self {
            MapField::Set { set } => Some(set),
            MapField::Delta { .. } => None,
        }
    }
    /// The added entries, when this is a delta carrying `add`.
    pub fn get_add(&self) -> Option<&HashMap<String, String>> {
        match self {
            MapField::Delta { add, .. } => add.as_ref(),
            MapField::Set { .. } => None,
        }
    }
    /// The removed keys, when this is a delta carrying `remove`.
    pub fn get_remove(&self) -> Option<&Vec<String>> {
        match self {
            MapField::Delta { remove, .. } => remove.as_ref(),
            MapField::Set { .. } => None,
        }
    }
}
