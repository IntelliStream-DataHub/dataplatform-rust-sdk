use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Field<T> {
    set: Option<T>,
    set_null: bool
}

impl<T> Field<T> {

    pub fn set(&mut self, value: T) where T: Clone {
        self.set = Some(value);
    }

    pub fn set_null(&mut self, is_null: bool) {
        self.set_null = is_null;
    }

    pub fn get_null(&self) -> bool {
        self.set_null
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ListField<T>{
    set: Option<Vec<T>>,
    add: Option<Vec<T>>,
    remove: Option<Vec<T>>
}

impl<T> ListField<T>{

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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MapField{
    set: Option<HashMap<String, String>>,
    add: Option<HashMap<String, String>>,
    remove: Option<Vec<String>>
}

impl MapField{

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