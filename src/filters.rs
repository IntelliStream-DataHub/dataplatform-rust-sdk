pub enum Operation{
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
    Contains,
    NotContains,
    StartsWith,
}

pub struct Filters{
    filters: Vec<Filter>,
}

pub struct Filter {
    key: String,
    value: String,
    operation: Operation
}

impl Filters{
    
    pub fn prefix(key: &str, value: &str) -> Filter {
        Filter {
            key: key.to_string(),
            value: value.to_string(),
            operation: Operation::Equal
        }
    }
    
    pub fn search(key: &str, value: &str) -> Filter {
        Filter {
            key: key.to_string(),
            value: value.to_string(),
            operation: Operation::Contains
        }
    }
    
    pub fn and(self, filter_a: Filter, filter_b: Filter) -> Self{
        Self{
            filters: vec![filter_a, filter_b]
        }
    }
}