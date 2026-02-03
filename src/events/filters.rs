use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use chrono::{DateTime, Utc};
use maplit::hashmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BasicEventFilter {
    id: Option<u64>,
    external_id_prefix: Option<String>,
    description: Option<String>,
    source: Option<String>,
    r#type: Option<String>,
    sub_type: Option<String>,
    data_set_ids: Option<Vec<u64>>,
    event_time: Option<TimeFilter>,
    metadata: Option<HashMap<String, String>>,
    related_resource_ids: Option<Vec<u64>>,
    related_resource_external_ids: Option<Vec<String>>, //todo implement IdCollection
    created_time: Option<TimeFilter>,
    last_updated_time: Option<TimeFilter>,
}

impl BasicEventFilter {
    pub fn new() -> Self {
        Self {
            id: None,
            external_id_prefix: None,
            description: None,
            source: None,
            r#type: None,
            sub_type: None,
            data_set_ids: None,
            event_time: None,
            metadata: None,
            related_resource_ids: None,
            related_resource_external_ids: None,
            created_time: None,
            last_updated_time: None,
        }
    }
    pub fn set_id(&mut self, id: &u64) -> &mut Self {
        self.id = Some(*id);
        self
    }
    pub fn set_external_id_prefix(&mut self, external_id: &str) -> &mut Self {
        self.external_id_prefix = Some(external_id.to_string());
        self
    }
    pub fn set_description(&mut self, external_id: &str) -> &mut Self {
        self.description = Some(external_id.to_string());
        self
    }
    pub fn set_source(&mut self, source: &str) -> &mut Self {
        self.source = Some(source.to_string());
        self
    }
    pub fn set_type(&mut self, r#type: &str) -> &mut Self {
        self.r#type = Some(r#type.to_string());
        self
    }
    pub fn set_sub_type(&mut self, sub_type: &str) -> &mut Self {
        self.sub_type = Some(sub_type.to_string());
        self
    }
    pub fn set_data_set_ids(&mut self, data_set_ids: &[u64]) -> &mut Self {
        self.data_set_ids = Some(data_set_ids.to_vec());
        self
    }
    pub fn set_event_time(&mut self, event_time: &TimeFilter) -> &mut Self {
        self.event_time = Some(event_time.clone());
        self
    }
    pub fn set_metadata(&mut self, metadata: &HashMap<String, String>) -> &mut Self {
        self.metadata = Some(metadata.clone());
        self
    }
    pub fn set_related_resource_ids(&mut self, related_resource_ids: &[u64]) -> &mut Self {
        self.related_resource_ids = Some(related_resource_ids.to_vec());
        self
    }
    pub fn set_related_resource_external_ids(
        &mut self,
        related_resource_external_ids: &[&str],
    ) -> &mut Self {
        self.related_resource_external_ids = Some(
            related_resource_external_ids
                .iter()
                .copied()
                .map(String::from)
                .collect(),
        );
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Hash)]
#[serde(rename_all = "camelCase")]
#[serde(untagged)]
pub enum TimeFilter {
    // order matters when deserializing untagged enum, most spesific first
    Between {
        min: DateTime<Utc>,
        max: DateTime<Utc>,
    },
    After {
        min: DateTime<Utc>,
    },
    Before {
        max: DateTime<Utc>,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EventFilter {
    pub(crate) filter: Option<BasicEventFilter>,
    limit: Option<u64>,
    cursor: Option<String>,
    advanced_filter: Option<AdvancedEventFilter>,
}

impl EventFilter {
    pub fn new() -> Self {
        Self {
            filter: None,
            limit: None,
            cursor: None, // todo implement cursor
            advanced_filter: None,
        }
    }
    pub fn set_filter(&mut self, filter: &BasicEventFilter) -> &mut Self {
        self.filter = Some(filter.clone());
        self
    }
    pub fn set_limit(&mut self, limit: u64) -> &mut Self {
        self.limit = Some(limit);
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdvancedEventFilter {
    filter: Option<Filter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    related_resource_filter: Option<RelatedResourceFilter>,
}

impl AdvancedEventFilter {
    pub fn new() -> Self {
        Self {
            filter: None,                  // filter that the returned event must satisfy
            related_resource_filter: None, // idea was to filter so that if
        }
    }
    pub fn set_filter(&mut self, filter: &Filter) -> &mut Self {
        self.filter = Some(filter.clone());
        self
    }
    pub fn set_related_resource_filter(&mut self, filter: &RelatedResourceFilter) -> &mut Self {
        self.related_resource_filter = Some(filter.clone());
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RelatedResourceFilter {
    // Idea is that you can filter on events that have spesific neighbors.
    // it will apply the filter on the neighbors if the relation type matches
    // can see this being useful, but we should be careful as
    // it can be very complicated and potentialy computatuinaly expensive
    // relatedResource probably dont need to check their neighbors aswell
    //
    // example case: we have a pump that will be worked on with
    // a work permit that will be a related resource?
    // assume pump1 exists and will produce a timeseries RPM_pump1 with associated events
    // like "Pump1 RPM below threshold"
    // when the work permit is activated pump1 will be updated indicating it has an active work permit on it
    // the produced event will have a related resource /source field indicating its source ie pump1
    // using the related resource filter we can filter on events that dont have an active work permit?
    // can probalby do a lot more complex stuff aswell.
    filter: Filter,
    relation_types: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Filter {
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    Equals {
        property: String,
        value: String,
    },
    In {
        property: String,
        values: Vec<String>,
    },
    Range {
        max: Option<String>,
        min: Option<String>,
        property: String,
    },
    IsSet {
        property: Vec<String>,
    },
    #[serde(rename_all = "camelCase")]
    ContainsAny {
        any_of: Vec<String>,
        property: String,
    },
    #[serde(rename_all = "camelCase")]
    ContainsAll {
        all_of: Vec<String>,
        property: String,
    },
}

impl Filter {
    pub fn and(filters: &Vec<Filter>) -> Self {
        Filter::And(filters.clone())
    }
    pub fn or(filters: &Vec<Filter>) -> Self {
        Filter::Or(filters.clone())
    }
    pub fn not(filter: &Filter) -> Self {
        Filter::Not(Box::new(filter.clone()))
    }
    pub fn eq(property: &str, value: &str) -> Self {
        Filter::Equals {
            property: property.to_string(),
            value: value.to_string(),
        }
    }
    pub fn in_values(property: &str, values: &Vec<String>) -> Self {
        Filter::In {
            property: property.to_string(),
            values: values.clone(),
        }
    }
    pub fn range(property: &str, min: Option<String>, max: Option<String>) -> Self {
        Filter::Range {
            property: property.to_string(),
            min,
            max,
        }
    }
    pub fn is_set(property: &Vec<String>) -> Self {
        Filter::IsSet {
            property: property.clone(),
        }
    }
    pub fn contains_any(property: &str, any_of: &[String]) -> Self {
        Filter::ContainsAny {
            property: property.to_string(),
            any_of: any_of.to_vec(),
        }
    }
    pub fn contains_all(property: &str, all_of: &[String]) -> Self {
        Filter::ContainsAll {
            property: property.to_string(),
            all_of: all_of.to_vec(),
        }
    }

    pub fn build(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_advanced_filter() {
        let mut filt = AdvancedEventFilter::new();
        assert_eq!(filt.filter, None);
        let leaf_filter1 = Filter::Equals {
            property: "test".to_string(),
            value: "test".to_string(),
        };
        let leaf_filter2 = Filter::contains_all(
            "policies",
            &vec!["policy1".to_string(), "policy2".to_string()],
        );
        let leaf_filter3 = Filter::is_set(&vec!["metdata".to_string(), "type".to_string()]);
        let leaf_filter4 = Filter::contains_any(
            "policies",
            &vec!["policy3".to_string(), "policy4".to_string()],
        );

        let bool_filter1 = Filter::and(&vec![leaf_filter1.clone(), leaf_filter2.clone()]);
        let bool_filter2 = Filter::or(&vec![leaf_filter2, leaf_filter4]);
        let bool_filter3 = Filter::not(&leaf_filter3);
        let expected_json = json!({"filter": {"equals": {"property": "test", "value": "test"}}});
        filt.filter = Some(leaf_filter1.clone());
        assert_eq!(
            serde_json::to_string(&filt).unwrap(),
            expected_json.to_string()
        );

        let expected_json2 = json!(
        {"filter":
            {"and":[
                {"equals":{"property": "test", "value": "test"}},
                {"containsAll":{"property":"policies","allOf":["policy1", "policy2"]}}
            ]
        }});
        assert_eq!(
            serde_json::to_string(&AdvancedEventFilter {
                filter: Some(bool_filter1.clone()),
                related_resource_filter: None
            })
            .unwrap(),
            expected_json2.to_string()
        );
        let expected_json3 = json!({
            "filter": {
                "and": [
                    {
                        "or": [
                            {
                                "and": [
                                    {"equals": {"property": "test", "value": "test"}},
                                    {"containsAll": {"property": "policies", "allOf": ["policy1", "policy2"]}}
                                ]
                            },
                            {
                                "or": [
                                    {"containsAll": {"property": "policies", "allOf": ["policy1", "policy2"]}},
                                    {"containsAny": {"property": "policies", "anyOf": ["policy3", "policy4"]}}
                                ]
                            }
                        ]
                    },
                    {
                        "not": {
                            "not": {
                                "isSet": {"property": ["metdata", "type"]}
                            }
                        }
                    }
                ]
            }
        });

        let filter1_or_2_and_not3 = Filter::and(&vec![
            Filter::or(&vec![bool_filter1.clone(), bool_filter2]),
            Filter::not(&bool_filter3),
        ]);
        assert_eq!(
            serde_json::to_string(&AdvancedEventFilter {
                filter: Some(filter1_or_2_and_not3),
                related_resource_filter: None
            })
            .unwrap(),
            expected_json3.to_string()
        )
    }
}
