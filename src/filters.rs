use crate::generic::IdAndExtId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Combine the two ergonomic id/external-id lists into the backend's `relatedResources` shape.
fn related_resource_refs(
    ids: Option<Vec<u64>>,
    external_ids: Option<Vec<String>>,
) -> Vec<IdAndExtId> {
    ids.into_iter()
        .flatten()
        .map(IdAndExtId::from_id)
        .chain(
            external_ids
                .into_iter()
                .flatten()
                .map(|ext| IdAndExtId::from_external_id(&ext)),
        )
        .collect()
}

// Not PartialEq: it carries `Vec<IdAndExtId>`, which is intentionally non-comparable (see
// `IdAndExtId`). Nothing compares filters by value; equality here would be meaningless anyway.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct BasicEventFilter {
    //#[serde(skip_serializing_if = "Option::is_none")]
    // NB: the backend types this filter field as a Long, so it cannot filter by an event's UUID id
    // (that request is rejected server-side). Use `EventsService::by_ids` to look up an event by its
    // UUID. This field is retained for API compatibility.
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub external_id_prefix: Option<String>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub sub_type: Option<String>,
    // Backend `EventFilter.dataSetIds` is a `Collection<IdCollection>` (`[{"id": ...}]`), matching
    // `relatedResources`. `IdAndExtId::from_id` serializes to exactly `{"id": ...}` (external id
    // omitted). Omitted from the request when unset so the backend keeps its default (no dataset
    // filter) — sending `null` or `[]` risks an NPE or an `IN ()` that matches nothing, depending
    // on how the field is guarded server-side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_set_ids: Option<Vec<IdAndExtId>>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub event_time: Option<TimeFilter>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    // The backend event filter reads related-resource selectors as a single `relatedResources`
    // array of `{id}` / `{externalId}` objects (its `Collection<IdCollection>`), matched with
    // `hasAll`. `IdAndExtId` serializes to exactly that shape (id as string, unset key omitted).
    #[serde(default)]
    pub related_resources: Vec<IdAndExtId>,
    //#[serde(skip_serializing_if = "Option::is_none")]//todo implement IdCollection
    pub created_time: Option<TimeFilter>,
    //#[serde(skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<TimeFilter>,
}

impl BasicEventFilter {
    pub fn new(
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
        related_resource_external_ids: Option<Vec<String>>,
        created_time: Option<TimeFilter>,
        last_updated_time: Option<TimeFilter>,
    ) -> Self {
        Self {
            id,
            external_id_prefix,
            description,
            source,
            r#type,
            sub_type,
            data_set_ids: data_set_ids
                .map(|ids| ids.into_iter().map(IdAndExtId::from_id).collect()),
            event_time,
            metadata,
            related_resources: related_resource_refs(
                related_resource_ids,
                related_resource_external_ids,
            ),
            created_time,
            last_updated_time,
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
        self.data_set_ids = Some(data_set_ids.iter().map(|id| IdAndExtId::from_id(*id)).collect());
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
    /// Select events referencing these resource ids. Appends to `related_resources`; the backend
    /// matches with `hasAll`, so all selectors must be present on an event.
    pub fn set_related_resource_ids(&mut self, related_resource_ids: &[u64]) -> &mut Self {
        self.related_resources
            .extend(related_resource_ids.iter().map(|id| IdAndExtId::from_id(*id)));
        self
    }
    /// Select events referencing these resource external ids. Appends to `related_resources`.
    pub fn set_related_resource_external_ids(
        &mut self,
        related_resource_external_ids: &[&str],
    ) -> &mut Self {
        self.related_resources.extend(
            related_resource_external_ids
                .iter()
                .map(|ext| IdAndExtId::from_external_id(ext)),
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

/// How the backend should order a result page: the properties to order by, and the direction.
///
/// Mirrors the api's `DataSort`. Sortable event properties are `eventTime`, `createdTime`,
/// `lastUpdatedTime`, `externalId`, `type`, `subType`, `status`, `source` and `dataSetId`; an
/// unsortable one is ignored server-side rather than rejected. Anything that is not exactly
/// `desc` sorts ascending, so a malformed order degrades to the default instead of silently
/// reversing the page.
///
/// Note that setting a [cursor](EventFilter::set_cursor) overrides this: a page is only
/// meaningful against the order it was produced in.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(rename_all = "camelCase")]
pub struct DataSort {
    pub property: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<String>,
}

impl DataSort {
    /// Ascending by one property, e.g. `DataSort::asc("eventTime")`.
    pub fn asc(property: &str) -> Self {
        Self {
            property: vec![property.to_string()],
            order: Some("asc".to_string()),
        }
    }

    /// Descending by one property, e.g. `DataSort::desc("eventTime")`.
    pub fn desc(property: &str) -> Self {
        Self {
            property: vec![property.to_string()],
            order: Some("desc".to_string()),
        }
    }
}

// Not PartialEq: holds `Option<BasicEventFilter>`, which is non-comparable (see `IdAndExtId`).
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct EventFilter {
    pub filter: Option<BasicEventFilter>,
    pub limit: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sort: Option<DataSort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    advanced_filter: Option<AdvancedEventFilter>,
}

impl EventFilter {

    pub fn default() -> Self {
        Self {
            filter: None,
            limit: 100,
            cursor: None,
            sort: None,
            advanced_filter: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
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
        related_resource_external_ids: Option<Vec<String>>,
        created_time: Option<TimeFilter>,
        last_updated_time: Option<TimeFilter>,
    ) -> Self {
        // Every argument used to be dropped on the floor here, so `new(...)` returned the same
        // unfiltered filter as `default()` and quietly matched every event in the tenant. The
        // arguments are the fields of a BasicEventFilter, so build one.
        Self {
            filter: Some(BasicEventFilter::new(
                id,
                external_id_prefix,
                description,
                source,
                r#type,
                sub_type,
                data_set_ids,
                event_time,
                metadata,
                related_resource_ids,
                related_resource_external_ids,
                created_time,
                last_updated_time,
            )),
            limit: 100,
            cursor: None,
            sort: None,
            advanced_filter: None,
        }
    }
    pub fn set_filter(&mut self, filter: BasicEventFilter) -> &mut Self {
        self.filter = Some(filter);
        self
    }
    pub fn filter(&self) -> Option<&BasicEventFilter> {
        self.filter.as_ref()
    }
    /// Resume a walk from where the previous page stopped.
    ///
    /// The value is `<eventTime epoch millis>_<event id>` taken from the last event of that page —
    /// build it with [`Event::page_cursor`](crate::events::Event::page_cursor) rather than by hand.
    /// Both halves are required: event times are not unique, so a position on the timestamp alone
    /// would either skip the events sharing that millisecond or repeat them forever.
    ///
    /// Setting this fixes the order to `(eventTime, id)` ascending, overriding [`set_sort`](Self::set_sort).
    pub fn set_cursor(&mut self, cursor: impl Into<String>) -> &mut Self {
        self.cursor = Some(cursor.into());
        self
    }
    /// Drop the paging position, restarting the walk from the beginning.
    pub fn clear_cursor(&mut self) -> &mut Self {
        self.cursor = None;
        self
    }
    pub fn sort(&self) -> Option<&DataSort> {
        self.sort.as_ref()
    }
    /// Order the result page. Ignored when a [cursor](Self::set_cursor) is set.
    pub fn set_sort(&mut self, sort: DataSort) -> &mut Self {
        self.sort = Some(sort);
        self
    }
    /// Drop the ordering, letting the backend return the page in no particular order.
    pub fn clear_sort(&mut self) -> &mut Self {
        self.sort = None;
        self
    }
    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
    pub fn set_limit(&mut self, limit: u64) -> &mut Self {
        self.limit = limit;
        self
    }
    pub fn set_advanced_filter(&mut self, filter: AdvancedEventFilter) -> &mut Self {
        self.advanced_filter = Some(filter);
        self
    }
    pub fn build(&self) -> Self {
        self.clone()
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
///
/// Constructs an arbitrary boolean statement for advanced filtering.
///
/// # Variants
///
/// - And(Vec<Filter>): Combines 2 or more filters on logical and.
///
/// - Or(Vec<Filter>): Combines 2 or more filters on logical or.
///
/// - Not(Box<Filter>): Negates the result of the child
///
/// - Equals: Porperty value must match the given value exactly.
///   - property: The property name to be evaluated.
///   - value: The value to match against the property.
///
/// - In: property matches any of a list of values.
///   - property: The property name to be evaluated.
///   - values: The list of values to check for inclusion.
///
/// - Range: check if property is between min and max values
///   - property: The property name to be evaluated.
///   - min: The optional lower bound of the range (inclusive or exclusive support could be added in the future).
///   - max: The optional upper bound of the range (inclusive or exclusive support could be added in the future).
///
/// - IsSet: check listed properties are not None
///   - property: A vector of property names to check.
///
/// - ContainsAny: property (list-type) contains at least 1 element in any_of
///   - property: The property name to be evaluated.
///   - any_of: The list of values to check for presence.
///
/// - ContainsAll: property (list-type) contains all elements in all_of
///   - property: The property name to be evaluated.
///   - all_of: The list of values that must all be present.
///
/// # Usage
///
/// This filter can be used to construct an arbitrary boolean statement:
///
/// ```rust
/// use serde_json;
/// use dataplatform_rust_sdk::{filters::Filter};
/// use serde_json::json;
/// let filter = Filter::And(vec![
///     Filter::Equals {
///         property: "status".to_string(),
///         value: "active".to_string()
///     },
///     Filter::Not(Box::new(Filter::In {
///         property: "category".to_string(),
///         values: vec!["restricted".to_string(), "archived".to_string()]
///     })),
/// ]);
/// let serialized = serde_json::to_string(&filter).unwrap();
/// println!("{}", serialized);
/// let deserialized: Filter = serde_json::from_str(&serialized).unwrap();
/// assert_eq!(filter, deserialized);
///```
/// //
#[serde(rename_all = "camelCase")]
pub enum Filter {
    // filters are constructed as a tree like structure using the standard boolean operators AND, OR, NOT,
    And(Vec<Filter>),
    Or(Vec<Filter>),
    Not(Box<Filter>),
    // these filters assume the property is a single value
    Equals {
        // evaluate if the given property is equal to the given value
        property: String,
        value: String,
    },
    In {
        // evaluate if any of the given values are equal to the given property
        property: String,
        values: Vec<String>,
    },
    Range {
        // evaluate if the given property is between the given min and max values
        // in future we could add support for inclusive/exclusive bounds
        max: Option<String>,
        min: Option<String>,
        property: String,
    },
    IsSet {
        // evaluate if the given property is set (not null)
        property: Vec<String>,
    },
    // the remaining filters assume a property is a list
    #[serde(rename_all = "camelCase")]
    ContainsAny {
        // check if the property contains any of the given values
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

    // The backend event filter reads a single `relatedResources: [{id}|{externalId}]` array
    // (`Collection<IdCollection>`), NOT flat `relatedResourceIds` / `relatedResourceExternalIds`
    // arrays. Serializing the flat keys silently disabled related-resource filtering, so lock the
    // wire shape here.
    #[test]
    fn basic_event_filter_serializes_related_resources_as_id_collection() {
        let mut filter = BasicEventFilter::default();
        filter.set_related_resource_ids(&[42, 7]);
        filter.set_related_resource_external_ids(&["asset_a"]);

        let value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&filter).unwrap()).unwrap();

        // The flat keys the backend ignores must be gone.
        assert!(value.get("relatedResourceIds").is_none());
        assert!(value.get("relatedResourceExternalIds").is_none());

        // Ids go over the wire as strings (IdAndExtId), each entry carrying only its populated key.
        assert_eq!(
            value["relatedResources"],
            json!([{"id": "42"}, {"id": "7"}, {"externalId": "asset_a"}])
        );
    }

    #[test]
    fn basic_event_filter_new_maps_related_resources() {
        // The ergonomic two-list constructor still populates the single wire field.
        let filter = BasicEventFilter::new(
            None, None, None, None, None, None, None, None, None,
            Some(vec![5]),
            Some(vec!["asset_b".to_string()]),
            None, None,
        );
        let value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&filter).unwrap()).unwrap();
        assert_eq!(
            value["relatedResources"],
            json!([{"id": "5"}, {"externalId": "asset_b"}])
        );
    }

    #[test]
    fn basic_event_filter_serializes_data_set_ids_as_id_collection() {
        // Backend dataSetIds is a Collection<IdCollection> ([{"id": ...}]), matching relatedResources.
        // When unset the key must be omitted entirely (not null or []) so the backend keeps its default.
        let mut filter = BasicEventFilter::default();
        assert!(
            serde_json::to_value(&filter).unwrap().get("dataSetIds").is_none(),
            "unset dataSetIds must be omitted from the payload"
        );

        filter.set_data_set_ids(&[42, 7]);
        assert_eq!(
            serde_json::to_value(&filter).unwrap()["dataSetIds"],
            json!([{"id": "42"}, {"id": "7"}])
        );
    }

    #[test]
    fn basic_event_filter_omits_empty_related_resources() {
        // A filter that doesn't select by related resources still emits an (empty) array, and never
        // the flat keys.
        let value: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&BasicEventFilter::default()).unwrap())
                .unwrap();
        assert_eq!(value["relatedResources"], json!([]));
    }

    /// `advanced_filter` used to serialize under its snake_case Rust name, which the api does not
    /// read — the filter was silently ignored and the query came back unfiltered.
    #[test]
    fn advanced_filter_serializes_as_camel_case() {
        let mut filter = EventFilter::default();
        filter.set_advanced_filter(AdvancedEventFilter::new());

        let value = serde_json::to_value(filter.build()).unwrap();
        assert!(
            value.get("advancedFilter").is_some(),
            "expected advancedFilter, got: {value}"
        );
        assert!(value.get("advanced_filter").is_none());
    }

    #[test]
    fn sort_and_cursor_are_settable_and_on_the_wire() {
        let mut filter = EventFilter::default();
        filter
            .set_sort(DataSort::asc("eventTime"))
            .set_cursor("1754476522104_0195f3a2-4c1b-7f9e-9c3a-1b2d4e6f8a90")
            .set_limit(200);

        let value = serde_json::to_value(filter.build()).unwrap();
        assert_eq!(value["sort"]["property"][0], "eventTime");
        assert_eq!(value["sort"]["order"], "asc");
        assert_eq!(
            value["cursor"],
            "1754476522104_0195f3a2-4c1b-7f9e-9c3a-1b2d4e6f8a90"
        );
        assert_eq!(value["limit"], 200);
    }

    /// Unset paging and sort must be absent rather than explicit nulls, so an ordinary filter keeps
    /// the body it had before these fields existed.
    #[test]
    fn unset_sort_and_cursor_are_omitted() {
        let value = serde_json::to_value(EventFilter::default()).unwrap();
        assert!(value.get("sort").is_none());
        assert!(value.get("cursor").is_none());
        assert!(value.get("advancedFilter").is_none());
    }

    /// `EventFilter::new` used to drop all thirteen arguments and return an unfiltered filter,
    /// which matched every event in the tenant instead of the ones asked for.
    #[test]
    fn new_keeps_its_arguments() {
        let filter = EventFilter::new(
            None,
            None,
            None,
            Some("SAP".to_string()),
            Some("alarm".to_string()),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let basic = filter.filter().expect("filter should be populated");
        assert_eq!(basic.r#type.as_deref(), Some("alarm"));
        assert_eq!(basic.source.as_deref(), Some("SAP"));
    }
}
