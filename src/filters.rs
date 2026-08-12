use crate::generic::IdAndExtId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A metadata criterion: the entries a node or event must carry.
///
/// Several entries **AND** together ("has all of these"). A `None` value matches the key alone,
/// whatever it carries — `{"health": None}` finds anything tagged `health` — which is why this is
/// a map of `Option<String>` rather than of `String`.
pub type MetadataFilter = HashMap<String, Option<String>>;

/// The criteria every node type can be filtered by — the shared base of
/// [`ResourceFilter`](crate::resources::ResourceFilter),
/// [`TimeSeriesFilter`](crate::timeseries::TimeSeriesFilter) and
/// [`BasicDatasetFilter`](crate::datasets::BasicDatasetFilter), mirroring the api's `NodeFilter`.
/// Each of those flattens it, so on the wire the fields sit alongside the type-specific ones.
///
/// # Matching rules
///
/// Every supplied field is combined with **AND**; entries *within* a list field **OR** together —
/// except [`labels`](Self::labels) and [`metadata`](Self::metadata), where every entry must be
/// present.
///
/// [`external_ids`](Self::external_ids), [`names`](Self::names) and [`sources`](Self::sources) are
/// **pattern** lists: `*` and `%` both mean "any run of characters", `_` is literal (identifiers
/// here are built out of underscores, so `sap_work_orders` must not also match `sapXwork_orders`),
/// and an entry carrying no wildcard matches exactly. Matching is case-insensitive throughout.
///
/// `None` places no restriction — and so does an **empty** list: an empty `IN` is not valid SQL,
/// and a caller who built a list and found nothing to put in it means "no restriction" far more
/// often than "match nothing". Blank entries are dropped for the same reason. The one field where
/// empty and `None` diverge is `data_set_ids`, which is not here; see
/// [`ResourceFilter::data_set_ids`](crate::resources::ResourceFilter::data_set_ids).
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NodeFilter {
    /// Nodes with any of these ids. Max 1000. Sent as strings, like every id on the wire.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "crate::serde_helper::opt_string_id_vec"
    )]
    pub ids: Option<Vec<u64>>,
    /// Nodes matching any of these external ids, literal or wildcard. Max 1000. Literal entries
    /// resolve through the indexed hash, so mixing an exact id with a pattern costs nothing extra.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ids: Option<Vec<String>>,
    /// Nodes whose name matches any entry. Max 1000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub names: Option<Vec<String>>,
    /// Nodes whose source matches any entry. Max 1000.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<String>>,
    /// Labels the node must carry — **all** of them. Names are canonicalised server-side (upper
    /// snake case), so `"pump a"` finds the label stored as `PUMP_A`. Max 1000. A name matching no
    /// label matches no nodes; a list of only blanks restricts nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<Vec<String>>,
    /// Metadata entries that must all be present. See [`MetadataFilter`] for the key-only form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_time: Option<TimeFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<TimeFilter>,
}

/// Criteria for `POST /events/filter`, and the `filter` of `POST /events/search`.
///
/// Deliberately **not** a [`NodeFilter`]: events are not nodes. They live in ClickHouse, their id
/// is a UUID string rather than a long, and the table has no `name` column — only a description.
/// What it does instead is match that base field for field wherever ClickHouse can back it, so
/// [`external_ids`](Self::external_ids), [`sources`](Self::sources), [`metadata`](Self::metadata),
/// [`created_time`](Self::created_time), [`last_updated_time`](Self::last_updated_time) and
/// [`data_set_ids`](Self::data_set_ids) carry the same names and semantics they have there —
/// including the `*` / `%` wildcards and the case-insensitive, OR-within-a-list matching.
///
/// Note the filter has no `id` field. Events are keyed by a UUID; use
/// [`EventsService::by_ids`](crate::events::EventsService::by_ids) to look one up.
// Not PartialEq: it carries `Vec<IdAndExtId>`, which is intentionally non-comparable (see
// `IdAndExtId`). Nothing compares filters by value; equality here would be meaningless anyway.
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct BasicEventFilter {
    /// Events matching any of these external ids — literal or wildcard, exactly as
    /// [`NodeFilter::external_ids`]. Replaced the old single-valued `externalIdPrefix`:
    /// `"work_order_*"` says the same thing and composes with exact ids in one list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_ids: Option<Vec<String>>,
    /// Events whose source matches any of these patterns.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<String>>,
    /// Events of any of these types — a pattern list, so `["alarm", "warning"]` is one call where
    /// the old single-valued `type` needed two.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub types: Option<Vec<String>>,
    /// Events matching any of these sub-types. Same rules as [`types`](Self::types).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_types: Option<Vec<String>>,
    /// Events in any of these statuses. Same rules as [`types`](Self::types).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statuses: Option<Vec<String>>,
    /// Restrict to events in these data sets, each named by id or external id. A data set stands
    /// in for everything beneath it in the `BELONGS_TO` hierarchy, so naming a parent covers its
    /// children; a reference naming no data set contributes nothing.
    ///
    /// **`None` and empty differ here, unlike every other list on this filter.** `None` (the key
    /// omitted) is "no data set restriction"; an explicit `Some(vec![])` is "narrow to no data
    /// sets" and matches nothing. They are opposite answers, so the distinction has to survive
    /// onto the wire — which is why the key is skipped when `None` rather than sent as `null`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_set_ids: Option<Vec<IdAndExtId>>,
    /// When the event occurred. Distinct from [`created_time`](Self::created_time), which is when
    /// the platform ingested it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time: Option<TimeFilter>,
    /// Metadata entries that must all be present; a `None` value matches the key alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataFilter>,
    // The backend event filter reads related-resource selectors as a single `relatedResources`
    // array of `{id}` / `{externalId}` objects (its `Collection<IdCollection>`), matched with
    // `hasAll`. `IdAndExtId` serializes to exactly that shape (id as string, unset key omitted).
    /// The event must be related to **all** of these resources, each named by id, external id, or
    /// both.
    #[serde(default)]
    pub related_resources: Vec<IdAndExtId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_time: Option<TimeFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_updated_time: Option<TimeFilter>,
}

impl BasicEventFilter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_external_ids(&mut self, external_ids: &[&str]) -> &mut Self {
        self.external_ids = Some(external_ids.iter().map(|s| s.to_string()).collect());
        self
    }
    pub fn set_sources(&mut self, sources: &[&str]) -> &mut Self {
        self.sources = Some(sources.iter().map(|s| s.to_string()).collect());
        self
    }
    pub fn set_types(&mut self, types: &[&str]) -> &mut Self {
        self.types = Some(types.iter().map(|s| s.to_string()).collect());
        self
    }
    pub fn set_sub_types(&mut self, sub_types: &[&str]) -> &mut Self {
        self.sub_types = Some(sub_types.iter().map(|s| s.to_string()).collect());
        self
    }
    pub fn set_statuses(&mut self, statuses: &[&str]) -> &mut Self {
        self.statuses = Some(statuses.iter().map(|s| s.to_string()).collect());
        self
    }
    /// Narrow to these data sets, by numeric id. Pass an empty slice to narrow to *no* data sets
    /// (which matches nothing); leave the field `None` for no restriction at all.
    pub fn set_data_set_ids(&mut self, data_set_ids: &[u64]) -> &mut Self {
        self.data_set_ids = Some(data_set_ids.iter().map(|id| IdAndExtId::from_id(*id)).collect());
        self
    }
    /// Narrow to these data sets, each named by id, external id, or both.
    pub fn set_data_set_refs(&mut self, data_set_refs: &[IdAndExtId]) -> &mut Self {
        self.data_set_ids = Some(data_set_refs.to_vec());
        self
    }
    pub fn set_event_time(&mut self, event_time: &TimeFilter) -> &mut Self {
        self.event_time = Some(event_time.clone());
        self
    }
    pub fn set_created_time(&mut self, created_time: &TimeFilter) -> &mut Self {
        self.created_time = Some(created_time.clone());
        self
    }
    pub fn set_last_updated_time(&mut self, last_updated_time: &TimeFilter) -> &mut Self {
        self.last_updated_time = Some(last_updated_time.clone());
        self
    }
    pub fn set_metadata(&mut self, metadata: &MetadataFilter) -> &mut Self {
        self.metadata = Some(metadata.clone());
        self
    }
    /// Require the entry `key`, whatever value it carries.
    pub fn require_metadata_key(&mut self, key: &str) -> &mut Self {
        self.metadata
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string(), None);
        self
    }
    /// Require the entry `key` to carry exactly `value`.
    pub fn require_metadata(&mut self, key: &str, value: &str) -> &mut Self {
        self.metadata
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string(), Some(value.to_string()));
        self
    }
    /// Select events referencing these resources, each named by id, external id, or both.
    /// Replaces any selectors already set; the backend matches with `hasAll`, so all of them must
    /// be present on an event.
    pub fn set_related_resources(&mut self, related_resources: &[IdAndExtId]) -> &mut Self {
        self.related_resources = related_resources.to_vec();
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

/// How the backend should order a result page: the property to order by, and the direction.
///
/// Mirrors the api's `DataSort`. **One** property is used — the first the server recognises — and
/// `id` is always appended behind it, which is what makes the order *total*: a sort column alone is
/// not a position unless it is unique, and a page boundary falling inside a run of equal values
/// repeats or drops exactly those rows. The field is a list only because the wire shape is.
///
/// Sortable properties differ by endpoint:
///
/// - **nodes** (`/datasets/filter`, `/resources/filter`, `/timeseries/filter`): `id`, `externalId`,
///   `name`, `source`, `description`, `createdTime`, `lastUpdatedTime`, `dataSetId`. Default is
///   `createdTime` descending. Nulls sort last ascending and first descending.
/// - **events** (`/events/filter`): `eventTime`, `createdTime`, `lastUpdatedTime`, `externalId`,
///   `type`, `subType`, `status`, `source`, `dataSetId`. Default is `eventTime` **ascending** — the
///   order the keyset pages in, so paging does not change it.
///
/// An unsortable property falls back to the default rather than being rejected, so a misspelling
/// returns the default order — visibly not what was asked for. Anything that is not exactly `desc`
/// sorts ascending, so a malformed direction degrades predictably instead of silently reversing.
///
/// A [cursor](EventFilter::set_cursor) must be sent with the sort that produced it; a mismatch is a
/// **400**, not a silently wrong page.
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

/// The ordering and paging half of a node filter request, shared by the three node retrievers.
///
/// Flattened into each of them, so `sort` and `cursor` sit beside `filter` and `limit` on the wire.
/// Events carry the same two fields but declare them directly on
/// [`EventFilter`](crate::filters::EventFilter), which also has `advancedFilter`.
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PageRequest {
    /// Absent means the endpoint's default order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<DataSort>,
    /// Where a previous page stopped: the `next_cursor` of that response, verbatim.
    ///
    /// **Opaque.** It is base64 of a versioned encoding of the sort, the last row's value and its
    /// id — do not build or parse one. An unreadable cursor restarts from the first page rather
    /// than erroring, which is obviously wrong to a caller; guessing at half a position would
    /// silently skip or repeat the rows around the boundary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

impl PageRequest {
    /// Order by one property, ascending.
    pub fn asc(property: &str) -> Self {
        Self {
            sort: Some(DataSort::asc(property)),
            cursor: None,
        }
    }

    /// Order by one property, descending.
    pub fn desc(property: &str) -> Self {
        Self {
            sort: Some(DataSort::desc(property)),
            cursor: None,
        }
    }

    /// Continue a walk from a previous response's `next_cursor`, keeping this sort.
    pub fn after(mut self, cursor: &str) -> Self {
        self.cursor = Some(cursor.to_string());
        self
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

    /// A request carrying these criteria and the default limit.
    pub fn new(filter: BasicEventFilter) -> Self {
        Self {
            filter: Some(filter),
            ..Self::default()
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
    /// The value is the `next_cursor` of the previous response, verbatim. It is **opaque** — base64
    /// of a versioned encoding carrying the sort, the last row's value and its id — so do not build
    /// or parse one. An unreadable cursor restarts from the first page rather than erroring.
    ///
    /// Send it with the **same** [`sort`](Self::set_sort) that produced it: a cursor is a position
    /// in one particular order, and continuing it under another is a **400** rather than a page
    /// that is quietly short. Sorting by `subType` or `status` cannot be paged at all — those
    /// columns may be null, and a keyset boundary on them would skip the events that have no
    /// value — so a cursor sent with either is refused.
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

    /// `dataSetIds` is the one list where empty and absent mean opposite things: `[]` narrows to no
    /// data sets (matching nothing) while an absent key places no restriction. Collapsing the two —
    /// by skipping an empty vec, or by emitting `null` for `None` — turns "nothing" into
    /// "everything" or the reverse, and neither failure surfaces as an error.
    #[test]
    fn event_filter_distinguishes_empty_data_set_ids_from_absent() {
        let mut narrowed_to_nothing = BasicEventFilter::default();
        narrowed_to_nothing.set_data_set_ids(&[]);
        assert_eq!(
            serde_json::to_value(&narrowed_to_nothing).unwrap()["dataSetIds"],
            json!([]),
            "an explicit empty scope must reach the wire as []"
        );

        let unrestricted = BasicEventFilter::default();
        let value = serde_json::to_value(&unrestricted).unwrap();
        assert!(
            !value.as_object().unwrap().contains_key("dataSetIds"),
            "no restriction must omit the key, not send null: {value}"
        );
    }

    /// A metadata entry with no value asks for the key alone. It has to reach the wire as an
    /// explicit `null` — dropping the entry would widen the query to every event instead.
    #[test]
    fn event_filter_metadata_null_value_means_key_only() {
        let mut filter = BasicEventFilter::default();
        filter.require_metadata_key("health");
        assert_eq!(
            serde_json::to_value(&filter).unwrap()["metadata"],
            json!({"health": null})
        );

        let mut with_value = BasicEventFilter::default();
        with_value.require_metadata("health", "good");
        assert_eq!(
            serde_json::to_value(&with_value).unwrap()["metadata"],
            json!({"health": "good"})
        );
    }

    /// The plural pattern fields replaced `externalIdPrefix`, `type`, `subType`, `source` and
    /// `status`. Their old names must be gone from the payload: the backend drops unknown keys
    /// silently, so a leftover `type` would filter nothing and read as "no events matched".
    #[test]
    fn event_filter_sends_plural_pattern_fields_only() {
        let mut filter = BasicEventFilter::default();
        filter
            .set_external_ids(&["work_order_1234", "work_order_*"])
            .set_sources(&["SAP", "opc_*"])
            .set_types(&["alarm", "warning"])
            .set_sub_types(&["Electrical"])
            .set_statuses(&["OPEN"]);

        let value = serde_json::to_value(filter.build()).unwrap();
        assert_eq!(value["externalIds"], json!(["work_order_1234", "work_order_*"]));
        assert_eq!(value["sources"], json!(["SAP", "opc_*"]));
        assert_eq!(value["types"], json!(["alarm", "warning"]));
        assert_eq!(value["subTypes"], json!(["Electrical"]));
        assert_eq!(value["statuses"], json!(["OPEN"]));

        for retired in ["externalIdPrefix", "type", "subType", "source", "status", "id"] {
            assert!(
                value.get(retired).is_none(),
                "retired field {retired} is still on the wire: {value}"
            );
        }
    }

    /// An untouched filter must be an empty object. Anything it emits by default is a criterion the
    /// caller never asked for — and `relatedResources` is the deliberate exception, since the
    /// backend defaults it to an empty collection anyway.
    #[test]
    fn default_event_filter_sends_no_criteria() {
        let value = serde_json::to_value(BasicEventFilter::default()).unwrap();
        let keys: Vec<&String> = value.as_object().unwrap().keys().collect();
        assert_eq!(keys, vec!["relatedResources"], "unexpected default criteria: {value}");
        assert_eq!(value["relatedResources"], json!([]));
    }

    #[test]
    fn event_filter_round_trips() {
        let mut filter = BasicEventFilter::default();
        filter
            .set_types(&["alarm"])
            .set_data_set_ids(&[43])
            .require_metadata_key("health")
            .set_related_resource_external_ids(&["pump_a"]);
        let json_text = serde_json::to_string(&filter.build()).unwrap();

        let parsed: BasicEventFilter = serde_json::from_str(&json_text).unwrap();
        assert_eq!(parsed.types.as_deref(), Some(["alarm".to_string()].as_slice()));
        assert_eq!(parsed.metadata.unwrap().get("health"), Some(&None));
        assert_eq!(parsed.data_set_ids.unwrap().len(), 1);
        assert_eq!(parsed.related_resources.len(), 1);
    }

    /// The shared node criteria flatten into their owner, so `names` and friends sit alongside the
    /// type-specific fields rather than nesting under a `node` object the api does not read.
    #[test]
    fn node_filter_fields_are_flat_and_omitted_when_unset() {
        assert_eq!(
            serde_json::to_value(NodeFilter::default()).unwrap(),
            json!({}),
            "an empty node filter must place no restriction"
        );

        let filter = NodeFilter {
            ids: Some(vec![12, 18]),
            external_ids: Some(vec!["sap_work_orders".to_string(), "plant_*".to_string()]),
            names: Some(vec!["SAP*".to_string()]),
            sources: Some(vec!["sap".to_string()]),
            labels: Some(vec!["PUMP".to_string(), "CRITICAL".to_string()]),
            metadata: Some(HashMap::from([("owner".to_string(), Some("plant-a".to_string()))])),
            created_time: None,
            last_updated_time: None,
        };
        let value = serde_json::to_value(&filter).unwrap();
        // Ids go over the wire as strings, like every other id — a JavaScript client would round a
        // 64-bit number through a double and lose precision.
        assert_eq!(value["ids"], json!(["12", "18"]));
        assert_eq!(value["externalIds"], json!(["sap_work_orders", "plant_*"]));
        assert_eq!(value["names"], json!(["SAP*"]));
        assert_eq!(value["labels"], json!(["PUMP", "CRITICAL"]));
        assert_eq!(value["metadata"], json!({"owner": "plant-a"}));
        assert!(value.get("createdTime").is_none());

        let parsed: NodeFilter = serde_json::from_value(value).unwrap();
        assert_eq!(parsed, filter);
    }

    /// `TimeFilter` is what every timestamp criterion is typed as now — the api collapsed
    /// `CreatedTimeFilter` and `LastUpdatedTimeFilter` into it, since the field name already said
    /// which column it applied to.
    #[test]
    fn time_filter_emits_only_the_bounds_it_was_given() {
        let min = DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let max = DateTime::parse_from_rfc3339("2026-02-01T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let since = serde_json::to_value(TimeFilter::After { min }).unwrap();
        assert!(since.get("max").is_none(), "an open-ended window must not send a max: {since}");

        let until = serde_json::to_value(TimeFilter::Before { max }).unwrap();
        assert!(until.get("min").is_none());

        let between = serde_json::to_value(TimeFilter::Between { min, max }).unwrap();
        assert!(between.get("min").is_some() && between.get("max").is_some());
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

    /// `EventFilter::new` used to take the twelve `BasicEventFilter` fields positionally and drop
    /// every one of them, returning an unfiltered filter that matched the whole tenant. It takes
    /// the built criteria now, so there is nothing left to drop.
    #[test]
    fn new_keeps_its_criteria() {
        let mut basic = BasicEventFilter::default();
        basic.set_types(&["alarm"]).set_sources(&["SAP"]);
        let filter = EventFilter::new(basic.build());

        let carried = filter.filter().expect("filter should be populated");
        assert_eq!(carried.types.as_deref(), Some(["alarm".to_string()].as_slice()));
        assert_eq!(carried.sources.as_deref(), Some(["SAP".to_string()].as_slice()));
    }
}

/// The sort and cursor half of the request bodies, which the four filter endpoints share.
///
/// Kept separate from the criteria tests above because these are the fields a *wrong* shape breaks
/// silently: an unknown sort property falls back to the default rather than erroring, and an
/// unreadable cursor restarts the walk from page one. Neither raises anything a caller would see.
#[cfg(test)]
mod paging_serde {
    use super::*;
    use serde_json::json;

    #[test]
    fn page_request_is_omitted_entirely_when_unset() {
        // An unsorted, unpaged request must look exactly like one written before these fields
        // existed — a stray `"sort": null` is a field the server then has to decide about.
        assert_eq!(serde_json::to_value(PageRequest::default()).unwrap(), json!({}));
    }

    #[test]
    fn page_request_sends_one_property_and_a_direction() {
        assert_eq!(
            serde_json::to_value(PageRequest::asc("name")).unwrap(),
            json!({"sort": {"property": ["name"], "order": "asc"}})
        );
        assert_eq!(
            serde_json::to_value(PageRequest::desc("createdTime")).unwrap(),
            json!({"sort": {"property": ["createdTime"], "order": "desc"}})
        );
    }

    #[test]
    fn a_cursor_travels_with_the_sort_that_produced_it() {
        // The api rejects a cursor whose sort disagrees with the request, so the two have to go out
        // together. `after` keeps the sort rather than replacing the request with a bare cursor.
        let value = serde_json::to_value(PageRequest::desc("name").after("djE6bmFtZQ")).unwrap();
        assert_eq!(value["sort"]["property"], json!(["name"]));
        assert_eq!(value["sort"]["order"], "desc");
        assert_eq!(value["cursor"], "djE6bmFtZQ");
    }

    #[test]
    fn page_request_round_trips() {
        let request = PageRequest::asc("externalId").after("djE6ZXh0ZXJuYWxJZA");
        let parsed: PageRequest =
            serde_json::from_str(&serde_json::to_string(&request).unwrap()).unwrap();
        assert_eq!(parsed, request);
    }

    /// The event filter declares `sort` and `cursor` itself rather than flattening `PageRequest`,
    /// because it also carries `advancedFilter`. The wire shape must still be identical.
    #[test]
    fn the_event_filter_sends_the_same_sort_shape() {
        let mut filter = EventFilter::default();
        filter.set_sort(DataSort::desc("eventTime")).set_cursor("djE6ZXZlbnRUaW1l");

        let value = serde_json::to_value(filter.build()).unwrap();
        assert_eq!(value["sort"], json!({"property": ["eventTime"], "order": "desc"}));
        assert_eq!(value["cursor"], "djE6ZXZlbnRUaW1l");
    }

    /// `next_cursor` is read off the response and never sent. `DataWrapper` doubles as a request
    /// body for create/delete, so a cursor leaking into one of those would be a field the server
    /// does not declare.
    #[test]
    fn next_cursor_is_read_from_a_response_and_never_serialized() {
        use crate::generic::DataWrapper;

        let wrapper: DataWrapper<String> =
            serde_json::from_str(r#"{"items":["a"],"nextCursor":"djE6bmFtZXxhc2N8N3x2YQ"}"#).unwrap();
        assert_eq!(wrapper.next_cursor(), Some("djE6bmFtZXxhc2N8N3x2YQ"));

        let value = serde_json::to_value(&wrapper).unwrap();
        assert!(
            value.get("nextCursor").is_none() && value.get("next_cursor").is_none(),
            "a cursor must not be echoed back inside a request body: {value}"
        );
    }

    /// An absent `nextCursor` is the end-of-walk signal, so it has to survive as `None` rather than
    /// becoming an empty string — "keep going while it is present" is the whole client loop.
    #[test]
    fn an_absent_next_cursor_reads_as_none() {
        use crate::generic::DataWrapper;

        let wrapper: DataWrapper<String> = serde_json::from_str(r#"{"items":[]}"#).unwrap();
        assert_eq!(wrapper.next_cursor(), None);
    }
}
