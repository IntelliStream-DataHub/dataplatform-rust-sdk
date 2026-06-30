use super::{EdgeProxy, RelForm};
use serde_json::{json, Value};
use std::collections::HashMap;

#[test]
fn edge_proxy_serializes_type_field_with_wire_name() {
    let edge = EdgeProxy {
        id: Some(341),
        start: Some(5_677_892),
        end: Some(5_677_893),
        relationship_type: Some("FLOWS_TO".to_string()),
        description: None,
        relationship_type_id: Some(12),
        metadata: HashMap::new(),
    };
    let v: Value = serde_json::to_value(&edge).unwrap();
    assert_eq!(v.get("type").and_then(Value::as_str), Some("FLOWS_TO"));
    assert!(v.get("relationshipType").is_none());
    assert!(v.get("edgeType").is_none());
    // Ids serialize as JSON strings on the wire (the field stays u64 in Rust).
    assert_eq!(
        v.get("relationshipTypeId").and_then(Value::as_str),
        Some("12")
    );
}

#[test]
fn edge_proxy_round_trip_without_relationship_type_id() {
    let payload = json!({
        "id": 1,
        "start": 100,
        "end": 200,
        "type": "PROCESSED_BY",
        "metadata": { "priority": "high" }
    });
    let parsed: EdgeProxy = serde_json::from_value(payload).unwrap();
    assert_eq!(parsed.id, Some(1));
    assert_eq!(parsed.start, Some(100));
    assert_eq!(parsed.end, Some(200));
    assert_eq!(parsed.relationship_type.as_deref(), Some("PROCESSED_BY"));
    assert_eq!(parsed.relationship_type_id, None);
    assert_eq!(parsed.metadata.get("priority").map(String::as_str), Some("high"));
}

#[test]
fn edge_proxy_tolerates_completely_empty_payload() {
    let parsed: EdgeProxy = serde_json::from_str("{}").unwrap();
    assert!(parsed.id.is_none());
    assert!(parsed.metadata.is_empty());
}

#[test]
fn rel_form_by_external_ids_omits_id_fields() {
    let rel = RelForm::by_external_ids("pump_a", "tank_b", "flows_to");
    let v: Value = serde_json::to_value(&rel).unwrap();
    assert_eq!(v.get("fromExternalId").and_then(Value::as_str), Some("pump_a"));
    assert_eq!(v.get("toExternalId").and_then(Value::as_str), Some("tank_b"));
    assert_eq!(v.get("relationshipType").and_then(Value::as_str), Some("flows_to"));
    // skip_serializing_if drops absent optionals
    assert!(v.get("fromId").is_none());
    assert!(v.get("toId").is_none());
    assert!(v.get("id").is_none());
    assert!(v.get("description").is_none());
    assert!(v.get("dataSetId").is_none());
    assert!(v.get("metadata").is_none(), "empty metadata should be skipped");
}

#[test]
fn rel_form_by_ids_omits_external_id_fields() {
    let rel = RelForm::by_ids(42, 43, "FLOWS_TO");
    let v: Value = serde_json::to_value(&rel).unwrap();
    assert_eq!(v.get("fromId").and_then(Value::as_str), Some("42"));
    assert_eq!(v.get("toId").and_then(Value::as_str), Some("43"));
    assert!(v.get("fromExternalId").is_none());
    assert!(v.get("toExternalId").is_none());
}

#[test]
fn rel_form_round_trip_with_all_fields() {
    let mut metadata = HashMap::new();
    metadata.insert("k".to_string(), "v".to_string());
    let original = RelForm {
        id: Some(9),
        from_external_id: Some("a".to_string()),
        to_external_id: Some("b".to_string()),
        from_id: None,
        to_id: None,
        relationship_type: "FLOWS_TO".to_string(),
        relationship_type_id: Some(7),
        metadata,
        data_set_id: Some(3),
        description: Some("desc".to_string()),
    };
    let json = serde_json::to_string(&original).unwrap();
    let parsed: RelForm = serde_json::from_str(&json).unwrap();
    assert_eq!(original, parsed);
}
