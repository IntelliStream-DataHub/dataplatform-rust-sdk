use super::{EdgeProxy, RelForm, RelatedNode, RelationDirection};
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
    assert_eq!(
        parsed.metadata.get("priority").map(String::as_str),
        Some("high")
    );
}

/// The shape the API actually sends: every id is a JSON *string*, because ids are 64-bit and a
/// JavaScript client would lose precision on a number. `start` and `end` were left as bare `u64`
/// when the rest of `EdgeProxy` was converted, so any response carrying relations — a
/// `resources/create` with edges, or anything off `/edges` — failed to deserialize outright.
#[test]
fn edge_proxy_accepts_string_ids_on_every_id_field() {
    let payload = json!({
        "id": "5932",
        "start": "13689",
        "end": "13690",
        "type": "SDK_TEST_LINK",
        "relationshipTypeId": "21",
        "description": null,
        "metadata": {}
    });
    let parsed: EdgeProxy = serde_json::from_value(payload).unwrap();
    assert_eq!(parsed.id, Some(5932));
    assert_eq!(parsed.start, Some(13689));
    assert_eq!(parsed.end, Some(13690));
    assert_eq!(parsed.relationship_type_id, Some(21));
}

/// Ids go back out as strings, matching what `id` and `relationship_type_id` already did.
#[test]
fn edge_proxy_serializes_ids_as_strings() {
    let edge = EdgeProxy {
        id: Some(5932),
        start: Some(13689),
        end: Some(13690),
        relationship_type: None,
        description: None,
        relationship_type_id: None,
        metadata: Default::default(),
    };
    let json = serde_json::to_value(&edge).unwrap();
    assert_eq!(json["id"], json!("5932"));
    assert_eq!(json["start"], json!("13689"));
    assert_eq!(json["end"], json!("13690"));
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
    assert_eq!(
        v.get("fromExternalId").and_then(Value::as_str),
        Some("pump_a")
    );
    assert_eq!(
        v.get("toExternalId").and_then(Value::as_str),
        Some("tank_b")
    );
    assert_eq!(
        v.get("relationshipType").and_then(Value::as_str),
        Some("flows_to")
    );
    // skip_serializing_if drops absent optionals
    assert!(v.get("fromId").is_none());
    assert!(v.get("toId").is_none());
    assert!(v.get("id").is_none());
    assert!(v.get("description").is_none());
    assert!(v.get("dataSetId").is_none());
    assert!(
        v.get("metadata").is_none(),
        "empty metadata should be skipped"
    );
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
fn related_node_output_shape_serializes_ids_as_strings() {
    let rn = RelatedNode {
        id: Some(34),
        external_id: Some("sensor_abc".to_string()),
        relationship_type: Some("PUBLISHES_DATA_TO".to_string()),
        direction: Some(RelationDirection::Outbound),
        edge_id: Some(98231),
    };
    let v: Value = serde_json::to_value(&rn).unwrap();
    // Ids serialize as JSON strings on the wire (the field stays u64 in Rust).
    assert_eq!(v.get("id").and_then(Value::as_str), Some("34"));
    assert_eq!(v.get("edgeId").and_then(Value::as_str), Some("98231"));
    assert_eq!(
        v.get("externalId").and_then(Value::as_str),
        Some("sensor_abc")
    );
    assert_eq!(
        v.get("relationshipType").and_then(Value::as_str),
        Some("PUBLISHES_DATA_TO")
    );
    assert_eq!(v.get("direction").and_then(Value::as_str), Some("OUTBOUND"));
}

#[test]
fn related_node_input_helpers_omit_output_only_fields() {
    let by_id = RelatedNode::from_id(42, "flows_to");
    let v: Value = serde_json::to_value(&by_id).unwrap();
    assert_eq!(v.get("id").and_then(Value::as_str), Some("42"));
    assert_eq!(
        v.get("relationshipType").and_then(Value::as_str),
        Some("flows_to")
    );
    // direction / edgeId / externalId are output-only or absent -> skipped.
    assert!(v.get("direction").is_none());
    assert!(v.get("edgeId").is_none());
    assert!(v.get("externalId").is_none());

    let by_ext = RelatedNode::from_external_id("pump_a", "flows_to");
    let v: Value = serde_json::to_value(&by_ext).unwrap();
    assert_eq!(v.get("externalId").and_then(Value::as_str), Some("pump_a"));
    assert!(v.get("id").is_none());
    assert!(v.get("direction").is_none());
    assert!(v.get("edgeId").is_none());
}

#[test]
fn related_node_round_trip_from_server_payload() {
    let payload = json!({
        "id": "34",
        "externalId": "sensor_abc",
        "relationshipType": "PROCESSED_BY",
        "direction": "INBOUND",
        "edgeId": "98231"
    });
    let parsed: RelatedNode = serde_json::from_value(payload).unwrap();
    assert_eq!(parsed.id, Some(34));
    assert_eq!(parsed.external_id.as_deref(), Some("sensor_abc"));
    assert_eq!(parsed.relationship_type.as_deref(), Some("PROCESSED_BY"));
    assert_eq!(parsed.direction, Some(RelationDirection::Inbound));
    assert_eq!(parsed.edge_id, Some(98231));
}

#[test]
fn related_node_tolerates_empty_payload() {
    let parsed: RelatedNode = serde_json::from_str("{}").unwrap();
    assert!(parsed.id.is_none());
    assert!(parsed.external_id.is_none());
    assert!(parsed.relationship_type.is_none());
    assert!(parsed.direction.is_none());
    assert!(parsed.edge_id.is_none());
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

/// Pure checks of the two type-catalogue shapes.
#[test]
fn relationship_type_forms_use_the_wire_names() {
    use super::{RelTypeForm, RelationshipType};

    let form = RelTypeForm::new("Flows To")
        .with_description("upstream to downstream")
        .with_i18n_code("rel.flows_to");
    assert_eq!(
        serde_json::to_value(&form).unwrap(),
        json!({
            "name": "Flows To",
            "description": "upstream to downstream",
            "i18nCode": "rel.flows_to",
        }),
        "unset fields stay off the wire; the server normalises the name itself"
    );

    // The stored shape: ids as strings, and every field but `name` optional.
    let stored: RelationshipType = serde_json::from_value(json!({
        "id": "88",
        "name": "FLOWS_TO",
        "description": null,
        "i18nCode": null,
    }))
    .expect("a stored relationship type should parse");
    assert_eq!(stored.id, Some(88));
    assert_eq!(stored.name, "FLOWS_TO");
    assert!(stored.hash.is_none());
}

mod live {
    use crate::create_api_service;
    use crate::generic::IdAndExtId;
    use crate::relations::{RelForm, RelTypeForm};
    use crate::resources::{RelatedResourcesForm, Resource, ResourceNetwork};
    use crate::tests::cleanup::cleanup_resources;
    use crate::tests::polling::poll_until;

    /// Relationship types cannot be deleted through the API, so this one is seeded once and
    /// then reused — every run after the first sees it as an existing type.
    const SEEDED_TYPE_NAME: &str = "Sdk Test Rel Type";

    /// A distinct external id per run.
    ///
    /// These tests link two resources and then act on the edge between them, so the graph shape
    /// they see has to be theirs alone. With fixed ids, rows left by an earlier run stayed
    /// reachable and silently supplied `b` with a second path to the graph root — which changed
    /// what `edges.delete` was allowed to do and made the outcome depend on database history
    /// rather than on the test.
    fn unique_id(kind: &str) -> String {
        crate::tests::ids::unique_id(&format!("edge_{kind}"))
    }

    /// Wait until the graph projection, reachable from `from`, satisfies `predicate`.
    ///
    /// `edges.delete` refuses anything that would strand a node, and that check runs against the
    /// graph projection, which lags the write. Acting too early gets the *wrong* answer rather than
    /// an error — a delete that should be refused succeeds, because the projection cannot yet see
    /// what the edge was holding up. Measured: deleting immediately after create succeeded 6 times
    /// out of 6; the same delete 500ms later was refused 6 out of 6.
    ///
    /// The predicate has to describe the *edges*, not just the nodes: after deleting one link the
    /// node count is unchanged, so waiting on nodes alone returns immediately and the next delete
    /// races the projection again.
    async fn await_graph<P>(api: &crate::ApiService, from: &str, what: &str, predicate: P)
    where
        P: Fn(&ResourceNetwork) -> bool,
    {
        let form = RelatedResourcesForm {
            id: None,
            external_id: Some(from.to_string()),
            depth: -1,
            relationship_types: None,
            limit: 100,
            excluded_labels: vec![],
        };
        let net = poll_until(
            || api.resources.fetch_related(&form),
            |r| r.as_ref().map(&predicate).unwrap_or(false),
        )
        .await
        .unwrap_or_default();
        assert!(
            predicate(&net),
            "graph projection did not catch up: {what} (saw {} nodes, {} edges)",
            net.nodes().len(),
            net.edges().len()
        );
    }

    /// Read an edge back, resolve it to its endpoints, then delete it — both when that is allowed
    /// and when it is not.
    ///
    /// The shape is a triangle, `a -> b`, `b -> c`, `a -> c`, because two nodes cannot express the
    /// rule. On a bare `a -> b` pair the single edge is the only thing holding `b` to the graph
    /// root, so *every* delete is refused and the allowed case is untestable. With the triangle,
    /// `c` has two routes: dropping `a -> c` leaves it reachable via `b`, and dropping `b -> c`
    /// afterwards would strand it.
    #[tokio::test]
    async fn test_edge_get_by_ids_and_delete() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();
        let (a, b, c) = (
            unique_id("node_a"),
            unique_id("node_b"),
            unique_id("node_c"),
        );
        let (a, b, c) = (a.as_str(), b.as_str(), c.as_str());
        let sel = vec![
            IdAndExtId::from_external_id(a),
            IdAndExtId::from_external_id(b),
            IdAndExtId::from_external_id(c),
        ];

        let created = api
            .resources
            .create(
                vec![
                    // `a` roots the component, so "reachable from the graph root" means
                    // "reachable from `a`" and the stranding check has one answer.
                    Resource {
                        external_id: a.to_string(),
                        name: "sdk test edge node a".to_string(),
                        // The create endpoint rejects a node with null labels.
                        labels: Some(vec!["ASSET".to_string()]),
                        is_root: true,
                        ..Resource::new()
                    },
                    Resource {
                        external_id: b.to_string(),
                        name: "sdk test edge node b".to_string(),
                        // The create endpoint rejects a node with null labels.
                        labels: Some(vec!["ASSET".to_string()]),
                        is_root: false,
                        ..Resource::new()
                    },
                    Resource {
                        external_id: c.to_string(),
                        name: "sdk test edge node c".to_string(),
                        // The create endpoint rejects a node with null labels.
                        labels: Some(vec!["ASSET".to_string()]),
                        is_root: false,
                        ..Resource::new()
                    },
                ],
                vec![
                    RelForm::by_external_ids(a, b, "SDK_TEST_LINK"),
                    RelForm::by_external_ids(b, c, "SDK_TEST_LINK"),
                    RelForm::by_external_ids(a, c, "SDK_TEST_LINK"),
                ],
            )
            .await?;
        let mut cleanup = cleanup_resources(vec![c.to_string(), b.to_string(), a.to_string()]);

        let nodes = created.nodes().unwrap_or_default();
        let relations = created.relations().cloned().unwrap_or_default();
        assert_eq!(relations.len(), 3, "all three links should be created");
        let id_of = |ext: &str| {
            nodes
                .iter()
                .find(|n| n.external_id == ext)
                .and_then(|n| n.id)
        };
        let edge_between = |from: &str, to: &str| {
            let (f, t) = (id_of(from), id_of(to));
            relations
                .iter()
                .find(|e| e.start == f && e.end == t)
                .and_then(|e| e.id)
                .unwrap_or_else(|| panic!("no edge {from} -> {to} in the create response"))
        };
        let a_to_c = edge_between(a, c);
        let b_to_c = edge_between(b, c);

        // --- get ---
        let fetched = api.edges.get(a_to_c).await?;
        assert_eq!(fetched.get_http_status_code(), Some(200));
        assert_eq!(fetched.get_items()[0].id, Some(a_to_c));
        assert_eq!(
            fetched.get_items()[0].relationship_type.as_deref(),
            Some("SDK_TEST_LINK")
        );

        // --- by_ids: a graph, both endpoints resolved in the same response ---
        let graph = api.edges.by_ids(&vec![IdAndExtId::from_id(a_to_c)]).await?;
        assert_eq!(graph.relations().map(|r| r.len()), Some(1));
        let resolved = graph.nodes().unwrap_or_default();
        assert_eq!(resolved.len(), 2, "byids should resolve both endpoints");
        assert!(resolved.iter().any(|n| n.external_id == a));
        assert!(resolved.iter().any(|n| n.external_id == c));

        await_graph(&api, a, "all three links visible", |n| n.edges().len() >= 3).await;

        // --- delete, allowed: c keeps its route to the root through b ---
        let deleted = api.edges.delete(&vec![IdAndExtId::from_id(a_to_c)]).await?;
        assert_eq!(deleted.get_http_status_code(), Some(204));
        let gone = api
            .edges
            .get(a_to_c)
            .await
            .expect_err("a deleted edge should read back as 404");
        assert_eq!(gone.get_status().as_u16(), 404);

        // Deleting an id that is already gone is a silent no-op, not an error.
        let again = api.edges.delete(&vec![IdAndExtId::from_id(a_to_c)]).await?;
        assert_eq!(again.get_http_status_code(), Some(204));

        // The endpoints themselves are untouched by an edge delete.
        assert_eq!(
            api.resources
                .by_ids(&sel)
                .await?
                .nodes()
                .unwrap_or_default()
                .len(),
            3,
            "deleting an edge must not touch the resources it connected"
        );

        // --- delete, refused: b -> c is now c's only route to the root ---
        // Wait for the *removal* to land in the projection: while it still believes `a -> c`
        // exists, `c` looks doubly-connected and the next delete would be allowed.
        await_graph(&api, a, "the deleted link is gone", |n| {
            !n.edges().iter().any(|e| e.id == Some(a_to_c))
        })
        .await;
        let refused = api
            .edges
            .delete(&vec![IdAndExtId::from_id(b_to_c)])
            .await
            .expect_err("deleting c's last route to the root should be refused");
        assert_eq!(
            refused.get_status().as_u16(),
            400,
            "a stranding delete is a client error, not a server fault: {refused:?}"
        );
        // With `a` declared root, the victim is unambiguous: `c` is the node that loses its last
        // route. Without a declared root this named a different node between runs, because the
        // projection picked the anchor itself.
        let message = refused.get_message();
        assert!(
            message.contains("disconnect"),
            "the refusal should say what it is refusing: {message}"
        );
        assert!(
            message.contains(c),
            "the refusal should name {c} as the stranded resource: {message}"
        );

        // Tear down in one request. Deleting these nodes individually is perfectly legal —
        // leaf-first works, and a node delete cascades its own edges — but each step has to wait
        // for the projection before the next, or the stranding check still sees the node just
        // removed and refuses to delete the one that was holding it up. Deleting the set together
        // sidesteps that race instead of polling between every step.
        api.resources.delete(&sel).await?;
        cleanup.disarm();
        Ok(())
    }

    /// The two read paths report "not found" differently, and deliberately so: a single fetch
    /// 404s, a batch lookup answers 200 with an empty collection. Pinned because the difference
    /// decides how callers check for absence.
    #[tokio::test]
    async fn test_unknown_edge_id_404s_but_byids_is_empty() -> Result<(), Box<dyn std::error::Error>>
    {
        let api = create_api_service();
        let missing = vec![IdAndExtId::from_id(999_999_999)];

        let err = api
            .edges
            .get(999_999_999)
            .await
            .expect_err("a single fetch of an unknown id should 404");
        assert_eq!(err.get_status().as_u16(), 404);

        let graph = api.edges.by_ids(&missing).await?;
        assert!(graph.relations().map(|r| r.is_empty()).unwrap_or(true));
        assert!(graph.nodes().unwrap_or_default().is_empty());
        Ok(())
    }

    /// The type catalogue: list, create, and the two edges of create's behaviour — a re-created
    /// type is omitted from the response, and an unusable name is a 400.
    #[tokio::test]
    async fn test_relationship_types() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();

        let listed = api.edges.types().await?;
        assert_eq!(listed.get_http_status_code(), Some(200));
        assert!(
            listed.get_items().iter().any(|t| t.name == "BELONGS_TO"),
            "BELONGS_TO is created by the platform itself and should always be present"
        );

        // Names normalise to uppercase snake case, so this lands on SDK_TEST_REL_TYPE.
        // There is no delete-type endpoint, so the type survives between runs: the first run
        // creates it, later runs find it already there (a duplicate, see the test below).
        match api
            .edges
            .create_types(&vec![
                RelTypeForm::new(SEEDED_TYPE_NAME).with_description("from the SDK tests")
            ])
            .await
        {
            Ok(created) => {
                assert_eq!(created.get_http_status_code(), Some(200));
                if let Some(t) = created.get_items().first() {
                    assert_eq!(t.name, "SDK_TEST_REL_TYPE", "names are normalised");
                }
            }
            // Seeded by an earlier run.
            Err(e) if e.get_status().as_u16() == 409 => {}
            Err(e) => return Err(e.into()),
        }
        assert!(
            api.edges
                .types()
                .await?
                .get_items()
                .iter()
                .any(|t| t.name == "SDK_TEST_REL_TYPE"),
            "the type should be in the catalogue after create"
        );

        // A name that normalises to nothing is refused.
        let bad = api
            .edges
            .create_types(&vec![RelTypeForm::new("!!!")])
            .await
            .expect_err("a name of only symbols should be rejected");
        assert_eq!(bad.get_status().as_u16(), 400);

        Ok(())
    }

    /// `POST /edges/create` links resources that already exist. Skips when the backend predates
    /// the endpoint (it answers 405 there) rather than failing a checkout against an older API.
    #[tokio::test]
    async fn test_create_edge_between_existing_resources() -> Result<(), Box<dyn std::error::Error>>
    {
        let api = create_api_service();
        let (a, b) = (unique_id("link_a"), unique_id("link_b"));
        let (a, b) = (a.as_str(), b.as_str());
        let sel = vec![
            IdAndExtId::from_external_id(a),
            IdAndExtId::from_external_id(b),
        ];

        api.resources
            .create(
                vec![
                    // Rooted for the same reason as the triangle above: an unrooted component
                    // leaves the projection to pick its own anchor.
                    Resource {
                        external_id: a.to_string(),
                        name: "sdk test edge link a".to_string(),
                        // The create endpoint rejects a node with null labels.
                        labels: Some(vec!["ASSET".to_string()]),
                        is_root: true,
                        ..Resource::new()
                    },
                    Resource {
                        external_id: b.to_string(),
                        name: "sdk test edge link b".to_string(),
                        // The create endpoint rejects a node with null labels.
                        labels: Some(vec!["ASSET".to_string()]),
                        is_root: false,
                        ..Resource::new()
                    },
                ],
                vec![],
            )
            .await?;
        let mut cleanup = cleanup_resources(vec![a.to_string(), b.to_string()]);

        let form = RelForm::by_external_ids(a, b, "SDK_TEST_LINK");
        let created = match api.edges.create(&vec![form.clone()]).await {
            Ok(created) => created,
            Err(e) if e.get_status().as_u16() == 405 => {
                println!(
                    "SKIP test_create_edge_between_existing_resources: this backend has no \
                     POST /edges/create (405); edges can only be made via resources.create here."
                );
                api.resources.delete(&sel).await?;
                cleanup.disarm();
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };

        assert_eq!(
            created.get_http_status_code(),
            Some(201),
            "creating relationships answers 201, not 200"
        );
        let edge_id = created.get_items()[0]
            .id
            .expect("the new edge should have an id");

        // The (start, end, type) triple is unique, so the same link again is a conflict.
        let dup = api.edges.create(&vec![form]).await;
        assert_eq!(
            dup.map(|_| ()).unwrap_err().get_status().as_u16(),
            409,
            "re-creating the same relationship should conflict"
        );

        // No separate edge delete here: this link is b's only route to the graph root, so removing
        // it alone is refused (see test_edge_get_by_ids_and_delete for both sides of that rule).
        // Deleting the resources takes the edge with them.
        api.resources.delete(&sel).await?;
        cleanup.disarm();
        Ok(())
    }

    /// Re-registering a relationship type that already exists must conflict, the way every other
    /// duplicate in this API does.
    ///
    /// **Currently red, on purpose.** `EdgeService.saveRelationshipType` has no find-or-create: it
    /// builds a fresh entity and saves it unconditionally, so a duplicate name collides on
    /// `relationship_hash_key` at *commit* time — after the handler has returned. The
    /// `DataIntegrityViolationException` escapes past the handler's `catch`, and the caller gets a
    /// **200 with an empty body**: the status of a success with the body of a crash.
    ///
    /// The batch case is the damaging one. `createRelationshipTypes` saves every form in one
    /// transaction, so a single duplicate rolls the whole thing back — valid new types in the same
    /// request are discarded too, and the response still says 200. Nothing tells the caller.
    ///
    /// This test encodes the intended behaviour (409, like `POST /edges/create` on a duplicate
    /// edge) and will pass once the server-side fix lands.
    #[tokio::test]
    async fn test_duplicate_relationship_type_conflicts() -> Result<(), Box<dyn std::error::Error>>
    {
        let api = create_api_service();

        // Make sure the type is there, whichever run this is.
        let _ = api
            .edges
            .create_types(&vec![RelTypeForm::new(SEEDED_TYPE_NAME)])
            .await;

        let dup = api
            .edges
            .create_types(&vec![RelTypeForm::new(SEEDED_TYPE_NAME)])
            .await
            .expect_err("re-registering an existing relationship type should conflict");
        assert_eq!(
            dup.get_status().as_u16(),
            409,
            "a duplicate type should answer 409, not a bodyless 200"
        );

        // A batch carrying one duplicate must not quietly swallow the valid entries beside it.
        let fresh = "Sdk Test Rel Type Batch";
        let batch = api
            .edges
            .create_types(&vec![
                RelTypeForm::new(fresh),
                RelTypeForm::new(SEEDED_TYPE_NAME),
            ])
            .await
            .expect_err("a batch containing an existing type should conflict");
        assert_eq!(batch.get_status().as_u16(), 409);
        assert!(
            !api.edges
                .types()
                .await?
                .get_items()
                .iter()
                .any(|t| t.name == "SDK_TEST_REL_TYPE_BATCH"),
            "the rejected batch is all-or-nothing, so the new type must not have been created"
        );

        Ok(())
    }
}
