use super::*;
use crate::create_api_service;
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::generic::{IdAndExtId, SearchAndFilterForm};
use crate::relations::RelForm;
use crate::tests::cleanup::{
    cleanup_datasets, cleanup_functions, cleanup_resources, cleanup_timeseries,
};
use maplit::hashmap;
use uuid::Uuid;
use crate::tests::ids::unique_id;
use crate::tests::polling::poll_until;
use crate::nodes::NodeType;

fn create_test_resources() -> Vec<Resource> {
    // helper function to create test resources will
    let count = 2;
    let ids = (0..count).map(|_| unique_id("resource")).collect::<Vec<String>>();
    let res1 = Resource {
        // used to be a serde skip if zero here. don't understand why
        id: None,
        external_id: ids[0].clone(),
        name: format!("Rust SDK Test Resource {}", ids[0]),
        metadata: Some(hashmap! {
            "foo".to_string() => "bar".to_string(),
            "is_test".to_string() => "true".to_string(),
            "test_source".to_string() => "rust_sdk".to_string()
        }),
        description: Some("root_test_data_set".to_string()),
        is_root: true,
        data_set_id: None,
        source: Some("Test_Rust_SDK".to_string()),
        labels: Some(vec!["ASSET".to_string()]),
        related_resources: vec![],
        geolocation: None,
        created_time: None,
        last_updated_time: None,
    };
    let res2 = Resource {
        // used to be a serde skip if zero here. don't understand why
        id: None,
        external_id: ids[1].clone(),
        name: format!("Rust SDK Test Resource {}", ids[1]),
        metadata: None,
        description: None,
        is_root: false,
        data_set_id: None,
        source: Some("Test_Rust_SDK".to_string()),
        labels: Some(vec!["ASSET".to_string()]),
        related_resources: vec![],
        geolocation: None,
        created_time: None,
        last_updated_time: None,
    };
    vec![res1, res2]
}
#[tokio::test]
async fn test_create_and_delete_resources() -> Result<(), ResponseError> {
    let api_service = create_api_service();
    let test_resources = create_test_resources();
    // Delete timeseries first, in case a test failed and the time series exists
    let ids = test_resources
        .iter()
        .map(|r| IdAndExtId::from_external_id(&r.external_id))
        .collect::<Vec<IdAndExtId>>();
    api_service.resources.delete(&ids).await?;

    assert_eq!(
        api_service.resources.by_ids(&ids).await?.nodes().unwrap(),
        vec![]
    );

    let result = api_service
        .resources
        .create(test_resources.clone(), vec![])
        .await?;
    let mut resource_cleanup = cleanup_resources(
        test_resources
            .iter()
            .map(|r| r.external_id.clone())
            .collect(),
    );
    let res_ids = result
        .nodes()
        .unwrap()
        .iter()
        .map(|r| to_snake_lower_cased_allow_start_with_digits(r.external_id()))
        .collect::<Vec<String>>();
    let input_ids = test_resources
        .iter()
        .map(|r| to_snake_lower_cased_allow_start_with_digits(&r.external_id))
        .collect::<Vec<String>>();
    assert_eq!(res_ids, input_ids);

    //let val = &result.json::<Resource>().await.unwrap();

    // Delete resources
    api_service.resources.delete(&ids).await?;
    resource_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    assert_eq!(
        api_service.resources.by_ids(&ids).await?.nodes().unwrap(),
        vec![]
    );

    Ok(())
}
#[tokio::test]
async fn test_search_resources() -> Result<(), ResponseError> {
    let api_service = create_api_service();
    let test_resources = create_test_resources();
    // Delete timeseries first, in case a test failed and the time series exists
    let query = SearchAndFilterForm::<ResourceFilter>::new("test resource").with_limit(5);
    let query2 = SearchAndFilterForm::<ResourceFilter>::new("test resource");

    let test_data = api_service
        .resources
        .create(test_resources.clone(), vec![])
        .await?;
    let mut resource_cleanup = cleanup_resources(
        test_resources
            .iter()
            .map(|r| r.external_id.clone())
            .collect(),
    );
    let search_result = api_service.resources.search(&query).await?;
    let search_result2 = api_service.resources.search(&query2).await?;
    println!("{:?}", search_result2.get_items().len());
    assert!(search_result.get_items().len() <= 5);
    // Case-insensitively, and against description too. The server's full-text index covers
    // `name || external_id || description` and matches case-insensitively, so a hit need not carry
    // the query verbatim in any one field. This assertion used to compare lowercase "test" against
    // the fixture's "Rust SDK Test Resource", and only passed because external ids were silently
    // lowercased server-side; the naming-policy work stopped that rewriting, so they are now
    // stored verbatim and the old comparison rejected the test's own fixture.
    assert!(search_result.get_items().iter().all(|r| {
        let haystack = format!(
            "{} {} {}",
            r.name().unwrap_or(""),
            r.external_id(),
            r.description().unwrap_or("")
        )
        .to_lowercase();
        haystack.contains("test")
    }));
    let resulting_ids = test_data
        .nodes()
        .unwrap()
        .iter()
        .map(|r| IdAndExtId::from_external_id(r.external_id()))
        .collect::<Vec<IdAndExtId>>();
    api_service.resources.delete(&resulting_ids).await?;
    resource_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    Ok(())
}

#[tokio::test]
async fn test_create_with_flows_to_relation() -> Result<(), ResponseError> {
    let api_service = create_api_service();
    let test_resources = create_test_resources();
    let from_ext = test_resources[0].external_id.clone();
    let to_ext = test_resources[1].external_id.clone();

    // The backend blocks deleting a node that is the START of an edge, and a
    // single batch containing both endpoints still trips that check, so the END
    // node (to_ext) must be deleted in its own request first — which auto-deletes
    // the edge — then the START node (from_ext).
    let end_id = vec![IdAndExtId::from_external_id(&to_ext)];
    let start_id = vec![IdAndExtId::from_external_id(&from_ext)];
    let _ = api_service.resources.delete(&end_id).await;
    let _ = api_service.resources.delete(&start_id).await;

    let relations = vec![RelForm::by_external_ids(
        from_ext.clone(),
        to_ext.clone(),
        "flows_to",
    )];
    let result = api_service
        .resources
        .create(test_resources.clone(), relations)
        .await?;
    let mut resource_cleanup = cleanup_resources(vec![from_ext.clone(), to_ext.clone()]);

    let nodes = result.nodes().unwrap();
    assert_eq!(nodes.len(), 2);

    let edges = result
        .relations()
        .expect("response should include a relations array");
    assert_eq!(edges.len(), 1);
    let edge = &edges[0];
    assert!(edge.id.is_some(), "server should assign an edge id");
    assert!(edge.start.is_some(), "server should populate start node id");
    assert!(edge.end.is_some(), "server should populate end node id");
    assert_eq!(
        edge.relationship_type.as_deref(),
        Some("FLOWS_TO"),
        "server should snake-upper-case the relationship type"
    );

    api_service.resources.delete(&end_id).await?;
    api_service.resources.delete(&start_id).await?;
    resource_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    Ok(())
}

#[tokio::test]
async fn test_create_with_empty_relations() -> Result<(), ResponseError> {
    let api_service = create_api_service();
    let test_resources = create_test_resources();
    let ids = test_resources
        .iter()
        .map(|r| IdAndExtId::from_external_id(&r.external_id))
        .collect::<Vec<IdAndExtId>>();
    api_service.resources.delete(&ids).await?;

    let result = api_service
        .resources
        .create(test_resources.clone(), vec![])
        .await?;
    let mut resource_cleanup = cleanup_resources(
        test_resources
            .iter()
            .map(|r| r.external_id.clone())
            .collect(),
    );
    assert_eq!(result.nodes().unwrap().len(), 2);

    api_service.resources.delete(&ids).await?;
    resource_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    Ok(())
}

/// End-to-end Neo4j persistence check for the node types that publish a graph node
/// (asset/resource, timeseries, function — datasets don't publish one, and there is no
/// SDK Policy service). It creates one connected component, then reads it back through
/// `fetch_related` (which loads from Neo4j) and asserts that the fields Neo4j actually
/// stores round-trip for each node type.
///
/// Every graph node is projected to a `Resource` by `ResourceTransformer.fromNode`, so
/// what a traversal says about the *edges*: that `related_resources` carries the relationship
/// type, the direction from each end's own point of view, and one shared `edge_id` — plus the
/// parts of a node's typed shape that are about the type rather than the projection.
///
/// Which fields the projection carries is no longer asserted here. That question is owned by
/// [`graph_projection_is_one_to_one_and_typed`], which compares every key against the flat
/// read instead of naming a few by hand — this test kept drifting as the projection grew
/// (`value_type`, then `unit`/`unit_external_id`), asserting absence that had become presence.
///
/// The write path is async (API -> Pulsar -> stateful consumer -> Neo4j), so the read is
/// polled until the nodes propagate. Nodes are matched by `external_id` so unrelated or
/// auto-provisioned nodes in the component don't affect the assertions.
#[tokio::test]
async fn graph_traversal_carries_edge_direction_and_typed_shape() -> Result<(), Box<dyn std::error::Error>> {
    use crate::datasets::Dataset;
    use crate::relations::{RelatedNode, RelationDirection};
    use crate::TimeSeries;

    let api = create_api_service();
    let uid = Uuid::new_v4().simple().to_string(); // dataset name only; the ids below are unique_id
    let asset_ext = unique_id("neo_fields_asset");
    let ts_ext = unique_id("neo_fields_ts");
    let func_ext = unique_id("neo_fields_fn");

    // A dataset so we can assert `data_set_id` persists (a Resource-common graph field).
    let dataset = Dataset::new(format!("Neo Fields DS {}", uid));
    let ds_created = api.datasets.create(&dataset).await?;
    let ds_created = ds_created
        .get_items()
        .first()
        .expect("dataset create should return the dataset");
    let ds_id = ds_created.id.expect("dataset create should return an id");
    // Guards are armed as each entity appears, and drop in reverse declaration order — so
    // teardown runs functions, timeseries, asset, dataset: end nodes before the nodes they hang
    // off, and the dataset last. The explicit deletes at the end of the happy path disarm them.
    // Without these, any panic between here and the end strands all four, and the next run
    // collides with the residue.
    let mut dataset_cleanup = cleanup_datasets(vec![ds_created.external_id().to_string()]);

    // Root asset with every Resource-shaped field populated (incl. metadata, which we
    // expect NOT to survive the graph projection).
    let mut asset = Resource::new();
    asset.external_id = asset_ext.clone();
    asset.name = "Neo Fields Asset".to_string();
    asset.description = Some("asset description".to_string());
    asset.is_root = true;
    asset.source = Some("probe_source".to_string());
    asset.data_set_id = Some(ds_id);
    asset.metadata = Some(hashmap! {"vendor".to_string() => "acme".to_string()});
    asset.labels = Some(vec!["ASSET".to_string()]);
    api.resources.create(vec![asset], vec![]).await?;
    let mut asset_cleanup = cleanup_resources(vec![asset_ext.clone()]);

    // Timeseries in the same dataset, linked to the asset via the unified
    // `related_resources` INPUT (asset --MEASURES--> ts).
    let mut ts = TimeSeries::new(&ts_ext, "Neo Fields TS");
    ts.set_unit("a.u")
        .set_description("ts description")
        .set_data_set_id(ds_id)
        .set_related_resources(vec![RelatedNode::from_external_id(&asset_ext, "measures")]);
    api.time_series.create_one(&ts).await?;
    let mut ts_cleanup = cleanup_timeseries(vec![ts_ext.clone()]);

    // Function, linked to the asset with a neutral edge type (asset --USES--> fn).
    let func =
        crate::functions::Function::new(func_ext.clone()).with_name("Neo Fields Fn".to_string());
    api.functions.create(&func).await?;
    let mut func_cleanup = cleanup_functions(vec![func_ext.clone()]);
    api.resources
        .create(
            Vec::<Node>::new(),
            vec![RelForm::by_external_ids(&asset_ext, &func_ext, "uses")],
        )
        .await?;

    // Read back from Neo4j, polling until the async write has propagated all three nodes.
    let form = RelatedResourcesForm::from_external_id(&asset_ext).with_depth(-1);
    let net = poll_until(
        || async { api.resources.fetch_related(&form).await.unwrap_or_default() },
        |net: &ResourceNetwork| {
            let have = |ext: &str| net.nodes().iter().any(|n| n.external_id() == ext);
            have(&asset_ext) && have(&ts_ext) && have(&func_ext)
        },
    )
    .await;

    let find = |ext: &str| -> Node {
        net.nodes()
            .iter()
            .find(|n| n.external_id() == ext)
            .unwrap_or_else(|| panic!("node {ext} not found in network after propagation"))
            .clone()
    };

    // Each node comes back as its own type, discriminated by its intrinsic type-label.
    assert_eq!(
        (
            find(&asset_ext).kind(),
            find(&ts_ext).kind(),
            find(&func_ext).kind()
        ),
        (NodeType::Asset, NodeType::TimeSeries, NodeType::Function)
    );

    // --- asset node ---
    let a = find(&asset_ext).into_asset().expect("asset variant");
    assert_eq!(a.name, "Neo Fields Asset");
    assert_eq!(a.description.as_deref(), Some("asset description"));
    assert!(a.is_root, "asset isRoot should persist as true");
    assert_eq!(a.source.as_deref(), Some("probe_source"));
    assert_eq!(a.data_set_id, Some(ds_id));
    assert!(a.id.is_some(), "server-assigned id should be present");
    assert!(a
        .labels
        .as_deref()
        .unwrap_or_default()
        .contains(&"ASSET".to_string()));
    assert!(
        a.created_time.is_some(),
        "createdTime should round-trip from Neo4j"
    );
    // metadata is NOT reassembled by fromNode — pin that projection gap.
    // asset --MEASURES--> ts, so ts is an OUTBOUND relation carrying the edge id.
    let a_to_ts = a
        .related_resources
        .iter()
        .find(|r| r.external_id.as_deref() == Some(ts_ext.as_str()))
        .expect("asset should carry a related_resources entry for the timeseries");
    assert_eq!(a_to_ts.relationship_type.as_deref(), Some("MEASURES"));
    assert_eq!(a_to_ts.direction, Some(RelationDirection::Outbound));
    assert!(
        a_to_ts.edge_id.is_some(),
        "related_resources entry should carry the edge id"
    );

    // --- timeseries node ---
    // `is_root` is gone from the typed shape: only assets and plain resources have that column,
    // and the flat `Resource` used to carry it onto every type whether it meant anything or not.
    assert_eq!(find(&ts_ext).is_root(), None);
    let t = find(&ts_ext)
        .into_time_series()
        .expect("timeseries variant");
    assert_eq!(t.name, "Neo Fields TS");
    assert_eq!(t.description.as_deref(), Some("ts description"));
    assert_eq!(
        t.source, None,
        "source is a resource-only field; null for timeseries"
    );
    assert_eq!(t.data_set_id, Some(ds_id));
    assert!(t
        .labels
        .as_deref()
        .unwrap_or_default()
        .contains(&"TIMESERIES".to_string()));
    assert!(t.created_time.is_some());
    // mirror direction: the asset->ts edge is INBOUND from the timeseries' perspective.
    let t_from_asset = t
        .related_resources
        .iter()
        .find(|r| r.external_id.as_deref() == Some(asset_ext.as_str()))
        .expect("timeseries should carry a related_resources entry for the asset");
    assert_eq!(t_from_asset.relationship_type.as_deref(), Some("MEASURES"));
    assert_eq!(t_from_asset.direction, Some(RelationDirection::Inbound));
    assert_eq!(
        t_from_asset.edge_id, a_to_ts.edge_id,
        "both ends reference the same edge id"
    );

    // --- function node ---
    let f = find(&func_ext).into_function().expect("function variant");
    assert_eq!(f.name.as_deref(), Some("Neo Fields Fn"));
    assert!(f.labels.contains(&"FUNCTION".to_string()));
    assert!(f.created_time.is_some());
    let f_from_asset = f
        .related_resources
        .iter()
        .find(|r| r.external_id.as_deref() == Some(asset_ext.as_str()))
        .expect("function should carry a related_resources entry for the asset");
    assert_eq!(f_from_asset.relationship_type.as_deref(), Some("USES"));
    assert_eq!(f_from_asset.direction, Some(RelationDirection::Inbound));

    // cleanup (best-effort; end nodes first so edge auto-deletes don't block start nodes)
    let _ = api
        .functions
        .delete(&vec![IdAndExtId::from_external_id(&func_ext)])
        .await;
    func_cleanup.disarm();
    let ts_del: DataWrapper<IdAndExtId> = vec![IdAndExtId::from_external_id(&ts_ext)].into();
    let _ = api.time_series.delete(&ts_del).await;
    ts_cleanup.disarm();
    let _ = api
        .resources
        .delete(&vec![IdAndExtId::from_external_id(&asset_ext)])
        .await;
    asset_cleanup.disarm();
    let _ = api.datasets.delete(&vec![IdAndExtId::from_id(ds_id)]).await;
    dataset_cleanup.disarm();
    Ok(())
}

/// Pure deserialization test (no backend): a fetch-related response where two sensors
/// are both `PART_OF` the cooling system proves the shared-subsystem reasoning.
#[test]
fn fetch_related_deserializes_shared_subsystem() {
    let body = r#"{
        "nodes": [
            {"id":"1","externalId":"cooling_system","name":"Cooling system","isRoot":false},
            {"id":"2","externalId":"sensor_a","name":"Sensor A","isRoot":false},
            {"id":"3","externalId":"sensor_b","name":"Sensor B","isRoot":false}
        ],
        "edges": [
            {"id":"10","start":2,"end":1,"type":"PART_OF"},
            {"id":"11","start":3,"end":1,"type":"PART_OF"}
        ],
        "labels": [
            {"id":"1","name":"SYSTEM"}
        ]
    }"#;

    let network: ResourceNetwork = serde_json::from_str(body).unwrap();

    assert_eq!(network.nodes().len(), 3);
    assert_eq!(network.edges().len(), 2);
    assert_eq!(network.labels().len(), 1);

    // string ids coerced to u64
    let cooling_id = network
        .nodes()
        .iter()
        .find(|n| n.external_id() == "cooling_system")
        .and_then(|n| n.id())
        .unwrap();
    assert_eq!(cooling_id, 1);

    // both sensors' edges point at the same node — their shared subsystem
    let targets: std::collections::HashSet<u64> =
        network.edges().iter().filter_map(|e| e.end).collect();
    assert_eq!(targets, std::collections::HashSet::from([cooling_id]));

    // and the edge type round-trips from the wire `type` field
    assert!(network
        .edges()
        .iter()
        .all(|e| e.relationship_type.as_deref() == Some("PART_OF")));
}

/// Pure serde test (no backend): the `geolocation` field is emitted on the wire under the
/// key `geoLocation` as a nested GeoJSON geometry object (not a quoted string), is omitted
/// entirely when `None`, and survives a round-trip verbatim. Covers both a `Point` and a
/// general `Polygon` geometry.
#[test]
fn geolocation_serializes_as_geojson_object() {
    let mut r = Resource::new();
    r.external_id = "geo_ser".to_string();
    r.name = "Geo Ser".to_string();
    r.labels = Some(vec!["ASSET".to_string()]);
    r.geolocation = Some(geojson::Geometry::new_point([10.75, 59.91]));

    let v = serde_json::to_value(&r).unwrap();
    // Correct wire key (camelCase `geoLocation`, not the Rust field name) and nested object.
    assert_eq!(
        v.get("geoLocation")
            .expect("wire key `geoLocation` should be present"),
        &serde_json::json!({"type": "Point", "coordinates": [10.75, 59.91]})
    );

    // Round-trips back to the same geometry.
    let back: Resource = serde_json::from_value(v).unwrap();
    assert_eq!(back.geolocation, r.geolocation);

    // A general (non-Point) geometry is carried faithfully too.
    let mut poly = Resource::new();
    poly.external_id = "geo_poly".to_string();
    poly.name = "Geo Poly".to_string();
    poly.geolocation = Some(geojson::Geometry::new_polygon(vec![vec![
        [0.0, 0.0],
        [1.0, 0.0],
        [1.0, 1.0],
        [0.0, 1.0],
        [0.0, 0.0],
    ]]));
    let pv = serde_json::to_value(&poly).unwrap();
    assert_eq!(pv["geoLocation"]["type"], "Polygon");

    // Absent geolocation omits the key entirely (matches the backend's NON_NULL behaviour).
    let mut none = Resource::new();
    none.external_id = "geo_none".to_string();
    none.name = "Geo None".to_string();
    let nv = serde_json::to_value(&none).unwrap();
    assert!(
        nv.get("geoLocation").is_none(),
        "None must omit the `geoLocation` key"
    );
}

/// End-to-end: create a resource carrying a GeoJSON Point, read it back through `by_ids`
/// (which loads from Postgres, where the geometry is stored verbatim and written
/// synchronously on create), and assert the geometry survives the round-trip. Uses
/// exactly-representable coordinates so the equality is not subject to float formatting.
///
/// Doubles as coverage for the write-side idiom the polymorphic create preserves: a bare
/// [`Resource`] carrying the `ASSET` label creates an asset, and it reads back as
/// [`Node::Asset`] — the one variant that echoes a geometry.
#[tokio::test]
async fn test_resource_geolocation_round_trips() -> Result<(), ResponseError> {
    let api = create_api_service();
    let ext = unique_id("geo");

    let mut asset = Resource::new();
    asset.external_id = ext.clone();
    asset.name = "Rust SDK Geo Asset".to_string();
    asset.labels = Some(vec!["ASSET".to_string()]);
    asset.geolocation = Some(geojson::Geometry::new_point([10.5, 59.25]));

    let ids = vec![IdAndExtId::from_external_id(&ext)];
    api.resources.delete(&ids).await?; // clear any leftover from a prior failed run

    api.resources.create(vec![asset], vec![]).await?;
    let mut cleanup = cleanup_resources(vec![ext.clone()]);

    // by_ids reads Postgres (synchronous on create); retry briefly to absorb any lag.
    let fetched = poll_until(
        || async {
            api.resources
                .by_ids(&ids)
                .await
                .map(|dw| dw.nodes().unwrap_or_default())
                .unwrap_or_default()
                .into_iter()
                .find(|r| r.external_id() == ext)
        },
        |found: &Option<Node>| found.is_some(),
    )
    .await
    .expect("resource should be readable via by_ids after create")
    .into_asset()
    .expect("an ASSET-labelled node reads back as the asset variant");

    let geom = fetched
        .geolocation
        .expect("geolocation should round-trip back from the backend");
    match geom.value {
        geojson::GeometryValue::Point { coordinates } => {
            assert!(
                (coordinates[0] - 10.5).abs() < 1e-9,
                "lon round-trips: {}",
                coordinates[0]
            );
            assert!(
                (coordinates[1] - 59.25).abs() < 1e-9,
                "lat round-trips: {}",
                coordinates[1]
            );
        }
        other => panic!("expected a Point geometry, got {other:?}"),
    }

    api.resources.delete(&ids).await?;
    cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    Ok(())
}

/// The resource filter's wire shape. The shared node criteria are flattened, so they sit directly
/// on the filter body; only `isRoot` and `dataSetId` are the resource's own.
#[test]
fn resource_filter_matches_the_documented_wire_shape() {
    use crate::filters::NodeFilter;

    let filter = ResourceFilter {
        node: NodeFilter {
            id: Some(vec![12]),
            external_id: Some(vec!["klp_pipe_*".to_string()]),
            name: Some(vec!["pipe*".to_string()]),
            source: Some(vec!["sap".to_string()]),
            labels: Some(vec!["PIPE".to_string()]),
            metadata: Some([("work_order".to_string(), Some("wo-sap-12344".to_string()))].into()),
            ..Default::default()
        },
        node_type: Some(vec!["resource".to_string(), "timeseries".to_string()]),
        is_root: Some(true),
        data_set_id: Some(vec![
            IdAndExtId::from_id(43),
            IdAndExtId::from_external_id("data_set_sap"),
        ]),
    };

    let f = serde_json::to_value(&filter).unwrap();
    assert_eq!(f["id"], serde_json::json!(["12"]));
    assert_eq!(f["externalId"], serde_json::json!(["klp_pipe_*"]));
    assert_eq!(f["name"], serde_json::json!(["pipe*"]));
    assert_eq!(f["source"], serde_json::json!(["sap"]));
    assert_eq!(f["labels"], serde_json::json!(["PIPE"]));
    assert_eq!(f["nodeType"], serde_json::json!(["resource", "timeseries"]));
    assert_eq!(f["isRoot"], true);
    // Data sets are named by id *or* external id now; this endpoint used to take ids only.
    assert_eq!(
        f["dataSetId"],
        serde_json::json!([{"id": "43"}, {"externalId": "data_set_sap"}])
    );

    // Each criterion is one list under the singular name the api binds. The plural spellings it
    // briefly used must be gone: unknown keys are dropped silently, so a leftover `names` would
    // place no restriction and return everything the caller can read.
    for retired in ["ids", "externalIds", "names", "sources", "nodeTypes", "dataSetIds"] {
        assert!(f.get(retired).is_none(), "retired field {retired} is still sent: {f}");
    }

    // A criterion-free filter must place no restriction at all.
    assert_eq!(
        serde_json::to_value(ResourceFilter::default()).unwrap(),
        serde_json::json!({})
    );
}

/// `dataSetId` is the one list where absent and empty mean opposite things — no restriction
/// versus narrow-to-nothing — so the difference has to survive serialization.
#[test]
fn resource_filter_empty_data_set_scope_is_not_the_same_as_none() {
    let narrowed_to_nothing = ResourceFilter {
        data_set_id: Some(vec![]),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&narrowed_to_nothing).unwrap()["dataSetId"],
        serde_json::json!([])
    );

    let value = serde_json::to_value(ResourceFilter::default()).unwrap();
    assert!(
        !value.as_object().unwrap().contains_key("dataSetId"),
        "no restriction must omit the key rather than send null: {value}"
    );
}

/// The request body around the filter: criteria, limit, and the flattened sort/cursor.
///
/// An unsorted, unpaged request must carry neither — a stray `"sort": null` is a field the server
/// then has to make a decision about, and the point of the default order is that the caller did
/// not ask for one.
#[test]
fn resource_filter_form_omits_paging_until_it_is_asked_for() {
    let value = serde_json::to_value(ResourceFilterForm::new(ResourceFilter::default())).unwrap();
    let keys: Vec<&String> = value.as_object().unwrap().keys().collect();
    assert_eq!(keys, vec!["filter"], "unexpected keys in the request body: {value}");

    let with_limit = serde_json::to_value(
        ResourceFilterForm::new(ResourceFilter::default()).with_limit(250),
    )
    .unwrap();
    assert_eq!(with_limit["limit"], 250);

    // Sort and cursor flatten in beside `filter` and `limit` rather than nesting.
    let paged = serde_json::to_value(
        ResourceFilterForm::new(ResourceFilter::default())
            .with_limit(2)
            .with_paging(crate::filters::PageRequest::desc("name").after("djF8bmFtZQ")),
    )
    .unwrap();
    assert_eq!(paged["sort"], serde_json::json!({"property": ["name"], "order": "desc"}));
    assert_eq!(paged["cursor"], "djF8bmFtZQ");
}

/// Every node type's graph projection, field for field against its flat read.
///
/// The invariant this pins is that a traversal is **1-1 and typed**: a node reached through
/// `/resources/fetch-related` is the same variant as the flat read, carrying the same values.
/// It is deliberately exhaustive rather than a list of hand-picked fields — both sides are
/// serialized and every key is compared — so a column added server-side is covered here the
/// day it lands, instead of the day someone remembers to assert on it.
///
/// Two exclusions, both genuine rather than concessions:
/// - `relatedResources` is inverted by design (the graph fills it, flat reads answer `[]`).
/// - `labels` is a set; the graph returns it in its own order, so it is compared as one.
///
/// This was written red and is now green. It found two real gaps when it went in, both since
/// closed server-side: a timeseries lost `unit`/`unitExternalId` (`d70b57ad`, which also made
/// `tableEngine` `@JsonIgnore` so no read returns it at all), and every node type lost its
/// `metadata`, flattened onto the graph as `metadata_<key>` properties that nothing put back
/// (`d37cb57b`). Both were read-side; the writer had been exhaustive throughout.
///
/// Two things it does *not* treat as gaps. `geoLocation` round-trips exactly for a Point — a
/// stored Polygon still comes back as one, because the writer keeps only `pointOrNull` for
/// distance queries. And a node last written before a given field was projected reports that
/// field absent, which is why the typed shapes keep their `Option`s even now that the
/// projection is complete.
#[tokio::test]
async fn graph_projection_is_one_to_one_and_typed() -> Result<(), Box<dyn std::error::Error>> {
    use crate::datasets::Dataset;
    use crate::nodes::Policy;
    use crate::TimeSeries;

    let api = create_api_service();
    let uid = Uuid::new_v4().simple().to_string();
    let asset_ext = unique_id("gp_asset");
    let ts_ext = unique_id("gp_ts");
    let func_ext = unique_id("gp_fn");
    let plain_ext = unique_id("gp_plain");
    let dsnode_ext = unique_id("gp_dsnode");
    let policy_ext = unique_id("gp_policy");

    let dataset = Dataset::new(format!("GP DS {}", uid));
    let ds_created = api.datasets.create(&dataset).await?;
    let ds_id = ds_created
        .get_items()
        .first()
        .and_then(|d| d.id)
        .expect("dataset create should return an id");
    let mut dataset_cleanup =
        cleanup_datasets(vec![ds_created.get_items()[0].external_id().to_string()]);

    // One node of every creatable type, each with every field its shape allows populated —
    // an unset field cannot show a projection gap.
    let mut asset = Resource::new();
    asset.external_id = asset_ext.clone();
    asset.name = "GP Asset".to_string();
    asset.description = Some("asset description".to_string());
    asset.source = Some("gp_source".to_string());
    asset.is_root = true;
    asset.data_set_id = Some(ds_id);
    asset.metadata = Some(hashmap! {"vendor".to_string() => "acme".to_string()});
    asset.labels = Some(vec!["ASSET".to_string()]);

    let mut plain = Resource::new();
    plain.external_id = plain_ext.clone();
    plain.name = "GP Plain".to_string();
    plain.description = Some("plain description".to_string());
    plain.source = Some("gp_source".to_string());
    plain.data_set_id = Some(ds_id);
    plain.metadata = Some(hashmap! {"pk".to_string() => "pv".to_string()});
    plain.labels = Some(vec!["TEST".to_string()]);

    let mut dsnode = Dataset::new(format!("GP DS Node {}", uid));
    dsnode.external_id = dsnode_ext.clone();
    dsnode.description = Some("dsnode description".to_string());
    dsnode.metadata = hashmap! {"dk".to_string() => "dv".to_string()};

    let mut policy = Policy::new(&policy_ext, "GP Policy");
    policy.description = Some("policy description".to_string());
    policy.policy_type = Some("IS_WRITE_PROTECTED".to_string());
    policy.source = Some("gp_source".to_string());
    policy.metadata = Some(hashmap! {"yk".to_string() => "yv".to_string()});

    api.resources
        .create(
            vec![
                Node::Asset({
                    let mut a = crate::nodes::Asset::new(&asset_ext, "GP Asset");
                    a.description = asset.description.clone();
                    a.source = asset.source.clone();
                    a.is_root = true;
                    a.data_set_id = Some(ds_id);
                    a.metadata = asset.metadata.clone();
                    a.labels = Some(vec!["ASSET".to_string()]);
                    a.geolocation = None;
                    a
                }),
                Node::Resource(plain.clone()),
                Node::Dataset(dsnode.clone()),
                Node::Policy(policy.clone()),
            ],
            vec![],
        )
        .await?;
    let mut resource_cleanup = cleanup_resources(vec![
        asset_ext.clone(),
        plain_ext.clone(),
        dsnode_ext.clone(),
        policy_ext.clone(),
    ]);

    let mut ts = TimeSeries::new(&ts_ext, "GP TS");
    ts.set_unit("deg C")
        .set_unit_external_id("celsius")
        .set_description("ts description")
        .set_data_set_id(ds_id)
        .set_metadata(hashmap! {"tk".to_string() => "tv".to_string()});
    api.time_series.create_one(&ts).await?;
    let mut ts_cleanup = cleanup_timeseries(vec![ts_ext.clone()]);

    let mut func =
        crate::functions::Function::new(func_ext.clone()).with_name("GP Fn".to_string());
    func.description = Some("fn description".to_string());
    func.source = Some("gp_source".to_string());
    func.data_set_id = Some(ds_id);
    func.metadata = hashmap! {"fk".to_string() => "fv".to_string()};
    api.functions.create(&func).await?;
    let mut func_cleanup = cleanup_functions(vec![func_ext.clone()]);

    let kinds = [
        (&asset_ext, NodeType::Asset),
        (&ts_ext, NodeType::TimeSeries),
        (&func_ext, NodeType::Function),
        (&plain_ext, NodeType::Resource),
        (&dsnode_ext, NodeType::Dataset),
        (&policy_ext, NodeType::Policy),
    ];

    // Traversing from each node in turn rather than from one hub: an edge to a data set must be
    // BELONGS_TO and one to a policy is refused outright, so a single hub cannot reach all six.
    // A node with no edges still comes back as the sole member of its own component.
    let mut graph: std::collections::HashMap<String, Node> = std::collections::HashMap::new();
    for (ext, _) in kinds.iter() {
        let form = RelatedResourcesForm::from_external_id(ext).with_depth(-1);
        let net = poll_until(
            || async { api.resources.fetch_related(&form).await.unwrap_or_default() },
            |net: &ResourceNetwork| net.nodes().iter().any(|n| n.external_id() == ext.as_str()),
        )
        .await;
        let node = net
            .nodes()
            .iter()
            .find(|n| n.external_id() == ext.as_str())
            .expect("polled until present")
            .clone();
        graph.insert((*ext).clone(), node);
    }

    let flat_wrapper = api
        .resources
        .by_ids(
            &kinds
                .iter()
                .map(|(e, _)| IdAndExtId::from_external_id(e))
                .collect::<Vec<_>>(),
        )
        .await?;
    let flat: std::collections::HashMap<String, Node> = flat_wrapper
        .nodes
        .unwrap_or_default()
        .into_iter()
        .map(|n| (n.external_id().to_string(), n))
        .collect();

    // `related_resources` is inverted by design; `labels` is a set the graph may reorder.
    const INVERTED: &[&str] = &["relatedResources"];
    let mut problems: Vec<String> = Vec::new();

    for (ext, want_kind) in kinds.iter() {
        let f = flat
            .get(*ext)
            .unwrap_or_else(|| panic!("{ext} missing from the flat read"));
        let g = graph.get(*ext).expect("collected above");

        if g.kind() != *want_kind {
            problems.push(format!(
                "{ext}: graph typed it {:?}, flat read is {:?}",
                g.kind(),
                want_kind
            ));
            continue;
        }
        if f.kind() != *want_kind {
            problems.push(format!("{ext}: flat read typed it {:?}", f.kind()));
        }

        let fv = serde_json::to_value(f)?;
        let gv = serde_json::to_value(g)?;
        let (fo, go) = (
            fv.as_object().expect("node is an object"),
            gv.as_object().expect("node is an object"),
        );

        for (key, want) in fo.iter() {
            if INVERTED.contains(&key.as_str()) {
                continue;
            }
            if key == "labels" {
                let set = |v: Option<&serde_json::Value>| -> std::collections::BTreeSet<String> {
                    v.and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                        .unwrap_or_default()
                };
                if set(Some(want)) != set(go.get(key)) {
                    problems.push(format!(
                        "{ext}: labels differ — flat {:?}, graph {:?}",
                        set(Some(want)),
                        set(go.get(key))
                    ));
                }
                continue;
            }
            match go.get(key) {
                None => problems.push(format!(
                    "{ext}: graph is missing `{key}` (flat has {want})"
                )),
                Some(got) if got != want => problems.push(format!(
                    "{ext}: `{key}` differs — flat {want}, graph {got}"
                )),
                Some(_) => {}
            }
        }
    }

    api.resources
        .delete(&vec![
            IdAndExtId::from_external_id(&asset_ext),
            IdAndExtId::from_external_id(&plain_ext),
            IdAndExtId::from_external_id(&dsnode_ext),
            IdAndExtId::from_external_id(&policy_ext),
            IdAndExtId::from_external_id(&ts_ext),
            IdAndExtId::from_external_id(&func_ext),
        ])
        .await
        .ok();
    api.datasets
        .delete(&vec![IdAndExtId::from_external_id(
            ds_created.get_items()[0].external_id(),
        )])
        .await
        .ok();
    resource_cleanup.disarm();
    ts_cleanup.disarm();
    func_cleanup.disarm();
    dataset_cleanup.disarm();

    assert!(
        problems.is_empty(),
        "the graph projection is not 1-1 with the flat read:\n  {}",
        problems.join("\n  ")
    );
    Ok(())
}

/// `GET /resources?limit=` — the plain listing the generic node query gained alongside the
/// `/datasets/list` consolidation.
///
/// Three claims, and the first is the one worth having: the listing is a read under `/resources`
/// like any other, so its rows are typed per node type rather than flattened to `Resource`. The
/// other two are the shared `?limit=` contract — a cap that truncates, and an over-cap value that
/// is a 400 rather than a silent clamp, because a caller who asked for 50000 and received 10000
/// cannot tell that from a tenant holding exactly 10000.
#[tokio::test]
async fn plain_listing_is_typed_capped_and_uncursored() -> Result<(), Box<dyn std::error::Error>> {
    let api_service = create_api_service();
    let ext_id = unique_id("list_asset");

    let asset = Resource {
        id: None,
        external_id: ext_id.clone(),
        name: format!("Rust SDK listing {}", ext_id),
        metadata: None,
        description: None,
        is_root: false,
        data_set_id: None,
        source: Some("Test_Rust_SDK".to_string()),
        labels: Some(vec!["ASSET".to_string()]),
        related_resources: vec![],
        geolocation: None,
        created_time: None,
        last_updated_time: None,
    };
    api_service.resources.create(vec![asset], vec![]).await?;
    let mut cleanup = cleanup_resources(vec![ext_id.clone()]);

    let listed = api_service.resources.list(None).await?;
    assert_eq!(listed.get_http_status_code(), Some(200));
    // Newest created first, and the default page is 1000, so a node created a moment ago is on it.
    let mine = listed
        .get_items()
        .iter()
        .find(|n| n.external_id() == to_snake_lower_cased_allow_start_with_digits(&ext_id))
        .expect("a resource created a moment ago should be on the newest-first page");
    assert_eq!(
        mine.kind(),
        NodeType::Asset,
        "the plain listing dispatches on the type-label like every other /resources read"
    );

    // No `nextCursor`: a walk needs a sort and a cursor and both live on the filter body, so the
    // api nulls it here rather than handing out one this endpoint could not accept back.
    assert!(
        listed.next_cursor().is_none(),
        "the plain listing is the first page and says so"
    );

    let capped = api_service.resources.list(Some(1)).await?;
    assert_eq!(capped.get_items().len(), 1, "limit truncates");

    let over_cap = api_service.resources.list(Some(10_001)).await;
    assert_eq!(
        over_cap.map(|_| ()).unwrap_err().get_status().as_u16(),
        400,
        "above 10000 is a rejection, not a clamp"
    );

    api_service
        .resources
        .delete(&vec![IdAndExtId::from_external_id(&ext_id)])
        .await?;
    cleanup.disarm();
    Ok(())
}
