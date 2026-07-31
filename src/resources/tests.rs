use super::*;
use crate::create_api_service;
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::generic::{CrudService, IdAndExtId, SearchForm};
use crate::relations::RelForm;
use crate::tests::cleanup::cleanup_resources;
use maplit::hashmap;
use uuid::Uuid;

fn create_test_resources() -> Vec<Resource> {
    // helper function to create test resources will
    let count = 2;
    let uuids = (0..count).map(|_| Uuid::new_v4()).collect::<Vec<Uuid>>();
    let res1 = Resource {
        // used to be a serde skip if zero here. don't understand why
        id: None,
        external_id: format!("Rust_SDK_Test_Resource_{:?}", uuids[0]),
        name: format!("Rust SDK Test Resource-{:?}", uuids[0]),
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
        external_id: format!("Rust_SDK_Test_Resource_{:?}", uuids[1]),
        name: format!("Rust SDK Test Resource-{:?}", uuids[1]),
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
        .map(|r| to_snake_lower_cased_allow_start_with_digits(&r.external_id))
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
    let query = SearchAndFilterForm {
        search: Some(SearchForm {
            name: None,
            description: None,
            query: Some("test resource".to_string()),
        }),
        limit: Some(5),
        filter: None,
    };
    let query2 = SearchAndFilterForm {
        search: Some(SearchForm {
            name: None,
            description: None,
            query: Some("test resource".to_string()),
        }),
        limit: None,
        filter: None,
    };

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
    assert!(search_result
        .get_items()
        .iter()
        .all(|r| r.name.contains("test") || r.external_id.contains("test")));
    let resulting_ids = test_data
        .nodes()
        .unwrap()
        .iter()
        .map(|r| IdAndExtId::from_external_id(&r.external_id))
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
/// the persisted, readable set is the Resource-common one: `id`, `external_id`, `name`,
/// `description`, `data_set_id`, `source`, `is_root`, `created_time`/`last_updated_time`,
/// `labels`, and the unified `related_resources` (direction + `edge_id`). Type-specific
/// fields (timeseries `value_type`/`unit`, etc.) are not in the graph node, and
/// **`metadata` is not read back** — it is written to Neo4j as flattened `metadata_*`
/// properties but `fromNode` never reassembles it (this test pins that behaviour).
///
/// The write path is async (API -> Pulsar -> stateful consumer -> Neo4j), so the read is
/// polled until the nodes propagate. Nodes are matched by `external_id` so unrelated or
/// auto-provisioned nodes in the component don't affect the assertions.
#[tokio::test]
async fn neo4j_persists_expected_fields_per_node_type() -> Result<(), Box<dyn std::error::Error>> {
    use crate::datasets::Dataset;
    use crate::relations::{RelatedNode, RelationDirection};
    use crate::TimeSeries;

    let api = create_api_service();
    let uid = Uuid::new_v4().simple().to_string();
    let asset_ext = format!("neo_fields_asset_{}", uid);
    let ts_ext = format!("neo_fields_ts_{}", uid);
    let func_ext = format!("neo_fields_fn_{}", uid);

    // A dataset so we can assert `data_set_id` persists (a Resource-common graph field).
    let dataset = Dataset::new(format!("Neo Fields DS {}", uid));
    let ds_created = api.datasets.create(&dataset).await?;
    let ds_id = ds_created
        .get_items()
        .first()
        .and_then(|d| d.id)
        .expect("dataset create should return an id");

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

    // Timeseries in the same dataset, linked to the asset via the unified
    // `related_resources` INPUT (asset --MEASURES--> ts).
    let mut ts = TimeSeries::new(&ts_ext, "Neo Fields TS");
    ts.set_unit("a.u")
        .set_description("ts description")
        .set_data_set_id(ds_id)
        .set_related_resources(vec![RelatedNode::from_external_id(&asset_ext, "measures")]);
    api.time_series.create_one(&ts).await?;

    // Function, linked to the asset with a neutral edge type (asset --USES--> fn). A
    // `PROCESSED_BY` edge would trigger the function-binding lifecycle and auto-provision
    // an output timeseries; a neutral type keeps the component deterministic.
    let func = crate::functions::Function::new(func_ext.clone(), "forecast-ema".to_string())
        .with_name("Neo Fields Fn".to_string());
    api.functions.create(&func).await?;
    api.resources
        .create(
            vec![],
            vec![RelForm::by_external_ids(&asset_ext, &func_ext, "uses")],
        )
        .await?;

    // Read back from Neo4j, polling until the async write has propagated all three nodes.
    let form = RelatedResourcesForm::from_external_id(&asset_ext).with_depth(-1);
    let mut net = ResourceNetwork::default();
    for _ in 0..25 {
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
        net = api.resources.fetch_related(&form).await?;
        let have = |ext: &str| net.nodes().iter().any(|n| n.external_id == ext);
        if have(&asset_ext) && have(&ts_ext) && have(&func_ext) {
            break;
        }
    }

    let find = |ext: &str| -> Resource {
        net.nodes()
            .iter()
            .find(|n| n.external_id == ext)
            .unwrap_or_else(|| panic!("node {ext} not found in network after propagation"))
            .clone()
    };

    // --- asset / resource node ---
    let a = find(&asset_ext);
    assert_eq!(a.name, "Neo Fields Asset");
    assert_eq!(a.description.as_deref(), Some("asset description"));
    assert!(a.is_root, "asset isRoot should persist as true");
    assert_eq!(a.source.as_deref(), Some("probe_source"));
    assert_eq!(a.data_set_id, Some(ds_id));
    assert!(a.id.is_some(), "server-assigned id should be present");
    assert!(a.labels.as_deref().unwrap_or_default().contains(&"ASSET".to_string()));
    assert!(a.created_time.is_some(), "createdTime should round-trip from Neo4j");
    // metadata is NOT reassembled by fromNode — pin that projection gap.
    assert!(
        a.metadata.as_ref().map(|m| m.is_empty()).unwrap_or(true),
        "metadata is not surfaced via the Neo4j graph path (written as metadata_* only): {:?}",
        a.metadata
    );
    // asset --MEASURES--> ts, so ts is an OUTBOUND relation carrying the edge id.
    let a_to_ts = a
        .related_resources
        .iter()
        .find(|r| r.external_id.as_deref() == Some(ts_ext.as_str()))
        .expect("asset should carry a related_resources entry for the timeseries");
    assert_eq!(a_to_ts.relationship_type.as_deref(), Some("MEASURES"));
    assert_eq!(a_to_ts.direction, Some(RelationDirection::Outbound));
    assert!(a_to_ts.edge_id.is_some(), "related_resources entry should carry the edge id");

    // --- timeseries node ---
    let t = find(&ts_ext);
    assert_eq!(t.name, "Neo Fields TS");
    assert_eq!(t.description.as_deref(), Some("ts description"));
    assert!(!t.is_root, "a timeseries is never a root node");
    assert_eq!(t.source, None, "no source was set on this timeseries, so it reads back null");
    assert_eq!(t.data_set_id, Some(ds_id));
    assert!(t.labels.as_deref().unwrap_or_default().contains(&"TIMESERIES".to_string()));
    assert!(t.created_time.is_some());
    // mirror direction: the asset->ts edge is INBOUND from the timeseries' perspective.
    let t_from_asset = t
        .related_resources
        .iter()
        .find(|r| r.external_id.as_deref() == Some(asset_ext.as_str()))
        .expect("timeseries should carry a related_resources entry for the asset");
    assert_eq!(t_from_asset.relationship_type.as_deref(), Some("MEASURES"));
    assert_eq!(t_from_asset.direction, Some(RelationDirection::Inbound));
    assert_eq!(t_from_asset.edge_id, a_to_ts.edge_id, "both ends reference the same edge id");

    // --- function node ---
    let f = find(&func_ext);
    assert_eq!(f.name, "Neo Fields Fn");
    assert!(!f.is_root);
    assert!(f.labels.as_deref().unwrap_or_default().contains(&"FUNCTION".to_string()));
    assert!(f.created_time.is_some());
    let f_from_asset = f
        .related_resources
        .iter()
        .find(|r| r.external_id.as_deref() == Some(asset_ext.as_str()))
        .expect("function should carry a related_resources entry for the asset");
    assert_eq!(f_from_asset.relationship_type.as_deref(), Some("USES"));
    assert_eq!(f_from_asset.direction, Some(RelationDirection::Inbound));

    // cleanup (best-effort; end nodes first so edge auto-deletes don't block start nodes)
    let _ = api.functions.delete(&vec![IdAndExtId::from_external_id(&func_ext)]).await;
    let ts_del: DataWrapper<IdAndExtId> = vec![IdAndExtId::from_external_id(&ts_ext)].into();
    let _ = api.time_series.delete(&ts_del).await;
    let _ = api.resources.delete(&vec![IdAndExtId::from_external_id(&asset_ext)]).await;
    let _ = api.datasets.delete(&vec![IdAndExtId::from_id(ds_id)]).await;
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
        .find(|n| n.external_id == "cooling_system")
        .and_then(|n| n.id)
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
        v.get("geoLocation").expect("wire key `geoLocation` should be present"),
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
    assert!(nv.get("geoLocation").is_none(), "None must omit the `geoLocation` key");
}

/// End-to-end: create a resource carrying a GeoJSON Point, read it back through `by_ids`
/// (which loads from Postgres, where the geometry is stored verbatim and written
/// synchronously on create), and assert the geometry survives the round-trip. Uses
/// exactly-representable coordinates so the equality is not subject to float formatting.
#[tokio::test]
async fn test_resource_geolocation_round_trips() -> Result<(), ResponseError> {
    let api = create_api_service();
    let uid = Uuid::new_v4().simple().to_string();
    let ext = format!("rust_sdk_geo_{}", uid);

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
    let mut fetched: Option<Resource> = None;
    for _ in 0..10 {
        let nodes = api.resources.by_ids(&ids).await?.nodes().unwrap_or_default();
        if let Some(r) = nodes.into_iter().find(|r| r.external_id == ext) {
            fetched = Some(r);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    let fetched = fetched.expect("resource should be readable via by_ids after create");

    let geom = fetched
        .geolocation
        .expect("geolocation should round-trip back from the backend");
    match geom.value {
        geojson::GeometryValue::Point { coordinates } => {
            assert!((coordinates[0] - 10.5).abs() < 1e-9, "lon round-trips: {}", coordinates[0]);
            assert!((coordinates[1] - 59.25).abs() < 1e-9, "lat round-trips: {}", coordinates[1]);
        }
        other => panic!("expected a Point geometry, got {other:?}"),
    }

    api.resources.delete(&ids).await?;
    cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    Ok(())
}
