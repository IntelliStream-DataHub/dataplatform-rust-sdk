use super::*;
use crate::create_api_service;
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::generic::{IdAndExtId, SearchForm};
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
        relations: None,
        geolocation: None,
        created_time: None,
        last_updated_time: None,
        relations_form: Some(vec![]),
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
        relations: None,
        geolocation: None,
        created_time: None,
        last_updated_time: None,
        relations_form: Some(vec![]),
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
