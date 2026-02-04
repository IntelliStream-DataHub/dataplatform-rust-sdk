use super::*;
use crate::create_api_service;
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::generic::{IdAndExtId, SearchForm};
use maplit::hashmap;
use uuid::Uuid;

fn create_test_resources() -> Vec<ResourceForm> {
    // helper function to create test resources will
    let count = 2;
    let uuids = (0..count).map(|_| Uuid::new_v4()).collect::<Vec<Uuid>>();
    let res1 = ResourceForm {
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
        data_set_id: Some(0),
        source: Some("Test_Rust_SDK".to_string()),
        labels: Some(vec![
            hashmap! {"name".to_string()=>"test_label".to_string()},
        ]),
        relations: None,
        geolocation: None,
        created_time: None,
        last_updated_time: None,
        relations_form: Some(vec![]),
    };
    let res2 = ResourceForm {
        // used to be a serde skip if zero here. don't understand why
        id: None,
        external_id: format!("Rust_SDK_Test_Resource_{:?}", uuids[1]),
        name: format!("Rust SDK Test Resource-{:?}", uuids[1]),
        metadata: None,
        description: None,
        is_root: false,
        data_set_id: Some(0),
        source: Some("Test_Rust_SDK".to_string()),
        labels: Some(vec![
            hashmap! {"name".to_string()=>"test_label".to_string()},
        ]),
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

    let result = api_service.resources.create(&test_resources).await?;
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

    api_service.resources.create(&test_resources).await?;
    let search_result = api_service.resources.search(&query).await?;
    let search_result2 = api_service.resources.search(&query2).await?;
    println!("{:?}", search_result2.get_items().len());
    assert!(search_result.get_items().len() <= 5);
    assert!(search_result
        .get_items()
        .iter()
        .all(|r| r.name.contains("test") || r.external_id.contains("test")));
    let resulting_ids = search_result2
        .get_items()
        .iter()
        .map(|r| IdAndExtId::from_external_id(&r.external_id))
        .collect::<Vec<IdAndExtId>>();
    api_service.resources.delete(&resulting_ids).await?;
    Ok(())
}
