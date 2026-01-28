use maplit::hashmap;
use super::*;
use crate::create_api_service;
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::generic::IdAndExtId;

fn test_resources() -> Vec<ResourceForm>{

    let res1 = ResourceForm {
        // used to be a serde skip if zero here. don't understand why
        id: None,
        external_id: "Rust_SDK_Test_2000_Resource".to_string(),
        name: "Test_2000_Resource".to_string(),
        metadata:Some(hashmap! {
            "foo".to_string() => "bar".to_string(),
            "is_test".to_string() => "true".to_string(),
            "test_soruce".to_string() => "rust_sdk".to_string()
        }),
        description: Some("root_test_data_set".to_string()),
        is_root: true,
        data_set_id: Some(0),
        source: Some("Test_Rust_SDK".to_string()),
        labels: Some(vec![hashmap!{"name".to_string()=>"test_label".to_string()}]),
        relations: None,
        geolocation: None,
        created_time: None,
        last_updated_time: None,
        relations_form: Some(vec![]),
    };
    let res2 = ResourceForm {
        // used to be a serde skip if zero here. don't understand why
        id: None,
        external_id: "Rust_SDK_Test_2001_Resource".to_string(),
        name: "Test_2001_Resource".to_string(),
        metadata:None,
        description: None,
        is_root: false,
        data_set_id: Some(0),
        source: Some("Test_Rust_SDK".to_string()),
        labels: Some(vec![hashmap!{"name".to_string()=>"test_label".to_string()}]),
        relations: None,
        geolocation: None,
        created_time: None,
        last_updated_time: None,
        relations_form: Some(vec![]),
    };
    vec![res1,res2]
}
#[tokio::test]
async fn test_create_and_delete_resources() -> Result<(), ResponseError> {

    let api_service = create_api_service();
    let test_resources = test_resources();
    // Delete timeseries first, in case a test failed and the time series exists
    let ids= test_resources.iter().map(
        |r| IdAndExtId::from_external_id(&r.external_id)).collect::<Vec<IdAndExtId>>();
    api_service.resources.delete(&ids).await?;

    assert_eq!(api_service.resources.by_ids(&ids).await?.nodes().unwrap(), vec![]);

    let result = api_service.resources.create(&test_resources).await?;
    let res_ids = result.nodes().unwrap().iter().map(|r| to_snake_lower_cased_allow_start_with_digits(&r.external_id)).collect::<Vec<String>>();
    let input_ids= test_resources.iter().map(|r| to_snake_lower_cased_allow_start_with_digits(&r.external_id)).collect::<Vec<String>>();
    assert_eq!(res_ids, input_ids);

    //let val = &result.json::<Resource>().await.unwrap();

    // Delete resources
    api_service.resources.delete(&ids).await?;
    assert_eq!(api_service.resources.by_ids(&ids).await?.nodes().unwrap(), vec![]);

    Ok(())
}