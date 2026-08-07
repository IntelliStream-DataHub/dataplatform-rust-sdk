use crate::create_api_service;
use crate::datasets::{Dataset, DatasetUpdate};
use crate::fields::{Field, MapField};
use crate::generic::IdAndExtId;
use crate::http::ResponseError;
use crate::tests::cleanup::cleanup_datasets;
use maplit::hashmap;

fn create_test_dataset() -> Vec<Dataset> {
    let mut datasets = vec![];
    for i in 0..10 {
        let key = format!("test_key{}", i);
        let value = format!("test_value{}", i);
        datasets.push(
            Dataset::new(format!("test_dataset{}", i))
                .set_description(format!("test_description{}", i))
                .set_metadata(std::collections::HashMap::from([(key, value)]))
                .set_policies(vec!["test_policy".to_string()])
                .build(),
        )
    }

    datasets
}
#[tokio::test]
async fn test_dataset_crud() -> Result<(), ResponseError> {
    fn equal_external_ids(lhs: &Vec<Dataset>, rhs: &Vec<Dataset>, expect_empty: bool) -> bool {
        if lhs.is_empty() && rhs.is_empty() {
            return expect_empty;
        }
        lhs.iter()
            .all(|l| rhs.iter().any(|r| l.external_id() == r.external_id()))
            && rhs
                .iter()
                .all(|r| lhs.iter().any(|l| l.external_id() == r.external_id()))
    }
    let api_service = create_api_service();
    let test_dataset = create_test_dataset();
    let test_ids = test_dataset
        .iter()
        .map(|dt| IdAndExtId::from_external_id(dt.external_id()))
        .collect::<Vec<IdAndExtId>>();
    api_service.datasets.delete(&test_ids).await?;
    assert!(equal_external_ids(
        api_service.datasets.by_ids(&test_ids).await?.get_items(),
        &vec![],
        true
    ));

    let create_res = api_service.datasets.create(&test_dataset).await?;
    let mut dataset_cleanup = cleanup_datasets(
        test_dataset
            .iter()
            .map(|d| d.external_id().to_string())
            .collect(),
    );
    assert!(equal_external_ids(
        &api_service.datasets.by_ids(&test_ids).await?.get_items(),
        &test_dataset,
        false
    ));
    api_service.datasets.delete(&test_ids).await?;
    dataset_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    assert!(equal_external_ids(
        api_service.datasets.by_ids(&test_ids).await?.get_items(),
        &vec![],
        true
    ));

    Ok(())
}

/// `list`, `search`, `update` and `policies` — the four that used to panic or point at a route
/// that does not exist. Exercised against one purpose-built dataset rather than asserting on
/// tenant-wide counts, which are shared state.
#[tokio::test]
async fn test_dataset_list_search_update_policies() -> Result<(), ResponseError> {
    let api_service = create_api_service();
    let ext_id = "sdk_test_dataset_list_search_update";
    let selector = vec![IdAndExtId::from_external_id(ext_id)];
    api_service.datasets.delete(&selector).await?;

    let dataset = Dataset::new("sdk test dataset for list search update".to_string())
        .set_external_id(ext_id.to_string())
        .set_description("before the update".to_string())
        .build();
    let created = api_service.datasets.create(&dataset).await?;
    assert_eq!(created.get_http_status_code(), Some(200));
    let mut cleanup = cleanup_datasets(vec![ext_id.to_string()]);

    // --- list: every dataset in the tenant, criteria-free ---
    let listed = api_service.datasets.list().await?;
    assert_eq!(listed.get_http_status_code(), Some(200));
    assert!(
        listed.get_items().iter().any(|d| d.external_id() == ext_id),
        "the dataset just created should appear in list()"
    );

    // --- search: free-text over names. Only the query reaches the server. ---
    let found = api_service
        .datasets
        .search_by_query("sdk test dataset for list search")
        .await?;
    assert_eq!(found.get_http_status_code(), Some(200));
    assert!(
        found.get_items().iter().any(|d| d.external_id() == ext_id),
        "search should surface the dataset by its name"
    );

    // A query under the server's 3-character minimum fails validation rather than matching loosely.
    let too_short = api_service.datasets.search_by_query("ab").await;
    assert_eq!(
        too_short.map(|_| ()).unwrap_err().get_status().as_u16(),
        400,
        "a 2-character query should be rejected by the server's @Size(min = 3)"
    );

    // --- update: change the description and add metadata in one call ---
    let updated = api_service
        .datasets
        .update(
            &DatasetUpdate::by_external_id(ext_id)
                .description(Field::value("after the update"))
                .metadata(MapField::add(
                    [("owner".to_string(), "sdk_tests".to_string())].into(),
                )),
        )
        .await?;
    assert_eq!(updated.get_http_status_code(), Some(200));
    let after = &updated.get_items()[0];
    assert_eq!(after.description(), Some(&"after the update".to_string()));
    assert_eq!(after.metadata().get("owner"), Some(&"sdk_tests".to_string()));

    // --- policies: reachable, but see the note on DatasetsService::policies ---
    let policies = api_service.datasets.policies().await?;
    assert_eq!(policies.get_http_status_code(), Some(200));

    api_service.datasets.delete(&selector).await?;
    cleanup.disarm();
    Ok(())
}

/// Pure check of the `/datasets/update` body: `items`-wrapped, camelCase, and — the point of the
/// builder — only the fields that were touched, since the server reads a missing field as
/// "leave unchanged".
#[test]
fn dataset_update_serializes_only_touched_fields() {
    use crate::generic::DataWrapper;

    let upd = DatasetUpdate::by_external_id("sap_work_orders")
        .description(Field::value("SAP work orders — live sync"))
        .write_protected(Field::value(true));
    assert_eq!(
        serde_json::to_value(DataWrapper::from_vec(vec![upd])).unwrap(),
        serde_json::json!({
            "items": [{
                "externalId": "sap_work_orders",
                "update": {
                    "description": { "set": "SAP work orders — live sync", "setNull": false },
                    "writeProtected": { "set": true, "setNull": false }
                }
            }]
        })
    );

    // Targeting by numeric id sends `id` as a string, and an untouched update block is empty.
    let by_id = DatasetUpdate::by_id(5677892);
    assert_eq!(
        serde_json::to_value(&by_id).unwrap(),
        serde_json::json!({ "id": "5677892", "update": {} })
    );
}
