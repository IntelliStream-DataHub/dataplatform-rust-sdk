use crate::create_api_service;
use crate::datasets::{BasicDatasetFilter, Dataset, DatasetFilter, DatasetUpdate};
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

    // --- filter: criteria are honoured server-side. This is the assertion that the old
    // `filter()`-onto-`/list` shim could not have passed: it returned the whole tenant, so an
    // exclusion check like the one below would have failed on any backend with a second dataset.
    let narrowed = api_service
        .datasets
        .filter(&DatasetFilter::from_filter(
            BasicDatasetFilter::new()
                .set_external_ids(vec![ext_id.to_string()])
                .build(),
        ))
        .await?;
    assert_eq!(narrowed.get_http_status_code(), Some(200));
    assert_eq!(
        narrowed
            .get_items()
            .iter()
            .map(|d| d.external_id())
            .collect::<Vec<_>>(),
        vec![ext_id],
        "filtering by external id should return exactly that dataset, not the whole tenant"
    );

    // An unmatchable criterion is an empty result, not an unfiltered one.
    let none = api_service
        .datasets
        .filter(&DatasetFilter::from_filter(
            BasicDatasetFilter::new()
                .set_external_ids(vec!["sdk_test_dataset_that_does_not_exist".to_string()])
                .build(),
        ))
        .await?;
    assert!(
        none.get_items().is_empty(),
        "an external id that matches nothing should return no datasets, got {}",
        none.get_items().len()
    );

    // A limit above the server's @Max(10000) is rejected rather than clamped.
    let over_cap = api_service
        .datasets
        .filter(DatasetFilter::new().set_limit(10_001))
        .await;
    assert_eq!(
        over_cap.map(|_| ()).unwrap_err().get_status().as_u16(),
        400,
        "limit above 10000 should be rejected by the server's @Max"
    );

    // --- search: full-text over name, external id and description. `filter` is ignored. ---
    let found = api_service
        .datasets
        .search_by_query("sdk test dataset for list search")
        .await?;
    assert_eq!(found.get_http_status_code(), Some(200));
    assert!(
        found.get_items().iter().any(|d| d.external_id() == ext_id),
        "search should surface the dataset by a phrase from its name"
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

/// The filter body must match `DataSetRetreiver` + `DataSetFilter` on the wire. Asserted against
/// the example in the `POST /datasets/filter` OpenAPI annotation, because a field the backend
/// cannot see is indistinguishable from no filter at all — the failure mode is silently getting
/// every dataset back, which is exactly what this endpoint's first SDK binding did.
#[test]
fn filter_body_matches_the_documented_wire_shape() {
    use crate::datasets::{BasicDatasetFilter, DatasetFilter};
    use crate::filters::TimeFilter;
    use chrono::{DateTime, Utc};

    let min: DateTime<Utc> = "2026-01-01T00:00:00Z".parse().unwrap();
    let filter = DatasetFilter::from_filter(
        BasicDatasetFilter::new()
            .set_names(vec!["SAP%".to_string()])
            .set_source("sap".to_string())
            .set_external_id_prefix("sap_".to_string())
            .set_write_protected(false)
            .set_metadata([("owner".to_string(), "plant-a".to_string())].into())
            .set_created_time(TimeFilter::After { min })
            .build(),
    );

    let json: serde_json::Value = serde_json::to_value(&filter).unwrap();
    assert_eq!(json["limit"], 100);
    let f = &json["filter"];
    assert_eq!(f["names"], serde_json::json!(["SAP%"]));
    assert_eq!(f["source"], "sap");
    assert_eq!(f["externalIdPrefix"], "sap_");
    assert_eq!(f["writeProtected"], false);
    assert_eq!(f["metadata"]["owner"], "plant-a");
    assert_eq!(f["createdTime"]["min"], "2026-01-01T00:00:00Z");

    // Unset criteria are omitted, not sent as null: the backend reads an empty/absent list as "no
    // restriction", so a stray `"ids": null` is harmless, but omitting keeps the body honest.
    assert!(f.get("ids").is_none(), "unset ids should be omitted");
    assert!(f.get("deactivated").is_none());

    // Ids go out as strings, like every other id on the wire.
    let by_id = DatasetFilter::from_filter(
        BasicDatasetFilter::new()
            .set_ids(vec![12, 9_007_199_254_740_993])
            .build(),
    );
    let json: serde_json::Value = serde_json::to_value(&by_id).unwrap();
    assert_eq!(
        json["filter"]["ids"],
        serde_json::json!(["12", "9007199254740993"]),
        "ids must be strings so a large id survives a JavaScript client"
    );
}
