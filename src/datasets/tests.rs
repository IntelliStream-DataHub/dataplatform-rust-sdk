use crate::create_api_service;
use crate::datasets::{BasicDatasetFilter, Dataset, DatasetFilter, DatasetSearch};
use crate::generic::{CrudService, IdAndExtId, SearchForm};
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
    let basicfilter = BasicDatasetFilter::new()
        .set_external_id_prefix("test_dataset".to_string())
        .set_policies(vec!["test_policy".to_string()])
        .build();

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

    return Ok(());
    // filter is not implemented for datasets yet, api should change.
    // all the below tests will fail
    // todo implement rest of test when api refactor is done.

    let empty_filter_res = api_service.datasets.filter(&DatasetFilter::new()).await?;
    assert!(empty_filter_res.get_items().len() >= test_dataset.len());

    let filter_res = api_service
        .datasets
        .filter(&DatasetFilter::new().set_filter(basicfilter))
        .await?;
    let expected_filter_res = test_dataset
        .iter()
        .filter(|dt| {
            dt.policies
                .as_ref()
                .unwrap()
                .contains(&"test_policy".to_string())
        })
        .cloned()
        .collect::<Vec<Dataset>>();
    assert!(equal_external_ids(
        filter_res.get_items(),
        &expected_filter_res,
        false
    ));

    let basicfilter = BasicDatasetFilter::new()
        .set_metadata(hashmap! {"test_key0".to_string()=>"test_value0".to_string()})
        .build();
    let searchform = SearchForm {
        query: Some("datasets".to_string()),
        name: None,
        description: None,
    };
    let search_query = DatasetSearch::new()
        .set_filter(basicfilter)
        .set_search(searchform)
        .build();
    let search_res = api_service.datasets.search(&search_query).await?;
    let expected_search_res = test_dataset
        .iter()
        .filter(|dt| dt.metadata.contains_key("test_key0"))
        .cloned()
        .collect::<Vec<Dataset>>();
    assert!(equal_external_ids(
        search_res.get_items(),
        &expected_search_res,
        false
    ));
    Ok(())
}

/// End-to-end: `source` (a shared node field) round-trips through create -> by_ids for a
/// dataset. `source` used to be absent from the SDK `Dataset`; this pins that it is now
/// sent on create and read back.
#[tokio::test]
async fn test_dataset_source_round_trips() -> Result<(), ResponseError> {
    let api = create_api_service();
    let ext = format!("rust_sdk_src_ds_{}", uuid::Uuid::new_v4().simple());
    let mut ds = Dataset::new("Rust SDK Source DS".to_string());
    ds.set_external_id(ext.clone());
    ds.set_source("rust_sdk_source".to_string());

    let ids = vec![IdAndExtId::from_external_id(&ext)];
    api.datasets.delete(&ids).await?;

    api.datasets.create(&ds).await?;
    let mut cleanup = cleanup_datasets(vec![ext.clone()]);

    let read = api.datasets.by_ids(&ids).await?;
    let got = read
        .get_items()
        .iter()
        .find(|d| d.external_id() == &ext)
        .expect("dataset should be readable via by_ids after create");
    assert_eq!(got.source().map(String::as_str), Some("rust_sdk_source"));

    api.datasets.delete(&ids).await?;
    cleanup.disarm();
    Ok(())
}
