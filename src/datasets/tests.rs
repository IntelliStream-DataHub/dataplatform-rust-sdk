use crate::create_api_service;
use crate::datasets::{BasicDatasetFilter, Dataset, DatasetFilter, DatasetSearch};
use crate::errors::DataHubError;
use crate::generic::{IdAndExtId, SearchForm};
use crate::http::ResponseError;
use maplit::hashmap;
use std::fs::metadata;

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
    assert!(equal_external_ids(
        &api_service.datasets.by_ids(&test_ids).await?.get_items(),
        &test_dataset,
        false
    ));
    api_service.datasets.delete(&test_ids).await?;
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
