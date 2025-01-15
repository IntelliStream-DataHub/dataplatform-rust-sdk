use std::rc::{Rc, Weak};
use reqwest::{ClientBuilder};
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use crate::datahub::DataHubApi;
use crate::events::EventsService;
use crate::generic::{IdAndExtIdCollection};
use crate::timeseries::{LimitParam, TimeSeries, TimeSeriesUpdateCollection, TimeSeriesService};
use crate::unit::{UnitsService};

mod unit;
mod generic;
mod timeseries;
mod datahub;
mod fields;
mod events;
mod http;

struct ApiService<'a>{
    config: Box<DataHubApi<'a>>,
    pub time_series: TimeSeriesService<'a>,
    pub units: UnitsService<'a>,
    pub events: EventsService<'a>,
    http_client: Client,
}

fn create_api_service() -> Rc<ApiService<'static>> {
    let dataplatform_api = DataHubApi::create_default();

    let t = "Bearer ".to_owned() + dataplatform_api.token.as_ref().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(t.as_str()).unwrap());
    headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers.insert(ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let http_client = ClientBuilder::new().default_headers(headers).build().unwrap();
    let boxed_config = Box::new(dataplatform_api);
    // Clone the base_url before moving boxed_config into ApiService
    let base_url_clone = boxed_config.base_url.clone();

    let mut api_service = Rc::new_cyclic(|weak_self| {
        ApiService {
            config: boxed_config,
            time_series: TimeSeriesService::new(Weak::clone(weak_self), &base_url_clone), // Initialize any other services here
            units: UnitsService::new ( Weak::clone(weak_self), &base_url_clone ), // Pass the Weak reference
            events: EventsService::new ( Weak::clone(weak_self), &base_url_clone ),
            http_client,
        }
    });

    api_service
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseries::{TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateFields};
    use maplit::hashmap;
    use reqwest::StatusCode;
    use crate::generic::{DataWrapper, IdAndExtId};

    #[tokio::test]
    async fn test_unit_requests() -> Result<(), Box<dyn std::error::Error>> {

        let api_service = create_api_service();

        let result = api_service.units.list().await;
        match result {
            Ok(unit_response) => {
                // Directly access the `items` field from the response.
                let units = unit_response.get_items();

                // Verify that the number of units matches the expected count.
                assert_eq!(units.len(), 23);
            }
            Err(error) => {
                // Log the error that occurred during the fetch operation.
                panic!("Error occurred while fetching units: {:?}", error.get_message());
            }
        }

        let id_collection = IdAndExtIdCollection::from_id_vec(vec![9, 23]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 2);
                let items = unit_response.get_items();
                let first_unit = items.get(0).unwrap();
                let second_unit = items.get(1).unwrap();
                assert_eq!(first_unit.external_id, "area_m2");
                assert_eq!(second_unit.external_id, "volume_barrel_pet_us");
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // Test empty id collection
        let id_collection = IdAndExtIdCollection::from_id_vec(vec![]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 0);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec!["energy_kw_hr", "concentration_ppm"]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 2);
                let items = unit_response.get_items();
                let first_unit = items.get(0).unwrap();
                let second_unit = items.get(1).unwrap();
                assert_eq!(first_unit.id, 2);
                assert_eq!(second_unit.id, 5);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // try unit that doesnt exist:
        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec!["australia", "london"]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 0);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // test empty external id
        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec![]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 0);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        let result = api_service.units.by_external_id("volume_barrel_pet_us").await;
        match result {
            Ok(units) => {
                assert_eq!(units.length(), 1);
                let items = units.get_items();
                let first_unit = items.get(0).unwrap();
                assert_eq!(first_unit.id, 23);
                assert_eq!(first_unit.name, "Barrel US petroleum");
                assert_eq!(first_unit.long_name, "Barrel (US)");
                assert_eq!(first_unit.symbol, "bbl{US petroleum}");
                assert_eq!(first_unit.description, "Unit of the volume for crude oil according to the Anglo-American system of units.");
                assert_eq!(first_unit.alias_names, vec!["bbl_us", "bbl", "bbl-us"]);
            },
            Err(e) => {
                println!("{:?}", e.get_message());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_timeseries_requests() -> Result<(), Box<dyn std::error::Error>> {

        let api_service = create_api_service();

        let mut params = LimitParam::new();
        params.set_limit(5);

        let result = api_service.time_series.list_with_limit(&params).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length() as i64, 5);
                println!("Length of time series returned is {:?}", timeseries.length());
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // Test negative number
        params.set_limit(-5);

        let result = api_service.time_series.list_with_limit(&params).await;
        match result {
            Ok(timeseries) => {
                panic!("This test is supposed to fail: {:?}", timeseries);
            },
            Err(e) => {
                assert_eq!(StatusCode::BAD_REQUEST, e.get_status());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_and_delete_timeseries() -> Result<(), Box<dyn std::error::Error>> {
        let unique_id: u64 = 1200;
        let api_service = create_api_service();

        // Delete timeseries first, in case a test failed and the time series exists
        delete_timeseries(unique_id, &api_service).await;

        let ts_collection = create_timeseries(unique_id);
        let result = api_service.time_series.create(&ts_collection).await;

        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 2);

                let mut items = timeseries.get_items();

                println!("{:?}", items);
                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_1200_ts") {
                    assert_eq!(item.external_id, "rust_sdk_test_1200_ts");
                    println!("timeseries with external id: {:?} is equal to: {:?}", item.external_id, "rust_sdk_test_1200_ts");
                    assert_eq!(item.metadata.as_ref().unwrap().len(), 2);
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_1201_ts") {
                    assert_eq!(item.external_id, "rust_sdk_test_1201_ts");
                    println!("timeseries with external id: {:?} is equal to: {:?}", item.external_id, "rust_sdk_test_1201_ts");
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

            },
            Err(e) => {
                assert_ne!(StatusCode::CREATED, e.get_status());
                println!("{:?}", e.get_message());
            }
        }

        // Delete timeseries
        delete_timeseries(unique_id, &api_service).await;

        Ok(())
    }

    async fn delete_timeseries(id: u64, api_service: &ApiService<'_>) {
        let id_collection = IdAndExtIdCollection::from_external_id_vec(
            vec![
                format!("rust_sdk_test_{id}_ts", id = id).as_str(),
                format!("rust_sdk_test_{id}_ts", id = id + 1).as_str()
            ]
        );
        let result = api_service.time_series.delete(&id_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 0);

            },
            Err(e) => {
                println!("{:?}", e.get_message());
            }
        }
    }

    #[tokio::test]
    async fn test_update_timeseries_without_id() -> Result<(), Box<dyn std::error::Error>> {

        let api_service = create_api_service();

        let mut ts_update_collection = TimeSeriesUpdateCollection::new();
        let mut ts_update_fields = TimeSeriesUpdateFields::new();
        let ts_update = TimeSeriesUpdate {
            id: None,
            external_id: None,
            update: ts_update_fields
        };
        ts_update_collection.add_item(ts_update);
        let result = api_service.time_series.update(&ts_update_collection).await;
        match result {
            Ok(timeseries) => {
                panic!("Should be bad request!");

            },
            Err(e) => {
                assert_eq!(StatusCode::BAD_REQUEST, e.get_status());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_and_update_and_delete_timeseries() -> Result<(), Box<dyn std::error::Error>> {
        println!("test_create_and_update_and_delete_timeseries");
        let unique_id: u64 = 1400;
        let api_service = create_api_service();

        // Delete timeseries first, in case a test failed and the time series exists
        delete_timeseries(unique_id, &api_service).await;

        let ts_collection = create_timeseries(unique_id);
        let result = api_service.time_series.create(&ts_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 2);
            },
            Err(e) => {
                println!("{:?}", e.get_message());
            }
        }

        let mut ts_update_collection = TimeSeriesUpdateCollection::new();
        let mut ts_update_fields = TimeSeriesUpdateFields::new();
        ts_update_fields.external_id.set("rust_sdk_test_1400_ts_renamed".to_string());
        ts_update_fields.name.set("Rust SDK Test 1400 TimeSeries Renamed".to_string());
        ts_update_fields.description.set("This is test timeseries generated by rust sdk test code. Renamed.".to_string());
        ts_update_fields.unit.set("fahrenheit".to_string());
        ts_update_fields.unit_external_id.set("temperature_deg_f".to_string());
        ts_update_fields.metadata.add(hashmap!{"newkey".to_string() => "newvalue".to_string()});
        let ts_update = TimeSeriesUpdate {
            id: None,
            external_id: Some("rust_sdk_test_1400_ts".to_string()),
            update: ts_update_fields
        };
        ts_update_collection.add_item(ts_update);

        println!("external_id: {:?}", &ts_update_collection.get_items()[0].external_id.clone().unwrap() );

        let mut ts2_id = None;
        let result = api_service.time_series.update(&ts_update_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 2);

                let mut items = timeseries.get_items();

                println!("updated_timeseries {:?}", items);
                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_1400_ts_renamed") {
                    assert_eq!(item.external_id, "rust_sdk_test_1400_ts_renamed");
                    assert_eq!(item.metadata.as_ref().unwrap().len(), 3);
                    assert_eq!(item.name, "Rust SDK Test 1400 TimeSeries Renamed");
                    match &item.description {
                        Some(desc) => assert_eq!(desc, "This is test timeseries generated by rust sdk test code. Renamed."),
                        None => panic!("Expected description to be present"),
                    }
                    assert_eq!(item.unit, "fahrenheit");
                    match &item.unit_external_id {
                        Some(unit_ext_id) => assert_eq!(unit_ext_id, "temperature_deg_f"),
                        None => panic!("Expected unit_external_id to be present"),
                    }
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_1401_ts") {
                    assert_eq!(item.external_id, "rust_sdk_test_1401_ts");
                    println!("Item ID: {:?}", item.id);
                    ts2_id = Some(item.id);
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

            },
            Err(e) => {
                println!("Message: {:?}, Status: {:?}", e.get_message(), e.get_status());
                panic!("{:?}", e.get_message());
            }
        }

        println!("ts2_id: {:?}", ts2_id);

        let mut id_collection = IdAndExtIdCollection::from_id_vec(vec![ts2_id.unwrap()]);
        id_collection.add_item(IdAndExtId{id: None, external_id: Some("rust_sdk_test_1400_ts".to_string())});
        let result = api_service.time_series.by_ids (&id_collection).await;

        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 2);

                let mut items = timeseries.get_items();

                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_1400_ts_renamed") {
                    assert_eq!(item.external_id, "rust_sdk_test_1400_ts_renamed");
                    assert_eq!(item.metadata.as_ref().unwrap().len(), 3);
                }
            },
            Err(e) => {
                println!("{:?}", e.get_message());
            }
        }

        delete_timeseries(unique_id, &api_service).await;

        Ok(())
    }

    fn create_timeseries(id: u64) -> DataWrapper<TimeSeries> {
        // Use a unique id as rust process tests in parallel
        let mut ts_collection = DataWrapper::new();
        let ts1 = TimeSeries::builder()
            .set_external_id(format!("rust_sdk_test_{id}_ts", id = id).as_str())
            .set_name(format!("Rust SDK Test {id} TimeSeries", id = id).as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_metadata(hashmap! {
                "foo".to_string() => "bar".to_string(),
                "bar".to_string() => "baz".to_string()
            })
            .set_value_type("float").clone();
        ts_collection.add_item(ts1);
        let ts2 = TimeSeries::builder()
            .set_external_id(format!("rust_sdk_test_{id}_ts", id = id + 1).as_str())
            .set_name(format!("Rust SDK Test {id} TimeSeries", id = id + 1).as_str())
            .set_unit("watt")
            .set_value_type("bigint").clone();
        ts_collection.add_item(ts2);
        ts_collection
    }
}
