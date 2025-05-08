use std::rc::{Rc, Weak};
use reqwest::{ClientBuilder};
use reqwest::Client;
use dotenv::dotenv;

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use crate::datahub::DataHubApi;
use crate::events::EventsService;
use crate::generic::{IdAndExtIdCollection};
use crate::timeseries::{LimitParam, TimeSeriesUpdateCollection, TimeSeriesService};
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
    dotenv().ok(); // Reads the .env file
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

    let api_service = Rc::new_cyclic(|weak_self| {
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
    use std::fs::File;
    use std::io::Read;
    use super::*;
    use crate::timeseries::{TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateFields};
    use maplit::hashmap;
    use reqwest::StatusCode;
    use crate::generic::{DataWrapper, Datapoint, DatapointsCollection, DeleteFilter, IdAndExtId, RetrieveFilter};
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use crate::http::ResponseError;

    #[tokio::test]
    async fn test_unit_requests() -> Result<(), Box<dyn std::error::Error>> {
        
        println!("test_unit_requests");

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
                println!("StatusCode::BAD_REQUEST == 400 is correct!");
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

                let items = timeseries.get_items();

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
                format!("rust_sdk_test_{id}_ts_renamed", id = id).as_str(),
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
        let ts_update_fields = TimeSeriesUpdateFields::new();
        let ts_update = TimeSeriesUpdate {
            id: None,
            external_id: None,
            update: ts_update_fields
        };
        ts_update_collection.add_item(ts_update);
        let result = api_service.time_series.update(&ts_update_collection).await;
        match result {
            Ok(_timeseries) => {
                panic!("Should be bad request!");
            },
            Err(e) => {
                assert_eq!(StatusCode::BAD_REQUEST, e.get_status());
                println!("StatusCode::BAD_REQUEST == 400 is correct!");
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
                assert_eq!(timeseries.length(), 1);

                let items = timeseries.get_items();

                println!("updated_timeseries {:?}", items);
                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_1400_ts_renamed") {
                    assert_eq!(item.external_id, "rust_sdk_test_1400_ts_renamed");
                    assert_eq!(item.metadata.as_ref().unwrap().len(), 3);
                    assert_eq!(item.name, "Rust SDK Test 1400 TimeSeries Renamed");
                    match &item.description {
                        Some(desc) => assert_eq!(desc, "This is test timeseries generated by rust sdk test code. Renamed."),
                        None => panic!("Expected description to be present"),
                    }
                    assert_eq!(item.unit.as_ref().unwrap(), "fahrenheit");
                    match &item.unit_external_id {
                        Some(unit_ext_id) => assert_eq!(unit_ext_id, "temperature_deg_f"),
                        None => panic!("Expected unit_external_id to be present"),
                    }

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
                assert_eq!(timeseries.length(), 1);

                let items = timeseries.get_items();

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

    #[tokio::test]
    async fn test_search_timeseries() -> Result<(), Box<dyn std::error::Error>> {

        let unique_id: u64 = 6400;
        let api_service = create_api_service();

        // Delete timeseries first, in case a test failed and the time series exists
        delete_timeseries(unique_id, &api_service).await;

        let mut ts_collection = DataWrapper::new();
        let new_ts_ext_id = format!("rust_sdk_test_{id}_ts", id = unique_id);
        let new_ts_name = format!("Rust SDK Test {id} TimeSeries", id = unique_id);
        let ts1 = TimeSeries::builder()
            .set_external_id(new_ts_ext_id.as_str())
            .set_name(new_ts_name.as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_metadata(hashmap! {
                "foo".to_string() => "bar".to_string(),
                "bar".to_string() => "baz".to_string()
            })
            .set_value_type("float").clone();
        ts_collection.add_item(ts1);
        let result = api_service.time_series.create(&ts_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
            },
            Err(e) => {
                eprintln!("error with timeseries create");
                println!("{:?}", e.get_message());
            }
        }

        let result = api_service.time_series.search_by_name(new_ts_name.as_str()).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
            },
            Err(e) => {
                eprintln!("error with timeseries search_by_name");
                println!("{:?}", e.get_message());
            }
        }

        let query = format!("SDK Test {id}", id = unique_id);
        let result = api_service.time_series.search_by_query(query.as_str()).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
            },
            Err(e) => {
                eprintln!("error with timeseries search_by_query");
                println!("{:?}", e.get_message());
            }
        }

        let query = "generated by rust sdk test";
        let result = api_service.time_series.search_by_description(query).await;
        match result {
            Ok(timeseries) => {
                let mut found_timeseries = vec![];
                for item in timeseries.get_items() {
                    if item.external_id.contains(unique_id.to_string().as_str()) {
                        found_timeseries.push(item.clone());
                    }
                }
                assert_eq!(found_timeseries.len(), 1);
            },
            Err(e) => {
                eprintln!("error with timeseries search_by_description");
                println!("{:?}", e.get_message());
            }
        }

        let id_collection = IdAndExtIdCollection::from_external_id_vec(
            vec![ format!("rust_sdk_test_{id}_ts", id = unique_id).as_str() ]
        );
        let result = api_service.time_series.delete(&id_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 0);

            },
            Err(e) => {
                eprintln!("error with timeseries delete");
                eprintln!("{:?}", e.get_message());
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_datapoints() -> Result<(), Box<dyn std::error::Error>> {

        let unique_id: u64 = 6540;
        let api_service = create_api_service();

        // Delete timeseries first, in case a test failed and the time series exists
        delete_timeseries(unique_id, &api_service).await;
        delete_timeseries(unique_id + 1, &api_service).await;

        let mut ts_collection = DataWrapper::new();

        let new_ts_ext_id = format!("rust_sdk_test_{id}_ts", id = unique_id);
        let new_ts_name = format!("Rust SDK Test {id} TimeSeries", id = unique_id);
        let ts1 = TimeSeries::builder()
            .set_external_id(new_ts_ext_id.as_str())
            .set_name(new_ts_name.as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_metadata(hashmap! {
                "foo".to_string() => "bar".to_string(),
                "bar".to_string() => "baz".to_string()
            })
            .set_value_type("float").clone();

        let new_ts_ext_id2 = format!("rust_sdk_test_{id}_ts", id = unique_id+1);
        let new_ts_name = format!("Rust SDK Test {id} TimeSeries", id = unique_id+1);
        let ts2 = TimeSeries::builder()
            .set_external_id(new_ts_ext_id2.as_str())
            .set_name(new_ts_name.as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("bar")
            .set_metadata(hashmap! {
                "foodda".to_string() => "bardda".to_string(),
                "bardda".to_string() => "bazdda".to_string()
            })
            .set_value_type("bigint").clone();

        ts_collection.add_item(ts1);
        ts_collection.add_item(ts2);

        let result = api_service.time_series.create(&ts_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 2);
                println!("Time series created successfully!");
            },
            Err(e) => {
                eprintln!("error with timeseries create");
                println!("{:?}", e.get_message());
            }
        }

        println!("Prepare datapoints...");
        // Create datapoints
        let mut data_request: DataWrapper<DatapointsCollection<Datapoint>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());

        let datetime = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        dp_collection.datapoints = create_daily_datapoints(datetime);

        data_request.get_items_mut().push( dp_collection );

        println!("Start datapoint insert!");
        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        match result {
            Ok(r) => {
                assert_eq!(r.get_http_status_code().unwrap(), StatusCode::CREATED.as_u16());
            },
            Err(e) => {
                eprintln!("error with timeseries datapoints create");
                println!("{:?}", e.get_message());
            }
        }

        println!("Prepare datapoints for second time series...");
        let mut data_request: DataWrapper<DatapointsCollection<Datapoint>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id2.as_str());

        let datetime = Utc.with_ymd_and_hms(2025, 2, 4, 9, 0, 0).unwrap();
        dp_collection.datapoints = create_daily_datapoints(datetime);
        for dp in  &mut dp_collection.datapoints {
            dp.value = dp.value;
        }

        data_request.get_items_mut().push( dp_collection );

        println!("Start datapoint insert for second time series!");
        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        match result {
            Ok(r) => {
                assert_eq!(r.get_http_status_code().unwrap(), StatusCode::CREATED.as_u16());
            },
            Err(e) => {
                eprintln!("error with timeseries datapoints create");
                println!("{:?}", e.get_message());
            }
        }

        // Before validating inserted data, sleep for 60 seconds...
        // This is because it takes some time before data is inserted and merged in clickhouse
        println!("Sleeping for 60 seconds...while waiting for data to be inserted into clickhouse.");
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        println!("Done sleeping.");

        validate_datapoints(&api_service, vec![new_ts_ext_id.clone(), new_ts_ext_id2.clone()]).await;

        // Before validating inserted data, sleep for 60 seconds...
        // This is because it takes some time before data is inserted into clickhouse and merged into the table
        println!("Sleeping for 60 seconds...while waiting for data to be inserted into clickhouse and merged into timeseries.");
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        println!("Done sleeping.");

        println!("Validate aggregated datapoints...");
        validate_daily_avg(&api_service, vec![new_ts_ext_id.clone(), new_ts_ext_id2.clone()]).await;

        println!("Validate raw datapoints...");
        validate_raw_datapoints_with_cursor(&api_service, new_ts_ext_id.clone()).await;

        println!("Delete datapoints");
        validate_deleted_datapoints(&api_service, new_ts_ext_id.clone()).await;

        // Delete timeseries when complete
        //delete_timeseries(unique_id, &api_service).await;
        //delete_timeseries(unique_id+1, &api_service).await;

        Ok(())
    }

    async fn validate_datapoints(api_service: &Rc<ApiService<'_>>, ts_external_id_vec: Vec<String>) {
        for ts_external_id in &ts_external_id_vec {
            let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
            let mut rf = RetrieveFilter::new();
            rf.set_external_id(ts_external_id);
            rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
            rf.set_limit(100000);
            data_request.add_item(rf);
            let result = api_service.time_series.retrieve_datapoints(&data_request).await;
            match result {
                Ok(r) => {
                    assert_eq!(r.get_items().first().unwrap().datapoints.len(), 100000);

                    let start_date = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
                    for dp in &r.get_items().first().unwrap().datapoints {
                        // Fail if the timestamp is before the start_date
                        assert!(
                            dp.timestamp >= start_date,
                            "Timestamp {} is before the specified start date {}",
                            dp.timestamp,
                            start_date
                        );
                        let min_val = 160.0;
                        let max_val = 200.0;
                        assert!(
                            dp.value.unwrap() >= min_val && dp.value.unwrap() <= max_val,
                            "Value {} is not in the range [160, 200]",
                            dp.value.unwrap()
                        );
                    }
                },
                Err(e) => {
                    eprintln!("error with datapoints fetch");
                    println!("{:?}", e.get_message());
                }
            }
        }

        let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
        for ts_external_id in &ts_external_id_vec {
            let mut rf = RetrieveFilter::new();
            rf.set_external_id(ts_external_id);
            rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
            rf.set_limit(200);
            data_request.add_item(rf);
        }

        let result = api_service.time_series.retrieve_datapoints(&data_request).await;
        match result {
            Ok(r) => {
                assert_eq!(r.get_items().len(), 2);
                r.get_items().iter().for_each(|item| {
                    assert_eq!(item.datapoints.len(), 200);
                });
            },
            Err(e) => {
                eprintln!("error with datapoints fetch");
                println!("{:?}", e.get_message());
            }
        }

        let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
        for ts_external_id in &ts_external_id_vec {
            let mut rf = RetrieveFilter::new();
            rf.set_external_id(ts_external_id);
            rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 6, 0, 0).unwrap());
            rf.set_end(Utc.with_ymd_and_hms(2025, 1, 1, 7, 0, 0).unwrap());
            rf.set_limit(3600);
            data_request.add_item(rf);
        }

        let result = api_service.time_series.retrieve_datapoints(&data_request).await;
        match result {
            Ok(r) => {
                assert_eq!(r.get_items().len(), 2);
                for item in r.get_items().iter() {
                    if let Some(external_id) = &item.external_id {
                        // Compare references to strings, not moving them
                        if external_id == &ts_external_id_vec[0] {
                            assert_eq!(item.datapoints.len(), 3600);
                        } else if external_id == &ts_external_id_vec[1] {
                            assert_eq!(item.datapoints.len(), 0);
                        }
                    } else {
                        panic!("Item missing external_id");
                    }
                }
            },
            Err(e) => {
                eprintln!("error with datapoints fetch");
                println!("{:?}", e.get_message());
            }
        }
    }

    async fn validate_deleted_datapoints(api_service: &Rc<ApiService<'_>>, ts_external_id: String){
        let delete_after_timestamp = Utc.with_ymd_and_hms(2025, 2, 5, 0, 0, 0).unwrap();

        let mut data_request: DataWrapper<DeleteFilter> = DataWrapper::new();
        let df = DeleteFilter::from_external_id(ts_external_id.clone(), Some(delete_after_timestamp), None);
        data_request.add_item( df );

        let result = api_service.time_series.delete_datapoints(&data_request).await;
        match result {
            Ok(r) => {
                assert_eq!(r.get_http_status_code().unwrap(), StatusCode::NO_CONTENT.as_u16());
            },
            Err(e) => {
                eprintln!("error with datapoints delete");
                println!("{:?}", e.get_message());
            }
        }

        println!("Sleeping for 60 seconds...while waiting for data to be deleted in clickhouse.");
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;

        // Validate datapoints that is left
        let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
        let mut rf = RetrieveFilter::new();
        rf.set_external_id(&ts_external_id);
        rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 6, 0, 0).unwrap());
        rf.set_limit(5000);
        data_request.add_item(rf);

        let result = api_service.time_series.retrieve_datapoints(&data_request).await;
        match result {
            Ok(r) => {
                assert_eq!(r.get_items().first().unwrap().datapoints.len(), 5000);
            },
            Err(e) => {
                eprintln!("error with checking datapoints left after delete");
                println!("{:?}", e.get_message());
            }
        }
    }

    async fn validate_daily_avg(api_service: &Rc<ApiService<'_>>, ts_external_id_vec: Vec<String>) {
        for ts_external_id in &ts_external_id_vec {
            let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
            let mut rf = RetrieveFilter::new();
            rf.set_external_id(ts_external_id);
            rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
            rf.set_end(Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap());
            rf.set_aggregates(vec!["avg".to_string(), "min".to_string(), "max".to_string()]);
            rf.set_granularity("1d");
            data_request.add_item(rf);
            let result = api_service.time_series.retrieve_datapoints(&data_request).await;
            match result {
                Ok(r) => {

                    if let Some(first_item) = r.get_items().first() {
                        if let Some(external_id) = &first_item.external_id {
                            if external_id == "rust_sdk_test_6540_ts" {
                                assert_eq!(r.get_items().first().unwrap().datapoints.len(), 59);
                            } else {
                                assert_eq!(r.get_items().first().unwrap().datapoints.len(), 25);
                            }
                        }
                    }

                    let start_date = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
                    let end_date = Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap();
                    for dp in &r.get_items().first().unwrap().datapoints {
                        // Fail if the timestamp is before the start_date
                        assert!(
                            dp.timestamp >= start_date,
                            "Timestamp {} is before the specified start date {}",
                            dp.timestamp,
                            start_date
                        );
                        // Fail if the timestamp is after the end_date
                        assert!(
                            dp.timestamp <= end_date,
                            "Timestamp {} is after the specified end date {}",
                            dp.timestamp,
                            end_date
                        );

                        if let Some(first_item) = r.get_items().first() {
                            if let Some(external_id) = &first_item.external_id {
                                if external_id == "rust_sdk_test_6540_ts" {
                                    if dp.get_timestamp() == Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap() {
                                        assert_eq!(truncate_10(dp.get_average().unwrap()), 179.9514040223);
                                    }
                                    else if dp.get_timestamp() == Utc.with_ymd_and_hms(2025, 1, 22, 0, 0, 0).unwrap() {
                                        assert_eq!(truncate_10(dp.get_average().unwrap()), 180.0561890050);
                                    }
                                    else if dp.get_timestamp() == Utc.with_ymd_and_hms(2025, 2, 22, 0, 0, 0).unwrap() {
                                        assert_eq!(truncate_10(dp.get_average().unwrap()), 179.9661931149);
                                    }
                                } else {
                                    if dp.get_timestamp() == Utc.with_ymd_and_hms(2025, 2, 5, 0, 0, 0).unwrap() {
                                        assert_eq!(truncate_10(dp.get_average().unwrap()), 179.4611111111);
                                    }
                                    else if dp.get_timestamp() == Utc.with_ymd_and_hms(2025, 2, 22, 0, 0, 0).unwrap() {
                                        assert_eq!(truncate_10(dp.get_average().unwrap()), 179.4927662037);
                                    }
                                }
                            }
                        }
                    }
                },
                Err(e) => {
                    eprintln!("error with datapoints fetch");
                    println!("{:?}", e.get_message());
                }
            }
        }
    }

    #[tokio::test]
    async fn test_raw_datapoints_query_if_data_is_already_inserted() -> Result<(), Box<dyn std::error::Error>> {

        //let unique_id: u64 = 6540;
        //let api_service = create_api_service();
        //let new_ts_ext_id = format!("rust_sdk_test_{id}_ts", id = unique_id);
        //validate_raw_datapoints_with_cursor(&api_service, new_ts_ext_id.clone()).await;

        Ok(())
    }

    async fn validate_raw_datapoints_with_cursor(api_service: &Rc<ApiService<'_>>, external_id: String) {
        println!("Validate raw datapoints with cursor...");
        let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
        let mut rf = RetrieveFilter::new();
        rf.set_external_id(external_id.as_str());
        rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
        rf.set_end(Utc.with_ymd_and_hms(2025, 3, 1, 0, 0, 0).unwrap());
        rf.set_limit(0);
        data_request.add_item(rf);
        println!("Request data... {:?}", data_request);
        let result = api_service.time_series.retrieve_datapoints(&data_request).await;
        match result {
            Ok(r) => {

                let ts = r.get_items().first().unwrap();
                let next_cursor = ts.next_cursor.clone().unwrap();
                assert!(!next_cursor.is_empty(), "next_cursor should not be empty");
                assert_eq!(ts.datapoints.len(), 100000);

                println!("Got cursor id: {:?}", next_cursor);

                let mut current_cursor: Option<String> = Some(next_cursor);
                let mut loop_count = 1; // We have already completed 1 request
                loop {
                    let mut new_data_request = data_request.clone();
                    let rf = new_data_request.get_items_mut().first_mut().unwrap();
                    rf.cursor = current_cursor.clone();
                    let result = api_service.time_series.retrieve_datapoints(&new_data_request).await;
                    match result {
                        Ok(r) => {
                            let ts = r.get_items().first().unwrap();
                            println!("Sum datapoints for loop count:{:?} | {:?}", loop_count +1, ts.datapoints.len());

                            if loop_count == 50 {
                                // Final data count is 97600
                                assert_eq!(ts.datapoints.len(), 97600);
                            } else {
                                assert_eq!(ts.datapoints.len(), 100000);
                            }
                            if ts.next_cursor.is_some(){
                                current_cursor = Some(ts.next_cursor.clone().unwrap());
                            } else {
                                current_cursor = None;
                            }
                            println!("Next cursor is {:?}", current_cursor);
                        },Err(e) => {
                            eprintln!("error with datapoints with cursor fetch");
                            println!("{:?}", e.get_message());
                        }
                    }
                    loop_count += 1;

                    if current_cursor.is_none() {
                        break;
                    }
                }
            },
            Err(e) => {
                eprintln!("error with datapoints with cursor fetch");
                println!("{:?}", e.get_message());
            }
        }
    }

    fn create_daily_datapoints(date: DateTime<Utc> ) -> Vec<Datapoint> {
        // Create space for all datapoints:
        const NUM_DATAPOINTS: usize = 60 * 24 * 3600;
        let mut datapoints = Vec::with_capacity(NUM_DATAPOINTS);

        println!("Reading datapoint from file...");
        let rdm_values_vec = read_values_from_file().unwrap();
        println!("Reading datapoints from file... Done.");

        // Generate one datapoint for each second of the day
        for idx in 0..NUM_DATAPOINTS {
            let current_time = date + Duration::seconds(idx as i64);
            datapoints.push( Datapoint::from(current_time, rdm_values_vec[idx]) );
        }

        datapoints
    }

    fn read_values_from_file() -> Result<Vec<f64>, Box<dyn std::error::Error>> {
        // Read the entire file content
        let mut file = File::open("resources/test/random_values.csv")?;
        let mut content = String::new();
        file.read_to_string(&mut content)?;

        // Parse comma-separated values into Vec<f64>
        let values: Vec<f64> = content
            .split(',')
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())       // Skip empty entries
            .filter_map(|s| s.parse::<f64>().ok())  // Ignore entries that fail to parse
            .collect();

        assert_eq!(values.len(), 60 * 24 * 3600);

        Ok(values)
    }

    fn truncate_10(x: f64) -> f64 {
        // Clickhouse will have rounding errors using for example avg(), so we truncate the returned
        // values to mitigate this
        let multiplier = 10f64.powf(10.0);
        (x * multiplier).floor() / multiplier
    }

    #[tokio::test]
    async fn test_latest_datapoint() -> Result<(), Box<dyn std::error::Error>> {
        let unique_id: u64 = 6610;
        let api_service = create_api_service();

        // Delete timeseries first, in case a test failed and the time series exists
        delete_timeseries(unique_id, &api_service).await;

        let mut ts_collection = DataWrapper::new();

        let new_ts_ext_id = format!("rust_sdk_test_{id}_ts", id = unique_id);
        let new_ts_name = format!("Rust SDK Test {id} TimeSeries", id = unique_id);
        let ts1 = TimeSeries::builder()
            .set_external_id(new_ts_ext_id.as_str())
            .set_name(new_ts_name.as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_value_type("float").clone();
        ts_collection.add_item(ts1);

        let result = api_service.time_series.create(&ts_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
                println!("Time series created successfully!");
            },
            Err(e) => {
                eprintln!("error with timeseries create");
                println!("{:?}", e.get_message());
            }
        }

        println!("Insert datapoints...");

        // Create datapoints
        let mut data_request: DataWrapper<DatapointsCollection<Datapoint>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());

        let datetime = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let latest_datetime = datetime + Duration::seconds(4);
        dp_collection.datapoints = vec![
            Datapoint::from(datetime, 177.6544096666),
            Datapoint::from(datetime + Duration::seconds(1), 179.9514040223),
            Datapoint::from(datetime + Duration::seconds(2), 178.3544091313),
            Datapoint::from(datetime + Duration::seconds(3), 180.0000091313),
            Datapoint::from(latest_datetime, 181.3044577713),
        ];

        data_request.get_items_mut().push( dp_collection );

        println!("Start datapoint insert!");
        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        validate_data_insertion(result);
        
        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec![&new_ts_ext_id]);
        validate_latest_datapoint(&api_service, latest_datetime, &id_collection).await;

        // Create a new Data point collection with older values
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());
        dp_collection.datapoints = vec![
            Datapoint::from(datetime - Duration::seconds(1), 179.9514040223),
            Datapoint::from(datetime - Duration::seconds(2), 178.3544091313),
            Datapoint::from(datetime - Duration::seconds(3), 180.0000091313),
        ];
        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        validate_data_insertion(result);

        // See if the latest data point is still the same
        validate_latest_datapoint(&api_service, latest_datetime, &id_collection).await;

        // Delete timeseries when complete
        delete_timeseries(unique_id, &api_service).await;

        Ok(())
    }

    fn validate_data_insertion(result: Result<DataWrapper<String>, ResponseError>) {
        match result {
            Ok(r) => {
                assert_eq!(r.get_http_status_code().unwrap(), StatusCode::CREATED.as_u16());
            },
            Err(e) => {
                eprintln!("error with timeseries datapoints create");
                println!("{:?}", e.get_message());
            }
        }
    }

    async fn validate_latest_datapoint(api_service: &ApiService<'_>, latest_datetime: DateTime<Utc>, id_collection: &IdAndExtIdCollection) {
        let result = api_service.time_series.retrieve_latest_datapoint(&id_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
                assert_eq!(timeseries.get_items().len(), 1);
                assert_eq!(timeseries.get_items().first().unwrap().datapoints.len(), 1);
                let datapoint = timeseries.get_items().first().unwrap().datapoints.first().unwrap();
                assert_eq!(datapoint.timestamp, latest_datetime);
                assert_eq!(datapoint.value.unwrap(), 181.3044577713);
            },
            Err(e) => {
                println!("{:?}", e.get_message());
                panic!("error with timeseries retrival for latest data point");
            }
        }
    }
}
