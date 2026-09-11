
#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Read;
    use std::rc::Rc;
    use std::sync::Arc;
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use maplit::hashmap;
    use reqwest::StatusCode;
    use crate::{create_api_service, ApiService};
    use crate::generic::{DataWrapper, DatapointString, DatapointsCollection, DeleteFilter, IdAndExtId, RetrieveFilter};
    use crate::http::ResponseError;
    use crate::timeseries::{TimeSeries, TimeSeriesFilter, TimeSeriesFilterForm, TimeSeriesUpdate, TimeSeriesUpdateCollection, TimeSeriesUpdateFields};
    use crate::timeseries::binary::BinaryIngestOptions;
    use crate::tests::cleanup::cleanup_timeseries;
    use crate::tests::ids::{unique_id, unique_token};
    use crate::tests::polling::poll_until_for;

    /// Delete the named series; what is not there is not an error.
    ///
    /// Every id here comes from `unique_id`, so a test only ever names series it created. The ids
    /// used to be derived arithmetically from one per-run number — `{id}`, `{id}_renamed` and
    /// `{id + 1}` — and this helper deleted all three, so a run whose number landed one away from
    /// another's deleted that run's series out from under it mid-test.
    async fn delete_timeseries(api_service: &ApiService, external_ids: &[&str]) {
        let id_collection = DataWrapper::from_vec(
            external_ids
                .iter()
                .map(|e| IdAndExtId::from_external_id(e))
                .collect::<Vec<_>>(),
        );
        match api_service.time_series.delete(&id_collection).await {
            Ok(timeseries) => assert_eq!(timeseries.length(), 0),
            Err(e) => println!("{:?}", e.get_message()),
        }
    }


    #[tokio::test]
    async fn test_timeseries_requests() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        let result = api_service.time_series.list(Some(5)).await;
        match result {
            Ok(timeseries) => {
                assert!(timeseries.length() <= 5);
                println!("Length of time series returned is {:?}", timeseries.length());
            },
            Err(e) => {
                panic!("{:?}", e.get_message());

            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn test_list()-> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();
        let result = api_service.time_series.list(None).await;
        match result {
            Ok(timeseries) => {
               // assert!(timeseries.length() <= 5);
                println!("Length of time series returned is {:?}", timeseries.length());
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_filter_timeseries() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        // A metadata value unique to this run isolates the assertions from other data.
        let ext_id = unique_id("ts");
        let unique_value = format!("filter_test_{ext_id}");
        let mut ts_collection = DataWrapper::new();
        let ts = TimeSeries::builder()
            .set_external_id(ext_id.as_str())
            .set_name(format!("Rust SDK Test {ext_id} TimeSeries").as_str())
            .set_unit("celsius")
            .set_metadata(hashmap! {
                    "rust_sdk_filter_key".to_string() => unique_value.clone()
                })
            .set_value_type("float")
            .clone();
        ts_collection.add_item(ts);
        api_service.time_series.create(&ts_collection).await
            .expect("could not create the filter-test timeseries");
        // Armed before the assertions: every one of them panics on failure, which would otherwise
        // skip the delete at the end and leave the series behind.
        let mut ts_cleanup = cleanup_timeseries(vec![ext_id.clone()]);

        // Key + value together must find exactly the created series. The retired
        // `metadataKey`/`metadataValue` pair is one map entry now; a `None` value would ask for the
        // key alone.
        let mut filter = TimeSeriesFilter::default();
        filter.node.metadata = Some(
            [("rust_sdk_filter_key".to_string(), Some(unique_value.clone()))].into(),
        );
        let form = TimeSeriesFilterForm::new(filter.clone(), Some(10));
        match api_service.time_series.filter(&form).await {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
                assert_eq!(timeseries.get_items()[0].external_id, ext_id);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // Adding the unit keeps it; a wrong unit must drop it.
        let mut with_unit = filter.clone();
        with_unit.unit = Some(vec!["celsius".to_string()]);
        match api_service.time_series.filter(&TimeSeriesFilterForm::new(with_unit, None)).await {
            Ok(timeseries) => assert_eq!(timeseries.length(), 1),
            Err(e) => panic!("{:?}", e.get_message()),
        }
        let mut wrong_unit = filter.clone();
        wrong_unit.unit = Some(vec!["watt".to_string()]);
        match api_service.time_series.filter(&TimeSeriesFilterForm::new(wrong_unit, None)).await {
            Ok(timeseries) => assert_eq!(timeseries.length(), 0),
            Err(e) => panic!("{:?}", e.get_message()),
        }

        delete_timeseries(&api_service, &[&ext_id]).await;
        ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
        Ok(())
    }
    #[tokio::test]
    async fn test_create_and_delete_timeseries() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();
        let ext_a = unique_id("ts");
        let ext_b = unique_id("ts");

        let ts_collection = create_timeseries(&ext_a, &ext_b);
        let result = api_service.time_series.create(&ts_collection).await;

        let mut ts_cleanup = cleanup_timeseries(vec![
            ext_a.clone(),
            ext_b.clone(),
        ]);

        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 2);

                let items = timeseries.get_items();

                println!("{:?}", items);
                if let Some(item) = items.iter().find(|&&ref item| item.external_id == ext_a) {
                    assert_eq!(item.external_id, ext_a);
                    println!("timeseries with external id: {:?} is equal to: {:?}", item.external_id, ext_a);
                    assert_eq!(item.metadata.as_ref().unwrap().len(), 2);
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

                if let Some(item) = items.iter().find(|&&ref item| item.external_id == ext_b) {
                    assert_eq!(item.external_id, ext_b);
                    println!("timeseries with external id: {:?} is equal to: {:?}", item.external_id, ext_b);
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
        delete_timeseries(&api_service, &[&ext_a, &ext_b]).await;
        ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown

        Ok(())
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
        let api_service = create_api_service();
        let ext = unique_id("ts");
        let ext_b = unique_id("ts");
        let ext_renamed = format!("{}_renamed", ext);
        let renamed_name = format!("Rust SDK Test {ext} TimeSeries Renamed");

        let ts_collection = create_timeseries(&ext, &ext_b);
        let result = api_service.time_series.create(&ts_collection).await;

        // Both names the subject can be under, plus the sibling the create made.
        let mut ts_cleanup = cleanup_timeseries(vec![
            ext.clone(),
            ext_renamed.clone(),
            ext_b.clone(),
        ]);

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
        ts_update_fields.external_id.set(ext_renamed.clone());
        ts_update_fields.name.set(renamed_name.clone());
        ts_update_fields.description.set("This is test timeseries generated by rust sdk test code. Renamed.".to_string());
        ts_update_fields.unit.set("fahrenheit".to_string());
        ts_update_fields.unit_external_id.set("temperature_deg_f".to_string());
        ts_update_fields.metadata =
            crate::fields::MapField::add(hashmap! {"newkey".to_string() => "newvalue".to_string()});
        let ts_update = TimeSeriesUpdate {
            id: None,
            external_id: Some(ext.clone()),
            update: ts_update_fields
        };
        ts_update_collection.add_item(ts_update);

        println!("external_id: {:?}", &ts_update_collection.get_items()[0].external_id.clone().unwrap());

        let mut ts2_id: Option<u64> = None;
        let result = api_service.time_series.update(&ts_update_collection).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);

                let items = timeseries.get_items();

                println!("updated_timeseries {:?}", items);
                if let Some(item) = items.iter().find(|&&ref item| item.external_id == ext_renamed) {
                    assert_eq!(item.external_id, ext_renamed);
                    assert_eq!(item.metadata.as_ref().unwrap().len(), 3);
                    assert_eq!(item.name, renamed_name);
                    match &item.description {
                        Some(desc) => assert_eq!(desc, "This is test timeseries generated by rust sdk test code. Renamed."),
                        None => panic!("Expected description to be present"),
                    }
                    assert_eq!(item.unit.as_ref().unwrap(), "fahrenheit");
                    match &item.unit_external_id {
                        Some(unit_ext_id) => assert_eq!(unit_ext_id, "temperature_deg_f"),
                        None => panic!("Expected unit_external_id to be present"),
                    }

                    ts2_id = item.id;
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

        let mut id_collection = DataWrapper::from_vec(vec![IdAndExtId::from_id(ts2_id.unwrap())]);
        id_collection.add_item(IdAndExtId { id: None, external_id: Some(ext.clone()) });
        let result = api_service.time_series.by_ids(&id_collection).await;

        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);

                let items = timeseries.get_items();

                if let Some(item) = items.iter().find(|&&ref item| item.external_id == ext_renamed) {
                    assert_eq!(item.external_id, ext_renamed);
                    assert_eq!(item.metadata.as_ref().unwrap().len(), 3);
                }
            },
            Err(e) => {
                println!("{:?}", e.get_message());
            }
        }

        delete_timeseries(&api_service, &[&ext, &ext_renamed, &ext_b]).await;
        ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown

        Ok(())
    }

    fn create_timeseries(ext_a: &str, ext_b: &str) -> DataWrapper<TimeSeries> {
        let mut ts_collection = DataWrapper::new();
        let ts1 = TimeSeries::builder()
            .set_external_id(ext_a)
            .set_name(format!("Rust SDK Test {ext_a} TimeSeries").as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_metadata(hashmap! {
                    "foo".to_string() => "bar".to_string(),
                    "bar".to_string() => "baz".to_string()
                })
            .set_value_type("float").clone();
        ts_collection.add_item(ts1);
        let ts2 = TimeSeries::builder()
            .set_external_id(ext_b)
            .set_name(format!("Rust SDK Test {ext_b} TimeSeries").as_str())
            .set_unit("watt")
            .set_value_type("bigint").clone();
        ts_collection.add_item(ts2);
        ts_collection
    }

    #[tokio::test]
    async fn test_search_timeseries() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        let mut ts_collection = DataWrapper::new();
        let new_ts_ext_id = unique_id("ts");
        // A token of its own for each searched column. The indexed document is name, externalId
        // and description concatenated, so a token shared with the external id would let a hit on
        // the description prove nothing about the description.
        let name_token = unique_token("tsname");
        let description_token = unique_token("tsdesc");
        let new_ts_name = format!("Rust SDK Test {name_token} TimeSeries");
        let description =
            format!("This is test timeseries generated by rust sdk test code. {description_token}");
        let ts1 = TimeSeries::builder()
            .set_external_id(new_ts_ext_id.as_str())
            .set_name(new_ts_name.as_str())
            .set_description(description.as_str())
            .set_unit("celsius")
            .set_metadata(hashmap! {
                    "foo".to_string() => "bar".to_string(),
                    "bar".to_string() => "baz".to_string()
                })
            .set_value_type("float").clone();
        ts_collection.add_item(ts1);
        let result = api_service.time_series.create(&ts_collection).await;

        let mut ts_cleanup = cleanup_timeseries(vec![new_ts_ext_id.clone()]);

        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
            },
            Err(e) => {
                eprintln!("error with timeseries create");
                println!("{:?}", e.get_message());
            }
        }

        // Matching a name is a filter, not a search: the api dropped `search.name`, whose exact
        // equality this pattern list supersedes.
        let mut by_name = TimeSeriesFilter::default();
        by_name.node.name = Some(vec![new_ts_name.clone()]);
        let result = api_service
            .time_series
            .filter(&TimeSeriesFilterForm::new(by_name, None))
            .await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
                assert_eq!(timeseries.get_items()[0].external_id, new_ts_ext_id);
            },
            Err(e) => {
                eprintln!("error filtering timeseries by name");
                println!("{:?}", e.get_message());
            }
        }

        // Bare words AND together, so the run's own token is what makes the phrase select one
        // series. Without it, "SDK Test" alone matches every series this suite has ever created,
        // and a search is capped at its `limit` (100 by default) after ranking — so the one being
        // looked for can be outranked off the page by other tests' data rather than missing.
        let query = format!("SDK Test {name_token}");
        let result = api_service.time_series.search_by_query(query.as_str()).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
                assert_eq!(timeseries.get_items()[0].external_id, new_ts_ext_id);
            },
            Err(e) => {
                eprintln!("error with timeseries search_by_query");
                println!("{:?}", e.get_message());
            }
        }

        // The description column is part of what the phrase already matches, which is why
        // `search.description` is gone. Only the token is unique to this run, so a hit proves the
        // description was searched.
        let query = format!("generated by rust sdk test {description_token}");
        let result = api_service.time_series.search_by_query(query.as_str()).await;
        match result {
            Ok(timeseries) => {
                assert_eq!(timeseries.length(), 1);
                assert_eq!(timeseries.get_items()[0].external_id, new_ts_ext_id);
            },
            Err(e) => {
                eprintln!("error with timeseries description search");
                println!("{:?}", e.get_message());
            }
        }

        delete_timeseries(&api_service, &[&new_ts_ext_id]).await;
        ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown

        Ok(())
    }

    #[tokio::test]
    #[ignore]
    async fn test_datapoints() -> Result<(), Box<dyn std::error::Error>> {
        // Deliberately fixed, and deliberately shared with
        // `test_raw_datapoints_query_if_data_is_already_inserted`, which reads back what this
        // test inserts. Making either unique severs that and the reader has nothing to find.
        // Nothing else in the suite mints an id of this shape, so no other test can address them.
        let unique_id: u64 = 6540;
        let api_service = create_api_service();

        let new_ts_ext_id = format!("rust_sdk_test_{id}_ts", id = unique_id);
        let new_ts_ext_id2 = format!("rust_sdk_test_{id}_ts", id = unique_id + 1);

        // The one place a pre-delete earns its keep: the ids are fixed, so a run that died before
        // its teardown left these behind.
        delete_timeseries(&api_service, &[&new_ts_ext_id, &new_ts_ext_id2]).await;

        let mut ts_collection = DataWrapper::new();

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

        let new_ts_name = format!("Rust SDK Test {id} TimeSeries", id = unique_id + 1);
        let ts2 = TimeSeries::builder()
            .set_external_id(new_ts_ext_id2.as_str())
            .set_name(new_ts_name.as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("bar")
            .set_metadata(hashmap! {
                    "foodda".to_string() => "bardda".to_string(),
                    "bardda".to_string() => "bazdda".to_string()
                })
            .set_value_type("float").clone();

        ts_collection.add_item(ts1);
        // ts_collection.add_item(ts2);

        let result = api_service.time_series.create(&ts_collection).await;

        let mut ts_cleanup = cleanup_timeseries(vec![
            new_ts_ext_id.clone(),
            new_ts_ext_id2.clone(),
        ]);

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

        println!("Prepare datapoints...");
        // Create datapoints
        let mut data_request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());

        let datetime = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        dp_collection.datapoints = create_daily_datapoints(datetime);

        data_request.get_items_mut().push(dp_collection);

        println!("Start datapoint insert!");
        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        match result {
            Ok(r) => {
                assert_eq!(r.get_http_status_code().unwrap(), StatusCode::NO_CONTENT.as_u16());
            },
            Err(e) => {
                eprintln!("error with timeseries datapoints create");
                println!("{:?}", e.get_message());
            }
        }

        //println!("Prepare datapoints for second time series...");
        //let mut data_request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
        //let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());

        //let datetime = Utc.with_ymd_and_hms(2025, 2, 4, 9, 0, 0).unwrap();
        //dp_collection.datapoints = create_daily_datapoints(datetime);
        //for dp in &mut dp_collection.datapoints {
        //    dp.value = dp.value.clone();
        //}

        //data_request.get_items_mut().push(dp_collection);

        println!("Start datapoint insert for second time series!");
        //let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        /*match result {
            Ok(r) => {
                assert_eq!(r.get_http_status_code().unwrap(), StatusCode::NO_CONTENT.as_u16());
            },
            Err(e) => {
                eprintln!("error with timeseries datapoints create");
                println!("{:?}", e.get_message());
            }
        }

         */

        // Wait for the ClickHouse insert+merge to expose every datapoint, then validate.
        poll_datapoint_count(&api_service, &new_ts_ext_id, 100000).await;
        validate_datapoints(&api_service, vec![new_ts_ext_id.clone()]).await;

        // The daily aggregate reads the same rows, so it is ready once they all are.
        poll_datapoint_count(&api_service, &new_ts_ext_id, 100000).await;
        println!("Validate aggregated datapoints...");
        validate_daily_avg(&api_service, vec![new_ts_ext_id.clone()]).await;

        println!("Validate raw datapoints...");
        validate_raw_datapoints_with_cursor(&api_service, new_ts_ext_id.clone()).await;

        println!("Delete datapoints");
        validate_deleted_datapoints(&api_service, new_ts_ext_id.clone()).await;

        // Delete timeseries when complete
        delete_timeseries(&api_service, &[&new_ts_ext_id, &new_ts_ext_id2]).await;
        ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown

        Ok(())
    }

    /// The binary twin of `test_datapoints`: the same sixty days of one-second values, sent
    /// through `POST /timeseries/data/binary` as zstd-compressed Arrow frames, then read back
    /// through the same validations. Needs a backend that serves the binary endpoint.
    #[tokio::test]
    #[ignore]
    async fn test_datapoints_binary() -> Result<(), Box<dyn std::error::Error>> {
        // Fixed for the same reason as test_datapoints's 6540, and two away from it so the two
        // ignored tests can run in one process without addressing each other's series.
        let unique_id: u64 = 6542;
        let api_service = create_api_service();
        let new_ts_ext_id = format!("rust_sdk_test_{id}_ts", id = unique_id);

        // Fixed id, so a run that died before its teardown left this behind.
        delete_timeseries(&api_service, &[&new_ts_ext_id]).await;

        let mut ts_collection = DataWrapper::new();
        let ts = TimeSeries::builder()
            .set_external_id(new_ts_ext_id.as_str())
            .set_name(format!("Rust SDK Test {id} TimeSeries", id = unique_id).as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_value_type("float")
            .clone();
        ts_collection.add_item(ts);
        let created = api_service.time_series.create(&ts_collection).await;
        let mut ts_cleanup = cleanup_timeseries(vec![new_ts_ext_id.clone()]);
        match created {
            Ok(timeseries) => assert_eq!(timeseries.length(), 1),
            Err(e) => panic!("could not create the timeseries: {:?}", e.get_message()),
        }

        println!("Prepare datapoints...");
        let mut data_request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());
        let datetime = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        dp_collection.datapoints = create_daily_datapoints(datetime);
        let last = dp_collection.datapoints.last().unwrap().clone();
        let inserted_points = dp_collection.datapoints.len();
        data_request.add_item(dp_collection);

        println!("Start binary datapoint insert of {inserted_points} points!");
        let started = std::time::Instant::now();
        let result = api_service
            .time_series
            .insert_datapoints_binary(&data_request, &BinaryIngestOptions::default())
            .await;
        match result {
            Ok(r) => assert_eq!(r.get_http_status_code().unwrap(), StatusCode::NO_CONTENT.as_u16()),
            Err(e) => panic!("binary insert failed with {}: {}", e.get_status(), e.get_message()),
        }
        println!("Binary insert took {:?}", started.elapsed());

        // The binary path refreshes the latest-value cache from each series' last row as the
        // request is accepted, so this is readable before ClickHouse has merged anything.
        let id_collection = DataWrapper::from_vec(vec![IdAndExtId::from_external_id(&new_ts_ext_id)]);
        let latest = api_service.time_series.retrieve_latest_datapoint(&id_collection).await?;
        let latest_dp = latest.get_items().first().unwrap().datapoints.first().unwrap();
        assert_eq!(latest_dp.timestamp.timestamp_millis(), last.timestamp.parse::<i64>().unwrap());
        // Compared within a few ULP, not bit-exactly. The value crosses three systems as decimal
        // text (this client, the api's cache, the read path's own rendering), and one of these
        // fixture values carries all 17 significant digits a f64 can hold, which came back one
        // ULP off. What this asserts is that the latest-value cache holds the right point.
        let want = last.value.parse::<f64>().unwrap();
        let got = latest_dp.value.unwrap();
        assert!(
            (got - want).abs() <= want.abs() * 4.0 * f64::EPSILON,
            "latest value {got} is not {want}"
        );

        // Wait for the ClickHouse insert+merge to expose every datapoint, then validate.
        poll_datapoint_count(&api_service, &new_ts_ext_id, 100000).await;
        validate_datapoints(&api_service, vec![new_ts_ext_id.clone()]).await;

        // Not validate_daily_avg: its expected averages are constants for a zero-order-hold
        // weighted average that this platform does not compute, so the JSON twin fails on them
        // too, at the same line with the same numbers. Comparing the two paths against each
        // other tests what this test is for and does not encode a platform version.
        println!("Validate aggregates against the JSON path...");
        let json_ext_id = format!("{new_ts_ext_id}_json");
        let mut json_ts_collection = DataWrapper::new();
        json_ts_collection.add_item(
            TimeSeries::builder()
                .set_external_id(json_ext_id.as_str())
                .set_name(json_ext_id.as_str())
                .set_unit("celsius")
                .set_value_type("float")
                .clone(),
        );
        api_service.time_series.create(&json_ts_collection).await
            .expect("could not create the JSON comparison series");
        let mut json_cleanup = cleanup_timeseries(vec![json_ext_id.clone()]);

        let mut json_request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
        let mut json_dps = DatapointsCollection::from_external_id(json_ext_id.as_str());
        json_dps.datapoints = create_daily_datapoints(datetime);
        json_request.add_item(json_dps);
        api_service.time_series.insert_datapoints(&mut json_request).await
            .expect("JSON insert failed");

        // Both series must be complete before the aggregates can be compared. poll_datapoint_count
        // reads with a 100k limit, so it cannot see past the first 100k of 5.18M and returns long
        // before the series has landed; comparing then comes back unequal because one side is
        // still filling, which looks exactly like a path that stores different values.
        poll_all_points(&api_service, &new_ts_ext_id, inserted_points).await;
        poll_all_points(&api_service, &json_ext_id, inserted_points).await;

        let binary_aggs = daily_aggregates(&api_service, &new_ts_ext_id).await;
        let json_aggs = daily_aggregates(&api_service, &json_ext_id).await;
        assert!(!binary_aggs.is_empty(), "no daily buckets came back");
        assert_eq!(
            binary_aggs, json_aggs,
            "the binary path aggregates differently from the JSON path for identical input"
        );

        println!("Validate raw datapoints with a cursor walk...");
        // Not validate_raw_datapoints_with_cursor: that one asserts a fixed final page size
        // measured against a series that had been written to more than once, so it only holds
        // for whatever the JSON test's series happens to contain. Here the count is known, so
        // the walk asserts the total and the page shape instead.
        let walked = walk_all_datapoints(&api_service, &new_ts_ext_id).await;
        assert_eq!(walked, inserted_points, "cursor walk returned {walked} of {inserted_points} points");

        println!("Delete datapoints");
        validate_deleted_datapoints(&api_service, new_ts_ext_id.clone()).await;

        delete_timeseries(&api_service, &[&new_ts_ext_id, &json_ext_id]).await;
        ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
        json_cleanup.disarm();

        Ok(())
    }

    /// JSON against binary ingest from Rust, reporting the same columns as the Java benchmark in
    /// the platform's `datahub-e2e` module and the Python one in `python_tests`, so the three
    /// clients can be compared.
    ///
    /// Sized by `DATAHUB_BENCH_POINTS` (default 10 million) across `DATAHUB_BENCH_SERIES`
    /// series. The api must have its daily quota and rate limiter off for a run of any size:
    /// `-Ddatahub.limits.quota.enabled=false -Ddatahub.limits.rate.enabled=false`.
    #[tokio::test]
    #[ignore]
    async fn bench_json_vs_binary() -> Result<(), Box<dyn std::error::Error>> {
        let total: usize = std::env::var("DATAHUB_BENCH_POINTS")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(10_000_000);
        let series_count: usize = std::env::var("DATAHUB_BENCH_SERIES")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(100);
        let chunk: usize = std::env::var("DATAHUB_BENCH_CHUNK")
            .ok().and_then(|v| v.parse().ok()).unwrap_or(1_000_000);
        let api_service = create_api_service();
        println!("\n=== {total} points across {series_count} series, float32 ===");

        let mut results = Vec::new();
        for binary in [false, true] {
            let label = if binary { "binary" } else { "JSON" };
            let run_id = unique_id(if binary { "bench_bin" } else { "bench_json" });
            let external_ids: Vec<String> =
                (0..series_count).map(|i| format!("{run_id}_{i}")).collect();

            let mut ts_collection = DataWrapper::new();
            for external_id in &external_ids {
                ts_collection.add_item(
                    TimeSeries::builder()
                        .set_external_id(external_id)
                        .set_name(external_id)
                        .set_unit("celsius")
                        .set_value_type("float32")
                        .clone(),
                );
            }
            api_service.time_series.create(&ts_collection).await
                .expect("could not create the benchmark series");
            let mut cleanup = cleanup_timeseries(external_ids.clone());

            let per_series = chunk / series_count;
            let mut latencies: Vec<f64> = Vec::new();
            let mut sent = 0usize;
            let mut offset = 0i64;
            let started = std::time::Instant::now();
            while sent < total {
                let this_chunk = std::cmp::min(chunk, total - sent);
                let per = std::cmp::max(1, this_chunk / series_count);
                let mut request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
                for (index, external_id) in external_ids.iter().enumerate() {
                    let mut collection = DatapointsCollection::from_external_id(external_id);
                    collection.datapoints = bench_points(offset, per, index);
                    request.add_item(collection);
                }
                let call = std::time::Instant::now();
                if binary {
                    api_service.time_series
                        .insert_datapoints_binary(&request, &BinaryIngestOptions::default())
                        .await
                        .unwrap_or_else(|e| panic!("{label} insert failed: {}", e.get_message()));
                } else {
                    let mut json_request = request.clone();
                    api_service.time_series
                        .insert_datapoints(&mut json_request)
                        .await
                        .unwrap_or_else(|e| panic!("{label} insert failed: {}", e.get_message()));
                }
                latencies.push(call.elapsed().as_secs_f64() * 1000.0);
                sent += per * series_count;
                offset += per as i64;
                let elapsed = started.elapsed().as_secs_f64();
                println!("  {label}: {sent} / {total} points, {:.0} pts/s", sent as f64 / elapsed);
            }
            let ingest_seconds = started.elapsed().as_secs_f64();

            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let mean = latencies.iter().sum::<f64>() / latencies.len() as f64;
            let p50 = latencies[latencies.len() / 2];
            let p99 = latencies[std::cmp::min(latencies.len() - 1, latencies.len() * 99 / 100)];
            results.push((
                label,
                sent,
                ingest_seconds,
                sent as f64 / ingest_seconds,
                latencies.len(),
                mean,
                p50,
                p99,
            ));

            delete_timeseries(&api_service, &external_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>()).await;
            cleanup.disarm();
        }

        println!("\n=== datapoint ingest from Rust: JSON against binary ===");
        println!("{total} points across {series_count} series, float32\n");
        println!("{:<26}{:>18}{:>18}", "metric", results[0].0, results[1].0);
        println!("{:<26}{:>18.1}{:>18.1}", "ingest wall time (s)", results[0].2, results[1].2);
        println!("{:<26}{:>18.0}{:>18.0}", "points per second", results[0].3, results[1].3);
        println!("{:<26}{:>18}{:>18}", "requests", results[0].4, results[1].4);
        println!("{:<26}{:>18.0}{:>18.0}", "latency mean (ms)", results[0].5, results[1].5);
        println!("{:<26}{:>18.0}{:>18.0}", "latency p50 (ms)", results[0].6, results[1].6);
        println!("{:<26}{:>18.0}{:>18.0}", "latency p99 (ms)", results[0].7, results[1].7);
        Ok(())
    }

    /// A slow sine plus noise, one signal per series. Identical series would let zstd compress
    /// the repetition across them and report a wire size no real fleet of sensors produces.
    fn bench_points(offset_seconds: i64, count: usize, series_index: usize) -> Vec<DatapointString> {
        let base = 150.0 + series_index as f64 * 0.7;
        let phase = series_index as f64 * 0.37;
        let mut points = Vec::with_capacity(count);
        for i in 0..count {
            let t = offset_seconds + i as i64;
            let mut z = (t as u64)
                .wrapping_add((series_index as u64).wrapping_mul(0x5851_F42D_4C95_7F2D))
                .wrapping_mul(0x9E37_79B9_7F4A_7C15);
            z ^= z >> 30;
            z = z.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z ^= z >> 27;
            let noise = ((z >> 40) as f64 / (1u64 << 24) as f64) - 0.5;
            let value = (base + 20.0 * (t as f64 / 600.0 + phase).sin() + noise) as f32;
            let timestamp = 1_735_689_600_000i64 + t * 1000;
            points.push(DatapointString::new(&timestamp.to_string(), &value.to_string()));
        }
        points
    }

    /// Waits until the whole series is readable, by walking it. Needed because the cheap count
    /// read is capped at its own limit and cannot tell "100k so far" from "all of it".
    async fn poll_all_points(api_service: &Arc<ApiService>, external_id: &str, want: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
        loop {
            let have = walk_all_datapoints(api_service, external_id).await;
            if have >= want {
                return;
            }
            if std::time::Instant::now() > deadline {
                panic!("{external_id} reached only {have} of {want} points before the deadline");
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    /// Daily avg/min/max for the whole window, truncated so two paths that stored the same
    /// values compare equal without depending on how many digits the read path prints.
    async fn daily_aggregates(
        api_service: &Arc<ApiService>,
        external_id: &str,
    ) -> Vec<(i64, f64, f64, f64)> {
        let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
        let mut rf = RetrieveFilter::new();
        rf.set_external_id(external_id);
        rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
        rf.set_end(Utc.with_ymd_and_hms(2025, 3, 2, 0, 0, 0).unwrap());
        rf.set_aggregates(vec!["avg".to_string(), "min".to_string(), "max".to_string()]);
        rf.set_granularity("1d");
        data_request.add_item(rf);
        let response = api_service
            .time_series
            .retrieve_datapoints(&data_request)
            .await
            .expect("aggregate read failed");
        response
            .get_items()
            .first()
            .map(|item| {
                item.datapoints
                    .iter()
                    .map(|dp| {
                        (
                            dp.timestamp().timestamp_millis(),
                            truncate_10(dp.average().unwrap_or(f64::NAN)),
                            truncate_10(dp.min().unwrap_or(f64::NAN)),
                            truncate_10(dp.max().unwrap_or(f64::NAN)),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Pages the whole series with the keyset cursor and returns how many points came back.
    /// Every page but the last must be full, which is what proves the cursor is not skipping.
    async fn walk_all_datapoints(api_service: &Arc<ApiService>, external_id: &str) -> usize {
        let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
        let mut rf = RetrieveFilter::new();
        rf.set_external_id(external_id);
        rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
        rf.set_end(Utc.with_ymd_and_hms(2025, 3, 2, 0, 0, 0).unwrap());
        // Limit 0, not an explicit page size: an explicit limit caps the whole result and comes
        // back without a cursor, so the walk would stop after one page and read as a series that
        // only ever received its first hundred thousand points.
        rf.set_limit(0);
        data_request.add_item(rf);

        let mut total = 0usize;
        let mut cursor: Option<String> = None;
        loop {
            let mut request = data_request.clone();
            request.get_items_mut().first_mut().unwrap().cursor = cursor.clone();
            let response = api_service
                .time_series
                .retrieve_datapoints(&request)
                .await
                .expect("cursor page failed");
            let page = response.get_items().first().expect("no series in the page");
            total += page.datapoints.len();
            cursor = page.next_cursor.clone();
            if cursor.is_none() {
                break;
            }
        }
        total
    }
    // total is 9 354 000

    /// Poll a series until `want` datapoints are readable from the start of 2025.
    ///
    /// Datapoint ingestion goes through a ClickHouse insert **and** a background merge, so a
    /// just-written series reads back partial for a while — far longer than any other projection
    /// in this suite, which is why this uses the long bound rather than `poll_until`'s default.
    /// It replaces a fixed 90-second sleep: same worst case, but it returns as soon as the data
    /// is actually there, and it says what it was waiting for when it never arrives.
    async fn poll_datapoint_count(
        api_service: &Arc<ApiService>,
        ts_external_id: &str,
        want: usize,
    ) -> usize {
        poll_until_for(
            std::time::Duration::from_secs(150),
            || async {
                let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
                let mut rf = RetrieveFilter::new();
                rf.set_external_id(ts_external_id);
                rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
                rf.set_limit(100000);
                data_request.add_item(rf);
                api_service
                    .time_series
                    .retrieve_datapoints(&data_request)
                    .await
                    .ok()
                    .and_then(|r| r.get_items().first().map(|i| i.datapoints.len()))
                    .unwrap_or(0)
            },
            |count: &usize| *count >= want,
        )
        .await
    }

    async fn validate_datapoints(api_service: &Arc<ApiService>, ts_external_id_vec: Vec<String>) {
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
                assert_eq!(r.get_items().len(), 1);
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
                assert_eq!(r.get_items().len(), 1);
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

    async fn validate_deleted_datapoints(api_service: &Arc<ApiService>, ts_external_id: String) {
        let delete_after_timestamp = Utc.with_ymd_and_hms(2025, 2, 5, 0, 0, 0).unwrap();

        let mut data_request: DataWrapper<DeleteFilter> = DataWrapper::new();
        let df = DeleteFilter::from_external_id(ts_external_id.clone(), Some(delete_after_timestamp), None);
        data_request.add_item(df);

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

        // Deletes land in ClickHouse asynchronously too. Wait for the whole series to shrink to
        // what should survive the delete, rather than sleeping a fixed 90 seconds — the read
        // below is capped at 5000, so on its own it cannot tell "the delete landed" from "the
        // delete has not started yet".
        poll_until_for(
            std::time::Duration::from_secs(150),
            || async {
                let mut data_request: DataWrapper<RetrieveFilter> = DataWrapper::new();
                let mut rf = RetrieveFilter::new();
                rf.set_external_id(&ts_external_id);
                rf.set_start(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
                rf.set_limit(100000);
                data_request.add_item(rf);
                api_service
                    .time_series
                    .retrieve_datapoints(&data_request)
                    .await
                    .ok()
                    .and_then(|r| r.get_items().first().map(|i| i.datapoints.len()))
                    .unwrap_or(usize::MAX)
            },
            |remaining: &usize| *remaining <= 5000,
        )
        .await;

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

    async fn validate_daily_avg(api_service: &Arc<ApiService>, ts_external_id_vec: Vec<String>) {
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
                    // Every caller inserts the full sixty days from 2025-01-01 and reads back to
                    // 2025-03-01: 59 daily buckets, whichever series carries them.
                    assert_eq!(r.get_items().first().unwrap().datapoints.len(), 59);

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

                        if dp.timestamp() == Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap() {
                            // normal avg would return 179.9514040223, but we use avgweighted over the window
                            assert_eq!(truncate_10(dp.average().unwrap()), 179.4516319444);
                        } else if dp.timestamp() == Utc.with_ymd_and_hms(2025, 1, 22, 0, 0, 0).unwrap() {
                            // normal avg would return 180.0561890050
                            assert_eq!(truncate_10(dp.average().unwrap()), 179.5567939814);
                        } else if dp.timestamp() == Utc.with_ymd_and_hms(2025, 2, 22, 0, 0, 0).unwrap() {
                            // normal avg would return 179.9661931149
                            assert_eq!(truncate_10(dp.average().unwrap()), 179.4659953703);
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

    /*#[tokio::test]
    async fn test_raw_datapoints_query_if_data_is_already_inserted() -> Result<(), Box<dyn std::error::Error>> {
        let unique_id: u64 = 6540;
        let api_service = create_api_service();
        let new_ts_ext_id = format!("rust_sdk_test_{id}_ts", id = unique_id);
        let new_ts_ext_id2 = format!("rust_sdk_test_{id}_ts", id = unique_id + 1);
        //validate_raw_datapoints_with_cursor(&api_service, new_ts_ext_id.clone()).await;
        //validate_daily_avg(&api_service, vec![new_ts_ext_id.clone(), new_ts_ext_id2.clone()]).await;

        Ok(())
    }*/

    async fn validate_raw_datapoints_with_cursor(api_service: &Arc<ApiService>, external_id: String) {
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
                            println!("Sum datapoints for loop count:{:?} | {:?}", loop_count + 1, ts.datapoints.len());
                            if ts.next_cursor.is_some() {
                                current_cursor = Some(ts.next_cursor.clone().unwrap());
                            } else {
                                current_cursor = None;
                            }
                            if current_cursor == None {
                                // Final data count is 97600 total 9_468_000
                                assert_eq!(ts.datapoints.len(), 97_600);
                            } else {
                                assert_eq!(ts.datapoints.len(), 100_000);
                            }

                            println!("Next cursor is {:?}", current_cursor);
                        },
                        Err(e) => {
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

    fn create_daily_datapoints(date: DateTime<Utc>) -> Vec<DatapointString> {
        // Create space for all datapoints:
        const NUM_DATAPOINTS: usize = 60 * 24 * 3600;
        let mut datapoints = Vec::with_capacity(NUM_DATAPOINTS);

        println!("Reading datapoint from file...");
        let rdm_values_vec = read_values_from_file().unwrap();
        println!("Reading datapoints from file... Done.");

        // Generate one datapoint for each second of the day
        for idx in 0..NUM_DATAPOINTS {
            let current_time = date + Duration::seconds(idx as i64);
            datapoints.push(DatapointString::from_datetime(current_time, &rdm_values_vec[idx].to_string()));
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
        let api_service = create_api_service();

        let mut ts_collection = DataWrapper::new();

        let new_ts_ext_id = unique_id("ts");
        let new_ts_name = format!("Rust SDK Test {new_ts_ext_id} TimeSeries");
        let ts1 = TimeSeries::builder()
            .set_external_id(new_ts_ext_id.as_str())
            .set_name(new_ts_name.as_str())
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_value_type("float").clone();
        ts_collection.add_item(ts1);

        let result = api_service.time_series.create(&ts_collection).await;

        let mut ts_cleanup = cleanup_timeseries(vec![new_ts_ext_id.clone()]);

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
        let mut data_request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());

        let datetime = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let latest_datetime = datetime + Duration::seconds(4);
        dp_collection.datapoints = vec![
            DatapointString::from_datetime(datetime, "177.6544096666"),
            DatapointString::from_datetime(datetime + Duration::seconds(1), "179.9514040223"),
            DatapointString::from_datetime(datetime + Duration::seconds(2), "178.3544091313"),
            DatapointString::from_datetime(datetime + Duration::seconds(3), "180.0000091313"),
            DatapointString::from_datetime(latest_datetime, "181.3044577713"),
        ];

        data_request.get_items_mut().push(dp_collection);

        println!("Start datapoint insert!");
        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        validate_data_insertion(result);

        let id_collection = DataWrapper::from_vec(vec![IdAndExtId::from_external_id(&new_ts_ext_id)]);
        validate_latest_datapoint(&api_service, latest_datetime, &id_collection).await;

        // Create a new Data point collection with older values
        let mut dp_collection = DatapointsCollection::from_external_id(new_ts_ext_id.as_str());
        dp_collection.datapoints = vec![
            DatapointString::from_datetime(datetime - Duration::seconds(1), "179.9514040223"),
            DatapointString::from_datetime(datetime - Duration::seconds(2), "178.3544091313"),
            DatapointString::from_datetime(datetime - Duration::seconds(3), "180.0000091313"),
        ];
        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        validate_data_insertion(result);

        // See if the latest data point is still the same
        validate_latest_datapoint(&api_service, latest_datetime, &id_collection).await;

        // Delete timeseries when complete
        delete_timeseries(&api_service, &[&new_ts_ext_id]).await;
        ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_datapoints_missing_timeseries_returns_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        // Freshly minted rather than a fixed name: this test's whole premise is that the series
        // does not exist, which a shared id cannot promise.
        let missing_ext_id = unique_id("ts_missing");
        let mut data_request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(&missing_ext_id);
        dp_collection.datapoints = vec![
            DatapointString::from_datetime(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(), "42.0"),
        ];
        data_request.add_item(dp_collection);

        let result = api_service.time_series.insert_datapoints(&mut data_request).await;
        match result {
            Ok(_) => panic!("Expected 404 Not Found for non-existent timeseries"),
            Err(e) => {
                assert_eq!(e.get_status(), StatusCode::NOT_FOUND);
                let msg = e.get_message();
                assert!(
                    msg.contains("Could not find following timeseries"),
                    "unexpected error body: {msg}"
                );
            }
        }
        Ok(())
    }

    /// The binary path resolves every series before it builds a frame, so a missing one is
    /// refused here, with the JSON path's wording, and no frame is ever sent.
    #[tokio::test]
    async fn test_insert_datapoints_binary_missing_timeseries_returns_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        let missing_ext_id = unique_id("ts_missing");
        let mut data_request: DataWrapper<DatapointsCollection<DatapointString>> = DataWrapper::new();
        let mut dp_collection = DatapointsCollection::from_external_id(&missing_ext_id);
        dp_collection.datapoints = vec![
            DatapointString::from_datetime(Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(), "42.0"),
        ];
        data_request.add_item(dp_collection);

        let result = api_service
            .time_series
            .insert_datapoints_binary(&data_request, &BinaryIngestOptions::default())
            .await;
        match result {
            Ok(_) => panic!("Expected 404 Not Found for non-existent timeseries"),
            Err(e) => {
                assert_eq!(e.get_status(), StatusCode::NOT_FOUND);
                let msg = e.get_message();
                assert!(
                    msg.contains("Could not find following timeseries"),
                    "unexpected error body: {msg}"
                );
            }
        }
        Ok(())
    }

    fn validate_data_insertion(result: Result<DataWrapper<String>, ResponseError>) {
        match result {
            Ok(r) => {
                assert_eq!(r.get_http_status_code().unwrap(), StatusCode::NO_CONTENT.as_u16());
            },
            Err(e) => {
                eprintln!("error with timeseries datapoints create");
                println!("{:?}", e.get_message());
            }
        }
    }

    async fn validate_latest_datapoint(
        api_service: &ApiService,
        latest_datetime: DateTime<Utc>,
        id_collection: &DataWrapper<IdAndExtId>
    ) {
        let result = api_service.time_series.retrieve_latest_datapoint(id_collection).await;
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
/// The timeseries filter's wire shape. The shared node criteria flatten into the body; only the
/// data set scope, the two unit lists and `valueType` are the timeseries' own.
#[test]
fn timeseries_filter_matches_the_documented_wire_shape() {
    use crate::filters::NodeFilter;
    use crate::generic::IdAndExtId;
    use crate::timeseries::{TimeSeriesFilter, TimeSeriesFilterForm};

    let filter = TimeSeriesFilter {
        node: NodeFilter {
            name: Some(vec!["RPM*".to_string()]),
            external_id: Some(vec!["rpm_pump_*".to_string()]),
            labels: Some(vec!["PUMP".to_string()]),
            // A `None` value asks for the key alone — what the retired
            // `metadataKey`-without-`metadataValue` used to mean.
            metadata: Some([("sensor_vendor".to_string(), None)].into()),
            ..Default::default()
        },
        data_set_id: Some(vec![IdAndExtId::from_id(12)]),
        unit: Some(vec!["bar".to_string(), "deg_*".to_string()]),
        unit_external_id: Some(vec!["mass_flow_rate_kghr".to_string()]),
        value_type: Some(vec!["FLOAT".to_string()]),
    };

    let body = serde_json::to_value(TimeSeriesFilterForm::new(filter, Some(100))).unwrap();
    assert_eq!(body["limit"], 100);
    let f = &body["filter"];
    assert_eq!(f["name"], serde_json::json!(["RPM*"]));
    assert_eq!(f["externalId"], serde_json::json!(["rpm_pump_*"]));
    assert_eq!(f["labels"], serde_json::json!(["PUMP"]));
    assert_eq!(f["metadata"], serde_json::json!({"sensor_vendor": null}));
    assert_eq!(f["dataSetId"], serde_json::json!([{"id": "12"}]));
    assert_eq!(f["unit"], serde_json::json!(["bar", "deg_*"]));
    assert_eq!(f["unitExternalId"], serde_json::json!(["mass_flow_rate_kghr"]));
    assert_eq!(f["valueType"], serde_json::json!(["FLOAT"]));

    // Every criterion is a list under a singular key. The plural spellings, and the
    // `metadataKey`/`metadataValue` pair before them, must be gone: the api drops unknown keys
    // silently, so a leftover one places no restriction and returns every timeseries the caller
    // can read, which looks like a working query.
    for retired in [
        "dataSetIds", "units", "unitExternalIds", "valueTypes", "names", "externalIds",
        "metadataKey", "metadataValue",
    ] {
        assert!(f.get(retired).is_none(), "retired field {retired} is still sent: {f}");
    }
}

/// A filter with nothing set must send an empty criteria object — every key it emits by default is
/// a restriction the caller never asked for.
#[test]
fn default_timeseries_filter_sends_no_criteria() {
    use crate::timeseries::{TimeSeriesFilter, TimeSeriesFilterForm};

    let body = serde_json::to_value(TimeSeriesFilterForm::new(
        TimeSeriesFilter::default(),
        None,
    ))
    .unwrap();
    assert_eq!(body["filter"], serde_json::json!({}));
    assert!(body.get("limit").is_none(), "unset limit must be omitted so the server default applies");
}

/// Absent and empty `dataSetId` are opposite answers — no restriction versus narrow-to-nothing —
/// and the flatten must not collapse them.
#[test]
fn timeseries_filter_empty_data_set_scope_is_not_the_same_as_none() {
    use crate::timeseries::TimeSeriesFilter;

    let narrowed_to_nothing = TimeSeriesFilter {
        data_set_id: Some(vec![]),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&narrowed_to_nothing).unwrap()["dataSetId"],
        serde_json::json!([])
    );
    assert!(serde_json::to_value(TimeSeriesFilter::default())
        .unwrap()
        .get("dataSetId")
        .is_none());
}

/// Flattened fields have to survive a round trip: serde buffers them through an intermediate
/// representation, which is where a custom (de)serializer like the string-id one silently breaks.
#[test]
fn timeseries_filter_round_trips_through_its_flattened_base() {
    use crate::filters::NodeFilter;
    use crate::timeseries::TimeSeriesFilter;

    let filter = TimeSeriesFilter {
        node: NodeFilter {
            id: Some(vec![9_007_199_254_740_993]),
            name: Some(vec!["RPM*".to_string()]),
            metadata: Some([("health".to_string(), None)].into()),
            ..Default::default()
        },
        unit: Some(vec!["bar".to_string()]),
        ..Default::default()
    };

    let parsed: TimeSeriesFilter =
        serde_json::from_str(&serde_json::to_string(&filter).unwrap()).unwrap();
    assert_eq!(parsed.node, filter.node);
    assert_eq!(parsed.unit, filter.unit);
}

/// The update body's wire shape: every field the server's `TimeseriesFields` has, and nothing
/// else. `source` is one of them; `valueType` is not — the api has no such field on the update
/// form, and an unknown key is dropped silently, so sending one would read like a working
/// re-type while the series kept its original type.
#[test]
fn timeseries_update_matches_the_server_field_set() {
    use crate::fields::Field;
    use crate::timeseries::{TimeSeriesUpdate, TimeSeriesUpdateFields};

    let mut fields = TimeSeriesUpdateFields::new();
    fields.source = Field::value("sap_pi");
    let update = TimeSeriesUpdate {
        id: None,
        external_id: Some("rpm_pump_a".to_string()),
        update: fields,
    };

    let body = serde_json::to_value(&update).unwrap();
    assert_eq!(body["externalId"], "rpm_pump_a");
    let u = &body["update"];
    assert_eq!(u["source"], serde_json::json!({"set": "sap_pi", "setNull": false}));
    assert!(u.get("valueType").is_none(), "valueType is not an updatable field: {u}");

    let cleared = {
        let mut fields = TimeSeriesUpdateFields::new();
        fields.source = Field::null();
        serde_json::to_value(&fields).unwrap()
    };
    assert_eq!(cleared["source"], serde_json::json!({"set": null, "setNull": true}));
}
