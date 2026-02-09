use crate::events::Event;
use crate::filters::{BasicEventFilter, EventFilter, TimeFilter};
use crate::generic::{IdAndExtId, IdAndExtIdCollection};
use crate::{create_api_service, ApiService};
use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::StreamExt;
use maplit::hashmap;
use std::collections::HashMap;
use tokio::task::id;

async fn delete_events(api_service: &ApiService, events: Vec<IdAndExtId>) {
    let delete_result = api_service.events.delete(&events).await;
    match delete_result {
        Ok(events) => {
            assert_eq!(events.length(), 0);
        }
        Err(e) => {
            eprintln!("{:?}", e.get_message());
            assert_eq!(e.status.as_u16(), 200);
        }
    }
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
}

fn create_test_events() -> Vec<Event> {
    let api_service = create_api_service();

    let unique_id: u64 = 7110;
    let total_events = 89;
    let mut external_ids: Vec<String> = vec![];
    let mut events: Vec<Event> = vec![];

    for i in 0..total_events {
        let id = unique_id + i;
        let mut event_time: DateTime<Utc> = Utc.with_ymd_and_hms(2025, 9, 5, 0, 0, 0).unwrap();
        event_time = event_time + Duration::minutes(((i * 24) + 24) as i64);
        event_time = event_time + Duration::seconds((i * 3 * 11) as i64);

        let external_id = format!("pump_event_alarm_{:?}", id);
        external_ids.push(external_id.clone());

        let mut new_event = Event::new(external_id.clone());

        new_event.metadata = Option::from(HashMap::from([
            ("bytes".to_string(), (id * 3482 + 15).to_string()),
            (
                "process_time".to_string(),
                ((i as f64 * 0.5).sin().abs() * 10.0).to_string(),
            ),
        ]));
        new_event.event_time = Option::from(event_time);
        new_event.r#type = Option::from("pump".to_string());
        if i % 3 == 0 {
            new_event.sub_type = Option::from("info".to_string());
            if i % 2 == 0 {
                new_event.description =
                    Option::from("Pump is working under safe operating limits".to_string());
            } else {
                new_event.description = Option::from("Pump is in normal state".to_string());
            }
            new_event.set_status("NORMAL");
        } else if i % 5 == 0 {
            new_event.sub_type = Option::from("alarm".to_string());
            if i % 2 == 0 {
                new_event.description = Option::from("Pump is not working properly".to_string());
            } else {
                new_event.description = Option::from(
                    "Pump pressure value has crossed the safe operating limit".to_string(),
                );
            }
            new_event.set_status("UNSAFE");
        } else if i % 6 == 0 {
            new_event.sub_type = Option::from("critical".to_string());
            if i % 2 == 0 {
                new_event.description = Option::from("Pump is under critical stress".to_string());
            } else {
                new_event.description = Option::from(
                    "Pump pressure value is far below safe operating limit".to_string(),
                );
            }
            new_event.set_status("CRITICAL");
        } else {
            new_event.sub_type = Option::from("warning".to_string());
            if i % 2 == 0 {
                new_event.description = Option::from("Pump is under stress".to_string());
            } else {
                new_event.description =
                    Option::from("Pump pressure value is below safe operating limit".to_string());
            }
            new_event.set_status("CAUTION");
        }

        new_event.add_metadata("version".to_string(), "0x0f".to_string());
        new_event.set_source("valheim-pump-events".to_string());

        events.push(new_event);
    }
    events.sort_by_key(|e| e.external_id.clone());
    events
}

//tests create, read delete all field of the basic filter.
#[tokio::test]
async fn test_event_filter() -> Result<(), Box<dyn std::error::Error>> {
    fn equal_external_ids(lhs: &Vec<Event>, rhs: &Vec<Event>, expect_empty: bool) -> bool {
        if lhs.is_empty() || rhs.is_empty() {
            return expect_empty;
        }
        println!("{:?} {:?}", lhs.len(), rhs.len());
        lhs.iter()
            .all(|e| rhs.iter().any(|r| r.external_id == e.external_id))
            && rhs
                .iter()
                .all(|e| lhs.iter().any(|r| r.external_id == e.external_id))
    } // helper function. Events derive PartialEq but that doesnt really work whe id is None.

    let mut test_events = create_test_events();
    let mut basic_filter = BasicEventFilter::new();
    let mut eventfilter = EventFilter::new();
    let api_service = create_api_service();
    let max_time = DateTime::parse_from_rfc3339("2025-09-06T06:08:00Z")
        .unwrap()
        .to_utc();
    let time_delta =
        Duration::minutes(((5 * 24) + 24) as i64) + Duration::seconds((5 * 3 * 11) as i64);
    let min_time = Utc.with_ymd_and_hms(2025, 9, 5, 16, 22, 0).unwrap();
    let time_range = (min_time, min_time + time_delta);
    println!("{}", api_service.config.get_api_token().await?);
    let ids = test_events
        .iter()
        .map(|e| IdAndExtId::from_external_id(&e.external_id))
        .collect::<Vec<IdAndExtId>>();

    api_service.events.delete(&ids).await?;

    api_service.events.create(&test_events).await?;
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    // test empty filter
    let mut empty_filter_res = api_service
        .events
        .filter(&eventfilter.set_filter(&basic_filter))
        .await
        .unwrap();
    // an empty filter should return all events
    assert!(empty_filter_res.get_items().len() >= test_events.len());

    // test external id prefix filter
    basic_filter.set_external_id_prefix("pump");
    let filter_eid_prefix_pump = api_service
        .events
        .filter(&eventfilter.set_filter(&basic_filter))
        .await
        .unwrap();
    let expected_events_post_external_id_filter = &test_events
        .iter()
        .cloned()
        .filter(|eve| eve.external_id.starts_with("pump"))
        .collect::<Vec<Event>>();
    assert!(equal_external_ids(
        &expected_events_post_external_id_filter,
        &filter_eid_prefix_pump.get_items(),
        false
    ));

    // test sub type filter
    basic_filter.set_sub_type("alarm");
    let filter_subtype_alarm = api_service
        .events
        .filter(&eventfilter.set_filter(&basic_filter))
        .await
        .unwrap();
    let expected_events_post_sub_type_filter = &expected_events_post_external_id_filter
        .iter()
        .cloned()
        .filter(|eve| eve.sub_type.as_ref().unwrap().eq("alarm"))
        .collect::<Vec<Event>>();
    assert!(equal_external_ids(
        filter_subtype_alarm.get_items(),
        &expected_events_post_sub_type_filter,
        false
    ));

    let filtermap = hashmap!("bytes".to_string()=>"24770963".to_string());
    let metadata_filter = BasicEventFilter::new().set_metadata(&filtermap).build();
    let res_filter_metadata = api_service
        .events
        .filter(&eventfilter.set_filter(&metadata_filter))
        .await
        .unwrap();
    let expected_events_post_metadata_filter = &test_events
        .iter()
        .cloned()
        .filter(|eve| {
            filtermap
                .iter()
                .all(|(k, v)| eve.metadata.as_ref().unwrap().get(k) == Some(v))
        })
        .collect::<Vec<Event>>();

    assert!(equal_external_ids(
        res_filter_metadata.get_items(),
        &expected_events_post_metadata_filter,
        false
    ));

    println!("Before max time filter:");
    basic_filter.set_event_time(&TimeFilter::Before { max: max_time });
    let res_filter_before_max_time = api_service
        .events
        .filter(&eventfilter.set_filter(&basic_filter))
        .await
        .unwrap();
    let expected_events_post_max_time_filter = &expected_events_post_sub_type_filter
        .iter()
        .cloned()
        .filter(|eve| eve.event_time.as_ref().unwrap().lt(&max_time))
        .collect::<Vec<Event>>();
    assert!(equal_external_ids(
        res_filter_before_max_time.get_items(),
        &expected_events_post_max_time_filter,
        false
    ));

    println!("Before min time filter:");
    let after_filter = BasicEventFilter::new()
        .set_event_time(&TimeFilter::After { min: min_time })
        .build();
    let res_filter_after_min_time = api_service
        .events
        .filter(&eventfilter.set_filter(&after_filter))
        .await
        .unwrap();
    let expected_events_min_time_filter = &test_events
        .iter()
        .cloned()
        .filter(|eve| eve.event_time.as_ref().unwrap().gt(&min_time))
        .collect::<Vec<Event>>();
    assert!(equal_external_ids(
        res_filter_after_min_time.get_items(),
        &expected_events_min_time_filter,
        false
    ));

    println!("Before time range filter:");
    let time_range_filter = BasicEventFilter::new()
        .set_event_time(&TimeFilter::Between {
            min: time_range.0,
            max: time_range.1,
        })
        .build();
    let res_filter_in_time_range = api_service
        .events
        .filter(&eventfilter.set_filter(&time_range_filter))
        .await
        .unwrap();

    let expected_events_time_range_filter = &test_events
        .iter()
        .cloned()
        .filter(|eve| (time_range.0..time_range.1).contains(eve.event_time.as_ref().unwrap()))
        .collect::<Vec<Event>>();
    println!("{:?}", expected_events_time_range_filter.len());
    println!("{:?}", expected_events_time_range_filter);
    println!(
        "{:?}",
        expected_events_time_range_filter
            .iter()
            .all(|eve| (time_range.0..time_range.1).contains(eve.event_time.as_ref().unwrap()))
    );
    assert!(equal_external_ids(
        res_filter_in_time_range.get_items(),
        &expected_events_time_range_filter,
        false
    ));

    println!("Source filter:");
    basic_filter.set_source("valheim-pump-events");
    let res_filter_source = api_service
        .events
        .filter(
            &eventfilter.set_filter(
                &BasicEventFilter::new()
                    .set_source("valheim-pump-events")
                    .build(),
            ),
        )
        .await
        .unwrap();
    let expected_events_source_filter = &test_events
        .iter()
        .cloned()
        .filter(|eve| eve.source.as_ref().unwrap().eq("valheim-pump-events"))
        .collect::<Vec<Event>>();

    println!(
        "{:?}",
        expected_events_source_filter
            .iter()
            .map(|e| e.source.as_ref().unwrap().clone())
            .collect::<Vec<String>>()
    );
    println!(
        "{:?}",
        expected_events_source_filter
            .iter()
            .map(|e| e.external_id.clone())
            .collect::<Vec<String>>()
    );
    println!(
        "{:?}",
        res_filter_source
            .get_items()
            .iter()
            .map(|e| e.source.as_ref().unwrap().clone())
            .collect::<Vec<String>>()
    );
    assert!(equal_external_ids(
        res_filter_source.get_items(),
        expected_events_source_filter,
        false
    ));

    println!("Type filter:");
    let valve_filter = BasicEventFilter::new().set_type("valve").build();
    let filter_type_valve = api_service
        .events
        .filter(&eventfilter.set_filter(&valve_filter))
        .await
        .unwrap();
    assert!(equal_external_ids(
        filter_type_valve.get_items(),
        &vec![],
        true
    ));

    // cleanup
    api_service.events.delete(&ids).await;
    Ok(())
}
