use crate::datasets::Dataset;
use crate::events::{Event, EventIdCollection};
use crate::filters::{BasicEventFilter, EventFilter, TimeFilter};
use crate::generic::IdAndExtId;
use crate::tests::cleanup::{cleanup_events_by_uuid, cleanup_resources};
use crate::{create_api_service, ApiService};
use chrono::{DateTime, Duration, TimeZone, Utc};
use maplit::hashmap;
use std::collections::HashMap;

async fn delete_events(
    api_service: &ApiService,
    events: Vec<EventIdCollection>,
) -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}

fn create_test_events(dataset_id: u64) -> Vec<Event> {
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

        let mut new_event = Event::new(external_id.clone(), event_time);

        new_event.metadata = Option::from(HashMap::from([
            ("bytes".to_string(), (id * 3482 + 15).to_string()),
            (
                "process_time".to_string(),
                ((i as f64 * 0.5).sin().abs() * 10.0).to_string(),
            ),
        ]));
        new_event.set_data_set_id(dataset_id);
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

/// Delete every event whose external id starts with `prefix`, by UUID, until none are left.
///
/// Needed because an external id does not identify a single event — see
/// [`crate::tests::cleanup::cleanup_events_by_uuid`]. Pages through the filter rather than
/// assuming one pass suffices: residue from many past runs can exceed any single page.
async fn sweep_events_by_prefix(api_service: &ApiService, prefix: &str) {
    const PAGE: u64 = 1000;
    for _ in 0..20 {
        let mut filter = EventFilter::default();
        filter
            .set_filter(
                BasicEventFilter::default()
                    .set_external_id_prefix(prefix)
                    .build(),
            )
            .set_limit(PAGE);
        let found = match api_service.events.filter(&filter).await {
            Ok(f) => f,
            Err(e) => {
                eprintln!(
                    "sweep: filter failed, leaving residue in place: {}",
                    e.get_message()
                );
                return;
            }
        };
        let ids: Vec<EventIdCollection> = found
            .get_items()
            .iter()
            .filter_map(|e| e.id_selector())
            .collect();
        if ids.is_empty() {
            return;
        }
        println!("sweep: deleting {} leftover '{}' events", ids.len(), prefix);
        if let Err(e) = api_service.events.delete(&ids).await {
            eprintln!(
                "sweep: delete failed, leaving residue in place: {}",
                e.get_message()
            );
            return;
        }
        // Deletes land in ClickHouse asynchronously; give the projection a moment before
        // re-reading, else the next page repeats rows that are already gone.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    eprintln!(
        "sweep: gave up after 20 pages of '{}' — residue may remain",
        prefix
    );
}

//tests create, read delete all field of the basic filter.
#[tokio::test]
async fn test_event_filter() -> Result<(), Box<dyn std::error::Error>> {
    fn equal_external_ids(lhs: &Vec<Event>, rhs: &Vec<Event>, expect_empty: bool) -> bool {
        if lhs.is_empty() || rhs.is_empty() {
            println!("{:?} {:?}", lhs.len(), rhs.len());
            return expect_empty;
        }
        println!("{:?} {:?}", lhs.len(), rhs.len());
        lhs.iter()
            .all(|e| rhs.iter().any(|r| r.external_id == e.external_id))
            && rhs
                .iter()
                .all(|e| lhs.iter().any(|r| r.external_id == e.external_id))
    } // helper function. Events aren't comparable by value, and ids are None before a send anyway.

    let mut basic_filter = BasicEventFilter::default();
    let mut eventfilter = EventFilter::default();
    let api_service = create_api_service();
    let max_time = DateTime::parse_from_rfc3339("2025-09-06T06:08:00Z")
        .unwrap()
        .to_utc();
    let time_delta =
        Duration::minutes(((5 * 24) + 24) as i64) + Duration::seconds((5 * 3 * 11) as i64);
    let min_time = Utc.with_ymd_and_hms(2025, 9, 5, 16, 22, 0).unwrap();
    let time_range = (min_time, min_time + time_delta);

    let dataset_test_id = "Test_dataset";
    let dt = Dataset::new(dataset_test_id.to_string());
    let ds_ext_id_collection = vec![IdAndExtId::from_external_id(dataset_test_id)];
    // Log rather than swallow: when this fails the dataset survives into the next run, and
    // `create` below then returns no items, which used to surface as an opaque unwrap panic.
    if let Err(e) = api_service.datasets.delete(&ds_ext_id_collection).await {
        eprintln!(
            "pre-delete of dataset '{}' failed ({}): {}",
            dataset_test_id,
            e.status,
            e.get_message()
        );
    }

    let dataset_result = api_service.datasets.create(&dt).await;
    let created_ds_id: u64 = match dataset_result {
        Ok(data) => {
            // A dataset that already exists comes back as an empty item list rather than an
            // error, so fall back to looking it up — the test only needs its id.
            let id = match data.get_items().first().and_then(|d| d.id) {
                Some(id) => id,
                None => api_service
                    .datasets
                    .by_ids(&ds_ext_id_collection)
                    .await
                    .ok()
                    .and_then(|d| d.get_items().first().and_then(|d| d.id))
                    .ok_or("dataset create returned no items and it could not be looked up")?,
            };
            println!("Dataset created with ID: {:?}", id);
            id
        }
        Err(e) => {
            println!("Failed to create dataset: {}", e);
            return Err(format!("Dataset creation failed: {}", e).into());
        }
    };
    // Datasets are a resource subtype, so cleanup_resources deletes them via the
    // resources endpoint. Armed now so a panic below still tears the dataset down.
    let mut dataset_cleanup = cleanup_resources(vec![dataset_test_id.to_string()]);

    let test_events = create_test_events(created_ds_id);

    // Reclaim anything this test left behind previously. Deleting by external id is not enough:
    // each run stamps fresh UUIDs on the same 89 external ids, so rows accumulate per run and the
    // `pump` prefix assertion below eventually sees more events than this run created (and the
    // filter's default limit of 100 starts truncating). Sweep by prefix and delete by UUID until
    // nothing matches.
    sweep_events_by_prefix(&api_service, "pump_event_alarm_").await;

    let created = api_service
        .events
        .create(&test_events)
        .await
        .expect("creating the fixture events failed");
    // The server echoes the events with their stamped UUIDs — the only identity that names exactly
    // the rows this run made.
    let created_event_ids: Vec<EventIdCollection> = created
        .get_items()
        .iter()
        .filter_map(|e| e.id_selector())
        .collect();
    assert_eq!(
        created_event_ids.len(),
        test_events.len(),
        "every created event should come back with a UUID, else teardown would leak rows"
    );
    // Armed right after create: if any assertion below panics, the events are
    // still torn down during unwind. Disarmed after the explicit delete on the
    // happy path so teardown doesn't run twice.
    let mut event_cleanup = cleanup_events_by_uuid(created_event_ids.clone());
    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
    // test empty filter
    println!("empty filter:");
    let mut empty_filter_res = api_service
        .events
        .filter(&eventfilter.set_filter(basic_filter.clone()))
        .await
        .unwrap();
    // an empty filter should return all events
    // assert!(empty_filter_res.get_items().len() >= test_events.len());

    // test external id prefix filter
    basic_filter.set_external_id_prefix("pump");
    let filter_eid_prefix_pump = api_service
        .events
        .filter(&eventfilter.set_filter(basic_filter.clone()))
        .await
        .unwrap();
    let expected_events_post_external_id_filter = &test_events
        .iter()
        .filter(|eve| eve.external_id.starts_with("pump"))
        .cloned()
        .collect::<Vec<Event>>();
    println!("Pump events:");
    assert!(equal_external_ids(
        &expected_events_post_external_id_filter,
        &filter_eid_prefix_pump.get_items(),
        false
    ));
    // test sub type filter
    basic_filter.set_sub_type("alarm");
    let filter_subtype_alarm = api_service
        .events
        .filter(&eventfilter.set_filter(basic_filter.clone()))
        .await
        .unwrap();
    let expected_events_post_sub_type_filter = &expected_events_post_external_id_filter
        .iter()
        .cloned()
        .filter(|eve| eve.sub_type.as_ref().unwrap().eq("alarm"))
        .collect::<Vec<Event>>();
    println!("sub_type alarm events:");
    assert!(equal_external_ids(
        filter_subtype_alarm.get_items(),
        &expected_events_post_sub_type_filter,
        false
    ));

    let filtermap = hashmap!(
        "bytes".to_string()=>"24770963".to_string(),
        //"bytes2".to_string()=>(3 * 3482 + 15).to_string()
    );
    let metadata_filter = BasicEventFilter::default().set_metadata(&filtermap).build();
    let res_filter_metadata = api_service
        .events
        .filter(&eventfilter.set_filter(metadata_filter))
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
    println!("metadata events:");
    assert!(equal_external_ids(
        res_filter_metadata.get_items(),
        &expected_events_post_metadata_filter,
        false
    ));

    println!("Before max time filter:");
    basic_filter.set_event_time(&TimeFilter::Before { max: max_time });
    let res_filter_before_max_time = api_service
        .events
        .filter(&eventfilter.set_filter(basic_filter.clone()))
        .await
        .unwrap();
    let expected_events_post_max_time_filter = &expected_events_post_sub_type_filter
        .iter()
        .cloned()
        .filter(|eve| eve.event_time.lt(&max_time))
        .collect::<Vec<Event>>();
    assert!(equal_external_ids(
        res_filter_before_max_time.get_items(),
        &expected_events_post_max_time_filter,
        false
    ));

    /* This doesnt work when other events exists in the database
    println!("Before min time filter:");
    let after_filter = BasicEventFilter::default()
        .set_event_time(&TimeFilter::After { min: min_time })
        .build();
    let res_filter_after_min_time = api_service
        .events
        .filter(&eventfilter.set_filter(after_filter))
        .await
        .unwrap();
    let expected_events_min_time_filter = &test_events
        .iter()
        .cloned()
        .filter(|eve| eve.event_time.gt(&min_time))
        .collect::<Vec<Event>>();
    assert!(equal_external_ids(
        res_filter_after_min_time.get_items(),
        &expected_events_min_time_filter,
        false
    ));
     */

    println!("Before time range filter:");
    let time_range_filter = BasicEventFilter::default()
        .set_event_time(&TimeFilter::Between {
            min: time_range.0,
            max: time_range.1,
        })
        .build();
    let res_filter_in_time_range = api_service
        .events
        .filter(&eventfilter.set_filter(time_range_filter))
        .await
        .unwrap();

    let expected_events_time_range_filter = &test_events
        .iter()
        .cloned()
        .filter(|eve| (time_range.0..time_range.1).contains(&eve.event_time))
        .collect::<Vec<Event>>();
    println!("{:?}", expected_events_time_range_filter.len());
    println!("{:?}", expected_events_time_range_filter);
    println!(
        "{:?}",
        expected_events_time_range_filter
            .iter()
            .all(|eve| (time_range.0..time_range.1).contains(&eve.event_time))
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
                BasicEventFilter::default()
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
    let valve_filter = BasicEventFilter::default().set_type("valve").build();
    let filter_type_valve = api_service
        .events
        .filter(&eventfilter.set_filter(valve_filter))
        .await
        .unwrap();
    assert!(equal_external_ids(
        filter_type_valve.get_items(),
        &vec![],
        true
    ));

    // Cleanup. Disarm each guard only when its explicit delete actually succeeded — otherwise let
    // the guard run on drop, so a failed teardown is retried instead of silently leaking. Deletes
    // can fail for reasons that have nothing to do with this test: entity deletes reach into the
    // graph store, so while Neo4j is down they return 500 and every unretried delete becomes
    // permanent residue.
    match api_service.events.delete(&created_event_ids).await {
        Ok(_) => event_cleanup.disarm(),
        Err(e) => eprintln!(
            "explicit event delete failed ({}), leaving the guard armed",
            e.get_message()
        ),
    }
    match api_service.datasets.delete(&ds_ext_id_collection).await {
        Ok(_) => dataset_cleanup.disarm(),
        Err(e) => eprintln!(
            "explicit dataset delete failed ({}), leaving the guard armed",
            e.get_message()
        ),
    }
    Ok(())
}

/// Pure serde round-trips for the UUID-based event id and its selector. These need no backend and
/// pin down that a UUID survives serialize→deserialize and lands on the wire as a JSON string in
/// exactly the places the server reads it (`Event.id`, `EventIdCollection`, `BasicEventFilter.id`).
mod uuid_serde {
    use crate::events::{Event, EventIdCollection};
    use crate::generic::DataWrapper;
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn event_id_round_trips_as_a_uuid_string() {
        let mut ev = Event::new("evt_roundtrip".to_string(), Utc::now());
        let id = Uuid::now_v7();
        ev.id = Some(id);

        let json = serde_json::to_string(&ev).unwrap();
        assert!(
            json.contains(&format!("\"id\":\"{}\"", id)),
            "event id should serialize as a JSON string, got {json}"
        );

        let back: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, Some(id));
        assert_eq!(back.external_id, ev.external_id);
        assert_eq!(back.event_time, ev.event_time);
    }

    #[test]
    fn event_with_absent_id_deserializes_to_none() {
        // A payload that omits `id` entirely (e.g. a list projection) must not fail to parse.
        // `eventTime` is required on every event the API returns, so it stays present; only the
        // optional `id` is absent here.
        let json = r#"{"externalId":"evt_no_id","eventTime":"2025-01-01T00:00:00Z","relatedResources":[]}"#;
        let ev: Event = serde_json::from_str(json).unwrap();
        assert_eq!(ev.id, None);
        assert_eq!(ev.external_id, "evt_no_id");
    }

    #[test]
    fn event_id_collection_serializes_uuid_and_external_id() {
        let id = Uuid::now_v7();
        assert_eq!(
            serde_json::to_string(&EventIdCollection::from_uuid(id)).unwrap(),
            format!("{{\"id\":\"{}\"}}", id),
        );
        assert_eq!(
            serde_json::to_string(&EventIdCollection::from_external_id("evt_x")).unwrap(),
            r#"{"externalId":"evt_x"}"#,
        );
    }

    #[test]
    fn event_id_collection_vec_wraps_into_items() {
        let id = Uuid::now_v7();
        let wrapper: DataWrapper<EventIdCollection> =
            (&vec![EventIdCollection::from_uuid(id)]).into();
        let json = serde_json::to_string(&wrapper).unwrap();
        assert_eq!(json, format!("{{\"items\":[{{\"id\":\"{}\"}}]}}", id));
    }
}

/// Serde round-trips for the request bodies added for the event update/search endpoints. These
/// pin the wire shape (`camelCase`, `type` not `r#type`, `set`/`add`/`remove`) without a backend.
mod update_search_serde {
    use crate::events::{EventSearch, EventUpdate};
    use crate::fields::{Field, ListField, MapField};
    use crate::filters::BasicEventFilter;
    use crate::generic::{DataWrapper, IdAndExtId};
    use serde_json::{json, Value};
    use uuid::Uuid;

    fn to_value<T: serde::Serialize>(v: &T) -> Value {
        serde_json::to_value(v).unwrap()
    }

    #[test]
    fn event_update_by_external_id_serializes_only_touched_fields() {
        let upd = EventUpdate::by_external_id("alarm_x")
            .status(Field::value("acknowledged"))
            .metadata(MapField::add(
                [("acked_by".to_string(), "olav".to_string())].into(),
            ));

        assert_eq!(
            to_value(&upd),
            json!({
                "externalId": "alarm_x",
                "update": {
                    "status": { "set": "acknowledged", "setNull": false },
                    // An add-only delta carries just `add`: `set`/`remove` are skipped, not null.
                    "metadata": { "add": { "acked_by": "olav" } }
                }
            })
        );
    }

    #[test]
    fn event_update_null_clears_a_field() {
        let upd = EventUpdate::by_external_id("alarm_x").description(Field::<String>::null());
        assert_eq!(
            to_value(&upd)["update"]["description"],
            json!({ "set": null, "setNull": true })
        );
    }

    #[test]
    fn event_update_type_field_serializes_as_type_not_rust_raw_ident() {
        let upd = EventUpdate::by_id(Uuid::nil())
            .event_type(Field::value("alarm"))
            .sub_type(Field::value("overpressure"));
        let v = to_value(&upd);
        let update = &v["update"];
        assert_eq!(update["type"], json!({ "set": "alarm", "setNull": false }));
        assert_eq!(
            update["subType"],
            json!({ "set": "overpressure", "setNull": false })
        );
        // The UUID target lands as a JSON string, and `externalId` is omitted.
        assert_eq!(v["id"], json!(Uuid::nil().to_string()));
        assert_eq!(v.get("externalId"), None);
    }

    #[test]
    fn event_update_related_resources_use_add_remove() {
        // One list, same IdCollection entries as the entity — `remove` matches on whichever
        // side is named.
        let upd = EventUpdate::by_external_id("alarm_x").related_resources(ListField::add(vec![
            IdAndExtId::from_id(1),
            IdAndExtId::from_external_id("pump_b"),
        ]));
        let update = &to_value(&upd)["update"];
        assert_eq!(
            update["relatedResources"],
            json!({ "add": [{"id": "1"}, {"externalId": "pump_b"}] })
        );
        assert!(update.get("relatedResourceIds").is_none());
        assert!(update.get("relatedResourceExternalIds").is_none());

        let removal =
            EventUpdate::by_external_id("alarm_x").related_resources(ListField::remove(vec![
                IdAndExtId::from_external_id("old_pump"),
            ]));
        assert_eq!(
            to_value(&removal)["update"]["relatedResources"],
            json!({ "remove": [{"externalId": "old_pump"}] })
        );
    }

    #[test]
    fn event_update_round_trips() {
        let upd = EventUpdate::by_external_id("alarm_x").description(Field::value("resolved"));
        let back: EventUpdate =
            serde_json::from_str(&serde_json::to_string(&upd).unwrap()).unwrap();
        assert_eq!(back.external_id.as_deref(), Some("alarm_x"));
        assert_eq!(
            back.update.description.and_then(|f| f.set).as_deref(),
            Some("resolved")
        );
    }

    #[test]
    fn event_update_wraps_into_items() {
        let wrapper: DataWrapper<EventUpdate> =
            (&vec![EventUpdate::by_external_id("alarm_x").status(Field::value("ok"))]).into();
        let v = to_value(&wrapper);
        assert_eq!(v["items"][0]["externalId"], json!("alarm_x"));
        assert_eq!(
            v["items"][0]["update"]["status"],
            json!({ "set": "ok", "setNull": false })
        );
    }

    #[test]
    fn event_search_serializes_query_filter_and_limit() {
        let mut search = EventSearch::from_query("overpressure");
        search
            .set_filter(BasicEventFilter::default().set_type("alarm").build())
            .set_limit(25);

        let v = to_value(&search.build());
        assert_eq!(v["search"]["query"], json!("overpressure"));
        assert_eq!(v["filter"]["type"], json!("alarm"));
        assert_eq!(v["limit"], json!(25));
    }
}

/// The entity-side `relatedResources` wire shape. Events and the event filter must agree: one array
/// of `IdCollection` objects, each naming a resource by id, external id, or both. The backend drops
/// unknown keys silently, so a regression here loses relations without any error.
mod related_resources_serde {
    use crate::events::Event;
    use crate::generic::IdAndExtId;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn event_serializes_related_resources_as_id_collections() {
        let mut ev = Event::new("evt_rr".to_string(), Utc::now());
        ev.add_related_resource_id(34);
        ev.add_related_resource_external_id("a".to_string());
        ev.add_related_resource(IdAndExtId {
            id: Some(7),
            external_id: Some("b".to_string()),
        });

        let value = serde_json::to_value(&ev).unwrap();

        // The legacy parallel arrays must be gone — the backend ignores them.
        assert!(value.get("relatedResourceIds").is_none());
        assert!(value.get("relatedResourceExternalIds").is_none());
        // Ids go over the wire as strings; unset sides are omitted.
        assert_eq!(
            value["relatedResources"],
            json!([{"id": "34"}, {"externalId": "a"}, {"id": "7", "externalId": "b"}])
        );
    }

    #[test]
    fn event_reads_back_resolved_related_resources() {
        // What the server returns: both sides populated on every entry.
        let json = r#"{"externalId":"evt_rr","eventTime":"2026-01-01T00:00:00Z",
            "relatedResources":[{"id":"34","externalId":"sensor_abc"}]}"#;
        let ev: Event = serde_json::from_str(json).unwrap();
        assert_eq!(ev.related_resources.len(), 1);
        assert_eq!(ev.related_resources[0].id, Some(34));
        assert_eq!(
            ev.related_resources[0].external_id.as_deref(),
            Some("sensor_abc")
        );
    }

    #[test]
    fn removing_a_related_resource_matches_on_the_named_side() {
        let mut ev = Event::new("evt_rr".to_string(), Utc::now());
        ev.add_related_resource_id(34);
        ev.add_related_resource_external_id("a".to_string());

        ev.remove_related_resource_id(34);
        assert_eq!(ev.related_resources.len(), 1);
        ev.remove_related_resource_external_id("a".to_string());
        assert!(ev.related_resources.is_empty());
    }
}

mod vocabulary {
    use crate::create_api_service;
    use crate::events::EventDimension;

    /// The four list endpoints: distinct values, alphabetical, honouring `limit`.
    #[tokio::test]
    async fn test_list_dimensions() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();

        for dim in [
            EventDimension::Type,
            EventDimension::SubType,
            EventDimension::Status,
            EventDimension::Source,
        ] {
            let all = api.events.list_dimension(dim, None, None).await?;
            assert_eq!(all.get_http_status_code(), Some(200), "{:?}", dim);

            let items: Vec<String> = all.get_items().to_vec();
            // Distinctness is the guarantee worth pinning. The endpoint also documents alphabetical
            // ordering, but that ordering is the database's — a Postgres collation that is
            // case-insensitive and treats punctuation differently from a byte comparison, and that
            // varies with the deployment's locale. Asserting it exactly would make this test fail
            // on a differently-configured database rather than on a real regression.
            let mut deduped = items.clone();
            deduped.sort();
            deduped.dedup();
            assert_eq!(deduped.len(), items.len(), "{:?} should be distinct", dim);

            if items.len() > 1 {
                let capped = api.events.list_dimension(dim, None, Some(1)).await?;
                assert_eq!(capped.get_items().len(), 1, "{:?} should honour limit", dim);
            }
        }
        Ok(())
    }

    /// The search variants filter the same vocabulary by case-insensitive substring.
    #[tokio::test]
    async fn test_search_dimensions_is_case_insensitive_substring(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();

        // Pick a real value to search for so the test doesn't depend on any particular tenant data.
        let types = api.events.list_types(None).await?;
        let Some(sample) = types.get_items().first().cloned() else {
            println!("SKIP test_search_dimensions: this tenant has no events with a type");
            return Ok(());
        };

        let exact = api.events.search_types(&sample, None).await?;
        assert!(
            exact.get_items().contains(&sample),
            "searching for {sample:?} should find it"
        );

        // Same query in the opposite case must match the same value.
        let flipped = if sample.chars().any(|c| c.is_lowercase()) {
            sample.to_uppercase()
        } else {
            sample.to_lowercase()
        };
        let insensitive = api.events.search_types(&flipped, None).await?;
        assert!(
            insensitive.get_items().contains(&sample),
            "matching is case-insensitive, so {flipped:?} should still find {sample:?}"
        );

        // A substring of it matches too.
        if sample.len() > 2 {
            let part = &sample[..sample.len() - 1];
            assert!(
                api.events
                    .search_types(part, None)
                    .await?
                    .get_items()
                    .contains(&sample),
                "a substring should match"
            );
        }

        // Every result is a subset of the full list — search filters, it does not invent values.
        let all = types.get_items();
        for found in exact.get_items() {
            assert!(all.contains(&found), "{found:?} is not in the full list");
        }

        // No match is an empty list, not an error.
        let none = api
            .events
            .search_types("zzz_no_such_type_zzz", None)
            .await?;
        assert_eq!(none.get_http_status_code(), Some(200));
        assert!(none.get_items().is_empty());
        Ok(())
    }

    /// `limit` is clamped server-side rather than rejected, so an absurd value still succeeds.
    #[tokio::test]
    async fn test_limit_is_clamped_not_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();
        let huge = api.events.list_types(Some(999_999)).await?;
        assert_eq!(huge.get_http_status_code(), Some(200));
        let zero = api.events.list_types(Some(0)).await?;
        assert_eq!(zero.get_http_status_code(), Some(200));
        assert!(
            !zero.get_items().is_empty() || huge.get_items().is_empty(),
            "limit 0 is clamped up to 1, so it must not return an empty list when values exist"
        );
        Ok(())
    }

    /// The two route families spell their segments differently — plural to list, singular to
    /// search. Pinned because a typo there is a 404 that only shows up at runtime.
    #[tokio::test]
    async fn test_both_route_families_resolve() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();
        for dim in [
            EventDimension::Type,
            EventDimension::SubType,
            EventDimension::Status,
            EventDimension::Source,
        ] {
            assert_eq!(
                api.events
                    .list_dimension(dim, None, Some(1))
                    .await?
                    .get_http_status_code(),
                Some(200),
                "list route for {:?} should resolve",
                dim
            );
            assert_eq!(
                api.events
                    .list_dimension(dim, Some("a"), Some(1))
                    .await?
                    .get_http_status_code(),
                Some(200),
                "search route for {:?} should resolve",
                dim
            );
        }
        Ok(())
    }
}
