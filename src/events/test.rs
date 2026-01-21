#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use crate::{create_api_service, ApiService};
    use crate::events::Event;
    use crate::generic::IdAndExtIdCollection;

    #[tokio::test]
    async fn test_create_and_delete_events() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        let unique_id: u64 = 7010;
        let event_time: DateTime<Utc> = Utc.with_ymd_and_hms(2025, 5, 17, 0, 0, 0).unwrap();
        let external_id = format!("valve_event_alarm_{:?}", unique_id);

        delete_events(&api_service, vec![&external_id]).await;

        let mut new_event = Event::new(
            external_id.clone(),
        );
        new_event.metadata = Option::from(
            HashMap::from([
                ("hostname".to_string(), "sveinar".to_string()),
                ("time".to_string(), "2025-05-16T09:04:53.257Z".to_string()),
            ])
        );
        new_event.event_time = Option::from(event_time);
        new_event.r#type = Option::from("valve".to_string());
        new_event.sub_type = Option::from("alarm".to_string());
        new_event.description = Option::from("Gas valve attached to pipe AS-PIP-2452".to_string());

        new_event.add_metadata("version".to_string(), "0x0".to_string());
        new_event.add_metadata("TEST-REMOVE".to_string(), "foobar".to_string());
        new_event.remove_metadata("TEST-REMOVE".to_string());
        new_event.set_source("valve-events".to_string());

        // TODO: uncomment when resources api is working
        /*new_event.related_resource_external_ids = vec!["AS-PIP-2452".to_string()];
        new_event.related_resource_ids = vec![1234];
        new_event.add_related_resource_id(2345);
        new_event.add_related_resource_id(1111);
        new_event.remove_related_resource_id(1111);
        new_event.add_related_resource_external_id("AS-PLP-2333".to_string());
        new_event.add_related_resource_external_id("TEST-REMOVE".to_string());
        new_event.remove_related_resource_external_id("TEST-REMOVE".to_string());*/

        let result = api_service.events.create_one(&new_event).await;
        match result {
            Ok(events) => {
                assert_eq!(events.length(), 1);
                let event = events.get_items().first().unwrap();
                assert_eq!((event.get_id()).is_some(), true);
                assert_eq!(event.get_external_id(), &external_id);
                assert_eq!(event.get_type().unwrap(), "valve");
                assert_eq!(event.get_sub_type().unwrap(), "alarm");
                assert_eq!(event.get_source().unwrap(), "valve-events");
                assert_eq!(*event.get_event_time().unwrap(), event_time);
                println!("Event created successfully!");
            },
            Err(e) => {
                eprintln!("{:?}", e.get_message());
                assert_eq!(e.status.as_u16(), 201);
                panic!("error with Event create");
            }
        }

        delete_events(&api_service, vec![&external_id]).await;
        find_events_by_external_ids(&api_service, vec![&external_id]).await;
        Ok(())
    }

    async fn delete_events(api_service: &ApiService, events: Vec<&str>) {
        let delete_result = api_service.events.delete_by_external_ids(events).await;
        match delete_result {
            Ok(events) => {
                assert_eq!(events.length(), 0);
            },
            Err(e) => {
                eprintln!("{:?}", e.get_message());
                assert_eq!(e.status.as_u16(), 200);
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }

    async fn find_events_by_external_ids(api_service: &ApiService, external_ids: Vec<&str>) {
        let result = api_service.events.by_ids(&IdAndExtIdCollection::from_external_id_vec(external_ids)).await;
        match result {
            Ok(events) => {
                assert_eq!(events.get_http_status_code().unwrap(), 200);
                assert_eq!(events.length(), 0);
            },
            Err(e) => {
                eprintln!("{:?}", e.get_message());
            }
        }
    }

    #[tokio::test]
    async fn test_event_filter() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        let unique_id: u64 = 7110;
        let total_events = 89;
        let mut external_ids: Vec<String> = vec![];
        let mut events: Vec<Event> = vec![];

        for i in 0..total_events {
            let id = unique_id + i;
            let mut event_time: DateTime<Utc> = Utc.with_ymd_and_hms(2025, 9, 5, 0, 0, 0).unwrap();
            event_time = event_time + Duration::minutes(((i * 24) + 24) as i64);
            event_time = event_time + Duration::seconds((i*3 * 11) as i64);

            let external_id = format!("pump_event_alarm_{:?}", id);
            external_ids.push(external_id.clone());

            let mut new_event = Event::new(
                external_id.clone(),
            );

            new_event.metadata = Option::from(
                HashMap::from([
                    ("bytes".to_string(), (id * 3482 + 15).to_string()),
                    ("process_time".to_string(), ((i as f64 * 0.5).sin().abs() * 10.0).to_string()),
                ])
            );
            new_event.event_time = Option::from(event_time);
            new_event.r#type = Option::from("pump".to_string());
            if i % 3 == 0 {
                new_event.sub_type = Option::from("info".to_string());
                if i % 2 == 0 {
                    new_event.description = Option::from("Pump is working under safe operating limits".to_string());
                } else {
                    new_event.description = Option::from("Pump is in normal state".to_string());
                }
                new_event.set_status("NORMAL");
            } else if i % 5 == 0 {
                new_event.sub_type = Option::from("alarm".to_string());
                if i % 2 == 0 {
                    new_event.description = Option::from("Pump is not working properly".to_string());
                } else {
                    new_event.description = Option::from("Pump pressure value has crossed the safe operating limit".to_string());
                }
                new_event.set_status("UNSAFE");
            } else if i % 6 == 0 {
                new_event.sub_type = Option::from("critical".to_string());
                if i % 2 == 0 {
                    new_event.description = Option::from("Pump is under critical stress".to_string());
                } else {
                    new_event.description = Option::from("Pump pressure value is far below safe operating limit".to_string());
                }
                new_event.set_status("CRITICAL");
            } else {
                new_event.sub_type = Option::from("warning".to_string());
                if i % 2 == 0 {
                    new_event.description = Option::from("Pump is under stress".to_string());
                } else {
                    new_event.description = Option::from("Pump pressure value is below safe operating limit".to_string());
                }
                new_event.set_status("CAUTION");
            }

            new_event.add_metadata("version".to_string(), "0x0f".to_string());
            new_event.set_source("valheim-pump-events".to_string());

            events.push(new_event);
        }

        let external_ids_for_use: Vec<&str> = external_ids
            .iter()
            .map(|s| s.as_str())
            .collect();
        delete_events(&api_service, external_ids_for_use).await;

        let allowed_sub_types = ["info", "critical", "warning", "alarm"];
        let allowed_statuses = ["CAUTION", "CRITICAL", "NORMAL", "UNSAFE"];
        let result = api_service.events.create(&events).await;
        match result {
            Ok(events) => {
                assert_eq!(events.length(), 89);

                for i in 0..total_events as usize {
                    let event = events.get_items().get(i).unwrap();
                    assert_eq!(event.get_id().is_some(), true);
                    let external_id = format!("pump_event_alarm_{:?}", unique_id + i as u64);
                    assert_eq!(event.get_external_id(), &external_id);
                    assert_eq!(event.get_type().unwrap(), "pump");
                    assert!(allowed_sub_types.contains(&event.get_sub_type().unwrap()),
                            "Event sub_type '{}' is not one of the allowed values.",
                            event.get_sub_type().unwrap());
                    assert_eq!(event.get_source().unwrap(), "valheim-pump-events");

                    assert!(allowed_statuses.contains(&event.get_status().unwrap()),
                            "Event status '{}' is not one of the allowed values.",
                            event.get_status().unwrap());
                    assert_eq!(event.get_source().unwrap(), "valheim-pump-events");

                    let mut event_time: DateTime<Utc> = Utc.with_ymd_and_hms(2025, 9, 5, 0, 0, 0).unwrap();
                    event_time = event_time + Duration::minutes(((i * 24) + 24) as i64);
                    event_time = event_time + Duration::seconds((i*3 * 11) as i64);
                    assert_eq!(*event.get_event_time().unwrap(), event_time);
                }
            },
            Err(e) => {
                eprintln!("{:?}", e.get_message());
                assert_eq!(e.status.as_u16(), 201);
                panic!("error with Event create");
            }
        }



        let external_ids_for_use: Vec<&str> = external_ids
            .iter()
            .map(|s| s.as_str())
            .collect();
        delete_events(&api_service, external_ids_for_use).await;
        Ok(())
    }
}