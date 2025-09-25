#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use chrono::{DateTime, TimeZone, Utc};
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

    async fn delete_events(api_service: &ApiService<'_>, events: Vec<&str>) {
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
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }

    async fn find_events_by_external_ids(api_service: &ApiService<'_>, external_ids: Vec<&str>) {
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
}