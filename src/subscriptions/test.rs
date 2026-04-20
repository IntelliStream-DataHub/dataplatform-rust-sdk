#[cfg(test)]
mod tests {
    use crate::generic::{IdAndExtId, IdAndExtIdCollection};
    use crate::subscriptions::listen::build_ws_url;
    use crate::subscriptions::{
        DataSort, EventAction, EventObject, Subscription, SubscriptionFilter, SubscriptionMessage,
        SubscriptionRetriever,
    };
    use crate::timeseries::TimeSeries;
    use crate::{create_api_service, ApiService};
    use uuid::Uuid;

    // Serde round-trip: verify the Subscription JSON layout matches the backend's camelCase contract.
    #[test]
    fn test_subscription_serde_round_trip() {
        let sub = Subscription::new(
            "sub_ext_id".to_string(),
            "Display Name".to_string(),
            vec![
                IdAndExtId::from_id(29),
                IdAndExtId::from_external_id("heater_2012_temp"),
            ],
        );

        let json = serde_json::to_value(&sub).unwrap();
        // Server-assigned fields must be omitted on create; id is None so must not be present.
        assert!(json.get("id").is_none(), "id should be skipped when None");
        assert!(json.get("dateCreated").is_none());
        assert!(json.get("lastUpdated").is_none());
        assert_eq!(json["externalId"], "sub_ext_id");
        assert_eq!(json["name"], "Display Name");
        let ts = &json["timeseries"];
        assert_eq!(ts[0]["id"], 29);
        assert_eq!(ts[1]["externalId"], "heater_2012_temp");

        // Simulate a server response (id + timestamps populated, camelCase).
        let server_json = serde_json::json!({
            "id": 7788,
            "externalId": "sub_ext_id",
            "name": "Display Name",
            "timeseries": [{"id": 29}],
            "dateCreated": "2026-04-18T12:00:00Z",
            "lastUpdated": "2026-04-18T12:00:00Z"
        });
        let parsed: Subscription = serde_json::from_value(server_json).unwrap();
        assert_eq!(parsed.id, Some(7788));
        assert!(parsed.date_created.is_some());
        assert!(parsed.last_updated.is_some());
    }

    #[test]
    fn test_retriever_default_serializes_cleanly() {
        // SubscriptionRetriever::default() must serialize to a body the backend accepts:
        // - empty filter.timeseries collapses to just `{"filter":{},"limit":100,"sort":{}}`
        // - sort fields are all None so they must be omitted so the @Pattern validator on `nulls` is skipped
        let json = serde_json::to_value(&SubscriptionRetriever::default()).unwrap();
        assert_eq!(json["limit"], 100);
        let filter_obj = json["filter"].as_object().unwrap();
        assert!(filter_obj.get("timeseries").is_none());
        let sort_obj = json["sort"].as_object().unwrap();
        assert!(sort_obj.get("nulls").is_none());
        assert!(sort_obj.get("property").is_none());
        assert!(sort_obj.get("order").is_none());
    }

    // Helpers for the integration test — mirrors delete_events in events/tests.rs.
    async fn delete_subscriptions(api_service: &ApiService, subs: &[IdAndExtId]) {
        if subs.is_empty() {
            return;
        }
        match api_service.subscriptions.delete(&subs.to_vec()).await {
            Ok(r) => {
                // Backend responds 204 No Content — DataWrapper comes back empty.
                assert!(r.get_http_status_code().map_or(true, |c| c == 204 || (200..300).contains(&c)));
            }
            Err(e) => {
                eprintln!("subscription delete failed: {:?}", e.get_message());
                panic!("subscription cleanup failed: {}", e.get_message());
            }
        }
    }

    async fn delete_timeseries(api_service: &ApiService, ids: Vec<IdAndExtId>) {
        if ids.is_empty() {
            return;
        }
        let mut coll = IdAndExtIdCollection::new();
        coll.set_items(ids);
        let _ = api_service.time_series.delete(&coll).await;
    }

    #[tokio::test]
    async fn test_subscription_crud() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        // Unique suffix so repeated runs don't collide with lingering state.
        let suffix = Uuid::new_v4().simple().to_string();
        let ts_a_ext = format!("sub_test_ts_a_{}", &suffix[..8]);
        let ts_b_ext = format!("sub_test_ts_b_{}", &suffix[..8]);
        let sub_ext = format!("sub_test_{}", &suffix[..8]);

        // 1. Create two timeseries to bind the subscription to. Subscription create will 400 if
        //    these don't exist (see SubscriptionService::resolveTimeseries in the backend).
        let ts_ids: Vec<IdAndExtId> = vec![
            IdAndExtId::from_external_id(&ts_a_ext),
            IdAndExtId::from_external_id(&ts_b_ext),
        ];
        let mut ts_a = TimeSeries::new(&ts_a_ext, "Sub Test TS A");
        ts_a.set_unit("Celsius").set_unit_external_id("temperature_deg_c");
        let mut ts_b = TimeSeries::new(&ts_b_ext, "Sub Test TS B");
        ts_b.set_unit("Celsius").set_unit_external_id("temperature_deg_c");
        let ts_list = vec![ts_a, ts_b];
        api_service.time_series.create_from_list(&ts_list).await?;

        // Run the body inside an async block so an early `?` or panic still lets cleanup run.
        let result: Result<(), Box<dyn std::error::Error>> = async {
            let sub = Subscription::new(
                sub_ext.clone(),
                format!("Sub Test {}", &suffix[..8]),
                ts_ids.clone(),
            );

            // 2. Create the subscription.
            let created = api_service.subscriptions.create(&sub).await?;
            assert_eq!(created.length(), 1, "create must echo back exactly 1 item");
            let created_item = &created.get_items()[0];
            assert_eq!(created_item.external_id, sub_ext);
            assert!(created_item.id.is_some(), "server must assign an id");
            assert!(created_item.date_created.is_some());
            assert_eq!(created_item.timeseries.len(), 2);

            // 3. List — the unfiltered list may include prior test data, so we assert *at least*
            //    our subscription is present (per CLAUDE.md: avoid exact-count assertions against
            //    shared backend state).
            let all = api_service
                .subscriptions
                .list(&SubscriptionRetriever::default())
                .await?;
            assert!(
                all.get_items().iter().any(|s| s.external_id == sub_ext),
                "unfiltered list must contain the subscription we just created"
            );

            // 4. List with a timeseries filter — only subscriptions bound to ts_a should come back.
            //    Our subscription is bound to ts_a, so it must appear.
            let filtered = api_service
                .subscriptions
                .list(&SubscriptionRetriever {
                    filter: SubscriptionFilter {
                        timeseries: vec![IdAndExtId::from_external_id(&ts_a_ext)],
                    },
                    limit: 100,
                    sort: DataSort::default(),
                })
                .await?;
            assert!(
                filtered.get_items().iter().any(|s| s.external_id == sub_ext),
                "timeseries-filtered list must contain the subscription"
            );

            // 5. Delete the subscription. Backend returns 204 No Content → empty DataWrapper.
            delete_subscriptions(&api_service, &[IdAndExtId::from_external_id(&sub_ext)]).await;

            // 6. Verify the subscription is gone from the filtered list.
            let after_delete = api_service
                .subscriptions
                .list(&SubscriptionRetriever {
                    filter: SubscriptionFilter {
                        timeseries: vec![IdAndExtId::from_external_id(&ts_a_ext)],
                    },
                    limit: 100,
                    sort: DataSort::default(),
                })
                .await?;
            assert!(
                !after_delete.get_items().iter().any(|s| s.external_id == sub_ext),
                "subscription should be gone after delete"
            );
            Ok(())
        }
        .await;

        // Cleanup always runs — a best-effort subscription delete (in case step 5 was skipped)
        // plus the supporting timeseries.
        let _ = api_service
            .subscriptions
            .delete(&vec![IdAndExtId::from_external_id(&sub_ext)])
            .await;
        delete_timeseries(&api_service, ts_ids).await;

        result
    }

    #[test]
    fn test_build_ws_url_http_to_ws() {
        let url = build_ws_url("http://localhost:8081/subscriptions", "sub_ext_id").unwrap();
        assert_eq!(url, "ws://localhost:8081/subscriptions/listen/sub_ext_id");
    }

    #[test]
    fn test_build_ws_url_https_to_wss() {
        let url = build_ws_url("https://api.example.com/subscriptions", "my_sub").unwrap();
        assert_eq!(url, "wss://api.example.com/subscriptions/listen/my_sub");
    }

    #[test]
    fn test_build_ws_url_rejects_unknown_scheme() {
        assert!(build_ws_url("ftp://example.com/subscriptions", "x").is_err());
    }

    // Deserialize a realistic server frame — SubscriptionWebSocketHandler::sendBatch always
    // wraps messages in `{ "messages": [...] }` with a base64 messageId and a DataWrapperMessage
    // payload. If this parses, the wire contract lines up with the backend.
    #[test]
    fn test_server_frame_deserialize() {
        let wire = serde_json::json!({
            "messages": [
                {
                    "messageId": "CAEQABgAIAA",
                    "payload": {
                        "eventAction": "CREATE",
                        "eventObject": "DATAPOINTS",
                        "tenantId": "tenant-1",
                        "items": [
                            {
                                "id": 30,
                                "externalId": "heater_2012_temp",
                                "valueType": "float",
                                "inclusiveBegin": null,
                                "exclusiveEnd": null,
                                "datapoints": [
                                    {"timestamp": "2026-04-18T12:00:00Z", "value": "21.5"},
                                    {"timestamp": "1723759200000", "value": "22.0"}
                                ]
                            }
                        ]
                    }
                }
            ]
        });

        // Batch wrapper is private — deserialize one element at a time via SubscriptionMessage.
        let arr = wire["messages"].as_array().unwrap();
        let msg: SubscriptionMessage = serde_json::from_value(arr[0].clone()).unwrap();
        assert_eq!(msg.message_id, "CAEQABgAIAA");
        assert_eq!(msg.payload.event_action, EventAction::Create);
        assert_eq!(msg.payload.event_object, EventObject::Datapoints);
        assert_eq!(msg.payload.tenant_id.as_deref(), Some("tenant-1"));
        assert_eq!(msg.payload.items.len(), 1);
        let coll = &msg.payload.items[0];
        assert_eq!(coll.id, Some(30));
        assert_eq!(coll.external_id.as_deref(), Some("heater_2012_temp"));
        assert_eq!(coll.datapoints.len(), 2);
        assert_eq!(coll.datapoints[0].value, "21.5");
    }

    #[test]
    fn test_event_object_resource_and_relation_snake() {
        // The odd enum — the Java side uses UPPER_SNAKE for this variant, others are single-word UPPER.
        let v: EventObject = serde_json::from_value(serde_json::json!("RESOURCE_AND_RELATION")).unwrap();
        assert_eq!(v, EventObject::ResourceAndRelation);
        let back = serde_json::to_value(&v).unwrap();
        assert_eq!(back, serde_json::json!("RESOURCE_AND_RELATION"));
    }

    // End-to-end: requires backend consumer running so datapoints written via the REST API are
    // fanned out over Pulsar to the subscription topic. Ignored by default. Run with:
    //   cargo test subscriptions::test::tests::test_subscription_listen_end_to_end -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_subscription_listen_end_to_end() -> Result<(), Box<dyn std::error::Error>> {
        use chrono::Utc;
        use std::time::Duration;

        let api_service = create_api_service();
        let suffix = Uuid::new_v4().simple().to_string();
        let ts_ext = format!("sub_listen_ts_{}", &suffix[..8]);
        let sub_ext = format!("sub_listen_{}", &suffix[..8]);

        let mut ts = TimeSeries::new(&ts_ext, "Sub Listen TS");
        ts.set_unit("Celsius").set_unit_external_id("temperature_deg_c");
        api_service.time_series.create_from_list(&vec![ts]).await?;

        let sub = Subscription::new(
            sub_ext.clone(),
            format!("Sub Listen {}", &suffix[..8]),
            vec![IdAndExtId::from_external_id(&ts_ext)],
        );
        api_service.subscriptions.create(&sub).await?;

        // Run the body inside an async block so an early `?` or panic still lets cleanup run.
        let result: Result<(), Box<dyn std::error::Error>> = async {
            // Open listener before writing datapoints so we catch the fan-out.
            let mut listener = api_service.subscriptions.listen(&sub_ext).await?;

            // Write a datapoint to the subscribed timeseries. The consumer fans it out to the
            // subscription topic; we expect it on the listener.
            api_service
                .time_series
                .insert_datapoint(None, Some(ts_ext.clone()), Utc::now(), "42.0".to_string())
                .await?;

            // Wait up to ~10s for the datapoint to land.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            let mut received: Option<SubscriptionMessage> = None;
            while tokio::time::Instant::now() < deadline {
                tokio::select! {
                    maybe = listener.next() => {
                        if let Some(Ok(msg)) = maybe {
                            received = Some(msg);
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(200)) => {}
                }
            }
            let msg = received.ok_or("no message arrived before the deadline")?;
            assert_eq!(msg.payload.event_object, EventObject::Datapoints);
            assert_eq!(msg.payload.event_action, EventAction::Create);
            listener.ack(&[msg.message_id.as_str()]).await?;
            listener.close().await?;
            Ok(())
        }
        .await;

        // Cleanup always runs, regardless of whether the body succeeded. Errors are logged but
        // not propagated so the original test result (if any) surfaces.
        if let Err(e) = api_service
            .subscriptions
            .delete(&IdAndExtId::from_external_id(&sub_ext))
            .await
        {
            eprintln!("subscription cleanup failed: {:?}", e.get_message());
        }
        let mut ts_coll = IdAndExtIdCollection::new();
        ts_coll.set_items(vec![IdAndExtId::from_external_id(&ts_ext)]);
        if let Err(e) = api_service.time_series.delete(&ts_coll).await {
            eprintln!("timeseries cleanup failed: {:?}", e.get_message());
        }

        result
    }

    #[allow(dead_code)]
    fn _require_send<T: Send>(_: &T) {}

    // Compile-time check: the returned listener is Send so it can be moved to other tasks.
    #[allow(dead_code)]
    async fn _listener_is_send() {
        let api_service = create_api_service();
        let listener = api_service.subscriptions.listen("x").await.unwrap();
        _require_send(&listener);
    }
}
