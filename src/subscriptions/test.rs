#[cfg(test)]
mod tests {
    use crate::generic::{IdAndExtId, IdAndExtIdCollection};
    use crate::subscriptions::{DataSort, Subscription, SubscriptionFilter, SubscriptionRetriever};
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

        // Clean up supporting timeseries.
        delete_timeseries(&api_service, ts_ids).await;
        Ok(())
    }
}
