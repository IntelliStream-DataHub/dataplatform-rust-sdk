#[cfg(test)]
mod tests {
    use crate::create_api_service;
    use crate::generic::{DataWrapper, IdAndExtId};
    use crate::labels::Label;
    use crate::tests::cleanup::{cleanup_labels, cleanup_resources};
    use uuid::Uuid;

    // Serde round-trips: no backend required.

    #[test]
    fn create_body_omits_unset_fields_and_stringifies_id() {
        // A create carries just the name — id/description/color/i18nCode are omitted, not null.
        let body = serde_json::to_string(&DataWrapper::from(Label::new("pump station"))).unwrap();
        assert_eq!(body, r#"{"items":[{"name":"pump station"}]}"#);

        // An update carries the id (as a JSON string) plus only the fields set.
        let label = Label::from_id(51).with_color("#123456");
        let body = serde_json::to_string(&DataWrapper::from(label)).unwrap();
        assert_eq!(body, r##"{"items":[{"id":"51","color":"#123456"}]}"##);
    }

    #[test]
    fn parses_label_response_with_numeric_id() {
        // The /labels response serializes `id` as a raw number and omits null fields.
        let json = r##"{"items":[{"id":50,"name":"PROBE_UNUSED_1","color":"#6d4495"}]}"##;
        let wrapper: DataWrapper<Label> = serde_json::from_str(json).unwrap();
        let label = &wrapper.get_items()[0];
        assert_eq!(label.id, Some(50));
        assert_eq!(label.name.as_deref(), Some("PROBE_UNUSED_1"));
        assert_eq!(label.color.as_deref(), Some("#6d4495"));
        assert_eq!(label.description, None);
    }

    // Live end-to-end exercise of the whole label lifecycle, including the
    // delete-while-in-use error. Ignored by default: needs a configured backend (.env) and
    // mutates tenant state. Run with `cargo test labels -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore]
    async fn test_label_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();

        // Unique per run: a fixed name strands residue the moment an assertion fails before the
        // delete, and every later run then collides with it. Upper-cased because the server
        // canonicalises label names that way, and the assertions below compare verbatim.
        let name = format!(
            "SDK_PROBE_LABEL_{}",
            Uuid::new_v4().to_string()[..8].to_uppercase()
        );
        let name = name.as_str();

        // create
        let created = api
            .labels
            .create(&Label::new(name).with_description("sdk test").with_color("#123456"))
            .await?;
        // Armed before the first assertion so a panic still tears the label down.
        let mut label_cleanup = cleanup_labels(vec![name.to_string()]);
        assert_eq!(created.get_http_status_code(), Some(200));
        let label = created.get_items().first().expect("created label").clone();
        let id = label.id.expect("server assigns an id");
        assert_eq!(label.name.as_deref(), Some(name)); // canonicalised upper-case
        assert_eq!(label.color.as_deref(), Some("#123456"));

        // get
        let fetched = api.labels.get(id).await?;
        assert_eq!(fetched.length(), 1);
        assert_eq!(fetched.get_items()[0].id, Some(id));

        // list contains it
        let all = api.labels.list().await?;
        assert!(all
            .get_items()
            .iter()
            .any(|l| l.name.as_deref() == Some(name)));

        // update description (PATCH: color must be untouched)
        let updated = api
            .labels
            .update(&Label::from_id(id).with_description("updated"))
            .await?;
        assert_eq!(updated.get_http_status_code(), Some(200));
        assert_eq!(updated.get_items()[0].color.as_deref(), Some("#123456"));
        assert_eq!(updated.get_items()[0].description.as_deref(), Some("updated"));

        // delete an unused label -> 204, then gone
        let deleted = api.labels.delete(&IdAndExtId::from_id(id)).await?;
        assert_eq!(deleted.get_http_status_code(), Some(204));

        // "Gone" is a 404, not a 200 with an empty list: every single-resource GET in the API
        // answers an unknown id that way, and `/labels/{id}` was brought in line with the rest.
        // Batch reads (`/byids`) are the ones that answer absence with an empty collection.
        let err = api
            .labels
            .get(id)
            .await
            .expect_err("a deleted label should answer 404, not an empty list");
        assert_eq!(err.get_status().as_u16(), 404);
        label_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown

        Ok(())
    }

    // Delete-while-in-use: create a label, attach it to a resource, and confirm the delete is
    // rejected with a 400 whose body names the blocking resource. Ignored by default.
    #[tokio::test]
    #[ignore]
    async fn test_delete_label_in_use_reports_blocker() -> Result<(), Box<dyn std::error::Error>> {
        use crate::relations::RelForm;
        use crate::resources::Resource;

        let api = create_api_service();
        // Unique per run. With fixed ids this test stranded its resource the first time an
        // assertion failed before the teardown, and every run after that died re-creating it —
        // a duplicate external id answers 500 with an empty body, which names nothing.
        let suffix = &Uuid::new_v4().to_string()[..8];
        // Label names are canonicalised to upper case server-side; external ids are not.
        let label_name = format!("SDK_PROBE_INUSE_{}", suffix.to_uppercase());
        let res_ext_id = format!("sdk_probe_res_{suffix}");

        // create a resource carrying the label (this is what populates the M2M the delete checks)
        let mut resource = Resource::new();
        resource.external_id = res_ext_id.clone();
        resource.name = "SDK Probe Resource".to_string();
        resource.labels = Some(vec![label_name.clone()]);
        resource.is_root = true;
        let _ = api
            .resources
            .create(vec![resource], Vec::<RelForm>::new())
            .await?;
        // Both armed before the assertion. Order matters on teardown: the guards drop in reverse
        // declaration order, so the resource goes first and frees the label for its own delete.
        let mut label_cleanup = cleanup_labels(vec![label_name.clone()]);
        let mut resource_cleanup = cleanup_resources(vec![res_ext_id.clone()]);

        // The label now exists and is in use — deleting it must be rejected. A non-2xx comes back
        // as `Err`, so this reads the status off the error rather than a wrapper.
        let err = api
            .labels
            .delete(&IdAndExtId::from_external_id(&label_name))
            .await
            .expect_err("deleting an in-use label should be rejected");
        assert_eq!(err.get_status().as_u16(), 400);
        // The point of the endpoint: the body names what is blocking the delete, so a caller can
        // act on it rather than guessing which resource still carries the label.
        assert!(
            err.get_message().contains(&res_ext_id),
            "the rejection should name the blocking resource, got: {}",
            err.get_message()
        );

        // clean up: remove the resource, then the now-free label
        let _ = api
            .resources
            .delete(&IdAndExtId::from_external_id(&res_ext_id))
            .await?;
        resource_cleanup.disarm();
        let _ = api
            .labels
            .delete(&IdAndExtId::from_external_id(&label_name))
            .await;
        label_cleanup.disarm();

        Ok(())
    }
}
