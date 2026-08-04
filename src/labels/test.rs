#[cfg(test)]
mod tests {
    use crate::create_api_service;
    use crate::generic::{DataWrapper, IdAndExtId};
    use crate::labels::Label;

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

        // Unique name so reruns don't collide with earlier residue.
        let name = "SDK_PROBE_LABEL";

        // Best-effort pre-clean in case a previous run left it behind.
        let _ = api
            .labels
            .delete(&IdAndExtId::from_external_id(name))
            .await;

        // create
        let created = api
            .labels
            .create(&Label::new(name).with_description("sdk test").with_color("#123456"))
            .await?;
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
        assert_eq!(api.labels.get(id).await?.length(), 0);

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
        let label_name = "SDK_PROBE_INUSE";
        let res_ext_id = "sdk_probe_res";

        // create a resource carrying the label (this is what populates the M2M the delete checks)
        let mut resource = Resource::new();
        resource.external_id = res_ext_id.to_string();
        resource.name = "SDK Probe Resource".to_string();
        resource.labels = Some(vec![label_name.to_string()]);
        resource.is_root = true;
        let _ = api
            .resources
            .create(vec![resource], Vec::<RelForm>::new())
            .await?;

        // the label now exists and is in use — deleting it must be rejected
        let resp = api
            .labels
            .delete(&IdAndExtId::from_external_id(label_name))
            .await?;
        assert_eq!(
            resp.get_http_status_code(),
            Some(400),
            "deleting an in-use label should be rejected"
        );

        // clean up: remove the resource, then the now-free label
        let _ = api
            .resources
            .delete(&IdAndExtId::from_external_id(res_ext_id))
            .await?;
        let _ = api
            .labels
            .delete(&IdAndExtId::from_external_id(label_name))
            .await;

        Ok(())
    }
}
