#[cfg(test)]
mod tests {
    use crate::create_api_service;
    use crate::functions::Function;
    use crate::generic::IdAndExtId;
    use serde_json::json;
    use uuid::Uuid;

    /// Create + list + by_external_id + delete round-trip against a live API.
    /// Marked `#[ignore]` since it depends on a running backend with a function template
    /// matching `forecast-ema` available; run with `cargo test functions:: -- --ignored`.
    #[tokio::test]
    #[ignore]
    async fn functions_full_roundtrip() {
        let api = create_api_service();
        let suffix = Uuid::new_v4().to_string()[0..8].to_string();
        let ext_id = format!("sdk_test_fn_{}", suffix);

        let fn_in = Function::new(ext_id.clone(), "forecast-ema".to_string())
            .with_name("SDK roundtrip ema".to_string())
            .with_config(json!({"alpha": 0.5}));

        let created = api.functions.create(&vec![fn_in]).await.unwrap();
        assert_eq!(created.get_items().len(), 1);
        assert_eq!(created.get_items()[0].external_id, ext_id);
        assert_eq!(created.get_items()[0].model_name, "forecast-ema");

        let listed = api.functions.list().await.unwrap();
        assert!(listed.get_items().iter().any(|f| f.external_id == ext_id));

        let by_ext = api.functions.by_external_id(&ext_id).await.unwrap();
        assert_eq!(by_ext.external_id, ext_id);

        let by_ids = api
            .functions
            .by_ids(&[IdAndExtId::from_external_id(&ext_id)])
            .await
            .unwrap();
        assert_eq!(by_ids.get_items().len(), 1);

        let _ = api
            .functions
            .delete(&vec![IdAndExtId::from_external_id(&ext_id)])
            .await
            .unwrap();
    }
}
