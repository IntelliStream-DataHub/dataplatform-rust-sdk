#[cfg(test)]
mod tests {
    use crate::create_api_service;
    use crate::functions::Function;
    use crate::generic::IdAndExtId;
    use crate::resources::ResourceUpdate;
    use crate::tests::cleanup::cleanup_functions;
    use crate::tests::ids::unique_id;

    /// The whole `/functions` surface: create, list, get by id, by_ids, update, delete.
    ///
    /// `#[ignore]` because it needs a live backend; run with
    /// `cargo test functions:: -- --ignored`. It needs nothing beyond that — the `forecast-ema`
    /// model template this comment used to name went away with the functions feature itself (see
    /// the server's "Remove functions feature. revert to simple metadata store"), and a function is
    /// now a plain node.
    #[tokio::test]
    #[ignore]
    async fn functions_full_roundtrip() {
        let api = create_api_service();
        let ext_id = unique_id("fn");

        let fn_in = Function::new(ext_id.clone()).with_name("SDK roundtrip fn".to_string());

        let created = api.functions.create(&vec![fn_in]).await.unwrap();
        let mut function_cleanup = cleanup_functions(vec![ext_id.clone()]);
        assert_eq!(created.get_items().len(), 1);
        assert_eq!(created.get_items()[0].external_id, ext_id);
        let id = created.get_items()[0].id.expect("create echoes the assigned id");
        // Unlike the `/resources/create` echo, this one is re-read through `FunctionTransformer`,
        // which never joins the edges in — so it is empty here as on every other read.
        assert!(created.get_items()[0].related_resources.is_empty());

        let listed = api.functions.list(None).await.unwrap();
        assert!(listed.get_items().iter().any(|f| f.external_id == ext_id));

        let got = api.functions.get_by_id(id).await.unwrap();
        assert_eq!(got.get_items().len(), 1);
        assert_eq!(got.get_items()[0].external_id, ext_id);
        assert!(
            got.get_items()[0]
                .labels
                .iter()
                .any(|l| l == "FUNCTION"),
            "the api forces the FUNCTION label back on every read: {:?}",
            got.get_items()[0].labels
        );

        let by_ext = api.functions.by_external_id(&ext_id).await.unwrap();
        assert_eq!(by_ext.external_id, ext_id);

        let by_ids = api
            .functions
            .by_ids(&[IdAndExtId::from_external_id(&ext_id)])
            .await
            .unwrap();
        assert_eq!(by_ids.get_items().len(), 1);

        let updated = api
            .functions
            .update(&vec![
                ResourceUpdate::by_external_id(&ext_id).set_name("SDK roundtrip fn (renamed)")
            ])
            .await
            .unwrap();
        // The echo is typed: a function comes back as `Node::Function`. It could not stay a flat
        // `Resource` — that shape requires `isRoot`, which a function does not have.
        let echoed = updated
            .nodes()
            .unwrap_or_default()
            .into_iter()
            .next()
            .expect("the update echoes the node")
            .into_function()
            .expect("a function echoes as Node::Function");
        assert_eq!(echoed.external_id, ext_id);
        let reread = api.functions.get_by_id(id).await.unwrap();
        assert_eq!(
            reread.get_items()[0].name.as_deref(),
            Some("SDK roundtrip fn (renamed)")
        );

        let deleted = api
            .functions
            .delete(&vec![IdAndExtId::from_external_id(&ext_id)])
            .await
            .unwrap();
        // 204: accepted, no body.
        assert_eq!(deleted.get_http_status_code(), Some(204));

        function_cleanup.disarm();

        let after = api.functions.get_by_id(id).await;
        let err = after.expect_err("a deleted function is a 404");
        assert_eq!(err.status.as_u16(), 404);
    }
}
