//! Tests for updating a resource's labels via `POST /resources/update`.
//!
//! Two groups:
//! - offline serde tests that lock the request wire-format (set/add/remove, node identity);
//! - `#[ignore]` live tests that exercise the backend's label-update semantics, in particular the
//!   privileged **type-labels** (`ASSET`/`DATASET`/`POLICY`/`TIMESERIES`/`FUNCTION`), which
//!   `TypeLabels.applyLabelUpdate` on the server forces to stay exactly the node's own type — no
//!   update may add, remove, or swap it — while ordinary labels follow `base(set|current)+add-remove`.

#[cfg(test)]
mod tests {
    use crate::create_api_service;
    use crate::generic::IdAndExtId;
    use crate::graph_data_wrapper::GraphDataWrapper;
    use crate::relations::RelForm;
    use crate::resources::{Resource, ResourceUpdate};
    use crate::tests::cleanup::cleanup_resources;

    // ----------------------------------------------------------------------------------------
    // Offline: request wire-format. No backend required.
    // ----------------------------------------------------------------------------------------

    #[test]
    fn set_add_remove_serialize_into_one_labels_object() {
        // Chaining set/add/remove layers onto the *same* labels object rather than replacing it.
        let u = ResourceUpdate::by_external_id("pump_a")
            .set_labels(vec!["LBLA", "LBLB"])
            .add_labels(vec!["LBLC"])
            .remove_labels(vec!["LBLD"]);
        let v: serde_json::Value = serde_json::to_value(&u).unwrap();

        assert_eq!(v["externalId"], "pump_a");
        assert_eq!(v["update"]["labels"]["set"], serde_json::json!(["LBLA", "LBLB"]));
        assert_eq!(v["update"]["labels"]["add"], serde_json::json!(["LBLC"]));
        assert_eq!(v["update"]["labels"]["remove"], serde_json::json!(["LBLD"]));
        // untouched fields are omitted entirely (PATCH semantics), and id is absent for an extId target
        assert!(v["update"].get("name").is_none());
        assert!(v["update"].get("metadata").is_none());
        assert!(v.get("id").is_none());
    }

    #[test]
    fn by_id_stringifies_id_and_omits_external_id() {
        let u = ResourceUpdate::by_id(4242).add_labels(vec!["LBLX"]);
        let v: serde_json::Value = serde_json::to_value(&u).unwrap();
        assert_eq!(v["id"], "4242"); // 64-bit ids travel as strings
        assert!(v.get("externalId").is_none());
        assert_eq!(v["update"]["labels"]["add"], serde_json::json!(["LBLX"]));
        assert!(v["update"]["labels"].get("set").is_none());
        assert!(v["update"]["labels"].get("remove").is_none());
    }

    // ----------------------------------------------------------------------------------------
    // Live: label-update semantics. Ignored by default (needs a backend + mutates state).
    // Run with `cargo test label_update -- --ignored --nocapture`.
    // ----------------------------------------------------------------------------------------

    /// Sorted label list from an update/read response's first node.
    fn labels_of(gdw: &GraphDataWrapper<Resource>) -> Vec<String> {
        let mut v = gdw
            .nodes()
            .unwrap_or_default()
            .first()
            .and_then(|r| r.labels.clone())
            .unwrap_or_default();
        v.sort();
        v
    }

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|x| x.to_string()).collect()
    }

    /// Create a fresh ASSET-typed resource carrying `extra` non-type labels, returning its api handle.
    async fn make_asset_resource(
        api: &crate::ApiService,
        ext_id: &str,
        extra: &[&str],
    ) -> Result<(), crate::http::ResponseError> {
        // best-effort pre-clean of a leftover from an interrupted run
        let _ = api
            .resources
            .delete(&IdAndExtId::from_external_id(ext_id))
            .await;
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let mut r = Resource::new();
        r.external_id = ext_id.to_string();
        r.name = "SDK label-update probe".to_string();
        r.is_root = true;
        let mut labels = vec!["ASSET".to_string()];
        labels.extend(extra.iter().map(|x| x.to_string()));
        r.labels = Some(labels);
        api.resources.create(vec![r], Vec::<RelForm>::new()).await?;
        // let the create (and its label M2M) settle before we start updating
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        Ok(())
    }

    /// The intrinsic type-label (ASSET here) is preserved no matter what an update tries.
    #[tokio::test]
    #[ignore]
    async fn special_type_label_is_preserved() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();
        let ext = "sdk_lblupd_type";
        let _guard = cleanup_resources(vec![ext.to_string()]);
        make_asset_resource(&api, ext, &["PUMP"]).await?;

        // removing the type-label: it must be forced back, PUMP untouched
        let r = api
            .resources
            .update(&ResourceUpdate::by_external_id(ext).remove_labels(vec!["ASSET"]))
            .await?;
        assert_eq!(labels_of(&r), s(&["ASSET", "PUMP"]), "type-label must survive removal");

        // adding a *foreign* type-label: it must be stripped
        let r = api
            .resources
            .update(&ResourceUpdate::by_external_id(ext).add_labels(vec!["DATASET"]))
            .await?;
        assert!(
            !labels_of(&r).contains(&"DATASET".to_string()),
            "a foreign type-label must not be addable: {:?}",
            labels_of(&r)
        );
        assert!(labels_of(&r).contains(&"ASSET".to_string()));

        // set to a foreign set: own type-label forced back, non-type labels replaced
        let r = api
            .resources
            .update(&ResourceUpdate::by_external_id(ext).set_labels(vec!["SENSOR"]))
            .await?;
        assert_eq!(
            labels_of(&r),
            s(&["ASSET", "SENSOR"]),
            "set replaces non-type labels but the type-label is forced back"
        );

        Ok(())
    }

    /// set / add / remove each applied on their own to ordinary (non-type) labels.
    #[tokio::test]
    #[ignore]
    async fn set_add_remove_individually() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();
        let ext = "sdk_lblupd_basic";
        let _guard = cleanup_resources(vec![ext.to_string()]);
        make_asset_resource(&api, ext, &["PUMP"]).await?;

        // add
        let r = api
            .resources
            .update(&ResourceUpdate::by_external_id(ext).add_labels(vec!["CRITICAL"]))
            .await?;
        assert_eq!(labels_of(&r), s(&["ASSET", "CRITICAL", "PUMP"]));

        // remove
        let r = api
            .resources
            .update(&ResourceUpdate::by_external_id(ext).remove_labels(vec!["PUMP"]))
            .await?;
        assert_eq!(labels_of(&r), s(&["ASSET", "CRITICAL"]));

        // set (replaces the whole non-type set; type-label forced back)
        let r = api
            .resources
            .update(&ResourceUpdate::by_external_id(ext).set_labels(vec!["ALPHA", "BETA"]))
            .await?;
        assert_eq!(labels_of(&r), s(&["ALPHA", "ASSET", "BETA"]));

        Ok(())
    }

    /// set + add + remove in one request, on **disjoint** label sets:
    /// base = set, then +add, then -remove (remove targets something not present → no-op).
    #[tokio::test]
    #[ignore]
    async fn combined_set_add_remove_disjoint() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();
        let ext = "sdk_lblupd_disjoint";
        let _guard = cleanup_resources(vec![ext.to_string()]);
        make_asset_resource(&api, ext, &["OLD"]).await?;

        // set={LBLA,LBLB}, add={LBLC}, remove={LBLD}. LBLD isn't in the set, so it drops out
        // harmlessly. Result = {LBLA,LBLB,LBLC} + forced ASSET. "OLD" is gone because `set`
        // replaces the base. (Label names must be 3–512 chars, so no single-letter labels.)
        let r = api
            .resources
            .update(
                &ResourceUpdate::by_external_id(ext)
                    .set_labels(vec!["LBLA", "LBLB"])
                    .add_labels(vec!["LBLC"])
                    .remove_labels(vec!["LBLD"]),
            )
            .await?;
        assert_eq!(labels_of(&r), s(&["ASSET", "LBLA", "LBLB", "LBLC"]));

        Ok(())
    }

    /// set + add + remove in one request, on **overlapping** sets, to pin the precedence:
    /// base(set) ∪ add, then − remove — so remove wins over add for a label in both.
    #[tokio::test]
    #[ignore]
    async fn combined_set_add_remove_overlapping() -> Result<(), Box<dyn std::error::Error>> {
        let api = create_api_service();
        let ext = "sdk_lblupd_overlap";
        let _guard = cleanup_resources(vec![ext.to_string()]);
        make_asset_resource(&api, ext, &["OLD"]).await?;

        // set={LBLA,LBLB}, add={LBLB,LBLC}, remove={LBLB}: {LBLA,LBLB} ∪ {LBLB,LBLC} =
        // {LBLA,LBLB,LBLC}, then remove LBLB => {LBLA,LBLC}. LBLB is in both add and remove →
        // remove (applied last) wins. Plus forced ASSET.
        let r = api
            .resources
            .update(
                &ResourceUpdate::by_external_id(ext)
                    .set_labels(vec!["LBLA", "LBLB"])
                    .add_labels(vec!["LBLB", "LBLC"])
                    .remove_labels(vec!["LBLB"]),
            )
            .await?;
        assert_eq!(
            labels_of(&r),
            s(&["ASSET", "LBLA", "LBLC"]),
            "remove applied after add: a label in both is removed"
        );

        // Degenerate overlap: set={LBLX}, add={LBLX}, remove={LBLX} => {} + forced ASSET only.
        let r = api
            .resources
            .update(
                &ResourceUpdate::by_external_id(ext)
                    .set_labels(vec!["LBLX"])
                    .add_labels(vec!["LBLX"])
                    .remove_labels(vec!["LBLX"]),
            )
            .await?;
        assert_eq!(labels_of(&r), s(&["ASSET"]), "everything removed leaves just the type-label");

        Ok(())
    }
}
