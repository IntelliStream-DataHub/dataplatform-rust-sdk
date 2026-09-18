//! Offline serde tests for the `/assets` bodies, plus the live round-trip.
//!
//! The offline half pins what reaches the wire, because the api reads request bodies strictly: a
//! key it does not declare is a 400 naming it. The `Asset` shape itself is pinned against the api's
//! own `AssetWireContractTest`, which asserts an exact key set.

use crate::generic::{DataWrapper, IdAndExtId, SearchAndFilterForm};
use crate::nodes::Asset;
use crate::resources::{ResourceFilter, ResourceFilterForm};

/// A created asset sends no `labels` at all: the api's `Asset` constructor forces `ASSET` in, so
/// unlike `/resources/create` the caller never has to set the type-label. Sending one that
/// contradicts it would earn a 400 naming both.
#[test]
fn create_body_carries_no_type_label() {
    let asset = Asset::new("plant_a", "Plant A");
    let v = serde_json::to_value(DataWrapper::from_vec(vec![asset])).unwrap();
    let item = &v["items"][0];

    assert!(item.get("labels").is_none(), "labels: {}", item);
    assert_eq!(item["externalId"], "plant_a");
    assert_eq!(item["name"], "Plant A");
    // `isRoot` is a plain bool, not an Option: the api defaults it to false either way, and
    // omitting it would make "not a root" indistinguishable from "unset".
    assert_eq!(item["isRoot"], false);
}

/// Every key an `Asset` can serialize must be one the api's `Asset` declares. The api's
/// `AssetWireContractTest` pins that set exactly; this is the SDK half of the same net.
#[test]
fn asset_serializes_only_keys_the_api_declares() {
    let mut asset = Asset::new("plant_a", "Plant A");
    asset.id = Some(34);
    asset.description = Some("desc".into());
    asset.data_set_id = Some(12);
    asset.source = Some("src".into());
    asset.labels = Some(vec!["ASSET".into(), "PLANT".into()]);
    asset.metadata = Some([("k".to_string(), "v".to_string())].into_iter().collect());
    asset.is_root = true;
    asset.geolocation = Some(
        serde_json::from_value(serde_json::json!({
            "type": "Point", "coordinates": [10.75, 59.91]
        }))
        .unwrap(),
    );

    let v = serde_json::to_value(&asset).unwrap();
    let keys: std::collections::BTreeSet<&str> =
        v.as_object().unwrap().keys().map(String::as_str).collect();

    let declared: std::collections::BTreeSet<&str> = [
        "id",
        "externalId",
        "name",
        "isRoot",
        "geoLocation",
        "relatedResources",
        "metadata",
        "description",
        "dataSetId",
        "source",
        "labels",
        "createdTime",
        "lastUpdatedTime",
    ]
    .into_iter()
    .collect();

    let undeclared: Vec<&&str> = keys.difference(&declared).collect();
    assert!(undeclared.is_empty(), "undeclared keys: {:?}", undeclared);

    // Ids ride as JSON strings, the way the api's ToStringSerializer writes them.
    assert_eq!(v["id"], "34");
    assert_eq!(v["dataSetId"], "12");
    // A nested GeoJSON object, never an escaped string.
    assert_eq!(v["geoLocation"]["type"], "Point");
}

/// The asset filter is the resource filter — `is_root` and `data_set_id` included — and the
/// retired plural spellings must not reappear on the wire.
#[test]
fn filter_body_is_the_resource_filter() {
    let mut filter = ResourceFilter::default();
    filter.node.name = Some(vec!["Pump*".into()]);
    filter.is_root = Some(true);
    filter.data_set_id = Some(vec![IdAndExtId::from_external_id("sap_work_orders")]);

    let v = serde_json::to_value(ResourceFilterForm::new(filter).with_limit(100)).unwrap();

    assert_eq!(v["limit"], 100);
    assert_eq!(v["filter"]["name"][0], "Pump*");
    assert_eq!(v["filter"]["isRoot"], true);
    assert_eq!(v["filter"]["dataSetId"][0]["externalId"], "sap_work_orders");
    // `nodeType` is pinned to ["asset"] server-side; the SDK does not send one.
    assert!(v["filter"].get("nodeType").is_none());
    for retired in ["names", "externalIds", "ids", "nodeTypes", "dataSetIds"] {
        assert!(
            v["filter"].get(retired).is_none(),
            "retired key {} is on the wire",
            retired
        );
    }
}

/// The search body is the shared three-field shape, and the filter is skipped rather than sent
/// empty when there is nothing to narrow by.
#[test]
fn search_body_omits_an_absent_filter() {
    let v = serde_json::to_value(SearchAndFilterForm::<ResourceFilter>::new("pump")).unwrap();
    assert_eq!(v["search"]["query"], "pump");
    assert!(v.get("filter").is_none());
    assert!(v.get("limit").is_none());
}

/// Live round-trip over the whole `/assets` surface.
///
/// `#[ignore]` like every test here that needs a backend; run with
/// `cargo test assets:: -- --ignored --nocapture`.
mod live {
    use super::*;
    use crate::create_api_service;
    use crate::fields::Field;
    use crate::resources::ResourceUpdate;
    use crate::tests::cleanup::cleanup_resources;
    use crate::tests::ids::unique_id;

    #[tokio::test]
    #[ignore]
    async fn assets_full_roundtrip() {
        let api = create_api_service();
        let ext_id = unique_id("asset");

        // Armed before the create, so a panicking assertion still tears the asset down.
        // Assets are resources: `/resources/delete` removes them.
        let mut guard = cleanup_resources(vec![ext_id.clone()]);

        let mut asset = Asset::new(&ext_id, "SDK roundtrip asset");
        asset.description = Some("created by assets_full_roundtrip".into());
        asset.geolocation = Some(
            serde_json::from_value(serde_json::json!({
                "type": "Point", "coordinates": [10.75, 59.91]
            }))
            .unwrap(),
        );

        let created = api.assets.create(&vec![asset]).await.unwrap();
        assert_eq!(created.get_items().len(), 1);
        let made = &created.get_items()[0];
        assert_eq!(made.external_id, ext_id);
        let id = made.id.expect("create echoes the assigned id");
        // The api forces the type-label back on every read.
        assert!(
            made.labels
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .any(|l| l == "ASSET"),
            "create echo lost the ASSET label: {:?}",
            made.labels
        );

        let got = api.assets.get_by_id(id).await.unwrap();
        assert_eq!(got.get_items().len(), 1);
        assert_eq!(got.get_items()[0].external_id, ext_id);
        assert!(
            got.get_items()[0].geolocation.is_some(),
            "an asset read flatly carries its geometry"
        );

        let by_ids = api
            .assets
            .by_ids(&vec![IdAndExtId::from_external_id(&ext_id)])
            .await
            .unwrap();
        assert_eq!(by_ids.get_items().len(), 1);

        let listed = api.assets.list(Some(1000)).await.unwrap();
        assert!(
            listed.get_items().iter().any(|a| a.external_id == ext_id),
            "the new asset is not in the newest 1000"
        );
        // The api nulls the cursor on a plain listing: there is nowhere to send one back to.
        assert!(listed.next_cursor().is_none(), "a listing must not page");

        let mut filter = ResourceFilter::default();
        filter.node.external_id = Some(vec![ext_id.clone()]);
        let filtered = api
            .assets
            .filter(&ResourceFilterForm::new(filter))
            .await
            .unwrap();
        assert_eq!(filtered.get_items().len(), 1);
        assert_eq!(filtered.get_items()[0].external_id, ext_id);

        let searched = api.assets.search_by_query(&ext_id).await.unwrap();
        assert!(
            searched.get_items().iter().any(|a| a.external_id == ext_id),
            "search did not find the asset by its external id"
        );

        let updated = api
            .assets
            .update(&vec![ResourceUpdate::by_external_id(&ext_id)
                .set_name("SDK roundtrip asset (renamed)")])
            .await
            .unwrap();
        // The echo is typed: an asset comes back as `Node::Asset`, not a flat `Resource`.
        let echoed = updated
            .nodes()
            .unwrap_or_default()
            .into_iter()
            .next()
            .expect("the update echoes the node")
            .into_asset()
            .expect("an asset echoes as Node::Asset");
        assert_eq!(echoed.external_id, ext_id);
        assert!(
            echoed.geolocation.is_some(),
            "the typed echo carries the asset's own geometry"
        );
        let reread = api.assets.get_by_id(id).await.unwrap();
        assert_eq!(reread.get_items()[0].name, "SDK roundtrip asset (renamed)");

        let deleted = api
            .assets
            .delete(&vec![IdAndExtId::from_external_id(&ext_id)])
            .await
            .unwrap();
        // 204: accepted, no body. The wrapper is empty by construction.
        assert_eq!(deleted.get_http_status_code(), Some(204));
        guard.disarm();

        let after = api.assets.get_by_id(id).await;
        let err = after.expect_err("a deleted asset is a 404");
        assert_eq!(err.status.as_u16(), 404);
    }

    /// A node that exists but is not an asset is reported as missing, not as a type error — so a
    /// 404 here does not tell you whether the id exists.
    #[tokio::test]
    #[ignore]
    async fn a_non_asset_id_is_reported_as_missing() {
        let api = create_api_service();
        let ext_id = unique_id("fn_not_asset");
        let mut guard = cleanup_resources(vec![ext_id.clone()]);

        let created = api
            .functions
            .create(&vec![crate::functions::Function::new(ext_id.clone())
                .with_name("not an asset".to_string())])
            .await
            .unwrap();
        let id = created.get_items()[0].id.expect("create echoes an id");

        let err = api
            .assets
            .get_by_id(id)
            .await
            .expect_err("a function is not an asset");
        assert_eq!(err.status.as_u16(), 404);

        let _ = api
            .functions
            .delete(&vec![IdAndExtId::from_external_id(&ext_id)])
            .await;
        guard.disarm();
    }

    /// `geoLocation` set through the shared update form reaches the asset — the one update field
    /// that means anything on exactly one node type.
    #[tokio::test]
    #[ignore]
    async fn geolocation_is_updatable() {
        let api = create_api_service();
        let ext_id = unique_id("asset_geo");
        let mut guard = cleanup_resources(vec![ext_id.clone()]);

        let created = api
            .assets
            .create(&vec![Asset::new(&ext_id, "SDK geo asset")])
            .await
            .unwrap();
        let id = created.get_items()[0].id.expect("create echoes an id");

        let geom: geojson::Geometry = serde_json::from_value(serde_json::json!({
            "type": "Point", "coordinates": [5.32, 60.39]
        }))
        .unwrap();

        let mut update = ResourceUpdate::by_external_id(&ext_id);
        update.update.geolocation = Some(Field::value(geom));
        api.assets.update(&vec![update]).await.unwrap();

        let reread = api.assets.get_by_id(id).await.unwrap();
        let stored = reread.get_items()[0]
            .geolocation
            .clone()
            .expect("the update stored a geometry");
        assert_eq!(
            serde_json::to_value(&stored).unwrap()["coordinates"],
            serde_json::json!([5.32, 60.39])
        );

        let _ = api
            .assets
            .delete(&vec![IdAndExtId::from_external_id(&ext_id)])
            .await;
        guard.disarm();
    }
}
