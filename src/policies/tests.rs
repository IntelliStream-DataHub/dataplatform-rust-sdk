use crate::create_api_service;
use crate::generic::IdAndExtId;
use crate::policies::*;

/// The read side: the template list, the stored list, and single fetch — including that an unknown
/// id is answered 200-with-no-items rather than 404.
#[tokio::test]
async fn test_policy_reads() -> Result<(), Box<dyn std::error::Error>> {
    let api = create_api_service();

    // Templates are synthesised from the server's PolicyType enum, so every variant this SDK
    // knows should be present — a new one added server-side shows up here as an unknown variant
    // and fails deserialization, which is the signal we want.
    let types = api.policies.types().await?;
    assert_eq!(types.get_http_status_code(), Some(200));
    let known = [
        PolicyType::SecurityPolicy,
        PolicyType::EncryptionPolicy,
        PolicyType::MaskingPolicy,
        PolicyType::IsWriteProtected,
        PolicyType::IsReadProtected,
        PolicyType::HasRequirement,
        PolicyType::NamingConvention,
    ];
    for t in known {
        assert!(
            types.get_items().iter().any(|p| p.policy_type == Some(t)),
            "template list should offer {}",
            t.as_str()
        );
    }

    let listed = api.policies.list().await?;
    assert_eq!(listed.get_http_status_code(), Some(200));

    if let Some(id) = listed.get_items().first().and_then(|p| p.id) {
        let one = api.policies.get(id).await?;
        assert_eq!(one.get_items().len(), 1);
        assert_eq!(one.get_items()[0].id, Some(id));
    }

    // An id that does not exist is not an error — the caller has to check the item count.
    let missing = api.policies.get(999_999_999).await?;
    assert_eq!(missing.get_http_status_code(), Some(200));
    assert!(missing.get_items().is_empty());

    Ok(())
}

/// Create, update and delete one policy. `NAMING_CONVENTION` is used because it is the only type
/// that may be tenant-wide, so the test needs no dataset fixture.
#[tokio::test]
async fn test_policy_create_update_delete() -> Result<(), Box<dyn std::error::Error>> {
    let api = create_api_service();
    let ext_id = "sdk_test_policy_naming_tenant";
    let selector = vec![IdAndExtId::from_external_id(ext_id)];
    let _ = api.policies.delete(&selector).await;

    let created = api
        .policies
        .create(
            &Policy::from_type(PolicyType::NamingConvention)
                .with_external_id(ext_id)
                .with_description("created by the SDK test suite"),
        )
        .await?;
    assert_eq!(created.get_http_status_code(), Some(200));
    let policy = created.get_items()[0].clone();
    let id = policy.id.expect("create should return the new policy's id");
    assert_eq!(policy.policy_type, Some(PolicyType::NamingConvention));
    assert!(!policy.deactivated);

    // Update takes a whole policy, not a set-of-changes block: send it back with the new values.
    let mut edited = policy.clone();
    edited.description = Some("updated by the SDK test suite".to_string());
    edited.deactivated = true;
    let updated = api.policies.update(&edited).await?;
    assert_eq!(updated.get_http_status_code(), Some(200));
    assert_eq!(
        updated.get_items()[0].description.as_deref(),
        Some("updated by the SDK test suite")
    );

    let deleted = api.policies.delete(&vec![IdAndExtId::from_id(id)]).await?;
    assert_eq!(deleted.get_http_status_code(), Some(204));
    assert!(
        !api.policies
            .list()
            .await?
            .get_items()
            .iter()
            .any(|p| p.id == Some(id)),
        "the policy should be gone after delete"
    );

    Ok(())
}

/// A dataset-only type with no `data_set_id` is rejected before anything is written. Worth pinning:
/// the SDK mirrors this rule in [`PolicyType::scope`], and the two must not drift.
#[tokio::test]
async fn test_policy_scope_is_enforced() -> Result<(), Box<dyn std::error::Error>> {
    let api = create_api_service();
    let err = api
        .policies
        .create(
            &Policy::from_type(PolicyType::HasRequirement)
                .with_external_id("sdk_test_policy_bad_scope"),
        )
        .await
        .expect_err("a DATASET_ONLY policy with no dataset should be refused");
    assert_eq!(err.get_status().as_u16(), 400);
    assert!(
        err.get_message().contains("data set"),
        "the 400 should say why: {}",
        err.get_message()
    );

    // Which is exactly what the client-side scope table predicts.
    assert!(!PolicyType::HasRequirement.can_apply_tenant_wide());
    assert!(PolicyType::NamingConvention.can_apply_tenant_wide());
    Ok(())
}

/// The naming preflight: only non-conforming ids come back, and nothing is written.
#[tokio::test]
async fn test_check_naming_reports_only_violations() -> Result<(), Box<dyn std::error::Error>> {
    let api = create_api_service();

    let check = api
        .policies
        .check_naming(&NamingCheckForm::external_ids(&[
            "BAD ID!!",
            "plant_a_pump_01",
        ]))
        .await?;

    // The conforming id is absent from the findings entirely — the response is violations-only.
    assert!(
        !check
            .findings
            .iter()
            .any(|f| f.external_id.as_deref() == Some("plant_a_pump_01")),
        "a conforming id should not be reported: {:?}",
        check.findings
    );

    // Whatever the tenant policy is, a finding must name the item, the rule and the verdict.
    for finding in &check.findings {
        assert!(
            finding.policy_external_id.is_some(),
            "finding names no rule"
        );
        assert!(finding.message.is_some(), "finding has no explanation");
        assert!(finding.index < 2, "index should point into the batch");
    }

    // `externalIds` is @NotEmpty server-side: an empty request is a 400, not a clean answer.
    let empty = api
        .policies
        .check_naming(&NamingCheckForm::default())
        .await
        .expect_err("an empty externalIds list should be rejected");
    assert_eq!(empty.get_status().as_u16(), 400);

    // Names are positional labels for the ids, not a second list to check.
    let with_names = api
        .policies
        .check_naming(
            &NamingCheckForm::external_ids(&["BAD ID!!"]).with_names(&["Valve 21 PT 1034"]),
        )
        .await?;
    assert!(
        with_names.findings.iter().all(|f| f.index == 0),
        "one id in means at most one finding, at index 0"
    );

    Ok(())
}

/// Pure serde checks: the wire spellings the server expects, and the `{"findings": [...]}`
/// envelope, which is not the usual `{"items": [...]}` one.
#[test]
fn policy_wire_shapes() {
    use crate::generic::DataWrapperDeserialization;

    let policy = Policy::from_type(PolicyType::IsWriteProtected)
        .with_external_id("ds_readonly")
        .with_data_set_id(12);
    let json = serde_json::to_value(&policy).unwrap();
    assert_eq!(json["type"], "IS_WRITE_PROTECTED");
    assert_eq!(json["dataSetId"], "12", "ids go on the wire as strings");
    assert_eq!(json["deactivated"], false);
    // Unset optionals stay off the wire entirely.
    assert!(json.get("id").is_none());
    assert!(json.get("templateId").is_none());

    let check = NamingCheck::deserialize_and_set_status(
        r#"{"findings":[{"index":0,"externalId":"BAD ID","decision":"NOT_OK",
            "policy":"naming_default","message":"nope","suggestion":"bad_id"}]}"#,
        200,
    )
    .expect("findings envelope should parse");
    assert_eq!(check.findings.len(), 1);
    assert!(!check.is_clean());
    assert_eq!(check.rejections().count(), 1);
    let f = &check.findings[0];
    assert_eq!(f.decision, PolicyDecision::NotOk);
    assert_eq!(f.policy_external_id.as_deref(), Some("naming_default"));
    assert_eq!(f.suggestion.as_deref(), Some("bad_id"));

    // The scope table the client mirrors from the server.
    assert_eq!(
        PolicyType::NamingConvention.scope(),
        PolicyScope::TenantWithDatasetOverride
    );
    assert_eq!(PolicyType::MaskingPolicy.scope(), PolicyScope::DatasetOnly);
}
