"""Tests for the Python policies bindings.

Mirrors `src/policies/tests.rs`. Four things about this surface differ from the rest of the
SDK and are pinned here: `get` answers an unknown id with `None` rather than raising,
`update` takes one whole policy per call, `check_naming` returns a `NamingCheck` rather than a
list, and every policy type except `NAMING_CONVENTION` must name a `data_set_id`.
"""
import datahub_sdk
import pytest

from fixtures import async_client, sync_client, unique_id


def test_types_offers_every_known_policy_type(sync_client):
    templates = sync_client.policies.types()
    assert isinstance(templates, list)

    offered = {t.type for t in templates if t.type is not None}
    expected = {
        datahub_sdk.PolicyType.SECURITY_POLICY,
        datahub_sdk.PolicyType.ENCRYPTION_POLICY,
        datahub_sdk.PolicyType.MASKING_POLICY,
        datahub_sdk.PolicyType.IS_WRITE_PROTECTED,
        datahub_sdk.PolicyType.IS_READ_PROTECTED,
        datahub_sdk.PolicyType.HAS_REQUIREMENT,
        datahub_sdk.PolicyType.NAMING_CONVENTION,
    }
    assert expected <= offered

    # Templates are synthesised from the server's enum, so they carry no stored id.
    for t in templates:
        assert t.id is None
        assert t.description


def test_list_and_get(sync_client):
    policies = sync_client.policies.list()
    assert isinstance(policies, list)

    if policies:
        first = policies[0]
        fetched = sync_client.policies.get(first.id)
        assert fetched is not None
        assert fetched.id == first.id

    # An unknown id is not an error — the binding turns the server's empty items list into None.
    assert sync_client.policies.get(999_999_999) is None


def test_create_update_delete(sync_client):
    # NAMING_CONVENTION is the only type that may be tenant-wide, so this needs no dataset.
    ext_id = unique_id("policy")
    created = sync_client.policies.create(
        [
            datahub_sdk.Policy(
                type=datahub_sdk.PolicyType.NAMING_CONVENTION,
                external_id=ext_id,
                description="created by the python test suite",
            )
        ]
    )
    assert len(created) == 1
    policy = created[0]
    assert policy.id is not None
    assert policy.type == datahub_sdk.PolicyType.NAMING_CONVENTION
    assert policy.deactivated is False

    try:
        # Update sends the whole policy back with the values as they should end up.
        policy.description = "updated by the python test suite"
        policy.deactivated = True
        updated = sync_client.policies.update([policy])
        assert updated[0].description == "updated by the python test suite"

        # More than one policy per call would silently drop all but the first, so the binding
        # refuses it rather than letting the server quietly ignore the rest.
        with pytest.raises(ValueError):
            sync_client.policies.update([policy, policy])
        with pytest.raises(ValueError):
            sync_client.policies.update([])
    finally:
        sync_client.policies.delete([policy.id])

    assert all(p.id != policy.id for p in sync_client.policies.list())


def test_dataset_only_type_is_refused_without_a_dataset(sync_client):
    # The client-side scope table says this cannot be tenant-wide...
    assert datahub_sdk.PolicyType.HAS_REQUIREMENT.can_apply_tenant_wide() is False
    assert datahub_sdk.PolicyType.NAMING_CONVENTION.can_apply_tenant_wide() is True
    assert (
        datahub_sdk.PolicyType.HAS_REQUIREMENT.scope
        == datahub_sdk.PolicyScope.DATASET_ONLY
    )
    assert (
        datahub_sdk.PolicyType.NAMING_CONVENTION.scope
        == datahub_sdk.PolicyScope.TENANT_WITH_DATASET_OVERRIDE
    )

    # ...and the server agrees.
    with pytest.raises(datahub_sdk.DataHubException):
        sync_client.policies.create(
            [
                datahub_sdk.Policy(
                    type=datahub_sdk.PolicyType.HAS_REQUIREMENT,
                    external_id=unique_id("policy_bad_scope"),
                )
            ]
        )


def test_check_naming_reports_only_violations(sync_client):
    check = sync_client.policies.check_naming(
        datahub_sdk.NamingCheckForm(external_ids=["BAD ID!!", "plant_a_pump_01"])
    )

    # Violations-only: a conforming id is absent from the findings entirely.
    assert all(f.external_id != "plant_a_pump_01" for f in check.findings)

    for finding in check.findings:
        assert finding.policy is not None, "a finding must name the rule that fired"
        assert finding.message
        assert finding.index < 2
        assert isinstance(finding.is_rejection, bool)

    # `is_clean` and truthiness are the two ways to ask "did anything trip".
    assert check.is_clean == (len(check) == 0)
    assert bool(check) == (not check.is_clean)
    assert len(check.rejections) <= len(check.findings)


def test_naming_check_form_validates_its_own_shape():
    # externalIds is @NotEmpty server-side; the binding refuses before the round trip.
    with pytest.raises(ValueError):
        datahub_sdk.NamingCheckForm(external_ids=[])

    # names are positional labels for the ids, so a mismatched length is a mistake.
    with pytest.raises(ValueError):
        datahub_sdk.NamingCheckForm(external_ids=["a_b_c", "d_e_f"], names=["only one"])

    form = datahub_sdk.NamingCheckForm(
        external_ids=["a_b_c"], names=["Valve 21"], data_set_id=12
    )
    assert form.external_ids == ["a_b_c"]
    assert form.names == ["Valve 21"]
    assert form.data_set_id == 12


def test_policy_requires_something_to_identify_it():
    with pytest.raises(ValueError):
        datahub_sdk.Policy()

    # A type is enough — the name defaults to it.
    p = datahub_sdk.Policy(type=datahub_sdk.PolicyType.MASKING_POLICY)
    assert p.name == "MASKING_POLICY"
    assert p.type == datahub_sdk.PolicyType.MASKING_POLICY


@pytest.mark.asyncio
async def test_async_reads(async_client):
    templates = await async_client.policies.types()
    assert any(
        t.type == datahub_sdk.PolicyType.NAMING_CONVENTION for t in templates
    )

    policies = await async_client.policies.list()
    assert isinstance(policies, list)

    assert await async_client.policies.get(999_999_999) is None

    check = await async_client.policies.check_naming(
        datahub_sdk.NamingCheckForm(external_ids=["BAD ID!!"])
    )
    assert isinstance(check.findings, list)
