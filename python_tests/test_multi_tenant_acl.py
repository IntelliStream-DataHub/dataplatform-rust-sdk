"""Multi-organization and dataset-ACL behaviour, through the Python bindings.

The Rust suite in ``src/multi_tenant_integration.rs`` is the thorough one and its module
docs are the reference for what the backend does and how the Keycloak fixtures must be
built. This file re-checks the parts that could plausibly break at the PyO3 boundary
rather than in the Rust core — chiefly that ``scope=`` is forwarded from the constructor,
since that is the *only* lever a Python caller has over which tenant they reach.

Every test skips when its ``MT_*`` fixture is absent from ``.env``, so a checkout without
the realm setup is unaffected. See the Rust module docs for the full env contract:

    MT_ORG_A_*      full access in the ACL organization (also seeds the ACL tests)
    MT_ORG_B_*      full access in a second organization
    MT_MULTI_*      a principal belonging to both
    MT_READONLY_*   /datasets/<ds>/read only, no blanket role
    MT_DATASET_EXT_ID

Two things worth knowing before reading the assertions:

- A rejected token is a plain **401 with no explanation**. The API drops the descriptive
  reason before it reaches any client, so these tests assert on ``status_code`` and tell
  the cases apart by how the fixture is built, never by the message.
- Denied **list/search** reads come back ``200`` with the rows quietly missing, not 403.
  Only single-item reads and writes raise.
"""
import os

import datahub_sdk
import pytest

from fixtures import ENV_FILE, unique_id

# Ask for every organization the caller belongs to: fine for a single-org principal,
# ambiguous — and therefore refused — for one that belongs to two.
SCOPE_ALL_ORGS = "openid organization:*"


def _read_env(path):
    """Minimal KEY=VALUE parser — the same flat .env the rest of the suite uses."""
    values = {}
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@pytest.fixture(scope="module")
def env():
    if not os.path.exists(ENV_FILE):
        pytest.skip(".env not present")
    values = _read_env(ENV_FILE)
    if not values.get("BASE_URL") or not values.get("TOKEN_URI"):
        pytest.skip("BASE_URL / TOKEN_URI not configured")
    return values


def client_for(env, prefix, scope=SCOPE_ALL_ORGS):
    """A client for the ``{prefix}`` principal, or skip when it isn't configured.

    Deliberately built from explicit kwargs rather than ``from_envfile``: the point is to
    exercise the constructor's ``scope`` forwarding, and a regression there would hide
    behind the env-loading path the rest of the suite uses.
    """
    client_id = env.get(f"{prefix}_CLIENT_ID")
    client_secret = env.get(f"{prefix}_CLIENT_SECRET")
    if not (client_id and client_secret):
        pytest.skip(f"{prefix}_CLIENT_ID / _SECRET not configured")
    return datahub_sdk.DataHubClient(
        env["BASE_URL"],
        token_url=env["TOKEN_URI"],
        client_id=client_id,
        client_secret=client_secret,
        scope=scope,
    )


def require_healthy_realm(env):
    """Skip unless a well-formed single-organization token is accepted.

    Without this the 401 assertions are vacuous: a realm that rejects *everything* (two
    mappers merging the ``organization`` claim into an array, ``addOrganizationId`` off,
    the tenant not provisioned) yields exactly the 401 those tests look for.
    """
    control = client_for(env, "MT_ORG_A")
    try:
        control.units.list()
    except datahub_sdk.DataHubException as e:
        pytest.skip(
            f"the control principal MT_ORG_A is itself refused with HTTP {e.status_code}, "
            "so a 401 would prove nothing — fix the realm first "
            "(exactly one mapper may write the `organization` claim, addOrganizationId on)"
        )
    return control


def scope_for(alias):
    return f"openid organization:{alias}"


# ---------------------------------------------------------------------------------------
# Multi-organization token resolution
# ---------------------------------------------------------------------------------------

def test_multi_org_principal_with_wildcard_scope_is_rejected(env):
    """A principal in two organizations is refused outright, not assigned one of them."""
    require_healthy_realm(env)
    multi = client_for(env, "MT_MULTI")

    with pytest.raises(datahub_sdk.DataHubException) as excinfo:
        multi.units.list()
    assert excinfo.value.status_code == 401


def test_principal_without_an_organization_selector_is_rejected(env):
    """The 'I forgot to pass scope=' mistake: no claim, so also a 401.

    Worth pinning separately because at the call site it is indistinguishable from bad
    credentials, and the natural reaction is to go rotate the secret.
    """
    require_healthy_realm(env)
    multi = client_for(env, "MT_MULTI", scope=None)

    with pytest.raises(datahub_sdk.DataHubException) as excinfo:
        multi.units.list()
    assert excinfo.value.status_code == 401


def test_single_org_principal_with_wildcard_scope_succeeds(env):
    """The control: ``organization:*`` is fine for a principal in exactly one org."""
    single = client_for(env, "MT_ORG_A")
    assert len(single.units.list()) > 0


def test_pinning_an_alias_selects_that_tenant(env):
    """Pinning unblocks the multi-org principal *and* chooses which database it reaches."""
    alias_a, alias_b = env.get("MT_ORG_A_ALIAS"), env.get("MT_ORG_B_ALIAS")
    if not (alias_a and alias_b):
        pytest.skip("MT_ORG_A_ALIAS / MT_ORG_B_ALIAS not configured")
    pinned_a = client_for(env, "MT_MULTI", scope=scope_for(alias_a))
    pinned_b = client_for(env, "MT_MULTI", scope=scope_for(alias_b))

    external_id = unique_id("mt_pin")
    pinned_a.resources.create([datahub_sdk.Resource(name=external_id, external_id=external_id)])
    try:
        assert pinned_a.resources.by_ids([external_id]), "the pinned tenant should see its own write"
        assert not _visible(pinned_b, external_id), (
            f"pinned to '{alias_b}', the same principal must not see '{alias_a}' data"
        )
    finally:
        pinned_a.resources.delete([external_id])


# ---------------------------------------------------------------------------------------
# Tenant isolation
# ---------------------------------------------------------------------------------------

def _visible(client, external_id):
    """Whether ``client`` can see the entity.

    Two shapes mean 'no': a bulk read narrowed by the tenant's database answers with an
    empty list, while a cross-tenant single-item read is a 404. Both are absence.
    """
    try:
        return bool(client.resources.by_ids([external_id]))
    except datahub_sdk.DataHubException as e:
        if e.status_code == 404:
            return False
        raise


def test_same_external_id_in_two_orgs_are_independent(env):
    """External ids are unique per tenant database, so both organizations can own one."""
    org_a = client_for(env, "MT_ORG_A")
    org_b = client_for(env, "MT_ORG_B")

    external_id = unique_id("mt_shared")
    org_a.resources.create(
        [datahub_sdk.Resource(name=external_id, external_id=external_id, description="org A")]
    )
    org_b.resources.create(
        [datahub_sdk.Resource(name=external_id, external_id=external_id, description="org B")]
    )
    try:
        assert org_a.resources.by_ids([external_id])[0].description == "org A"
        assert org_b.resources.by_ids([external_id])[0].description == "org B"

        # Deleting one leaves the other standing.
        org_a.resources.delete([external_id])
        assert _visible(org_b, external_id), "org B's entity must survive org A's delete"
    finally:
        for client in (org_a, org_b):
            try:
                client.resources.delete([external_id])
            except datahub_sdk.DataHubException:
                pass


# ---------------------------------------------------------------------------------------
# Dataset ACL
# ---------------------------------------------------------------------------------------

@pytest.fixture(scope="module")
def acl_dataset_id(env):
    """The numeric id, in the ACL organization, of the dataset the grants name."""
    external_id = env.get("MT_DATASET_EXT_ID")
    if not external_id:
        pytest.skip("MT_DATASET_EXT_ID not configured")
    admin = client_for(env, "MT_ORG_A")
    try:
        found = admin.datasets.by_ids([external_id])
    except datahub_sdk.DataHubException as e:
        if e.status_code == 401:
            # The seeding principal's own token is refused, so there is nothing to test here.
            # test_single_org_principal_with_wildcard_scope_succeeds is the one that fails loudly.
            pytest.skip("MT_ORG_A's token is refused (401) — fix the realm first")
        raise
    if not found:
        pytest.skip(f"dataset '{external_id}' does not exist in the ACL organization")
    return found[0].id


def test_read_only_grant_reads_but_cannot_write(env, acl_dataset_id):
    """Read and write are independent grants; a reader is refused a write with 403."""
    admin = client_for(env, "MT_ORG_A")
    reader = client_for(env, "MT_READONLY")

    seeded = unique_id("mt_acl_read")
    admin.resources.create(
        [datahub_sdk.Resource(name=seeded, external_id=seeded, data_set_id=acl_dataset_id)]
    )
    try:
        assert _visible(reader, seeded), "a read grant should see the seeded resource"

        denied = unique_id("mt_acl_denied")
        with pytest.raises(datahub_sdk.DataHubException) as excinfo:
            reader.resources.create(
                [datahub_sdk.Resource(name=denied, external_id=denied, data_set_id=acl_dataset_id)]
            )
        assert excinfo.value.status_code == 403
        # Unlike a 401, a 403 does carry a body — RFC 9457 problem+json naming the
        # dataset and the permission that was missing.
        assert "write" in excinfo.value.message
    finally:
        admin.resources.delete([seeded])


def test_search_omits_denied_rows_rather_than_raising(env, acl_dataset_id):
    """A denied search is 200-with-nothing, not 403.

    The behaviour most likely to be assumed wrong: code that reads 'no exception' as 'saw
    everything' silently under-reports for a partially-granted caller.
    """
    admin = client_for(env, "MT_ORG_A")
    outsider = client_for(env, "MT_NOGRANT")

    seeded = unique_id("mt_acl_narrowed")
    admin.resources.create(
        [datahub_sdk.Resource(name=seeded, external_id=seeded, data_set_id=acl_dataset_id)]
    )
    try:
        form = datahub_sdk.SearchAndFilterForm(query=seeded)
        # Control first, so an empty result below means 'narrowed', not 'never created'.
        assert any(r.external_id == seeded for r in admin.resources.search(form))
        assert not any(r.external_id == seeded for r in outsider.resources.search(form))
    finally:
        admin.resources.delete([seeded])
