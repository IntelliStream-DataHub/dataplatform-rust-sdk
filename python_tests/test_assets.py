"""Tests for the Python assets module.

Mirrors `src/assets/tests.rs`. Round-trips an asset through the live API and pins the two things
that make `/assets` worth having over the generic `/resources`: reads come back typed as `Asset`,
and `geolocation` is reachable without a type check.

Skipped if the backend is unreachable (the fixture takes care of that).
"""
import intellistream_datahub_sdk
import pytest

from fixtures import sync_client, unique_id


@pytest.fixture
def make_asset(sync_client):
    """Factory that creates assets and deletes them at teardown.

    Assets are resources, so the sweep in conftest picks them up through `/resources/filter`
    anyway; this keeps a passing run from leaving rows behind in the first place.
    """
    created = []

    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("asset"))
        kwargs.setdefault("name", "Asset SDK roundtrip")
        asset = intellistream_datahub_sdk.Asset(**kwargs)
        created.append(asset.external_id)
        return sync_client.assets.create([asset])[0]

    yield _make

    for ext_id in reversed(created):
        try:
            sync_client.assets.delete([ext_id])
        except Exception:
            pass


def test_create_read_update_delete(sync_client, make_asset):
    ext_id = unique_id("asset")
    created = make_asset(
        external_id=ext_id,
        name="Asset SDK roundtrip",
        description="created by test_assets",
        geolocation={"type": "Point", "coordinates": [10.75, 59.91]},
    )

    assert isinstance(created, intellistream_datahub_sdk.Asset)
    assert created.external_id == ext_id
    assert created.id is not None
    # The api forces the type-label back on every read.
    assert "ASSET" in (created.labels or [])

    got = sync_client.assets.get_by_id(created.id)
    assert isinstance(got, intellistream_datahub_sdk.Asset)
    assert got.external_id == ext_id
    # Read flatly, an asset carries its geometry — the whole point of the typed family.
    assert got.geolocation is not None
    assert got.geolocation["type"] == "Point"

    by_ids = sync_client.assets.by_ids([ext_id])
    assert [a.external_id for a in by_ids] == [ext_id]

    listed = sync_client.assets.list(limit=1000)
    assert any(a.external_id == ext_id for a in listed)
    assert all(isinstance(a, intellistream_datahub_sdk.Asset) for a in listed)

    page = sync_client.assets.filter(external_id=[ext_id])
    assert [a.external_id for a in page] == [ext_id]

    hits = sync_client.assets.search(ext_id)
    assert any(a.external_id == ext_id for a in hits)

    sync_client.assets.update(
        [
            intellistream_datahub_sdk.ResourceUpdate(
                ext_id,
                name=intellistream_datahub_sdk.FieldStr("Asset SDK roundtrip (renamed)"),
            )
        ]
    )
    assert sync_client.assets.get_by_id(created.id).name == "Asset SDK roundtrip (renamed)"

    sync_client.assets.delete([ext_id])
    with pytest.raises(Exception):
        sync_client.assets.get_by_id(created.id)


def test_list_is_not_a_page(sync_client, make_asset):
    """`list()` returns a plain list; only `filter()` pages.

    The api nulls the cursor on a plain listing deliberately — a walk needs a sort and a cursor
    and both live in a request body — so handing one out would invite a loop that never advances.
    """
    make_asset()
    listed = sync_client.assets.list(limit=10)
    assert isinstance(listed, list)
    assert not hasattr(listed, "next_cursor")

    page = sync_client.assets.filter(limit=10)
    assert hasattr(page, "next_cursor")


def test_filter_rejects_both_forms(sync_client):
    """The either-or rule every filter binding shares."""
    with pytest.raises(TypeError):
        sync_client.assets.filter(
            filter=intellistream_datahub_sdk.ResourceFilter(name=["x*"]),
            name=["y*"],
        )


def test_a_non_asset_id_is_reported_as_missing(sync_client):
    """A node that exists but is not an asset is missing, not a type error.

    So a 404 from `assets.get_by_id` does not tell you whether the id exists.
    """
    ext_id = unique_id("fn_not_asset")
    fn = intellistream_datahub_sdk.Function(external_id=ext_id, name="not an asset")
    created = sync_client.functions.create([fn])[0]
    try:
        with pytest.raises(Exception):
            sync_client.assets.get_by_id(created.id)
    finally:
        try:
            sync_client.functions.delete([ext_id])
        except Exception:
            pass


def test_geolocation_is_updatable(sync_client, make_asset):
    """`geolocation` through the shared update form — the one field that means anything on
    exactly one node type."""
    created = make_asset()

    sync_client.assets.update(
        [
            intellistream_datahub_sdk.ResourceUpdate(
                created.external_id,
                geolocation=intellistream_datahub_sdk.FieldGeoJson(
                    {"type": "Point", "coordinates": [5.32, 60.39]}
                ),
            )
        ]
    )

    stored = sync_client.assets.get_by_id(created.id).geolocation
    assert stored is not None
    assert stored["coordinates"] == [5.32, 60.39]


def test_update_echo_is_typed(sync_client, make_asset):
    """The update echo is typed, not flat resources.

    It answered with a plain `Resource` whatever the node's real type until the api's node-update
    refactor landed. Worth pinning: the flat shape could not even carry an asset's geometry.
    """
    created = make_asset(geolocation={"type": "Point", "coordinates": [10.75, 59.91]})

    result = sync_client.assets.update(
        [
            intellistream_datahub_sdk.ResourceUpdate(
                created.external_id,
                description=intellistream_datahub_sdk.FieldStr("typed echo"),
            )
        ]
    )

    assert len(result.nodes) == 1
    echoed = result.nodes[0]
    assert isinstance(echoed, intellistream_datahub_sdk.Asset)
    assert echoed.external_id == created.external_id
