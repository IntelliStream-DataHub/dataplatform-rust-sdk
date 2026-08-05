"""Tests for updating a resource's labels via ``resources.update``.

Mirrors ``src/resources/label_update_tests.rs``. The privileged **type-labels**
(``ASSET``/``DATASET``/``POLICY``/``TIMESERIES``/``FUNCTION``) are forced by the server to
stay exactly the node's own type — no update may add, remove, or swap one — while ordinary
labels follow ``base(set|current) + add - remove``.

A label update is **either** a replace (``ListFieldStr.set([...])``) **or** a delta
(``ListFieldStr.delta(add=[...], remove=[...])``), never both — the two classmethods are the only
way to build one, so the illegal mix is unrepresentable rather than rejected at runtime.
"""
import time

import pytest
from datahub_sdk import ListFieldStr, ListFieldU64, MapField, Resource, ResourceUpdate

from fixtures import async_client, make_resource, sync_client, unique_id

# Give a create a moment to commit (and its label M2M to settle) before updating it.
WRITE_SETTLE = 3.0


def _labels(result) -> list[str]:
    """Sorted labels of the first node in an update/create result."""
    return sorted(result.nodes[0].labels or [])


def _make_asset(make_resource, ext: str, extra: list[str]):
    """Create a fresh ASSET-typed resource carrying `extra` non-type labels."""
    make_resource(
        [Resource(external_id=ext, name="Py label-update probe", is_root=True,
                  labels=["ASSET", *extra])]
    )
    time.sleep(WRITE_SETTLE)


# --------------------------------------------------------------------------- #
# Binding shape — no backend call needed.
# --------------------------------------------------------------------------- #

def test_resource_update_requires_a_target():
    # the target resource (identifier) is a required positional arg
    with pytest.raises(TypeError):
        ResourceUpdate()


def test_field_update_is_set_or_delta_only():
    # `set` and `delta` are the only constructors; there is no bare initializer, so a replace can
    # never be built carrying an add/remove delta — the illegal mix is simply not expressible.
    for wrapper in (ListFieldStr, ListFieldU64, MapField):
        with pytest.raises(TypeError):
            wrapper()  # no __init__


# --------------------------------------------------------------------------- #
# Live: label-update semantics.
# --------------------------------------------------------------------------- #

def test_special_type_label_is_preserved(sync_client, make_resource):
    ext = unique_id("res_lblupd_type")
    _make_asset(make_resource, ext, ["PUMP"])

    # removing the type-label: forced back, PUMP untouched
    r = sync_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.delta(remove=["ASSET"]))])
    assert _labels(r) == ["ASSET", "PUMP"]

    # adding a foreign type-label: stripped
    r = sync_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.delta(add=["DATASET"]))])
    assert "DATASET" not in (r.nodes[0].labels or [])
    assert "ASSET" in (r.nodes[0].labels or [])

    # set to a foreign set: own type-label forced back, non-type labels replaced
    r = sync_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.set(["SENSOR"]))])
    assert _labels(r) == ["ASSET", "SENSOR"]


def test_set_add_remove_individually(sync_client, make_resource):
    ext = unique_id("res_lblupd_basic")
    _make_asset(make_resource, ext, ["PUMP"])

    r = sync_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.delta(add=["CRITICAL"]))])
    assert _labels(r) == ["ASSET", "CRITICAL", "PUMP"]

    r = sync_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.delta(remove=["PUMP"]))])
    assert _labels(r) == ["ASSET", "CRITICAL"]

    # set replaces the whole non-type set; type-label forced back
    r = sync_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.set(["ALPHA", "BETA"]))])
    assert _labels(r) == ["ALPHA", "ASSET", "BETA"]


def test_add_and_remove_together_is_a_valid_delta(sync_client, make_resource):
    # add + remove in one delta applies in a single update.
    ext = unique_id("res_lblupd_delta")
    _make_asset(make_resource, ext, ["OLD"])

    r = sync_client.resources.update(
        [ResourceUpdate(ext, labels=ListFieldStr.delta(add=["NEWLBL"], remove=["OLD"]))]
    )
    assert _labels(r) == ["ASSET", "NEWLBL"]


@pytest.mark.asyncio
async def test_update_async(async_client, sync_client):
    """The async binding mirrors the sync one: add then remove a label."""
    ext = unique_id("res_lblupd_async")
    sync_client.resources.create(
        [Resource(external_id=ext, name="Py async", is_root=True, labels=["ASSET"])]
    )
    time.sleep(WRITE_SETTLE)
    try:
        r = await async_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.delta(add=["ASYNCLBL"]))])
        assert _labels(r) == ["ASSET", "ASYNCLBL"]
        r = await async_client.resources.update([ResourceUpdate(ext, labels=ListFieldStr.delta(remove=["ASYNCLBL"]))])
        assert _labels(r) == ["ASSET"]
    finally:
        sync_client.resources.delete([ext])
