"""Tests for the Python resources module.

Exercises every reachable endpoint on `ResourcesServiceSync`: create, by_ids, delete.
`search` is `todo!()` in the binding and would panic if called.
"""
import uuid

import datahub_sdk
import pytest

from fixtures import sync_client


def _suffix() -> str:
    return uuid.uuid4().hex[:8]


def test_create_by_ids_delete_roundtrip(sync_client):
    ext_a = f"py_test_resource_a_{_suffix()}"
    ext_b = f"py_test_resource_b_{_suffix()}"
    res_a = datahub_sdk.Resource(
        name=f"Resource {ext_a}",
        external_id=ext_a,
        description="resource a",
        metadata={"env": "test"},
        labels=["ASSET", "TEST"],
    )
    res_b = datahub_sdk.Resource(
        name=f"Resource {ext_b}",
        external_id=ext_b,
        labels=["ASSET", "TEST"],
    )

    try:
        # create returns a GraphResult; the entities are on .nodes.
        created = sync_client.resources.create([res_a, res_b]).nodes
        assert len(created) == 2
        ext_ids = {r.external_id for r in created}
        assert ext_ids == {ext_a, ext_b}
        assert all(r.id is not None for r in created)

        fetched = sync_client.resources.by_ids(created)
        assert {r.external_id for r in fetched} == {ext_a, ext_b}

        sync_client.resources.delete(created)
        after = sync_client.resources.by_ids(created)
        assert not any(r.external_id in {ext_a, ext_b} for r in after)
    finally:
        try:
            sync_client.resources.delete([res_a, res_b])
        except Exception:
            pass


def test_create_preserves_metadata_and_labels(sync_client):
    ext = f"py_test_resource_meta_{_suffix()}"
    res = datahub_sdk.Resource(
        name=f"Resource {ext}",
        external_id=ext,
        description="with metadata",
        metadata={"team": "platform", "tier": "1"},
        labels=["ASSET", "TEST"],
    )
    try:
        created = sync_client.resources.create([res]).nodes[0]
        assert created.description == "with metadata"
        assert created.metadata.get("team") == "platform"
        assert created.metadata.get("tier") == "1"
        assert set(created.labels) >= {"ASSET", "TEST"}
    finally:
        try:
            sync_client.resources.delete([res])
        except Exception:
            pass
