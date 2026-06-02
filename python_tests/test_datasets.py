"""Tests for the Python datasets module.

Exercises every endpoint on `DatasetsServiceSync`: create, by_ids, delete.
"""
import uuid

import datahub_sdk
import pytest

from fixtures import sync_client


def _suffix() -> str:
    return uuid.uuid4().hex[:8]


def test_create_by_ids_delete_roundtrip(sync_client):
    ext_a = f"py_test_dataset_a_{_suffix()}"
    ext_b = f"py_test_dataset_b_{_suffix()}"
    ds_a = datahub_sdk.Dataset(
        external_id=ext_a,
        name=ext_a,
        description="dataset a",
        metadata={"env": "test"},
    )
    ds_b = datahub_sdk.Dataset(external_id=ext_b, name=ext_b)

    try:
        created = sync_client.datasets.create([ds_a, ds_b])
        assert len(created) == 2
        ext_ids = {d.external_id for d in created}
        assert ext_ids == {ext_a, ext_b}
        # Server-assigned ids should be populated.
        assert all(d.id is not None for d in created)

        # by_ids accepts the entity directly via DatasetIdentifiable.
        fetched = sync_client.datasets.by_ids(created)
        assert {d.external_id for d in fetched} == {ext_a, ext_b}

        # Also accepts a raw external_id string.
        fetched_by_ext = sync_client.datasets.by_ids([ext_a])
        assert len(fetched_by_ext) == 1
        assert fetched_by_ext[0].external_id == ext_a

        sync_client.datasets.delete(created)
        after = sync_client.datasets.by_ids([ext_a, ext_b])
        assert not any(d.external_id in {ext_a, ext_b} for d in after)
    finally:
        try:
            sync_client.datasets.delete([ext_a, ext_b])
        except Exception:
            pass


def test_create_preserves_metadata_and_description(sync_client):
    ext = f"py_test_dataset_meta_{_suffix()}"
    ds = datahub_sdk.Dataset(
        external_id=ext,
        name=ext,
        description="with metadata",
        metadata={"team": "platform", "tier": "1"},
    )
    try:
        created = sync_client.datasets.create([ds])[0]
        assert created.description == "with metadata"
        assert created.metadata.get("team") == "platform"
        assert created.metadata.get("tier") == "1"
    finally:
        try:
            sync_client.datasets.delete([ext])
        except Exception:
            pass
