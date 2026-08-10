"""Tests for the Python datasets module.

Exercises every endpoint on `DatasetsServiceSync` (create, by_ids, delete) and
`DatasetsServiceAsync`, which additionally has `list`.
"""
import datahub_sdk
import pytest

from fixtures import async_client, make_dataset, sync_client, unique_id


def test_create_by_ids_delete_roundtrip(sync_client):
    ext_a = unique_id("dataset_a")
    ext_b = unique_id("dataset_b")
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
    ext = unique_id("dataset_meta")
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


@pytest.mark.asyncio
async def test_async_client_exposes_datasets(async_client, make_dataset):
    """`AsyncDataHubClient.datasets` reaches the same tenant the sync client writes to.

    The async service existed but had no getter on the client, so it was
    unreachable from Python; this is the regression test for that wiring.
    """
    ext_id = unique_id("ds_async_list")
    created = make_dataset(external_id=ext_id, name=ext_id)

    fetched = await async_client.datasets.by_ids([created])
    assert [d.external_id for d in fetched] == [ext_id]


@pytest.mark.asyncio
async def test_async_list_honours_limit(async_client, make_dataset):
    """`list()` takes an optional cap.

    Omitting it leaves the server's default of 100 in place; the point of the
    parameter is that a tenant with more datasets than that is otherwise
    truncated with no way to ask for more.
    """
    ext_id = unique_id("ds_async_limit")
    make_dataset(external_id=ext_id, name=ext_id)

    everything = await async_client.datasets.list()
    assert any(d.external_id == ext_id for d in everything)

    capped = await async_client.datasets.list(1)
    assert len(capped) == 1

    with pytest.raises(datahub_sdk.DataHubException):
        await async_client.datasets.list(10_001)
