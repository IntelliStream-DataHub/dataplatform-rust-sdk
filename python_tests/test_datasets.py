"""Tests for the Python datasets module.

Exercises every endpoint on `DatasetsServiceSync` (create, by_ids, delete) and
`DatasetsServiceAsync`, which additionally has `list`.
"""
import uuid

import intellistream_datahub_sdk
import pytest

from fixtures import async_client, make_dataset, sync_client, unique_id


def test_create_by_ids_delete_roundtrip(sync_client):
    ext_a = unique_id("dataset_a")
    ext_b = unique_id("dataset_b")
    ds_a = intellistream_datahub_sdk.Dataset(
        external_id=ext_a,
        name=ext_a,
        description="dataset a",
        metadata={"env": "test"},
    )
    ds_b = intellistream_datahub_sdk.Dataset(external_id=ext_b, name=ext_b)

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
    ds = intellistream_datahub_sdk.Dataset(
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

    with pytest.raises(intellistream_datahub_sdk.DataHubException):
        await async_client.datasets.list(10_001)


# --------------------------------------------------------------------------- #
# filter / search / update / policies — sync and async
# --------------------------------------------------------------------------- #


def test_sync_list_and_filter(sync_client, make_dataset):
    """`filter` narrows server-side; `list` does not.

    The distinction matters: pointing `filter` at `/list` would return the whole
    tenant, and an exclusion check is the only assertion that catches it.
    """
    ext_id = unique_id("ds_filter")
    make_dataset(external_id=ext_id, name=ext_id, metadata={"suite": "ds_filter"})

    assert any(d.external_id == ext_id for d in sync_client.datasets.list())

    narrowed = sync_client.datasets.filter(
        intellistream_datahub_sdk.DatasetFilterForm(
            intellistream_datahub_sdk.DatasetFilter(external_id=[ext_id])
        )
    )
    assert [d.external_id for d in narrowed] == [ext_id]

    # An unmatchable criterion is an empty result, not an unfiltered one.
    assert (
        sync_client.datasets.filter(
            intellistream_datahub_sdk.DatasetFilterForm(
                intellistream_datahub_sdk.DatasetFilter(external_id=["ds_does_not_exist_xyz"])
            )
        )
        == []
    )

    # An argument-free filter places no restriction, same as list().
    assert any(
        d.external_id == ext_id
        for d in sync_client.datasets.filter(intellistream_datahub_sdk.DatasetFilterForm())
    )


def test_sync_filter_by_metadata_and_prefix(sync_client, make_dataset):
    ext_id = unique_id("ds_meta")
    make_dataset(external_id=ext_id, name=ext_id, metadata={"owner": ext_id})

    by_metadata = sync_client.datasets.filter(
        intellistream_datahub_sdk.DatasetFilterForm(
            intellistream_datahub_sdk.DatasetFilter(metadata={"owner": ext_id})
        )
    )
    assert [d.external_id for d in by_metadata] == [ext_id]

    by_prefix = sync_client.datasets.filter(
        intellistream_datahub_sdk.DatasetFilterForm(
            intellistream_datahub_sdk.DatasetFilter(external_id=f"{ext_id}*")
        )
    )
    assert [d.external_id for d in by_prefix] == [ext_id]


def test_sync_search(sync_client, make_dataset):
    """Search matches the name, and the phrase may be anything 3 characters or longer.

    ``query`` used to be validated against `^[\\p{IsLatin}\\p{Zs}\\p{Nd}]+` — letters, spaces and
    digits — which made an external id an illegal query even though the index covers it, since the
    convention here is snake_case. Only the length bounds remain.
    """
    token = uuid.uuid4().hex[:12]
    ext_id = unique_id("ds_search")
    make_dataset(external_id=ext_id, name=f"sdk search fixture {token}")

    hits = sync_client.datasets.search(token)
    assert any(d.external_id == ext_id for d in hits)

    # A query under the server's 3-character minimum is rejected, not matched loosely.
    with pytest.raises(intellistream_datahub_sdk.DataHubException):
        sync_client.datasets.search("ab")

    by_ext_id = sync_client.datasets.search(ext_id)
    assert any(d.external_id == ext_id for d in by_ext_id), (
        "an external id is a legal query now, and the index covers it"
    )


def test_sync_update(sync_client, make_dataset):
    """Only the fields passed are sent; the rest are left untouched.

    There is no ``write_protected`` / ``deactivated`` here any more: both were removed server-side
    as inert — stored as node metadata rather than as columns, read by nothing, and clobbering a
    metadata delta sent in the same update. ``test_filter_datasets.py`` pins that the filter no
    longer accepts them either.
    """
    ext_id = unique_id("ds_update")
    make_dataset(
        external_id=ext_id,
        name=ext_id,
        description="before",
        metadata={"keep": "me"},
    )

    updated = sync_client.datasets.update(
        [
            intellistream_datahub_sdk.DatasetUpdate(
                ext_id,
                description=intellistream_datahub_sdk.FieldStr("after"),
                metadata=intellistream_datahub_sdk.MapField.delta(add={"owner": "sdk_tests"}),
            )
        ]
    )
    assert len(updated) == 1
    after = updated[0]
    assert after.description == "after"
    assert after.metadata["owner"] == "sdk_tests"
    # An untouched field survives, and a delta is not a replace.
    assert after.metadata["keep"] == "me"
    assert after.name == ext_id


def test_sync_policies(sync_client):
    """Reachable and well-typed. See the binding docstring: the server currently
    answers 200 with no body, so this asserts the shape and not the contents."""
    assert isinstance(sync_client.datasets.policies(), list)


@pytest.mark.asyncio
async def test_async_filter_search_update_policies(async_client, make_dataset):
    token = uuid.uuid4().hex[:12]
    ext_id = unique_id("ds_async_all")
    make_dataset(
        external_id=ext_id, name=f"sdk async fixture {token}", description="before"
    )

    narrowed = await async_client.datasets.filter(
        intellistream_datahub_sdk.DatasetFilterForm(
            intellistream_datahub_sdk.DatasetFilter(external_id=[ext_id])
        )
    )
    assert [d.external_id for d in narrowed] == [ext_id]

    hits = await async_client.datasets.search(token)
    assert any(d.external_id == ext_id for d in hits)

    updated = await async_client.datasets.update(
        [
            intellistream_datahub_sdk.DatasetUpdate(
                ext_id, description=intellistream_datahub_sdk.FieldStr("after")
            )
        ]
    )
    assert updated[0].description == "after"

    assert isinstance(await async_client.datasets.policies(), list)
