"""Tests for the Python functions module.

Mirrors `src/functions/test.rs`. Round-trips a function through the live API. Skipped if
the backend is unreachable (the fixture takes care of that).
"""
import intellistream_datahub_sdk
import pytest

from fixtures import make_function, sync_client, unique_id


def test_create_list_by_external_id_delete(sync_client):
    ext_id = unique_id("fn")
    fn = intellistream_datahub_sdk.Function(
        external_id=ext_id,
        name="Function SDK roundtrip",
    )

    try:
        created = sync_client.functions.create([fn])
        assert len(created) == 1
        assert created[0].external_id == ext_id
        assert created[0].id is not None

        listed = sync_client.functions.list()
        assert any(f.external_id == ext_id for f in listed)

        by_ext = sync_client.functions.by_external_id(ext_id)
        assert by_ext.external_id == ext_id

        by_ids = sync_client.functions.by_ids([ext_id])
        assert any(f.external_id == ext_id for f in by_ids)

        sync_client.functions.delete([ext_id])
        after = sync_client.functions.list()
        assert not any(f.external_id == ext_id for f in after)
    finally:
        # Best-effort cleanup if an assertion failed before the explicit delete.
        try:
            sync_client.functions.delete([ext_id])
        except Exception:
            pass


def test_by_external_id_raises_when_missing(sync_client):
    with pytest.raises(Exception):
        sync_client.functions.by_external_id(unique_id("does_not_exist"))


def test_by_ids_filter_and_search(sync_client, make_function):
    from polling import poll_until

    # One unbroken lexeme for the full-text index; underscores would split it.
    token = unique_id("fn").replace("_", "")
    fn = make_function(name=f"{token} filter probe")

    by_id = sync_client.functions.by_ids([fn.id])
    assert [f.external_id for f in by_id] == [fn.external_id]
    assert sync_client.functions.by_ids([unique_id("fn_absent")]) == []

    page = sync_client.functions.filter(external_id=fn.external_id)
    assert [f.external_id for f in page] == [fn.external_id]
    same = sync_client.functions.filter(
        filter=intellistream_datahub_sdk.FunctionFilter(external_id=fn.external_id)
    )
    assert [f.external_id for f in same] == [fn.external_id]
    with pytest.raises(TypeError):
        sync_client.functions.filter(
            filter=intellistream_datahub_sdk.FunctionFilter(), external_id=fn.external_id
        )

    hits = poll_until(
        lambda: sync_client.functions.search(token),
        lambda found: any(f.external_id == fn.external_id for f in found),
    )
    assert any(f.external_id == fn.external_id for f in hits)
