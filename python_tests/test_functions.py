"""Tests for the Python functions module.

Mirrors `src/functions/test.rs`. Round-trips a function through the live API. Skipped if
the backend is unreachable (the fixture takes care of that).
"""
import uuid

import datahub_sdk
import pytest

from fixtures import sync_client


def _suffix() -> str:
    return uuid.uuid4().hex[:8]


def test_create_list_by_external_id_delete(sync_client):
    ext_id = f"py_test_fn_{_suffix()}"
    fn = datahub_sdk.Function(
        external_id=ext_id,
        model_name="forecast-ema",
        name="Function SDK roundtrip ema",
        config={"alpha": 0.5},
    )

    try:
        created = sync_client.functions.create([fn])
        assert len(created) == 1
        assert created[0].external_id == ext_id
        assert created[0].model_name == "forecast-ema"
        assert created[0].config.get("alpha") == 0.5
        # Server applies template defaults on top of user overrides.
        assert "horizon" in created[0].config
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
        sync_client.functions.by_external_id(f"does_not_exist_{_suffix()}")
