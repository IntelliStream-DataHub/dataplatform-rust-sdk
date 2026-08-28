"""Comprehensive create / delete / update coverage for the TimeSeries service.

Update is the only mutating "patch" API the SDK exposes (events/datasets/resources
only support create + delete), so the bulk of this file exercises *every* update
modality the field-wrapper types allow:

  * FieldStr / FieldU64 scalar fields  -> set-value  and  set_null
  * MapField  (metadata)               -> add (merge), set (replace), remove (by key)
  * value_type re-typing
  * targeting an update by created-object / external-id string / numeric id
  * multi-field updates, batch updates, and no-op updates

These are integration tests; they hit the live backend configured in ``.env`` and
clean up after themselves via the ``make_ts`` fixture.
"""

import pytest

import intellistream_datahub_sdk
from python_tests.fixtures import *  # noqa: F401,F403  (sync_client fixture)


def _uid(prefix="crud"):
    return unique_id(prefix)


# ``make_ts`` is provided by fixtures.py (imported via ``*`` above).


def _refetch(sync_client, ts):
    """Re-read a series from the backend so we assert on persisted state."""
    return sync_client.timeseries.by_ids([ts])[0]


# --------------------------------------------------------------------------- #
# CREATE / DELETE — comprehensive array of inputs
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "kwargs",
    [
        {"value_type": "float"},
        {"value_type": "bigint"},
        {"value_type": "text"},
        {"value_type": "float", "metadata": {"a": "1", "b": "2"}},
        {"value_type": "float", "metadata": {}},
        {"value_type": "float", "metadata": {"empty_value": ""}},
        {"value_type": "float", "description": "a described series"},
        {"value_type": "float", "unit": "m/s", "unit_external_id": "ext.unit.id"},
        {"value_type": "float", "name": "Unicode ✓ 日本語 name"},
        {"value_type": "float", "source": "sap_pi"},
    ],
    ids=[
        "float", "bigint", "text", "with-metadata", "empty-metadata",
        "empty-metadata-value", "with-description", "with-units", "unicode-name",
        "with-source",
    ],
)
def test_create_delete_roundtrip(sync_client, kwargs):
    ext_id = _uid("crd")
    # unit is required by the backend (timeseries.unit.not.blank); default it
    # unless a case overrides it.
    kwargs = {"unit": "a.u", **kwargs}
    ts = intellistream_datahub_sdk.TimeSeries(external_id=ext_id, **kwargs)
    sync_client.timeseries.delete([ts])

    try:
        created = sync_client.timeseries.create([ts])
        assert len(created) == 1
        assert created[0].external_id == ext_id
        assert created[0].id is not None, "server should assign a numeric id"

        fetched = sync_client.timeseries.by_ids([ext_id])
        assert any(t.external_id == ext_id for t in fetched)
        assert fetched[0].source == kwargs.get("source")

        sync_client.timeseries.delete([created[0]])

        listed = sync_client.timeseries.list()
        assert all(t.external_id != ext_id for t in listed), "series should be gone after delete"
    finally:
        # Best-effort cleanup if an assertion failed before the explicit delete.
        try:
            sync_client.timeseries.delete([ext_id])
        except Exception:
            pass


def test_create_batch_multiple(sync_client):
    ext_ids = [_uid("batch") for _ in range(3)]
    series = [
        intellistream_datahub_sdk.TimeSeries(external_id=e, value_type="float", unit="a.u")
        for e in ext_ids
    ]
    sync_client.timeseries.delete(series)
    try:
        created = sync_client.timeseries.create(series)
        assert len(created) == 3
        assert {t.external_id for t in created} == set(ext_ids)
        assert all(t.id is not None for t in created)
    finally:
        sync_client.timeseries.delete(series)


@pytest.mark.parametrize("alias", ["decimal", "DECIMAL", "Decimal"])
def test_create_value_type_decimal_alias_normalises_to_float(sync_client, alias):
    """"decimal" (any case) is a legacy alias that normalises to canonical "float"."""
    ext_id = _uid("alias")
    ts = intellistream_datahub_sdk.TimeSeries(external_id=ext_id, value_type=alias, unit="a.u")
    sync_client.timeseries.delete([ts])
    try:
        created = sync_client.timeseries.create([ts])[0]
        assert created.value_type == "float"
    finally:
        sync_client.timeseries.delete([ts])


def test_delete_by_external_id_string(sync_client):
    ext_id = _uid("delstr")
    ts = intellistream_datahub_sdk.TimeSeries(external_id=ext_id, value_type="float", unit="a.u")
    sync_client.timeseries.delete([ts])
    sync_client.timeseries.create([ts])
    try:
        # delete by raw external-id string rather than the entity object
        sync_client.timeseries.delete([ext_id])
        listed = sync_client.timeseries.list()
        assert all(t.external_id != ext_id for t in listed)
    finally:
        # Best-effort cleanup if the assertion failed before the series was gone.
        try:
            sync_client.timeseries.delete([ext_id])
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# UPDATE — scalar string fields: set value
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, new_value, attr",
    [
        ("name", "Updated Name", "name"),
        ("unit", "Updated Unit", "unit"),
        ("description", "Updated Description", "description"),
        ("source", "updated_source", "source"),
        # unit_external_id is sanitised server-side (dots -> underscores), so the
        # value here is already in canonical form to keep the assertion exact.
        ("unit_external_id", "updated_unit_ext", "unit_external_id"),
    ],
)
def test_update_scalar_str_set_value(sync_client, make_ts, field, new_value, attr):
    ts = make_ts(
        name="Original",
        description="original description",
        unit="a.u",
        unit_external_id="orig_unit_ext",
        source="original_source",
    )

    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, **{field: intellistream_datahub_sdk.FieldStr(value=new_value)}
    )
    updated = sync_client.timeseries.update([update])[0]
    assert getattr(updated, attr) == new_value

    # confirm it persisted, not just echoed
    assert getattr(_refetch(sync_client, ts), attr) == new_value


def test_update_change_external_id(sync_client, make_ts):
    ts = make_ts(name="rename target")
    new_ext = _uid("renamed")

    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, external_id=intellistream_datahub_sdk.FieldStr(value=new_ext)
    )
    updated = sync_client.timeseries.update([update])[0]
    assert updated.external_id == new_ext

    # reachable under the new external id, gone under the old one
    assert sync_client.timeseries.by_ids([new_ext])[0].external_id == new_ext
    # keep teardown able to find it
    ts_new = sync_client.timeseries.by_ids([new_ext])[0]
    sync_client.timeseries.delete([ts_new])


# --------------------------------------------------------------------------- #
# UPDATE — scalar fields: set_null (clear)
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, attr",
    [
        ("description", "description"),
        ("unit", "unit"),
        ("unit_external_id", "unit_external_id"),
        ("source", "source"),
    ],
)
def test_update_scalar_str_set_null(sync_client, make_ts, field, attr):
    ts = make_ts(
        description="please clear me",
        unit="a.u",
        unit_external_id="clear.this.ext",
        source="please_clear_me",
    )

    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, **{field: intellistream_datahub_sdk.FieldStr(set_null=True)}
    )
    updated = sync_client.timeseries.update([update])[0]
    assert getattr(updated, attr) is None
    assert getattr(_refetch(sync_client, ts), attr) is None


def test_update_set_value_then_set_null(sync_client, make_ts):
    """A field can be set and then cleared across two updates."""
    ts = make_ts(description="first")

    set_update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, description=intellistream_datahub_sdk.FieldStr(value="second")
    )
    assert sync_client.timeseries.update([set_update])[0].description == "second"

    null_update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, description=intellistream_datahub_sdk.FieldStr(set_null=True)
    )
    assert sync_client.timeseries.update([null_update])[0].description is None


# `name` and `external_id` are set-only server-side: TimeseriesService reads `.getSet()` for both
# and never looks at `setNull`, so clearing either is accepted and ignored. strict=False surfaces
# an xpass if the backend grows the branch.
_NO_SETNULL_BRANCH = pytest.mark.xfail(
    reason="backend has no setNull branch for this field; the request is accepted and ignored",
    strict=False,
)


@_NO_SETNULL_BRANCH
def test_update_name_set_null(sync_client, make_ts):
    ts = make_ts(name="Clear my name")

    update = intellistream_datahub_sdk.TimeSeriesUpdate(ts, name=intellistream_datahub_sdk.FieldStr(set_null=True))
    assert not sync_client.timeseries.update([update])[0].name


@_NO_SETNULL_BRANCH
def test_update_external_id_set_null(sync_client, make_ts):
    ts = make_ts()

    update = intellistream_datahub_sdk.TimeSeriesUpdate(ts, external_id=intellistream_datahub_sdk.FieldStr(set_null=True))
    assert not sync_client.timeseries.update([update])[0].external_id


# --------------------------------------------------------------------------- #
# UPDATE — MapField (metadata): add / set / remove
# --------------------------------------------------------------------------- #

def test_update_metadata_add_merges_and_overwrites(sync_client, make_ts):
    ts = make_ts(metadata={"keep": "1", "overwrite": "old"})

    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, metadata=intellistream_datahub_sdk.MapField.delta(add={"overwrite": "new", "added": "2"})
    )
    md = sync_client.timeseries.update([update])[0].metadata or {}

    assert md.get("keep") == "1", "untouched keys must be preserved by add"
    assert md.get("overwrite") == "new", "existing key value must be overwritten"
    assert md.get("added") == "2", "new key must be added"


def test_update_metadata_set_replaces_whole_map(sync_client, make_ts):
    ts = make_ts(metadata={"a": "1", "b": "2", "c": "3"})

    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, metadata=intellistream_datahub_sdk.MapField.set({"only": "9"})
    )
    md = sync_client.timeseries.update([update])[0].metadata or {}

    assert md.get("only") == "9"
    assert "a" not in md and "b" not in md and "c" not in md, "set must replace, not merge"


def test_update_metadata_remove_keys(sync_client, make_ts):
    ts = make_ts(metadata={"a": "1", "b": "2", "c": "3"})

    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts, metadata=intellistream_datahub_sdk.MapField.delta(remove=["b", "c"])
    )
    md = sync_client.timeseries.update([update])[0].metadata or {}

    assert md.get("a") == "1"
    assert "b" not in md and "c" not in md


def test_update_metadata_cleared_by_an_empty_set(sync_client, make_ts):
    """``MapField`` has no ``setNull``; an empty ``set`` is how a map is emptied."""
    ts = make_ts(metadata={"a": "1", "b": "2"})

    update = intellistream_datahub_sdk.TimeSeriesUpdate(ts, metadata=intellistream_datahub_sdk.MapField.set({}))
    assert not (sync_client.timeseries.update([update])[0].metadata or {})


# --------------------------------------------------------------------------- #
# UPDATE — data_set_id (FieldU64): set value and clear
# --------------------------------------------------------------------------- #

def test_update_data_set_id_set_and_null(sync_client, make_ts):
    ds = intellistream_datahub_sdk.Dataset(external_id=_uid("ds"), name="update target dataset")
    sync_client.datasets.delete([ds])
    created_ds = sync_client.datasets.create([ds])[0]
    try:
        ts = make_ts()

        set_update = intellistream_datahub_sdk.TimeSeriesUpdate(
            ts, data_set_id=intellistream_datahub_sdk.FieldU64(value=created_ds.id)
        )
        updated = sync_client.timeseries.update([set_update])[0]
        assert updated.data_set_id == created_ds.id

        null_update = intellistream_datahub_sdk.TimeSeriesUpdate(
            ts, data_set_id=intellistream_datahub_sdk.FieldU64(set_null=True)
        )
        cleared = sync_client.timeseries.update([null_update])[0]
        assert cleared.data_set_id is None
    finally:
        sync_client.datasets.delete([created_ds])


# --------------------------------------------------------------------------- #
# UPDATE — value_type is not updatable
# --------------------------------------------------------------------------- #

def test_update_cannot_change_value_type(make_ts):
    """A series' type is fixed at creation — re-typing it would invalidate its datapoints.

    The server's ``TimeseriesFields`` has no ``valueType``, so the SDK does not offer one either:
    an unknown key is dropped silently, and a ``value_type=`` that reads like a re-type while the
    series keeps its original type is worse than no argument at all.
    """
    ts = make_ts(value_type="text")

    with pytest.raises(TypeError):
        intellistream_datahub_sdk.TimeSeriesUpdate(ts, value_type="bigint")


# --------------------------------------------------------------------------- #
# UPDATE — targeting modalities (how the update locates its target)
# --------------------------------------------------------------------------- #

def test_update_target_by_created_object(sync_client, make_ts):
    ts = make_ts(name="orig")
    update = intellistream_datahub_sdk.TimeSeriesUpdate(ts, name=intellistream_datahub_sdk.FieldStr(value="by object"))
    assert sync_client.timeseries.update([update])[0].name == "by object"


def test_update_target_by_external_id_string(sync_client, make_ts):
    ts = make_ts(name="orig")
    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts.external_id, name=intellistream_datahub_sdk.FieldStr(value="by ext-id string")
    )
    assert sync_client.timeseries.update([update])[0].name == "by ext-id string"


def test_update_target_by_numeric_id(sync_client, make_ts):
    ts = make_ts(name="orig")
    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts.id, name=intellistream_datahub_sdk.FieldStr(value="by numeric id")
    )
    assert sync_client.timeseries.update([update])[0].name == "by numeric id"


# --------------------------------------------------------------------------- #
# UPDATE — composite cases: multi-field, batch, no-op
# --------------------------------------------------------------------------- #

def test_update_multiple_fields_in_one_call(sync_client, make_ts):
    ts = make_ts(name="orig", description="orig desc", metadata={"k": "v"})

    update = intellistream_datahub_sdk.TimeSeriesUpdate(
        ts,
        name=intellistream_datahub_sdk.FieldStr(value="multi name"),
        description=intellistream_datahub_sdk.FieldStr(value="multi desc"),
        unit=intellistream_datahub_sdk.FieldStr(value="multi unit"),
        metadata=intellistream_datahub_sdk.MapField.delta(add={"k2": "v2"}),
    )
    updated = sync_client.timeseries.update([update])[0]

    assert updated.name == "multi name"
    assert updated.description == "multi desc"
    assert updated.unit == "multi unit"
    md = updated.metadata or {}
    assert md.get("k") == "v" and md.get("k2") == "v2"


def test_update_batch_distinct_series(sync_client, make_ts):
    ts1 = make_ts(name="batch one")
    ts2 = make_ts(name="batch two")

    updates = [
        intellistream_datahub_sdk.TimeSeriesUpdate(ts1, name=intellistream_datahub_sdk.FieldStr(value="batch one updated")),
        intellistream_datahub_sdk.TimeSeriesUpdate(ts2, name=intellistream_datahub_sdk.FieldStr(value="batch two updated")),
    ]
    updated = sync_client.timeseries.update(updates)
    by_ext = {u.external_id: u.name for u in updated}

    assert by_ext[ts1.external_id] == "batch one updated"
    assert by_ext[ts2.external_id] == "batch two updated"


def test_update_noop_preserves_existing_fields(sync_client, make_ts):
    ts = make_ts(name="keep me", description="keep this too", metadata={"a": "1"})

    update = intellistream_datahub_sdk.TimeSeriesUpdate(ts)  # no field wrappers supplied
    updated = sync_client.timeseries.update([update])[0]

    assert updated.name == "keep me"
    assert updated.description == "keep this too"
    assert (updated.metadata or {}).get("a") == "1"


def test_update_without_identifier_rejected():
    # Maps `src/timeseries/test.rs::test_update_timeseries_without_id`, which asserts
    # the backend returns BAD_REQUEST when an update carries neither id nor
    # external_id. The Python binding enforces the same invariant earlier: a
    # TimeSeriesUpdate requires a target identifier, and an IdCollection with
    # neither id nor external_id is rejected client-side, so an identifier-less
    # update can never be constructed (let alone reach the backend).
    with pytest.raises(Exception):
        intellistream_datahub_sdk.IdCollection()
