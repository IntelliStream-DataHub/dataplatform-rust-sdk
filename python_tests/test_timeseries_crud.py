"""Comprehensive create / delete / update coverage for the TimeSeries service.

Update is the only mutating "patch" API the SDK exposes (events/datasets/resources
only support create + delete), so the bulk of this file exercises *every* update
modality the field-wrapper types allow:

  * FieldStr / FieldU64 scalar fields  -> set-value  and  set_null
  * MapField  (metadata)               -> add (merge), set (replace), remove (by key)
  * ListFieldU64 (security_categories) -> add, set (replace), remove (by value)
  * targeting an update by created-object / external-id string / numeric id
  * multi-field updates, batch updates, and no-op updates

These are integration tests; they hit the live backend configured in ``.env`` and
clean up after themselves via the ``make_ts`` fixture.
"""

import uuid

import pytest

import datahub_sdk
from python_tests.fixtures import *  # noqa: F401,F403  (sync_client fixture)


def _uid(prefix="crud"):
    return f"pytest_{prefix}_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def make_ts(sync_client):
    """Factory that creates timeseries and deletes them at teardown.

    Defaults give a valid, minimally-populated float series; pass keyword
    overrides for any TimeSeries constructor argument.
    """
    created = []

    def _make(**kwargs):
        kwargs.setdefault("external_id", _uid("ts"))
        kwargs.setdefault("value_type", "float")
        kwargs.setdefault("unit", "a.u")
        ts = datahub_sdk.TimeSeries(**kwargs)
        # ensure a clean slate in case a previous failed run leaked the ext id
        sync_client.timeseries.delete([ts])
        result = sync_client.timeseries.create([ts])[0]
        created.append(result)
        return result

    yield _make

    for ts in created:
        try:
            sync_client.timeseries.delete([ts])
        except Exception:
            pass


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
    ],
    ids=[
        "float", "bigint", "text", "with-metadata", "empty-metadata",
        "empty-metadata-value", "with-description", "with-units", "unicode-name",
    ],
)
def test_create_delete_roundtrip(sync_client, kwargs):
    ext_id = _uid("crd")
    # unit is required by the backend (timeseries.unit.not.blank); default it
    # unless a case overrides it.
    kwargs = {"unit": "a.u", **kwargs}
    ts = datahub_sdk.TimeSeries(external_id=ext_id, **kwargs)
    sync_client.timeseries.delete([ts])

    created = sync_client.timeseries.create([ts])
    assert len(created) == 1
    assert created[0].external_id == ext_id
    assert created[0].id is not None, "server should assign a numeric id"

    fetched = sync_client.timeseries.by_ids([ext_id])
    assert any(t.external_id == ext_id for t in fetched)

    sync_client.timeseries.delete([created[0]])

    listed = sync_client.timeseries.list()
    assert all(t.external_id != ext_id for t in listed), "series should be gone after delete"


def test_create_batch_multiple(sync_client):
    ext_ids = [_uid("batch") for _ in range(3)]
    series = [
        datahub_sdk.TimeSeries(external_id=e, value_type="float", unit="a.u")
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
    ts = datahub_sdk.TimeSeries(external_id=ext_id, value_type=alias, unit="a.u")
    sync_client.timeseries.delete([ts])
    try:
        created = sync_client.timeseries.create([ts])[0]
        assert created.value_type == "float"
    finally:
        sync_client.timeseries.delete([ts])


def test_delete_by_external_id_string(sync_client):
    ext_id = _uid("delstr")
    ts = datahub_sdk.TimeSeries(external_id=ext_id, value_type="float", unit="a.u")
    sync_client.timeseries.delete([ts])
    sync_client.timeseries.create([ts])
    # delete by raw external-id string rather than the entity object
    sync_client.timeseries.delete([ext_id])
    listed = sync_client.timeseries.list()
    assert all(t.external_id != ext_id for t in listed)


# --------------------------------------------------------------------------- #
# UPDATE — scalar string fields: set value
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, new_value, attr",
    [
        ("name", "Updated Name", "name"),
        ("unit", "Updated Unit", "unit"),
        ("description", "Updated Description", "description"),
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
    )

    update = datahub_sdk.TimeSeriesUpdate(
        ts, **{field: datahub_sdk.FieldStr(value=new_value)}
    )
    updated = sync_client.timeseries.update([update])[0]
    assert getattr(updated, attr) == new_value

    # confirm it persisted, not just echoed
    assert getattr(_refetch(sync_client, ts), attr) == new_value


def test_update_change_external_id(sync_client, make_ts):
    ts = make_ts(name="rename target")
    new_ext = _uid("renamed")

    update = datahub_sdk.TimeSeriesUpdate(
        ts, external_id=datahub_sdk.FieldStr(value=new_ext)
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
    ],
)
def test_update_scalar_str_set_null(sync_client, make_ts, field, attr):
    ts = make_ts(
        description="please clear me",
        unit="a.u",
        unit_external_id="clear.this.ext",
    )

    update = datahub_sdk.TimeSeriesUpdate(
        ts, **{field: datahub_sdk.FieldStr(set_null=True)}
    )
    updated = sync_client.timeseries.update([update])[0]
    assert getattr(updated, attr) is None
    assert getattr(_refetch(sync_client, ts), attr) is None


def test_update_set_value_then_set_null(sync_client, make_ts):
    """A field can be set and then cleared across two updates."""
    ts = make_ts(description="first")

    set_update = datahub_sdk.TimeSeriesUpdate(
        ts, description=datahub_sdk.FieldStr(value="second")
    )
    assert sync_client.timeseries.update([set_update])[0].description == "second"

    null_update = datahub_sdk.TimeSeriesUpdate(
        ts, description=datahub_sdk.FieldStr(set_null=True)
    )
    assert sync_client.timeseries.update([null_update])[0].description is None


# --------------------------------------------------------------------------- #
# UPDATE — MapField (metadata): add / set / remove
# --------------------------------------------------------------------------- #

def test_update_metadata_add_merges_and_overwrites(sync_client, make_ts):
    ts = make_ts(metadata={"keep": "1", "overwrite": "old"})

    update = datahub_sdk.TimeSeriesUpdate(
        ts, metadata=datahub_sdk.MapField(add={"overwrite": "new", "added": "2"})
    )
    md = sync_client.timeseries.update([update])[0].metadata or {}

    assert md.get("keep") == "1", "untouched keys must be preserved by add"
    assert md.get("overwrite") == "new", "existing key value must be overwritten"
    assert md.get("added") == "2", "new key must be added"


def test_update_metadata_set_replaces_whole_map(sync_client, make_ts):
    ts = make_ts(metadata={"a": "1", "b": "2", "c": "3"})

    update = datahub_sdk.TimeSeriesUpdate(
        ts, metadata=datahub_sdk.MapField(set={"only": "9"})
    )
    md = sync_client.timeseries.update([update])[0].metadata or {}

    assert md.get("only") == "9"
    assert "a" not in md and "b" not in md and "c" not in md, "set must replace, not merge"


def test_update_metadata_remove_keys(sync_client, make_ts):
    ts = make_ts(metadata={"a": "1", "b": "2", "c": "3"})

    update = datahub_sdk.TimeSeriesUpdate(
        ts, metadata=datahub_sdk.MapField(remove=["b", "c"])
    )
    md = sync_client.timeseries.update([update])[0].metadata or {}

    assert md.get("a") == "1"
    assert "b" not in md and "c" not in md


# --------------------------------------------------------------------------- #
# UPDATE — ListFieldU64 (security_categories): add / set / remove
#
# These exercise the three ListFieldU64 serialisation paths (set/add/remove).
# They are xfail because the backend silently drops arbitrary security-category
# ids — verified by creating a series with security_categories=[1, 2] and getting
# back securityCategories=[]. Real categories aren't creatable through this SDK,
# so persistence can't be asserted; strict=False surfaces an xpass if the backend
# starts honouring them.
# --------------------------------------------------------------------------- #

_SEC_CAT_XFAIL = pytest.mark.xfail(
    reason="backend does not persist arbitrary security-category ids "
    "(needs pre-existing categories not creatable via this SDK)",
    strict=False,
)


@_SEC_CAT_XFAIL
def test_update_security_categories_set(sync_client, make_ts):
    ts = make_ts(security_categories=[1, 2])

    update = datahub_sdk.TimeSeriesUpdate(
        ts, security_categories=datahub_sdk.ListFieldU64(set=[3, 4])
    )
    updated = sync_client.timeseries.update([update])[0]
    assert sorted(updated.security_categories or []) == [3, 4]


@_SEC_CAT_XFAIL
def test_update_security_categories_add(sync_client, make_ts):
    ts = make_ts(security_categories=[1, 2])

    update = datahub_sdk.TimeSeriesUpdate(
        ts, security_categories=datahub_sdk.ListFieldU64(add=[3])
    )
    updated = sync_client.timeseries.update([update])[0]
    assert set(updated.security_categories or []) >= {1, 2, 3}


@_SEC_CAT_XFAIL
def test_update_security_categories_remove(sync_client, make_ts):
    ts = make_ts(security_categories=[1, 2, 3])

    update = datahub_sdk.TimeSeriesUpdate(
        ts, security_categories=datahub_sdk.ListFieldU64(remove=[2])
    )
    updated = sync_client.timeseries.update([update])[0]
    cats = set(updated.security_categories or [])
    assert 2 not in cats
    assert {1, 3} <= cats


# --------------------------------------------------------------------------- #
# UPDATE — data_set_id (FieldU64): set value and clear
# --------------------------------------------------------------------------- #

def test_update_data_set_id_set_and_null(sync_client, make_ts):
    ds = datahub_sdk.Dataset(external_id=_uid("ds"), name="update target dataset")
    sync_client.datasets.delete([ds])
    created_ds = sync_client.datasets.create([ds])[0]
    try:
        ts = make_ts()

        set_update = datahub_sdk.TimeSeriesUpdate(
            ts, data_set_id=datahub_sdk.FieldU64(value=created_ds.id)
        )
        updated = sync_client.timeseries.update([set_update])[0]
        assert updated.data_set_id == created_ds.id

        null_update = datahub_sdk.TimeSeriesUpdate(
            ts, data_set_id=datahub_sdk.FieldU64(set_null=True)
        )
        cleared = sync_client.timeseries.update([null_update])[0]
        assert cleared.data_set_id is None
    finally:
        sync_client.datasets.delete([created_ds])


# NOTE: value_type is intentionally NOT an updatable field — the backend timeseries
# update form (`TimeseriesFields`) has no `valueType`, so it has been dropped from
# `TimeSeriesUpdate` in the SDK. (Re-typing a series is unsupported.)


# --------------------------------------------------------------------------- #
# UPDATE — targeting modalities (how the update locates its target)
# --------------------------------------------------------------------------- #

def test_update_target_by_created_object(sync_client, make_ts):
    ts = make_ts(name="orig")
    update = datahub_sdk.TimeSeriesUpdate(ts, name=datahub_sdk.FieldStr(value="by object"))
    assert sync_client.timeseries.update([update])[0].name == "by object"


def test_update_target_by_external_id_string(sync_client, make_ts):
    ts = make_ts(name="orig")
    update = datahub_sdk.TimeSeriesUpdate(
        ts.external_id, name=datahub_sdk.FieldStr(value="by ext-id string")
    )
    assert sync_client.timeseries.update([update])[0].name == "by ext-id string"


def test_update_target_by_numeric_id(sync_client, make_ts):
    ts = make_ts(name="orig")
    update = datahub_sdk.TimeSeriesUpdate(
        ts.id, name=datahub_sdk.FieldStr(value="by numeric id")
    )
    assert sync_client.timeseries.update([update])[0].name == "by numeric id"


# --------------------------------------------------------------------------- #
# UPDATE — composite cases: multi-field, batch, no-op
# --------------------------------------------------------------------------- #

def test_update_multiple_fields_in_one_call(sync_client, make_ts):
    ts = make_ts(name="orig", description="orig desc", metadata={"k": "v"})

    update = datahub_sdk.TimeSeriesUpdate(
        ts,
        name=datahub_sdk.FieldStr(value="multi name"),
        description=datahub_sdk.FieldStr(value="multi desc"),
        unit=datahub_sdk.FieldStr(value="multi unit"),
        metadata=datahub_sdk.MapField(add={"k2": "v2"}),
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
        datahub_sdk.TimeSeriesUpdate(ts1, name=datahub_sdk.FieldStr(value="batch one updated")),
        datahub_sdk.TimeSeriesUpdate(ts2, name=datahub_sdk.FieldStr(value="batch two updated")),
    ]
    updated = sync_client.timeseries.update(updates)
    by_ext = {u.external_id: u.name for u in updated}

    assert by_ext[ts1.external_id] == "batch one updated"
    assert by_ext[ts2.external_id] == "batch two updated"


def test_update_noop_preserves_existing_fields(sync_client, make_ts):
    ts = make_ts(name="keep me", description="keep this too", metadata={"a": "1"})

    update = datahub_sdk.TimeSeriesUpdate(ts)  # no field wrappers supplied
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
        datahub_sdk.IdCollection()
