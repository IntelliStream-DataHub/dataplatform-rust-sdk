"""Field-by-field coverage of ``POST /resources/update``.

``labels`` lives in ``test_resource_label_updates.py`` — it carries the type-label rules and is
covered there. This file takes the other six fields of ``ResourceUpdate``: a set-a-value test and
a clear-it test each, plus the add/remove delta paths for ``metadata``.

``externalId`` and ``name`` have no ``setNull`` branch server-side (the update service only reads
``.getSet()`` for them), so clearing either is silently a no-op. Those two are marked
``xfail(strict=False)``, and flip to xpass if the server grows the branch.
"""
import pytest

import datahub_sdk
from fixtures import make_dataset, make_resource, sync_client, unique_id
from polling import poll_until

_NO_SETNULL_BRANCH = pytest.mark.xfail(
    reason="server has no setNull branch for this field; the request is accepted and ignored",
    strict=False,
)


def _apply(sync_client, update):
    """Send one update and return the echoed node, riding out post-create lag."""
    last_error = []

    def attempt():
        try:
            return sync_client.resources.update([update]).nodes
        except Exception as exc:  # noqa: BLE001 — re-raised below if it never succeeds
            last_error.append(exc)
            return []

    nodes = poll_until(attempt, bool, timeout=15.0)
    if not nodes and last_error:
        raise last_error[-1]
    assert nodes, "resources.update returned no node"
    return nodes[0]


@pytest.fixture
def new_resource(sync_client, make_resource):
    """Factory for a freshly-created resource, deleted at teardown."""
    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("res_upd"))
        kwargs.setdefault("name", "SDK update probe")
        kwargs.setdefault("is_root", True)
        kwargs.setdefault("labels", ["ASSET"])
        make_resource([datahub_sdk.Resource(**kwargs)])
        return kwargs["external_id"]

    return _make


# --------------------------------------------------------------------------- #
# Scalar string fields — set a value
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, value, attr",
    [
        ("name", "Updated resource name", "name"),
        ("description", "updated description", "description"),
        ("source", "sdk_updated_source", "source"),
    ],
)
def test_scalar_set_value(sync_client, new_resource, field, value, attr):
    ext = new_resource(description="original description", source="original_source")

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, **{field: datahub_sdk.FieldStr(value=value)}
    ))
    assert getattr(updated, attr) == value


def test_external_id_set_value(sync_client, new_resource):
    ext = new_resource()
    new_ext = unique_id("res_renamed")

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, external_id=datahub_sdk.FieldStr(value=new_ext)
    ))
    assert updated.external_id == new_ext

    found = poll_until(
        lambda: sync_client.resources.by_ids([new_ext]),
        lambda nodes: any(n.external_id == new_ext for n in nodes),
    )
    assert any(n.external_id == new_ext for n in found)
    sync_client.resources.delete([new_ext])


def test_name_below_the_minimum_length_is_rejected(sync_client, new_resource):
    """Names are validated on update, not just on create: 3–512 characters."""
    ext = new_resource()

    with pytest.raises(datahub_sdk.DataHubException):
        sync_client.resources.update([datahub_sdk.ResourceUpdate(
            ext, name=datahub_sdk.FieldStr(value="ab")
        )])


# --------------------------------------------------------------------------- #
# Scalar string fields — set_null
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, attr",
    [
        ("description", "description"),
        ("source", "source"),
    ],
)
def test_scalar_set_null(sync_client, new_resource, field, attr):
    ext = new_resource(description="please clear me", source="please_clear_me")

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, **{field: datahub_sdk.FieldStr(set_null=True)}
    ))
    assert getattr(updated, attr) is None


@_NO_SETNULL_BRANCH
def test_name_set_null(sync_client, new_resource):
    ext = new_resource(name="Clear my name")

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, name=datahub_sdk.FieldStr(set_null=True)
    ))
    assert not updated.name


@_NO_SETNULL_BRANCH
def test_external_id_set_null(sync_client, new_resource):
    ext = new_resource()

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, external_id=datahub_sdk.FieldStr(set_null=True)
    ))
    assert not updated.external_id


# --------------------------------------------------------------------------- #
# data_set_id
# --------------------------------------------------------------------------- #

def test_data_set_id_set_and_null(sync_client, new_resource, make_dataset):
    dataset = make_dataset(name=unique_id("res_upd_ds"))
    ext = new_resource()

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, data_set_id=datahub_sdk.FieldU64(value=dataset.id)
    ))
    assert updated.data_set_id == dataset.id

    cleared = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, data_set_id=datahub_sdk.FieldU64(set_null=True)
    ))
    assert cleared.data_set_id is None


def test_data_set_id_unknown_is_rejected(sync_client, new_resource):
    ext = new_resource()

    with pytest.raises(datahub_sdk.DataHubException):
        sync_client.resources.update([datahub_sdk.ResourceUpdate(
            ext, data_set_id=datahub_sdk.FieldU64(value=2**62)
        )])


# --------------------------------------------------------------------------- #
# metadata (MapField): set / add / remove
# --------------------------------------------------------------------------- #

def test_metadata_set_replaces_whole_map(sync_client, new_resource):
    ext = new_resource(metadata={"a": "1", "b": "2"})

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, metadata=datahub_sdk.MapField.set({"only": "9"})
    ))
    md = updated.metadata or {}
    assert md.get("only") == "9"
    assert "a" not in md and "b" not in md


def test_metadata_add_merges_and_overwrites(sync_client, new_resource):
    ext = new_resource(metadata={"keep": "1", "overwrite": "old"})

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, metadata=datahub_sdk.MapField.delta(add={"overwrite": "new", "added": "2"})
    ))
    md = updated.metadata or {}
    assert md.get("keep") == "1"
    assert md.get("overwrite") == "new"
    assert md.get("added") == "2"


def test_metadata_remove_keys(sync_client, new_resource):
    ext = new_resource(metadata={"a": "1", "b": "2", "c": "3"})

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, metadata=datahub_sdk.MapField.delta(remove=["b", "c"])
    ))
    md = updated.metadata or {}
    assert md.get("a") == "1"
    assert "b" not in md and "c" not in md


def test_metadata_cleared_by_an_empty_set(sync_client, new_resource):
    """``MapField`` has no ``setNull``; an empty ``set`` is how a map is emptied."""
    ext = new_resource(metadata={"a": "1", "b": "2"})

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, metadata=datahub_sdk.MapField.set({})
    ))
    assert not (updated.metadata or {})


# --------------------------------------------------------------------------- #
# geolocation (FieldGeoJson) — the same set/set_null pair, carrying a GeoJSON geometry
# --------------------------------------------------------------------------- #

_POINT = {"type": "Point", "coordinates": [10.75, 59.91]}
_MOVED = {"type": "Point", "coordinates": [5.32, 60.39]}


def test_geolocation_set_value(sync_client, new_resource):
    ext = new_resource(geolocation=_POINT)

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, geolocation=datahub_sdk.FieldGeoJson(value=_MOVED)
    ))

    assert updated.geolocation["type"] == "Point"
    lon, lat = updated.geolocation["coordinates"]
    assert abs(lon - 5.32) < 1e-9 and abs(lat - 60.39) < 1e-9


def test_geolocation_set_null(sync_client, new_resource):
    ext = new_resource(geolocation=_POINT)

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, geolocation=datahub_sdk.FieldGeoJson(set_null=True)
    ))
    assert updated.geolocation is None


def test_geolocation_can_be_added_to_a_resource_that_had_none(sync_client, new_resource):
    ext = new_resource()

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, geolocation=datahub_sdk.FieldGeoJson(value=_POINT)
    ))
    assert updated.geolocation["coordinates"] == [10.75, 59.91]


def test_geolocation_accepts_any_geojson_geometry(sync_client, new_resource):
    ext = new_resource()
    polygon = {
        "type": "Polygon",
        "coordinates": [[[10.0, 59.0], [11.0, 59.0], [11.0, 60.0], [10.0, 59.0]]],
    }

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, geolocation=datahub_sdk.FieldGeoJson(value=polygon)
    ))
    assert updated.geolocation["type"] == "Polygon"


def test_geolocation_rejects_non_geojson():
    """The dict is parsed into a geometry client-side, so a bad one fails before any request."""
    with pytest.raises(ValueError):
        datahub_sdk.FieldGeoJson(value={"lat": 59.91, "lon": 10.75})


# --------------------------------------------------------------------------- #
# Composite: multi-field, persistence, no-op, targeting
# --------------------------------------------------------------------------- #

def test_multiple_fields_in_one_update(sync_client, new_resource):
    ext = new_resource(description="orig", metadata={"k": "v"})

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext,
        name=datahub_sdk.FieldStr(value="Multi name"),
        description=datahub_sdk.FieldStr(value="multi desc"),
        source=datahub_sdk.FieldStr(value="multi_source"),
        metadata=datahub_sdk.MapField.delta(add={"k2": "v2"}),
    ))

    assert updated.name == "Multi name"
    assert updated.description == "multi desc"
    assert updated.source == "multi_source"
    md = updated.metadata or {}
    assert md.get("k") == "v" and md.get("k2") == "v2"


def test_update_persists_beyond_the_echo(sync_client, new_resource):
    ext = new_resource(description="before")

    _apply(sync_client, datahub_sdk.ResourceUpdate(
        ext, description=datahub_sdk.FieldStr(value="after")
    ))

    stored = poll_until(
        lambda: sync_client.resources.by_ids([ext]),
        lambda nodes: any(n.description == "after" for n in nodes),
    )
    assert any(n.description == "after" for n in stored)


def test_noop_update_preserves_existing_fields(sync_client, new_resource):
    ext = new_resource(description="keep this too", metadata={"a": "1"})

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(ext))

    assert updated.name == "SDK update probe"
    assert updated.description == "keep this too"
    assert (updated.metadata or {}).get("a") == "1"


def test_update_targeting_by_numeric_id(sync_client, new_resource):
    ext = new_resource()
    node_id = poll_until(lambda: sync_client.resources.by_ids([ext]), bool)[0].id

    updated = _apply(sync_client, datahub_sdk.ResourceUpdate(
        node_id, description=datahub_sdk.FieldStr(value="by numeric id")
    ))
    assert updated.description == "by numeric id"
