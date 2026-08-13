"""Field-by-field coverage of ``POST /datasets/update``.

``DatasetUpdate`` carries five fields; each gets a set-a-value test and a clear-it test, plus the
add/remove delta paths for ``metadata`` and ``labels``. ``test_datasets.py`` keeps the
round-trip/no-op cases that came before this file.

The endpoint forwards to the resource update under the hood, and that forwarding is where the
gaps are: only ``description`` has its ``setNull`` passed through, so clearing ``name`` or
``externalId`` is silently a no-op. Labels go through the resource layer's type-label rule, so the
``DATASET`` label is forced back on every edit.

A dataset carries no ``labels`` field in its own response shape, so the label assertions re-read
the node through ``resources.by_ids``.

Updating a dataset needs an all-datasets write grant; without one every test here fails with 403.
"""
import pytest

import datahub_sdk
from fixtures import make_dataset, sync_client, unique_id
from polling import poll_until

_NO_SETNULL_PASSTHROUGH = pytest.mark.xfail(
    reason="DataSetService forwards only description's setNull to the resource layer",
    strict=False,
)


def _apply(sync_client, update):
    result = sync_client.datasets.update([update])
    assert result, "datasets.update returned no dataset"
    return result[0]


def _labels_of(sync_client, ext_id):
    nodes = poll_until(lambda: sync_client.resources.by_ids([ext_id]), bool)
    assert nodes, f"dataset {ext_id} is not readable as a node"
    return sorted(nodes[0].labels or [])


@pytest.fixture
def new_dataset(make_dataset):
    """Factory for a freshly-created dataset, deleted at teardown."""
    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("ds_upd"))
        kwargs.setdefault("name", "SDK dataset update probe")
        return make_dataset(**kwargs)

    return _make


# --------------------------------------------------------------------------- #
# Scalar string fields — set a value
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, value, attr",
    [
        ("name", "Updated dataset name", "name"),
        ("description", "updated description", "description"),
    ],
)
def test_scalar_set_value(sync_client, new_dataset, field, value, attr):
    dataset = new_dataset(description="original description")

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, **{field: datahub_sdk.FieldStr(value=value)}
    ))
    assert getattr(updated, attr) == value


def test_external_id_set_value(sync_client, new_dataset):
    dataset = new_dataset()
    new_ext = unique_id("ds_renamed")

    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, external_id=datahub_sdk.FieldStr(value=new_ext)
    ))

    found = poll_until(
        lambda: sync_client.datasets.filter(datahub_sdk.DatasetFilter(
            datahub_sdk.BasicDatasetFilter(external_id=[new_ext])
        )),
        bool,
    )
    assert [d.external_id for d in found] == [new_ext]
    sync_client.datasets.delete([new_ext])


def test_duplicate_external_id_is_rejected(sync_client, new_dataset):
    taken = new_dataset()
    other = new_dataset()

    with pytest.raises(datahub_sdk.DataHubException):
        sync_client.datasets.update([datahub_sdk.DatasetUpdate(
            other.external_id, external_id=datahub_sdk.FieldStr(value=taken.external_id)
        )])


def test_name_below_the_minimum_length_is_rejected(sync_client, new_dataset):
    dataset = new_dataset()

    with pytest.raises(datahub_sdk.DataHubException):
        sync_client.datasets.update([datahub_sdk.DatasetUpdate(
            dataset.external_id, name=datahub_sdk.FieldStr(value="ab")
        )])


# --------------------------------------------------------------------------- #
# Scalar string fields — set_null
# --------------------------------------------------------------------------- #

def test_description_set_null(sync_client, new_dataset):
    """The only field whose ``setNull`` the dataset service forwards to the resource layer."""
    dataset = new_dataset(description="please clear me")

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, description=datahub_sdk.FieldStr(set_null=True)
    ))
    assert updated.description is None


@_NO_SETNULL_PASSTHROUGH
def test_name_set_null(sync_client, new_dataset):
    dataset = new_dataset(name="Clear my name")

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, name=datahub_sdk.FieldStr(set_null=True)
    ))
    assert not updated.name


@_NO_SETNULL_PASSTHROUGH
def test_external_id_set_null(sync_client, new_dataset):
    dataset = new_dataset()

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, external_id=datahub_sdk.FieldStr(set_null=True)
    ))
    assert not updated.external_id


# --------------------------------------------------------------------------- #
# metadata (MapField): set / add / remove
# --------------------------------------------------------------------------- #

def test_metadata_set_replaces_whole_map(sync_client, new_dataset):
    dataset = new_dataset(metadata={"a": "1", "b": "2"})

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, metadata=datahub_sdk.MapField.set({"only": "9"})
    ))
    md = updated.metadata or {}
    assert md.get("only") == "9"
    assert "a" not in md and "b" not in md


def test_metadata_add_merges_and_overwrites(sync_client, new_dataset):
    dataset = new_dataset(metadata={"keep": "1", "overwrite": "old"})

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id,
        metadata=datahub_sdk.MapField.delta(add={"overwrite": "new", "added": "2"}),
    ))
    md = updated.metadata or {}
    assert md.get("keep") == "1"
    assert md.get("overwrite") == "new"
    assert md.get("added") == "2"


def test_metadata_remove_keys(sync_client, new_dataset):
    dataset = new_dataset(metadata={"a": "1", "b": "2", "c": "3"})

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, metadata=datahub_sdk.MapField.delta(remove=["b", "c"])
    ))
    md = updated.metadata or {}
    assert md.get("a") == "1"
    assert "b" not in md and "c" not in md


def test_metadata_cleared_by_an_empty_set(sync_client, new_dataset):
    """``MapField`` has no ``setNull``; an empty ``set`` is how a map is emptied."""
    dataset = new_dataset(metadata={"a": "1", "b": "2"})

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, metadata=datahub_sdk.MapField.set({})
    ))
    assert not (updated.metadata or {})


# --------------------------------------------------------------------------- #
# labels (ListFieldStr): set / add / remove — the DATASET type-label is immutable
# --------------------------------------------------------------------------- #

def test_labels_add(sync_client, new_dataset):
    dataset = new_dataset()

    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, labels=datahub_sdk.ListFieldStr.delta(add=["CURATED"])
    ))
    assert _labels_of(sync_client, dataset.external_id) == ["CURATED", "DATASET"]


def test_labels_remove(sync_client, new_dataset):
    dataset = new_dataset()

    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, labels=datahub_sdk.ListFieldStr.delta(add=["CURATED", "DRAFT"])
    ))
    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, labels=datahub_sdk.ListFieldStr.delta(remove=["DRAFT"])
    ))
    assert _labels_of(sync_client, dataset.external_id) == ["CURATED", "DATASET"]


def test_labels_set_replaces_and_keeps_the_type_label(sync_client, new_dataset):
    dataset = new_dataset()

    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, labels=datahub_sdk.ListFieldStr.delta(add=["OLD"])
    ))
    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, labels=datahub_sdk.ListFieldStr.set(["FRESH"])
    ))
    assert _labels_of(sync_client, dataset.external_id) == ["DATASET", "FRESH"]


def test_type_label_cannot_be_removed(sync_client, new_dataset):
    dataset = new_dataset()

    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, labels=datahub_sdk.ListFieldStr.delta(remove=["DATASET"])
    ))
    assert "DATASET" in _labels_of(sync_client, dataset.external_id)


# --------------------------------------------------------------------------- #
# Composite: multi-field, persistence, targeting
# --------------------------------------------------------------------------- #

def test_multiple_fields_in_one_update(sync_client, new_dataset):
    dataset = new_dataset(description="orig", metadata={"k": "v"})

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id,
        name=datahub_sdk.FieldStr(value="Multi name"),
        description=datahub_sdk.FieldStr(value="multi desc"),
        metadata=datahub_sdk.MapField.delta(add={"k2": "v2"}),
    ))

    assert updated.name == "Multi name"
    assert updated.description == "multi desc"
    md = updated.metadata or {}
    assert md.get("k") == "v" and md.get("k2") == "v2"


def test_update_persists_beyond_the_echo(sync_client, new_dataset):
    dataset = new_dataset(description="before")

    _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.external_id, description=datahub_sdk.FieldStr(value="after")
    ))

    stored = poll_until(
        lambda: sync_client.datasets.filter(datahub_sdk.DatasetFilter(
            datahub_sdk.BasicDatasetFilter(external_id=[dataset.external_id])
        )),
        lambda found: any(d.description == "after" for d in found),
    )
    assert any(d.description == "after" for d in stored)


def test_update_targeting_by_numeric_id(sync_client, new_dataset):
    dataset = new_dataset()

    updated = _apply(sync_client, datahub_sdk.DatasetUpdate(
        dataset.id, description=datahub_sdk.FieldStr(value="by numeric id")
    ))
    assert updated.description == "by numeric id"
