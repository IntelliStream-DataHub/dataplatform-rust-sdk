"""Tests for the label CRUD service (`client.labels`).

Mirrors `src/labels/test.rs`. Label names must be 3-512 chars and are canonicalised to
SNAKE_UPPER_CASE server-side. Live cases create/list/update/delete labels and check that
deleting a label still used by a resource is rejected with 400.
"""
import time

import pytest
from intellistream_datahub_sdk import DataHubException, Label, Resource

from fixtures import TEST_LABEL, async_client, make_resource, sync_client, unique_id

WRITE_SETTLE = 3.0


def _label_by_name(client, name: str):
    """The label whose (canonical, upper-case) name matches, or None."""
    canon = name.upper()
    return next((l for l in client.labels.list() if l.name == canon), None)


# --------------------------------------------------------------------------- #
# Entity construction — no backend needed.
# --------------------------------------------------------------------------- #

def test_label_entity_fields():
    lbl = Label(name="pump_station", color="#123456", description="d")
    assert lbl.name == "pump_station"
    assert lbl.color == "#123456"
    assert lbl.description == "d"
    assert lbl.id is None
    lbl.id = 7
    assert lbl.id == 7


# --------------------------------------------------------------------------- #
# Live: full lifecycle.
# --------------------------------------------------------------------------- #

def test_label_lifecycle(sync_client):
    name = unique_id("lbl").upper()  # labels are stored upper-cased
    labels = sync_client.labels

    # pre-clean a leftover from an interrupted run (delete by id)
    existing = _label_by_name(sync_client, name)
    if existing is not None:
        labels.delete([existing.id])

    try:
        # create
        created = labels.create([Label(name=name, description="sdk test", color="#123456")])
        assert len(created) == 1
        lbl = created[0]
        assert lbl.name == name
        assert lbl.color == "#123456"
        assert lbl.id is not None
        label_id = lbl.id

        # get
        fetched = labels.get(label_id)
        assert fetched is not None and fetched.id == label_id

        # list contains it
        assert any(l.name == name for l in labels.list())

        # update description; color must be untouched (PATCH)
        updated = labels.update([Label(id=label_id, description="updated")])
        assert updated[0].description == "updated"
        assert updated[0].color == "#123456"

        # delete -> gone
        labels.delete([label_id])
        assert labels.get(label_id) is None
    finally:
        leftover = _label_by_name(sync_client, name)
        if leftover is not None:
            labels.delete([leftover.id])


# --------------------------------------------------------------------------- #
# Live: update, field by field.
#
# `labels.update` takes a whole `Label` rather than field wrappers, so it is a PATCH by omission:
# a field left `None` is not sent and stays as it was. There is no `setNull` and therefore no way
# to clear a label's description or colour back to null.
# --------------------------------------------------------------------------- #

@pytest.fixture
def new_label(sync_client):
    """A freshly-created label, deleted at teardown."""
    created = sync_client.labels.create([Label(
        name=unique_id("lblupd").upper(), description="original", color="#123456"
    )])[0]
    yield created
    try:
        sync_client.labels.delete([created.id])
    except Exception:
        pass


@pytest.mark.parametrize(
    "field, value, expected",
    [
        ("description", "updated description", "updated description"),
        ("color", "#abcdef", "#abcdef"),
        # i18n codes are lower-cased server-side, like names are upper-cased.
        ("i18n_code", "nb_NO", "nb_no"),
    ],
)
def test_update_sets_a_field(sync_client, new_label, field, value, expected):
    updated = sync_client.labels.update([Label(id=new_label.id, **{field: value})])[0]
    assert getattr(updated, field) == expected


def test_update_renames_a_label(sync_client, new_label):
    new_name = unique_id("lblrenamed").upper()

    updated = sync_client.labels.update([Label(id=new_label.id, name=new_name)])[0]
    assert updated.name == new_name


@pytest.mark.parametrize("field", ["description", "color", "i18n_code"])
def test_update_cannot_clear_a_field(sync_client, new_label, field):
    """Passing ``None`` omits the field from the request, so the stored value survives."""
    sync_client.labels.update([Label(id=new_label.id, i18n_code="nb_NO")])

    updated = sync_client.labels.update([Label(id=new_label.id, **{field: None})])[0]
    assert getattr(updated, field) is not None


def test_update_leaves_omitted_fields_unchanged(sync_client, new_label):
    updated = sync_client.labels.update([Label(id=new_label.id, description="only this")])[0]

    assert updated.description == "only this"
    assert updated.color == "#123456"
    assert updated.name == new_label.name


def test_duplicate_name_conflicts(sync_client):
    name = unique_id("lbldup").upper()
    labels = sync_client.labels
    created = labels.create([Label(name=name)])
    label_id = created[0].id
    try:
        with pytest.raises(DataHubException) as exc:
            labels.create([Label(name=name)])
        assert exc.value.status_code == 409
    finally:
        labels.delete([label_id])


def test_delete_label_in_use_reports_blocker(sync_client, make_resource):
    # The shared label, not a fresh one: this test never deletes the definition, so a unique name
    # would only add a row that stays undeletable for as long as its resource lives.
    label_name = TEST_LABEL
    res_ext = unique_id("lblres")
    labels = sync_client.labels

    # A resource carrying the label pins it — and creates it on the way in if this is the first
    # time anything used the name, which is why the shared label never needs seeding.
    make_resource([Resource(external_id=res_ext, name="Py in-use", is_root=True,
                            labels=[label_name])])
    time.sleep(WRITE_SETTLE)

    lbl = _label_by_name(sync_client, label_name)
    assert lbl is not None, f"a resource carrying {label_name} should leave the label existing"

    with pytest.raises(DataHubException) as exc:
        labels.delete([lbl.id])
    assert exc.value.status_code == 400
    assert "still being used" in exc.value.message
    # No teardown for the label: it is the shared one and outlives the run. `make_resource` drops
    # the resource, which is the only thing this test brought into being.


@pytest.mark.asyncio
async def test_labels_async(async_client, sync_client):
    name = unique_id("lblasync").upper()
    labels = async_client.labels

    created = await labels.create([Label(name=name, color="#0a0a0a")])
    label_id = created[0].id
    try:
        assert created[0].name == name
        fetched = await labels.get(label_id)
        assert fetched is not None and fetched.id == label_id
        all_labels = await labels.list()
        assert any(l.name == name for l in all_labels)
    finally:
        await labels.delete([label_id])
    assert await labels.get(label_id) is None
