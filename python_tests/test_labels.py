"""Tests for the label CRUD service (`client.labels`).

Mirrors `src/labels/test.rs`. Label names must be 3-512 chars and are canonicalised to
SNAKE_UPPER_CASE server-side. Live cases create/list/update/delete labels and check that
deleting a label still used by a resource is rejected with 400.
"""
import time

import pytest
from datahub_sdk import DataHubException, Label, Resource

from fixtures import async_client, make_resource, sync_client, unique_id

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
    label_name = unique_id("lblinuse").upper()
    res_ext = unique_id("lblres")
    labels = sync_client.labels

    # a resource carrying the label auto-creates it and pins it
    make_resource([Resource(external_id=res_ext, name="Py in-use", is_root=True,
                            labels=[label_name])])
    time.sleep(WRITE_SETTLE)

    lbl = _label_by_name(sync_client, label_name)
    assert lbl is not None, "resource create should have auto-created the label"

    try:
        with pytest.raises(DataHubException) as exc:
            labels.delete([lbl.id])
        assert exc.value.status_code == 400
        assert "still being used" in exc.value.message
    finally:
        # free the label (drop the resource) then delete it
        sync_client.resources.delete([res_ext])
        time.sleep(WRITE_SETTLE)
        labels.delete([lbl.id])


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
