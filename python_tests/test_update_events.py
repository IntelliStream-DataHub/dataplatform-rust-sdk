"""Field-by-field coverage of ``POST /events/update``.

Every field of ``EventUpdate`` gets a set-a-value test and a clear-it test, plus the
add/remove delta paths for the two collection fields (``metadata``, ``related_resources``).

``eventTime`` is **not** an update field: an event's time is immutable after creation, so the
api has no such field and rejects a body naming one.

Three fields have **no ``setNull`` branch server-side** — ``externalId``, ``dataSetId``, and the
two collections — so a ``setNull`` there is silently a no-op rather than an error. The tests
below pin that as behaviour; ``xfail(strict=False)`` marks the ones where clearing is the
arguably-correct answer, so they flip to xpass if the server grows the branch.

The update response echoes the stored event, so assertions read it directly; a re-read is only
used where persistence itself is the point (the event projection lags a write).
"""
import uuid
from datetime import datetime, timezone

import pytest

import intellistream_datahub_sdk
from fixtures import make_dataset, make_resource, sync_client, unique_id
from polling import poll_until

# Clearing these is unrepresentable server-side: the update service reads `.getSet()` for them
# and never looks at `setNull`.
_NO_SETNULL_BRANCH = pytest.mark.xfail(
    reason="server has no setNull branch for this field; the request is accepted and ignored",
    strict=False,
)


def _apply(sync_client, update):
    """Send one update and return the echoed event.

    Under write lag the update can momentarily not find the event and echo nothing back; the
    update is idempotent, so retry until something comes back.
    """
    result = poll_until(lambda: sync_client.events.update([update]), bool)
    assert result, "events.update echoed no event back"
    return result[0]


@pytest.fixture
def new_event(sync_client):
    """Factory for a freshly-created event, deleted at teardown.

    A create is not immediately visible to the update path — an update sent too early finds
    nothing and echoes an empty list back rather than failing — so the factory does not hand the
    event over until a no-op update round-trips. Without that wait a test asserting a rejection
    would pass against an event that was never found.
    """
    created_ids = []

    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("evt_upd"))
        kwargs.setdefault("type", "sdk_update_probe")
        kwargs.setdefault("event_time", datetime.now(timezone.utc))
        event = sync_client.events.create([intellistream_datahub_sdk.Event(**kwargs)])[0]
        created_ids.append(event.id)
        _apply(sync_client, intellistream_datahub_sdk.EventUpdate(event))
        return event

    yield _make

    for event_id in created_ids:
        try:
            sync_client.events.delete([event_id])
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Scalar string fields — set a value
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, value, attr",
    [
        ("description", "updated description", "description"),
        ("type", "sdk_updated_type", "type"),
        ("sub_type", "sdk_updated_sub_type", "sub_type"),
        ("status", "acknowledged", "status"),
        ("source", "sdk_updated_source", "source"),
    ],
)
def test_scalar_set_value(sync_client, new_event, field, value, attr):
    event = new_event(
        description="original description",
        sub_type="original_sub_type",
        status="new",
        source="original_source",
    )

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, **{field: intellistream_datahub_sdk.FieldStr(value=value)}
    ))
    assert getattr(updated, attr) == value


# --------------------------------------------------------------------------- #
# Scalar string fields — set_null
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "field, attr",
    [
        ("description", "description"),
        ("sub_type", "sub_type"),
        ("status", "status"),
        ("source", "source"),
    ],
)
def test_scalar_set_null(sync_client, new_event, field, attr):
    event = new_event(
        description="please clear me",
        sub_type="please_clear_me",
        status="new",
        source="please_clear_me",
    )

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, **{field: intellistream_datahub_sdk.FieldStr(set_null=True)}
    ))
    assert getattr(updated, attr) is None


def test_type_set_null(sync_client, new_event):
    """``type`` is required on create but the update service honours ``setNull`` on it.

    The SDK models ``Event.type`` as a non-optional ``String``, so the response to a cleared type
    fails to deserialize (``missing field `type```) — clearing it is a one-way trip that leaves an
    event the client can no longer read.
    """
    event = new_event(type="sdk_clearable_type")

    with pytest.raises(intellistream_datahub_sdk.DataHubException):
        _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
            event, type=intellistream_datahub_sdk.FieldStr(set_null=True)
        ))


# --------------------------------------------------------------------------- #
# external_id
# --------------------------------------------------------------------------- #

def test_external_id_set_value(sync_client, new_event):
    event = new_event()
    new_ext = unique_id("evt_renamed")

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, external_id=intellistream_datahub_sdk.FieldStr(value=new_ext)
    ))
    assert updated.external_id == new_ext

    stored = poll_until(
        lambda: sync_client.events.get(event.id),
        lambda e: e is not None and e.external_id == new_ext,
    )
    assert stored is not None and stored.external_id == new_ext


@_NO_SETNULL_BRANCH
def test_external_id_set_null(sync_client, new_event):
    event = new_event()

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, external_id=intellistream_datahub_sdk.FieldStr(set_null=True)
    ))
    assert updated.external_id is None


# --------------------------------------------------------------------------- #
# event_time — not updatable at all
# --------------------------------------------------------------------------- #

def test_event_time_is_not_an_update_field():
    """An event's time is immutable after creation.

    The server's events table is partitioned by it, so the mutation cannot move the row and is
    refused outright; the api answers a body naming ``eventTime`` with a 400 rather than the
    false 200 it used to. The binding drops the keyword so the request is never built.
    """
    with pytest.raises(TypeError):
        intellistream_datahub_sdk.EventUpdate(
            uuid.uuid4(), event_time=intellistream_datahub_sdk.FieldStr(value="2026-03-04T05:06:07+00:00")
        )


def test_event_time_survives_an_update_of_other_fields(sync_client, new_event):
    when = datetime(2025, 1, 1, tzinfo=timezone.utc)
    event = new_event(event_time=when)

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, description=intellistream_datahub_sdk.FieldStr(value="untouched event time")
    ))
    assert updated.event_time.replace(microsecond=0) == when


# --------------------------------------------------------------------------- #
# data_set_id
# --------------------------------------------------------------------------- #

def test_data_set_id_set_value(sync_client, new_event, make_dataset):
    dataset = make_dataset(name=unique_id("evt_upd_ds"))
    event = new_event()

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, data_set_id=intellistream_datahub_sdk.FieldU64(value=dataset.id)
    ))
    assert updated.data_set_id == dataset.id


@_NO_SETNULL_BRANCH
def test_data_set_id_set_null(sync_client, new_event, make_dataset):
    dataset = make_dataset(name=unique_id("evt_upd_ds_null"))
    event = new_event(data_set_id=dataset.id)

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, data_set_id=intellistream_datahub_sdk.FieldU64(set_null=True)
    ))
    assert updated.data_set_id is None


@pytest.mark.xfail(
    reason="the event update only ACL-checks dataSetId, never looks it up — an unknown id is "
    "stored and the event ends up pointing at a data set that does not exist",
    strict=False,
)
def test_data_set_id_unknown_is_rejected(sync_client, new_event):
    """The resource update rejects this with 400; the event update accepts it."""
    event = new_event()

    with pytest.raises(intellistream_datahub_sdk.DataHubException):
        sync_client.events.update([intellistream_datahub_sdk.EventUpdate(
            event, data_set_id=intellistream_datahub_sdk.FieldU64(value=2**62)
        )])


# --------------------------------------------------------------------------- #
# metadata (MapField): set / add / remove
# --------------------------------------------------------------------------- #

def test_metadata_set_replaces_whole_map(sync_client, new_event):
    event = new_event(metadata={"a": "1", "b": "2"})

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, metadata=intellistream_datahub_sdk.MapField.set({"only": "9"})
    ))
    md = updated.metadata or {}
    assert md.get("only") == "9"
    assert "a" not in md and "b" not in md


def test_metadata_add_merges_and_overwrites(sync_client, new_event):
    event = new_event(metadata={"keep": "1", "overwrite": "old"})

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, metadata=intellistream_datahub_sdk.MapField.delta(add={"overwrite": "new", "added": "2"})
    ))
    md = updated.metadata or {}
    assert md.get("keep") == "1"
    assert md.get("overwrite") == "new"
    assert md.get("added") == "2"


def test_metadata_remove_keys(sync_client, new_event):
    event = new_event(metadata={"a": "1", "b": "2", "c": "3"})

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, metadata=intellistream_datahub_sdk.MapField.delta(remove=["b", "c"])
    ))
    md = updated.metadata or {}
    assert md.get("a") == "1"
    assert "b" not in md and "c" not in md


def test_metadata_cleared_by_an_empty_set(sync_client, new_event):
    """``MapField`` has no ``setNull``; an empty ``set`` is how a map is emptied."""
    event = new_event(metadata={"a": "1", "b": "2"})

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, metadata=intellistream_datahub_sdk.MapField.set({})
    ))
    assert not (updated.metadata or {})


# --------------------------------------------------------------------------- #
# related_resources (ListFieldIdCollection): set / add / remove
# --------------------------------------------------------------------------- #

@pytest.fixture
def two_resources(sync_client, make_resource):
    ext_a, ext_b = unique_id("evt_rel_a"), unique_id("evt_rel_b")
    make_resource([
        intellistream_datahub_sdk.Resource(external_id=ext_a, name="Event update probe A",
                             is_root=True, labels=["ASSET"]),
        intellistream_datahub_sdk.Resource(external_id=ext_b, name="Event update probe B",
                             is_root=True, labels=["ASSET"]),
    ])
    return ext_a, ext_b


def _related_ext_ids(event):
    return sorted(r.external_id for r in event.related_resources if r.external_id)


def test_related_resources_set_replaces_list(sync_client, new_event, two_resources):
    ext_a, ext_b = two_resources
    event = new_event(related_resources=[intellistream_datahub_sdk.IdCollection(external_id=ext_a)])

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event,
        related_resources=intellistream_datahub_sdk.ListFieldIdCollection.set(
            [intellistream_datahub_sdk.IdCollection(external_id=ext_b)]
        ),
    ))
    assert _related_ext_ids(updated) == [ext_b]


def test_related_resources_add_keeps_existing(sync_client, new_event, two_resources):
    ext_a, ext_b = two_resources
    event = new_event(related_resources=[intellistream_datahub_sdk.IdCollection(external_id=ext_a)])

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event,
        related_resources=intellistream_datahub_sdk.ListFieldIdCollection.delta(
            add=[intellistream_datahub_sdk.IdCollection(external_id=ext_b)]
        ),
    ))
    assert _related_ext_ids(updated) == sorted([ext_a, ext_b])


def test_related_resources_remove(sync_client, new_event, two_resources):
    ext_a, ext_b = two_resources
    event = new_event(related_resources=[
        intellistream_datahub_sdk.IdCollection(external_id=ext_a),
        intellistream_datahub_sdk.IdCollection(external_id=ext_b),
    ])

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event,
        related_resources=intellistream_datahub_sdk.ListFieldIdCollection.delta(
            remove=[intellistream_datahub_sdk.IdCollection(external_id=ext_a)]
        ),
    ))
    assert _related_ext_ids(updated) == [ext_b]


def test_related_resources_cleared_by_an_empty_set(sync_client, new_event, two_resources):
    """The list has no ``setNull`` either; an empty ``set`` detaches every resource."""
    ext_a, _ = two_resources
    event = new_event(related_resources=[intellistream_datahub_sdk.IdCollection(external_id=ext_a)])

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, related_resources=intellistream_datahub_sdk.ListFieldIdCollection.set([])
    ))
    assert updated.related_resources == []


def test_related_resource_without_an_identifier_is_rejected(sync_client, new_event):
    """An entry naming neither an id nor an external id cannot be built at all."""
    with pytest.raises(Exception):
        intellistream_datahub_sdk.IdCollection()


# --------------------------------------------------------------------------- #
# Composite: multi-field, persistence, no-op
# --------------------------------------------------------------------------- #

def test_multiple_fields_in_one_update(sync_client, new_event):
    event = new_event(description="orig", status="new", metadata={"k": "v"})

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event,
        description=intellistream_datahub_sdk.FieldStr(value="multi desc"),
        status=intellistream_datahub_sdk.FieldStr(value="resolved"),
        source=intellistream_datahub_sdk.FieldStr(value="multi_source"),
        metadata=intellistream_datahub_sdk.MapField.delta(add={"k2": "v2"}),
    ))

    assert updated.description == "multi desc"
    assert updated.status == "resolved"
    assert updated.source == "multi_source"
    md = updated.metadata or {}
    assert md.get("k") == "v" and md.get("k2") == "v2"


def test_update_persists_beyond_the_echo(sync_client, new_event):
    event = new_event(description="before")

    _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event, description=intellistream_datahub_sdk.FieldStr(value="after")
    ))

    stored = poll_until(
        lambda: sync_client.events.get(event.id),
        lambda e: e is not None and e.description == "after",
    )
    assert stored is not None and stored.description == "after"


def test_noop_update_preserves_existing_fields(sync_client, new_event):
    event = new_event(description="keep me", status="new", metadata={"a": "1"})

    updated = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(event))

    assert updated.description == "keep me"
    assert updated.status == "new"
    assert (updated.metadata or {}).get("a") == "1"


def test_update_targeting_by_uuid_and_by_external_id(sync_client, new_event):
    event = new_event(description="orig")

    by_uuid = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event.id, description=intellistream_datahub_sdk.FieldStr(value="by uuid")
    ))
    assert by_uuid.description == "by uuid"

    by_ext = _apply(sync_client, intellistream_datahub_sdk.EventUpdate(
        event.external_id, description=intellistream_datahub_sdk.FieldStr(value="by external id")
    ))
    assert by_ext.description == "by external id"


def test_update_of_an_unknown_event_changes_nothing(sync_client):
    assert sync_client.events.update([intellistream_datahub_sdk.EventUpdate(
        uuid.uuid4(), description=intellistream_datahub_sdk.FieldStr(value="nobody")
    )]) == []
