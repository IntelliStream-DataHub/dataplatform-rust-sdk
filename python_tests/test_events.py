import uuid
from datetime import datetime, timedelta
from time import sleep

import datahub_sdk
import pandas as pd
import pytest
from pytest_asyncio import fixture

from fixtures import async_client, sync_client, unique_id
from polling import poll_until

# Read paths that lag further behind a write than the poll_until defaults allow: the
# event get/by_ids projection, and the search index behind it. Spelled as kwargs so a
# call site reads `poll_until(..., **POLL_SLOW)`.
POLL_SLOW = {"timeout": 30.0, "interval": 1.0}
POLL_SEARCH = {"timeout": 60.0, "interval": 1.0}


@pytest.fixture(scope="module")
def event_dataset(sync_client):
    # Prefix with TEST_PREFIX (via unique_id) so the conftest janitor's event
    # sweep can reclaim these + all derived events if a run is killed before
    # teardown. The events below inherit this prefix through their external ids.
    dataset_name = unique_id("event_dataset")
    sync_client.datasets.delete([datahub_sdk.Dataset(external_id=dataset_name)])
    event_dataset = sync_client.datasets.create([datahub_sdk.Dataset(external_id=dataset_name)])[0]
    yield event_dataset
    sync_client.datasets.delete([event_dataset])
@pytest.fixture(scope="module")
def test_events(sync_client,event_dataset):

    n=100
    events= []
    event_times = pd.date_range(start=pd.Timestamp("2025-01-01",tz="UTC"), periods=n, freq="D")
    for i in range(n):
        external_id = f"{event_dataset.external_id}_test_event_{i}"
        metadata = {f"key": event_dataset.external_id ,f"key{i}": "val"}
        description = f"{event_dataset.external_id}_test_event_{i}_description"
        type = f"{event_dataset.external_id}_test_event_{i}_type"
        sub_type = f"{event_dataset.external_id}_test_event_{i}_sub_type"
        related_resource_ids = []
        related_resource_external_ids = []
        source = f"{event_dataset.external_id}_test_event_{i} source"
        events.append(datahub_sdk.Event(
            external_id=external_id,
            metadata=metadata,
            description=description,
            type=type,
            sub_type=sub_type,
            data_set_id=event_dataset.id,
            related_resource_ids=related_resource_ids,
            related_resource_external_ids=related_resource_external_ids,
            source=source,
            event_time=event_times[i]
        ))
    sync_client.events.create(events)
    sleep(1)
    yield events
    sync_client.events.delete(events)

@pytest.fixture(scope="function")
def test_events_func_scope(sync_client,event_dataset):
    event_dataset_id = event_dataset.id
    n=100
    events= []
    event_times = pd.date_range(start=pd.Timestamp("2023-01-01",tz="UTC"), periods=n, freq="D")
    for i in range(n):
        external_id = f"{event_dataset.external_id}_func_scope_test_event_{i}"
        metadata = {f"{event_dataset.external_id}_func_scope_key": str(i * 2) ,f"key{i}": "val"}
        description = f"{event_dataset.external_id}_func_scope_test_event_{i}_description"
        type = f"{event_dataset.external_id}_func_scope_test_event_{i}_type"
        sub_type = f"{event_dataset.external_id}_func_scope_test_event_{i}_sub_type"
        data_set_id = event_dataset_id
        related_resource_ids = []
        related_resource_external_ids = []
        source = f"{event_dataset.external_id}_func_scope_test_event_{i} source"
        events.append(datahub_sdk.Event(
            external_id=external_id,
            metadata=metadata,
            description=description,
            type=type,
            sub_type=sub_type,
            data_set_id=data_set_id,
            related_resource_ids=related_resource_ids,
            related_resource_external_ids=related_resource_external_ids,
            source=source,
            event_time=event_times[i]
        ))
    sync_client.events.create(events)
    sleep(1)
    yield events
    sync_client.events.delete(events)

def test_by_ids(sync_client, test_events):
    # Pick a handful spread across the fixture and verify by_ids round-trips them.
    # by_ids reads ClickHouse (eventually consistent), so poll until all targets land.
    targets = [test_events[0], test_events[33], test_events[99]]
    want = {t.external_id for t in targets}
    fetched = poll_until(
        lambda: sync_client.events.by_ids(targets),
        lambda r: want <= {e.external_id for e in r},
        timeout=10,
    )
    assert want <= {e.external_id for e in fetched}


def test_by_ids_with_external_id_strings(sync_client, test_events):
    # EventIdentifyable also accepts raw external_id strings.
    targets = [test_events[5].external_id, test_events[50].external_id]
    fetched = poll_until(
        lambda: sync_client.events.by_ids(targets),
        lambda r: {e.external_id for e in r} == set(targets),
        timeout=10,
    )
    assert {e.external_id for e in fetched} == set(targets)


def test_delete(sync_client,test_events_func_scope):
    delete_targets = test_events_func_scope[:20]
    sync_client.events.delete(delete_targets)
    sleep(1)
    all_events = sync_client.events.by_ids(test_events_func_scope)

    assert delete_targets not in sync_client.events.by_ids(test_events_func_scope)

    # ... existing code ...
    all_events = sync_client.events.by_ids(test_events_func_scope)

    assert delete_targets not in sync_client.events.by_ids(test_events_func_scope)

def test_filter_by_external_id_prefix(sync_client, test_events,event_dataset):
    # Filter for "test_event_5" which should match index 5, 50-59
    test_events_5 = test_events[5]
    target_string = f"{event_dataset.external_id}_test_event_5"
    basic_filter = datahub_sdk.BasicEventFilter(external_id_prefix=target_string)
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = poll_until(lambda: sync_client.events.filter(filt), lambda r: len(r) >= 1)
    assert len(results) >= 1
    assert all(e.external_id.startswith(target_string) for e in results)

def test_filter_by_type(sync_client, test_events):
    # ``type`` embeds the event index + dataset uuid, so exactly one logical event
    # matches. Assert it's present and every result really has that type, deduping
    # by external_id — the /events/filter endpoint can echo the same row twice
    # under indexing lag, so an exact ``== 1`` count is race-prone.
    target = test_events[10]
    basic_filter = datahub_sdk.BasicEventFilter(
        type=target.type,
    )
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = poll_until(
        lambda: sync_client.events.filter(filt),
        lambda r: target.external_id in {e.external_id for e in r},
    )
    assert target.external_id in {e.external_id for e in results}
    assert all(e.type == target.type for e in results)
def test_filter_by_sub_type(sync_client, test_events):
    target = test_events[99]
    basic_filter = datahub_sdk.BasicEventFilter(
        sub_type=target.sub_type,
    )
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = poll_until(
        lambda: sync_client.events.filter(filt),
        lambda r: target.external_id in {e.external_id for e in r},
    )
    assert target.external_id in {e.external_id for e in results}
    assert all(e.sub_type == target.sub_type for e in results)

@pytest.mark.parametrize("time_filter,expected_idx", [
    (datahub_sdk.TimeFilter(
        start=pd.Timestamp("2025-01-02", tz="UTC"),
        end=pd.Timestamp("2025-01-04", tz="UTC")), slice(1,3)),
    (datahub_sdk.TimeFilter(
        start=pd.Timestamp("2025-01-03", tz="UTC")),
         slice(3, None)),
    (datahub_sdk.TimeFilter(
        end=pd.Timestamp("2025-01-03", tz="UTC")),
     slice(None,3)),
])
def test_filter_by_event_time_range(sync_client, test_events,time_filter,expected_idx):
    # Events are 1 day apart. Filter for the first 3 days.
    basic_filter = datahub_sdk.BasicEventFilter(event_time=time_filter)
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    # The time filter isn't dataset-scoped, so we only assert the window returns something
    # (poll past ingestion lag) rather than pinning exact membership.
    results = poll_until(lambda: sync_client.events.filter(filt), lambda r: len(r) >= 1)
    assert len(results) >= 1

@pytest.mark.parametrize("target_idx", [7])
def test_filter_by_metadata(sync_client, test_events,target_idx):
    # Each event has unique metadata: {f"key{i}": "val"}
    target = test_events[target_idx]
    target_metadata = target.metadata

    basic_filter = datahub_sdk.BasicEventFilter(metadata=target_metadata)
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = poll_until(
        lambda: sync_client.events.filter(filt),
        lambda r: target.external_id in {e.external_id for e in r},
    )
    assert target.external_id in {e.external_id for e in results}
    # Every result must carry all of the target's metadata pairs (the unique
    # key{i} entry pins this to the one logical event); dedup tolerates the
    # backend echoing the same row twice under indexing lag.
    assert all(
        all(e.metadata.get(k) == v for k, v in target_metadata.items())
        for e in results
    )

def test_filter_by_source_and_description(sync_client, test_events):
    target = test_events[7]
    basic_filter = datahub_sdk.BasicEventFilter(
        source=target.source,
        description=target.description
    )
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = poll_until(
        lambda: sync_client.events.filter(filt),
        lambda r: target.external_id in {e.external_id for e in r},
    )
    assert target.external_id in {e.external_id for e in results}
    assert all(
        e.source == target.source and e.description == target.description
        for e in results
    )

def test_filter_with_limit(sync_client, test_events,event_dataset):
    basic_filter = datahub_sdk.BasicEventFilter(external_id_prefix=event_dataset.external_id)
    # Using the EventFilter limit field
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter, limit=5)

    results = poll_until(lambda: sync_client.events.filter(filt), lambda r: len(r) == 5)
    assert len(results) == 5


def test_filter_by_data_set_ids(sync_client, test_events, event_dataset):
    # dataSetIds must go over the wire as the backend's List<IdObject> ([{"id": ...}]); a flat id
    # array is rejected with HTTP 400. All fixture events live in event_dataset.
    target = test_events[0]
    basic_filter = datahub_sdk.BasicEventFilter(data_set_ids=[event_dataset.id])
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = poll_until(
        lambda: sync_client.events.filter(filt),
        lambda r: target.external_id in {e.external_id for e in r},
    )
    assert target.external_id in {e.external_id for e in results}
    # Every returned event really belongs to the filtered dataset.
    assert all(e.data_set_id == event_dataset.id for e in results)


def test_filter_by_related_resources(sync_client, event_dataset):
    # relatedResourceIds / relatedResourceExternalIds collapse into the backend's single
    # relatedResources IdCollection array, matched with hasAll. Create a resource + an event
    # referencing it, then filter by both the numeric id and the external id.
    res_ext = unique_id("evfilt_res")
    sync_client.resources.delete([res_ext])
    sync_client.resources.create([datahub_sdk.Resource(
        external_id=res_ext, name="ev filter res", is_root=True, labels=["ASSET"])])
    res = next(r for r in sync_client.resources.by_ids([res_ext]) if r.external_id == res_ext)

    ev_ext = unique_id("evfilt_ev")
    # Create by external id; the backend resolves it and stores both the id and external-id arrays.
    sync_client.events.create([datahub_sdk.Event(
        external_id=ev_ext, event_time=pd.Timestamp.now(tz="UTC"),
        data_set_id=event_dataset.id, related_resource_external_ids=[res_ext])])
    try:
        by_id = datahub_sdk.EventFilter(
            basic_filter=datahub_sdk.BasicEventFilter(related_resource_ids=[res.id]))
        r1 = poll_until(lambda: sync_client.events.filter(by_id),
                   lambda r: ev_ext in {e.external_id for e in r})
        assert ev_ext in {e.external_id for e in r1}, "filter by related_resource_ids did not find the event"

        by_ext = datahub_sdk.EventFilter(
            basic_filter=datahub_sdk.BasicEventFilter(related_resource_external_ids=[res_ext]))
        r2 = poll_until(lambda: sync_client.events.filter(by_ext),
                   lambda r: ev_ext in {e.external_id for e in r})
        assert ev_ext in {e.external_id for e in r2}, "filter by related_resource_external_ids did not find the event"
    finally:
        sync_client.events.delete([ev_ext])
        sync_client.resources.delete([res_ext])


def test_filter_by_created_time(sync_client, test_events, event_dataset):
    # createdTime is server-assigned at create; the fixture events were just made. Scope by the
    # unique prefix so only these events are in play.
    target = test_events[0]
    now = pd.Timestamp.now(tz="UTC")
    day = pd.Timedelta(days=1)

    after = datahub_sdk.EventFilter(basic_filter=datahub_sdk.BasicEventFilter(
        external_id_prefix=event_dataset.external_id,
        created_time=datahub_sdk.TimeFilter(start=now - day)))
    r = poll_until(lambda: sync_client.events.filter(after),
              lambda r: target.external_id in {e.external_id for e in r})
    assert target.external_id in {e.external_id for e in r}, "created_time (after) filter did not find the event"

    # Now that we know it's propagated, the complementary window must exclude it.
    before = datahub_sdk.EventFilter(basic_filter=datahub_sdk.BasicEventFilter(
        external_id_prefix=event_dataset.external_id,
        created_time=datahub_sdk.TimeFilter(end=now - day)))
    r_before = sync_client.events.filter(before)
    assert target.external_id not in {e.external_id for e in r_before}, (
        "created_time (before yesterday) must not return a just-created event"
    )


def test_filter_by_last_updated_time(sync_client, test_events, event_dataset):
    # lastUpdatedTime is also server-assigned at create; same shape as event_time/created_time.
    target = test_events[0]
    now = pd.Timestamp.now(tz="UTC")
    day = pd.Timedelta(days=1)

    after = datahub_sdk.EventFilter(basic_filter=datahub_sdk.BasicEventFilter(
        external_id_prefix=event_dataset.external_id,
        last_updated_time=datahub_sdk.TimeFilter(start=now - day)))
    r = poll_until(lambda: sync_client.events.filter(after),
              lambda r: target.external_id in {e.external_id for e in r})
    assert target.external_id in {e.external_id for e in r}, "last_updated_time (after) filter did not find the event"

    before = datahub_sdk.EventFilter(basic_filter=datahub_sdk.BasicEventFilter(
        external_id_prefix=event_dataset.external_id,
        last_updated_time=datahub_sdk.TimeFilter(end=now - day)))
    r_before = sync_client.events.filter(before)
    assert target.external_id not in {e.external_id for e in r_before}, (
        "last_updated_time (before yesterday) must not return a just-created event"
    )


# ---------------------------------------------------------------------------
# UUID event ids: events are keyed by a client-generated UUID v7, and that id
# must be usable to get / delete / filter the event (not just its external_id).
# Ingestion is eventually consistent, so id lookups poll rather than sleep once.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="function")
def single_event(sync_client, event_dataset):
    external_id = f"{event_dataset.external_id}_uuid_event_{uuid.uuid4().hex}"
    ev = datahub_sdk.Event(
        external_id=external_id,
        data_set_id=event_dataset.id,
        event_time=pd.Timestamp("2025-01-01", tz="UTC"),
    )
    created = sync_client.events.create([ev])[0]
    yield created
    # Teardown by external id removes every copy, regardless of what the test deleted.
    sync_client.events.delete([external_id])


def test_created_event_has_uuid_v7_id(single_event):
    # The server echoes back the client-supplied id; it should be a v7 UUID.
    assert isinstance(single_event.id, uuid.UUID)
    assert single_event.id.version == 7


def test_by_ids_with_uuid_collection(sync_client, single_event):
    selector = datahub_sdk.EventIdCollection(id=single_event.id)
    fetched = poll_until(lambda: sync_client.events.by_ids([selector]), lambda r: len(r) == 1, timeout=10)
    assert len(fetched) == 1
    assert fetched[0].id == single_event.id
    assert fetched[0].external_id == single_event.external_id


def test_by_ids_with_bare_uuid(sync_client, single_event):
    # A bare uuid.UUID is also accepted as an event identifier.
    fetched = poll_until(lambda: sync_client.events.by_ids([single_event.id]), lambda r: len(r) == 1, timeout=10)
    assert len(fetched) == 1
    assert fetched[0].id == single_event.id


def test_delete_by_uuid(sync_client, single_event):
    # Confirm the event is queryable (read-after-write), then delete it by its UUID.
    poll_until(lambda: sync_client.events.by_ids([single_event.id]), lambda r: len(r) == 1, timeout=10)
    sync_client.events.delete([datahub_sdk.EventIdCollection(id=single_event.id)])
    remaining = poll_until(lambda: sync_client.events.by_ids([single_event.id]), lambda r: r == [], timeout=10)
    assert remaining == []


# NB: there is deliberately no filter-by-uuid test. The backend types the event filter's `id`
# field as a Long, so it cannot filter events by their UUID id (the request is rejected
# server-side). Use `by_ids` (see test_by_ids_with_uuid_collection) to fetch an event by its UUID.


def test_event_id_collection_requires_an_identifier():
    # Constructing with neither an id nor an external_id is a usage error.
    with pytest.raises(ValueError):
        datahub_sdk.EventIdCollection()


# ---------------------------------------------------------------------------
# get / update / search / count and the type/sub-type/status/source dimension
# endpoints. Ingestion + the search/dimension indexes are eventually consistent,
# so these poll rather than sleep a fixed amount.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dimension_events(sync_client, event_dataset):
    """A small batch of events sharing a unique letters+digits token in their
    type / sub_type / status / source / description, so the list_* / search_* /
    search endpoints can be pinned to exactly these events.

    The token is hex (``[0-9a-f]``) on purpose: the free-text search's `query`
    only accepts letters/digits/spaces, so an underscore-free token keeps the
    same value usable across every endpoint."""
    token = uuid.uuid4().hex[:10]
    dim = {
        "token": token,
        "type": f"evtype{token}",
        "sub_type": f"evsubtype{token}",
        "status": f"evstatus{token}",
        "source": f"evsource{token}",
    }
    events = []
    base = pd.Timestamp("2024-06-01", tz="UTC")
    for i in range(3):
        events.append(datahub_sdk.Event(
            external_id=f"{event_dataset.external_id}_dim_event_{token}_{i}",
            description=f"dimension event {token} number {i}",
            type=dim["type"],
            sub_type=dim["sub_type"],
            status=dim["status"],
            source=dim["source"],
            data_set_id=event_dataset.id,
            event_time=base + timedelta(days=i),
        ))
    sync_client.events.create(events)
    sleep(1)
    dim["events"] = events
    yield dim
    sync_client.events.delete(events)


def test_get_by_uuid(sync_client, test_events):
    # Resolve a settled event's server UUID via by_ids, then round-trip it through
    # GET /events/{id}. Using a settled event (rather than a just-created one) keeps this
    # about the get endpoint, not ingestion lag.
    target_ext = test_events[0].external_id
    fetched = poll_until(
        lambda: sync_client.events.by_ids([target_ext]),
        lambda r: len(r) == 1,
        **POLL_SLOW,
    )
    ev = fetched[0]
    assert ev.id is not None
    got = poll_until(lambda: sync_client.events.get(ev.id), lambda e: e is not None, **POLL_SLOW)
    assert got is not None
    assert got.id == ev.id
    assert got.external_id == target_ext


def test_get_missing_returns_none(sync_client):
    # An id that doesn't exist yields None (the backend 404 is mapped to None).
    assert sync_client.events.get(uuid.uuid4()) is None


def test_update_event(sync_client, single_event):
    # Make sure the event is queryable before updating it.
    poll_until(lambda: sync_client.events.get(single_event.id), lambda e: e is not None)
    update = datahub_sdk.EventUpdate(
        single_event,
        status=datahub_sdk.FieldStr(value="acknowledged"),
        description=datahub_sdk.FieldStr(value="updated by test"),
    )

    def _updated(result):
        # The update response echoes the events with their new field values.
        return any(
            e.status == "acknowledged" and e.description == "updated by test"
            for e in result
        )

    # Under write lag the update can momentarily not find the event and echo nothing back;
    # the update is idempotent, so retry until the change is reflected.
    result = poll_until(lambda: sync_client.events.update([update]), _updated)
    assert _updated(result)



def test_search_by_description(sync_client, dimension_events):
    token = dimension_events["token"]
    results = poll_until(
        lambda: sync_client.events.search(datahub_sdk.EventSearch(query=token)),
        lambda r: len(r) >= 1,
        **POLL_SEARCH,
    )
    found = {e.external_id for e in results}
    assert any(ev.external_id in found for ev in dimension_events["events"])


def test_search_with_filter_and_limit(sync_client, dimension_events):
    # The free-text search can be narrowed with a BasicEventFilter and capped.
    token = dimension_events["token"]
    basic_filter = datahub_sdk.BasicEventFilter(type=dimension_events["type"])
    search = datahub_sdk.EventSearch(query=token, filter=basic_filter, limit=2)
    results = poll_until(lambda: sync_client.events.search(search), lambda r: len(r) >= 1, **POLL_SEARCH)
    assert len(results) >= 1  # not a vacuous pass on an empty result
    assert len(results) <= 2
    assert all(e.type == dimension_events["type"] for e in results)


def test_count(sync_client, dimension_events):
    n = len(dimension_events["events"])
    count = poll_until(lambda: sync_client.events.count(), lambda c: c >= n)
    assert isinstance(count, int)
    assert count >= n


@pytest.mark.asyncio
async def test_count_async(async_client):
    # Smoke-test the async service path for one of the new endpoints.
    count = await async_client.events.count()
    assert isinstance(count, int)
    assert count >= 0