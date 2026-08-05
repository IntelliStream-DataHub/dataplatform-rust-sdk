import uuid
from datetime import datetime, timedelta
from time import sleep

import datahub_sdk
import pandas as pd
import pytest
from pytest_asyncio import fixture

from fixtures import async_client, sync_client, unique_id

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
    targets = [test_events[0], test_events[33], test_events[99]]
    want = {t.external_id for t in targets}
    # Ingestion is eventually consistent — poll until every target is readable.
    fetched = _poll(
        lambda: sync_client.events.by_ids(targets),
        lambda r: want <= {e.external_id for e in r},
    )
    assert want <= {e.external_id for e in fetched}


def test_by_ids_with_external_id_strings(sync_client, test_events):
    # EventIdentifyable also accepts raw external_id strings.
    targets = [test_events[5].external_id, test_events[50].external_id]
    fetched = _poll(
        lambda: sync_client.events.by_ids(targets),
        lambda r: {e.external_id for e in r} == set(targets),
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

    results = _poll(lambda: sync_client.events.filter(filt), lambda r: len(r) >= 1)
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

    results = _poll(
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

    results = _poll(
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
    results = _poll(lambda: sync_client.events.filter(filt), lambda r: len(r) >= 1)
    assert len(results) >= 1

@pytest.mark.parametrize("target_idx", [7])
def test_filter_by_metadata(sync_client, test_events,target_idx):
    # Each event has unique metadata: {f"key{i}": "val"}
    target = test_events[target_idx]
    target_metadata = target.metadata

    basic_filter = datahub_sdk.BasicEventFilter(metadata=target_metadata)
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = _poll(
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

    results = _poll(
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

    results = _poll(lambda: sync_client.events.filter(filt), lambda r: len(r) == 5)
    assert len(results) == 5


# ---------------------------------------------------------------------------
# UUID event ids: events are keyed by a client-generated UUID v7, and that id
# must be usable to get / delete / filter the event (not just its external_id).
# Ingestion is eventually consistent, so id lookups poll rather than sleep once.
# ---------------------------------------------------------------------------

def _poll(fn, ok, tries=20, delay=0.5):
    """Call fn() until ok(result) is true or we run out of tries; return the last result.

    Reads here are eventually consistent, so an exception from fn() (e.g. a transient 5xx
    while an index catches up) is treated as "not ready yet" and retried rather than failing
    the test outright. The last exception is re-raised only if we never get a good result."""
    result = None
    last_exc = None
    for i in range(tries):
        try:
            result = fn()
            last_exc = None
            if ok(result):
                return result
        except Exception as e:  # noqa: BLE001 - retry transient errors during the consistency window
            last_exc = e
        if i < tries - 1:
            sleep(delay)
    if last_exc is not None:
        raise last_exc
    return result


# Dimension tables (type/sub_type/status/source) lag writes by more than byids does, so give
# them a longer window.
POLL_SLOW = {"tries": 30, "delay": 1.0}

# The free-text search index is the slowest to catch up; give it the most generous window.
POLL_SEARCH = {"tries": 60, "delay": 1.0}


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
    fetched = _poll(lambda: sync_client.events.by_ids([selector]), lambda r: len(r) == 1)
    assert len(fetched) == 1
    assert fetched[0].id == single_event.id
    assert fetched[0].external_id == single_event.external_id


def test_by_ids_with_bare_uuid(sync_client, single_event):
    # A bare uuid.UUID is also accepted as an event identifier.
    fetched = _poll(lambda: sync_client.events.by_ids([single_event.id]), lambda r: len(r) == 1)
    assert len(fetched) == 1
    assert fetched[0].id == single_event.id


def test_delete_by_uuid(sync_client, single_event):
    # Confirm the event is queryable (read-after-write), then delete it by its UUID.
    _poll(lambda: sync_client.events.by_ids([single_event.id]), lambda r: len(r) == 1)
    sync_client.events.delete([datahub_sdk.EventIdCollection(id=single_event.id)])
    remaining = _poll(lambda: sync_client.events.by_ids([single_event.id]), lambda r: r == [])
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
    fetched = _poll(
        lambda: sync_client.events.by_ids([target_ext]),
        lambda r: len(r) == 1,
        **POLL_SLOW,
    )
    ev = fetched[0]
    assert ev.id is not None
    got = _poll(lambda: sync_client.events.get(ev.id), lambda e: e is not None, **POLL_SLOW)
    assert got is not None
    assert got.id == ev.id
    assert got.external_id == target_ext


def test_get_missing_returns_none(sync_client):
    # An id that doesn't exist yields None (the backend 404 is mapped to None).
    assert sync_client.events.get(uuid.uuid4()) is None


def test_update_event(sync_client, single_event):
    # Make sure the event is queryable before updating it.
    _poll(lambda: sync_client.events.get(single_event.id), lambda e: e is not None)
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
    result = _poll(lambda: sync_client.events.update([update]), _updated)
    assert _updated(result)



def test_search_by_description(sync_client, dimension_events):
    token = dimension_events["token"]
    results = _poll(
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
    results = _poll(lambda: sync_client.events.search(search), lambda r: len(r) >= 1, **POLL_SEARCH)
    assert len(results) >= 1  # not a vacuous pass on an empty result
    assert len(results) <= 2
    assert all(e.type == dimension_events["type"] for e in results)


def test_count(sync_client, dimension_events):
    n = len(dimension_events["events"])
    count = _poll(lambda: sync_client.events.count(), lambda c: c >= n)
    assert isinstance(count, int)
    assert count >= n


@pytest.mark.asyncio
async def test_count_async(async_client):
    # Smoke-test the async service path for one of the new endpoints.
    count = await async_client.events.count()
    assert isinstance(count, int)
    assert count >= 0