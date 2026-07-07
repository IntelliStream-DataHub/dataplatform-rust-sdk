from datetime import datetime, timedelta
from time import sleep

import datahub_sdk
import pandas as pd
import pytest
from pytest_asyncio import fixture

from fixtures import sync_client, unique_id

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
    fetched = sync_client.events.by_ids(targets)
    fetched_ext_ids = {e.external_id for e in fetched}
    for t in targets:
        assert t.external_id in fetched_ext_ids


def test_by_ids_with_external_id_strings(sync_client, test_events):
    # EventIdentifyable also accepts raw external_id strings.
    targets = [test_events[5].external_id, test_events[50].external_id]
    fetched = sync_client.events.by_ids(targets)
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

    results = sync_client.events.filter(filt)
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

    results = sync_client.events.filter(filt)
    assert target.external_id in {e.external_id for e in results}
    assert all(e.type == target.type for e in results)
def test_filter_by_sub_type(sync_client, test_events):
    target = test_events[99]
    basic_filter = datahub_sdk.BasicEventFilter(
        sub_type=target.sub_type,
    )
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = sync_client.events.filter(filt)
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

    results = sync_client.events.filter(filt)
    # Depending on whether "end" is inclusive:
    assert [res.external_id  in [test_event.external_id for test_event in test_events[expected_idx]] for res in results]

@pytest.mark.parametrize("target_idx", [7])
def test_filter_by_metadata(sync_client, test_events,target_idx):
    # Each event has unique metadata: {f"key{i}": "val"}
    target = test_events[target_idx]
    target_metadata = target.metadata

    basic_filter = datahub_sdk.BasicEventFilter(metadata=target_metadata)
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = sync_client.events.filter(filt)
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

    results = sync_client.events.filter(filt)
    assert target.external_id in {e.external_id for e in results}
    assert all(
        e.source == target.source and e.description == target.description
        for e in results
    )

def test_filter_with_limit(sync_client, test_events,event_dataset):
    basic_filter = datahub_sdk.BasicEventFilter(external_id_prefix=event_dataset.external_id)
    # Using the EventFilter limit field
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter, limit=5)

    results = sync_client.events.filter(filt)
    assert len(results) == 5