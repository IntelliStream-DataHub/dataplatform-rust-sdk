import uuid
from datetime import datetime, timedelta
from time import sleep

import datahub_sdk
import pandas as pd
import pytest
from pygments.styles.dracula import yellow
from pytest_asyncio import fixture

from fixtures import sync_client

@pytest.fixture(scope="module")
def event_dataset(sync_client):
    dataset_name= f"event_test_dataset_{uuid.uuid4().hex}"
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
    target = test_events[10]
    basic_filter = datahub_sdk.BasicEventFilter(
        type=target.type,
    )
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = sync_client.events.filter(filt)
    assert len(results) == 1
    assert results[0].external_id == target.external_id
def test_filter_by_sub_type(sync_client, test_events):
    target = test_events[99]
    basic_filter = datahub_sdk.BasicEventFilter(
        sub_type=target.sub_type,
    )
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = sync_client.events.filter(filt)
    assert len(results) == 1
    assert results[0].external_id == target.external_id

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
    assert len(results) == 1
    assert results[0].external_id == target.external_id

def test_filter_by_source_and_description(sync_client, test_events):
    target = test_events[7]
    basic_filter = datahub_sdk.BasicEventFilter(
        source=target.source,
        description=target.description
    )
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter)

    results = sync_client.events.filter(filt)
    assert len(results) == 1
    assert results[0].external_id == target.external_id

def test_filter_with_limit(sync_client, test_events,event_dataset):
    basic_filter = datahub_sdk.BasicEventFilter(external_id_prefix=event_dataset.external_id)
    # Using the EventFilter limit field
    filt = datahub_sdk.EventFilter(basic_filter=basic_filter, limit=5)

    results = sync_client.events.filter(filt)
    assert len(results) == 5