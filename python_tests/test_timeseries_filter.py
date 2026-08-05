"""Filter coverage for the TimeSeries service (``POST /timeseries/filter``).

Mirrors ``src/timeseries/test.rs::test_filter_timeseries``: creates a series carrying a
metadata entry unique to the run, then filters by metadata key/value and unit. The
dataset-hierarchy expansion of ``data_set_id`` is exercised server-side (Neo4j walk) and
covered by the backend's own tests; here we assert the wire contract round-trips.
"""
import pytest

import datahub_sdk
from python_tests.fixtures import *  # noqa: F401,F403  (sync_client fixture)


def test_filter_by_metadata_and_unit(sync_client, make_ts):
    ext_id = unique_id("filter")
    unique_value = f"value_{ext_id}"
    make_ts(
        external_id=ext_id,
        name=f"Py SDK Filter {ext_id}",
        unit="celsius",
        metadata={"py_sdk_filter_key": unique_value},
    )

    # Key + value together must find exactly the created series.
    form = datahub_sdk.TimeSeriesFilterForm(
        metadata_key="py_sdk_filter_key",
        metadata_value=unique_value,
        limit=10,
    )
    results = sync_client.timeseries.filter(form)
    assert [t.external_id for t in results] == [ext_id]

    # Value alone matches too; a wrong unit must drop it.
    results = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(metadata_value=unique_value, unit="celsius")
    )
    assert [t.external_id for t in results] == [ext_id]

    results = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(metadata_value=unique_value, unit="watt")
    )
    assert results == []


@pytest.mark.asyncio
async def test_filter_async(async_client, sync_client, make_ts):
    ext_id = unique_id("filter_async")
    unique_value = f"value_{ext_id}"
    make_ts(
        external_id=ext_id,
        name=f"Py SDK Filter Async {ext_id}",
        metadata={"py_sdk_filter_key": unique_value},
    )

    results = await async_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(metadata_value=unique_value)
    )
    assert [t.external_id for t in results] == [ext_id]
