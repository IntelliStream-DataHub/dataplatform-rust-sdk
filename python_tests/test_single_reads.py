"""By-id reads, the value-type recommendation, and the live datapoint tail."""
import threading

import intellistream_datahub_sdk
import pandas as pd
import pytest

from fixtures import async_client, make_dataset, make_ts, sync_client, unique_id


def test_timeseries_get_by_id(sync_client, make_ts):
    ts = make_ts()
    fetched = sync_client.timeseries.get_by_id(ts.id)
    assert fetched.external_id == ts.external_id


def test_timeseries_get_by_id_of_an_unknown_id_raises_404(sync_client):
    with pytest.raises(intellistream_datahub_sdk.DataHubException) as err:
        sync_client.timeseries.get_by_id(2**62)
    assert err.value.status_code == 404
    assert err.value.problem_slug == "not-found"


def test_dataset_get_by_id(sync_client, make_dataset):
    ds = make_dataset()
    fetched = sync_client.datasets.get_by_id(ds.id)
    assert fetched.external_id == ds.external_id


def test_recommend_value_type(sync_client):
    known = sync_client.timeseries.recommend_value_type("temperature_deg_c")
    assert known.unit_external_id == "temperature_deg_c"
    assert known.recognized
    unknown = sync_client.timeseries.recommend_value_type(unique_id("unit"))
    assert not unknown.recognized


@pytest.mark.asyncio
async def test_async_get_by_id(async_client, make_ts):
    ts = make_ts()
    fetched = await async_client.timeseries.get_by_id(ts.id)
    assert fetched.external_id == ts.external_id


def test_listen_datapoints_delivers_points_written_after_connecting(sync_client, make_ts):
    ts = make_ts()
    received = []
    listener = sync_client.timeseries.listen_datapoints([ts.external_id])

    reader = threading.Thread(target=lambda: received.append(listener.next_datapoint()), daemon=True)
    reader.start()
    # The server-side consumer reads from latest, so write after it has attached.
    reader.join(timeout=2)
    sync_client.timeseries.insert_from_lists(
        timestamps=pd.DatetimeIndex([pd.Timestamp.now(tz="UTC")]), values=[21.5], ts=ts
    )
    reader.join(timeout=30)
    # A reader still waiting holds the listener, so closing now would block on it rather than
    # fail; leave the daemon thread and its socket to the interpreter.
    if not reader.is_alive():
        listener.close()

    assert received, "no datapoint within 30s"
    assert received[0].external_id == ts.external_id
    assert float(received[0].value) == 21.5
