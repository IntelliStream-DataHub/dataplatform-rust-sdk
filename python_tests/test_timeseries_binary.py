"""Binary datapoint ingest (`POST /timeseries/data/binary`) through the Python bindings.

What goes in through the binary path must read back through the ordinary JSON read path, point for
point. The Rust side covers the frame layout offline (`src/timeseries/binary.rs`) and the
multi-million-point load in the ignored `test_datapoints_binary`; this is the binding surface.
"""
import datetime

import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException
from polling import poll_until
from python_tests.fixtures import *  # noqa: F401,F403

START = datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc)
TIMESTAMPS = [START + datetime.timedelta(minutes=i) for i in range(50)]


def _read_all(client, ts, expected):
    rf = intellistream_datahub_sdk.RetrieveFilter(
        ts=ts,
        start=START - datetime.timedelta(days=1),
        end=START + datetime.timedelta(days=1),
    )

    def fetch():
        collections = client.timeseries.retrieve_datapoints(rf)
        return collections[0].get_datapoints() if collections else []

    return poll_until(fetch, lambda dps: len(dps) >= expected)


@pytest.mark.parametrize("zstd_level", [None, 1, 3, 9])
def test_insert_from_lists_binary_round_trips_floats(sync_client, make_ts, zstd_level):
    ts = make_ts(value_type="float")
    # Out of order on purpose: the frame writer sorts, the read must still be in time order.
    values = [i * 1.25 - 20.0 for i in range(len(TIMESTAMPS))]
    shuffled = list(zip(TIMESTAMPS, values))[::-1]

    result = sync_client.timeseries.insert_from_lists_binary(
        [t for t, _ in shuffled], [v for _, v in shuffled], ts, zstd_level=zstd_level
    )
    assert result == []

    dps = _read_all(sync_client, ts, len(TIMESTAMPS))
    assert [dp.timestamp for dp in dps] == TIMESTAMPS
    assert [dp.value for dp in dps] == pytest.approx(values)


def test_insert_datapoints_binary_round_trips_bigints(sync_client, make_ts):
    ts = make_ts(value_type="bigint")
    values = [(-1) ** i * i * 1_000_003 for i in range(len(TIMESTAMPS))]
    data = [
        intellistream_datahub_sdk.DatapointString.from_int(t, v)
        for t, v in zip(TIMESTAMPS, values)
    ]
    collection = intellistream_datahub_sdk.DatapointsCollectionString(datapoints=data, ts=ts)

    assert sync_client.timeseries.insert_datapoints_binary([collection]) == []

    dps = _read_all(sync_client, ts, len(TIMESTAMPS))
    assert [dp.timestamp for dp in dps] == TIMESTAMPS
    assert [dp.value for dp in dps] == values


def test_insert_datapoints_binary_spans_several_series(sync_client, make_ts):
    first, second = make_ts(value_type="float"), make_ts(value_type="bigint")
    collections = [
        intellistream_datahub_sdk.DatapointsCollectionString(
            datapoints=[intellistream_datahub_sdk.DatapointString.from_float(t, 0.5) for t in TIMESTAMPS],
            ts=first,
        ),
        intellistream_datahub_sdk.DatapointsCollectionString(
            datapoints=[intellistream_datahub_sdk.DatapointString.from_int(t, 7) for t in TIMESTAMPS[:10]],
            ts=second,
        ),
    ]

    assert sync_client.timeseries.insert_datapoints_binary(collections) == []

    assert [dp.value for dp in _read_all(sync_client, first, 50)] == [0.5] * 50
    assert [dp.value for dp in _read_all(sync_client, second, 10)] == [7] * 10


def test_binary_insert_duplicate_timestamps_keep_one_point(sync_client, make_ts):
    ts = make_ts(value_type="float")
    at = TIMESTAMPS[0]

    sync_client.timeseries.insert_from_lists_binary([at, at, TIMESTAMPS[1]], [1.0, 2.0, 3.0], ts)

    dps = _read_all(sync_client, ts, 2)
    assert [dp.timestamp for dp in dps] == [at, TIMESTAMPS[1]]


def test_binary_insert_refuses_an_unknown_zstd_level(sync_client, make_ts):
    ts = make_ts(value_type="float")
    with pytest.raises(DataHubException, match="zstd level 2") as excinfo:
        sync_client.timeseries.insert_from_lists_binary([START], [1.0], ts, zstd_level=2)
    assert excinfo.value.status_code == 400


def test_binary_insert_into_a_missing_series_is_not_found(sync_client):
    missing = unique_id("ts_missing")
    with pytest.raises(DataHubException, match="Could not find following timeseries") as excinfo:
        sync_client.timeseries.insert_from_lists_binary([START], [1.0], missing)
    assert excinfo.value.status_code == 404


def test_insert_from_lists_binary_rejects_mismatched_lengths(sync_client):
    with pytest.raises(ValueError, match="got 2 and 1"):
        sync_client.timeseries.insert_from_lists_binary([START, START], [1.0], "never_sent")
