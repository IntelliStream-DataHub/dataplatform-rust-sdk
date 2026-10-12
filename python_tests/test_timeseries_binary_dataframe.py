"""Binary ingest from a dataframe (`insert_dataframe_binary`): a polars, pandas or pyarrow table,
one column per series named by its external id, handed over through the Arrow C stream.

Offline, a stand-in server answers the series lookup and keeps what is posted; the posted frames
are then served back through the binary read, so a round trip needs no backend. The live test
round-trips a wide table through a real one.
"""
import datetime
import http.server
import json
import threading

import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException
from python_tests.fixtures import *  # noqa: F401,F403

pl = pytest.importorskip("polars")
UTC = datetime.timezone.utc
T0 = datetime.datetime(2025, 1, 1, tzinfo=UTC)
SERIES = {"pump.temp": (11, "float"), "pump.count": (12, "bigint"), "pump.state": (13, "text")}


class _Api(http.server.BaseHTTPRequestHandler):
    """Answers /timeseries/byids from SERIES, keeps posted frames, and serves them back on a read."""
    protocol_version = "HTTP/1.1"
    posted = []

    def _reply(self, status, content_type, body):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path == "/timeseries/byids":
            wanted = [i.get("externalId") for i in json.loads(body)["items"]]
            items = [
                {"id": str(SERIES[e][0]), "externalId": e, "name": e, "valueType": SERIES[e][1]}
                for e in wanted if e in SERIES
            ]
            self._reply(200, "application/json", json.dumps({"items": items}).encode())
        elif self.path == "/timeseries/data/binary":
            type(self).posted.append(body)
            self._reply(204, "application/json", b"")
        elif self.path == "/timeseries/data/list/binary":
            self._reply(200, "application/vnd.intellistream.datapoint-block", b"".join(type(self).posted) + b"DHDE")
        else:
            self._reply(404, "text/plain", b"")

    def log_message(self, *args):
        pass


@pytest.fixture
def api():
    _Api.posted = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Api)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    client = intellistream_datahub_sdk.DataHubClient(
        base_url=f"http://127.0.0.1:{server.server_address[1]}", token="test-token"
    )
    yield client, _Api
    server.shutdown()


def _read_back(client):
    """What the posted frames read back as: the wide table, by external id."""
    return client.timeseries.retrieve_datapoints_binary(
        intellistream_datahub_sdk.RetrieveFilter(ts="pump.temp", start=T0)
    ).to_polars(wide=True)


def test_a_polars_frame_round_trips(api):
    client, server = api
    df = pl.DataFrame({
        "timestamp": [T0 + datetime.timedelta(seconds=s) for s in (2, 0, 1)],  # unsorted on purpose
        "pump.temp": [2.5, 0.5, None],                                         # a gap
        "pump.count": [3, 1, 2],
        "pump.state": ["off", "on", "on"],
    })

    assert client.timeseries.insert_dataframe_binary(df) == 8
    assert len(server.posted) == 1

    # The stand-in replays the posted frames, which are grouped by value type; a real read follows
    # the request's items. So compare by name.
    back = _read_back(client)
    assert sorted(back.columns) == ["pump.count", "pump.state", "pump.temp", "timestamp"]
    assert back["timestamp"].to_list() == [T0 + datetime.timedelta(seconds=s) for s in range(3)]
    assert back["pump.temp"].to_list() == [0.5, None, 2.5]
    assert back["pump.count"].to_list() == [1, 2, 3]
    assert back["pump.state"].to_list() == ["on", "on", "off"]


def test_a_pandas_frame_uses_its_timestamp_index(api):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    client, _ = api
    index = pd.date_range("2025-01-01", periods=3, freq="s", tz="UTC", name="time")
    df = pd.DataFrame({"pump.temp": [1.0, float("nan"), 3.0]}, index=index)

    # pandas exports NaN as null, so the middle row is no point at all.
    assert client.timeseries.insert_dataframe_binary(df) == 2
    assert _read_back(client)["pump.temp"].to_list() == [1.0, 3.0]


def test_a_pyarrow_table_with_a_named_timestamp_column(api):
    pa = pytest.importorskip("pyarrow")
    client, _ = api
    table = pa.table({
        "at": pa.array([T0, T0 + datetime.timedelta(milliseconds=5)], pa.timestamp("ns", "UTC")),
        "pump.temp": [1.0, 2.0],
    })
    assert client.timeseries.insert_dataframe_binary(table, timestamp="at") == 2
    assert _read_back(client)["timestamp"].to_list() == [T0, T0 + datetime.timedelta(milliseconds=5)]


def test_an_unknown_column_is_not_found_before_anything_is_sent(api):
    client, server = api
    df = pl.DataFrame({"timestamp": [T0], "pump.temp": [1.0], "no.such.series": [2.0]})
    with pytest.raises(DataHubException, match="no.such.series") as excinfo:
        client.timeseries.insert_dataframe_binary(df)
    assert excinfo.value.status_code == 404
    assert server.posted == []


@pytest.mark.parametrize("timestamps, message", [
    ([datetime.datetime(2025, 1, 1)], "no time zone"),
    ([T0 + datetime.timedelta(microseconds=1500)], "finer than a millisecond"),
])
def test_timestamps_that_are_not_an_instant_in_milliseconds_are_refused(api, timestamps, message):
    client, server = api
    df = pl.DataFrame({"timestamp": timestamps, "pump.temp": [1.0]})
    with pytest.raises(DataHubException, match=message) as excinfo:
        client.timeseries.insert_dataframe_binary(df)
    assert excinfo.value.status_code == 422
    assert server.posted == []


def test_a_column_its_series_cannot_hold_is_refused(api):
    client, server = api
    df = pl.DataFrame({"timestamp": [T0], "pump.count": [1.5]})
    with pytest.raises(DataHubException, match="cannot hold"):
        client.timeseries.insert_dataframe_binary(df)
    assert server.posted == []


def test_the_timestamp_column_has_to_be_found(api):
    client, _ = api
    with pytest.raises(ValueError, match="no timestamp column"):
        client.timeseries.insert_dataframe_binary(pl.DataFrame({"pump.temp": [1.0]}))
    two = pl.DataFrame({"a": [T0], "b": [T0], "pump.temp": [1.0]})
    with pytest.raises(ValueError, match="several timestamp columns"):
        client.timeseries.insert_dataframe_binary(two)
    with pytest.raises(TypeError, match="__arrow_c_stream__"):
        client.timeseries.insert_dataframe_binary([1, 2, 3])


# --- live ---------------------------------------------------------------------------------------

START = datetime.datetime(2024, 3, 1, tzinfo=UTC)


def test_a_wide_table_round_trips_through_the_api(sync_client, make_ts):
    temp, count = make_ts(value_type="float"), make_ts(value_type="bigint")
    n = 150_000  # past one frame's 100 000 rows
    stamps = pl.datetime_range(START, START + datetime.timedelta(seconds=n - 1), "1s", time_zone="UTC", eager=True)
    df = pl.DataFrame({
        "timestamp": stamps,
        temp.external_id: pl.Series([i * 0.5 for i in range(n)]),
        # Every third point missing: a ragged column, its nulls left out.
        count.external_id: pl.Series([None if i % 3 == 0 else i for i in range(n)], dtype=pl.Int64),
    })

    sent = sync_client.timeseries.insert_dataframe_binary(df)
    assert sent == n + n - (n + 2) // 3

    end = START + datetime.timedelta(seconds=n)
    filters = [intellistream_datahub_sdk.RetrieveFilter(ts=s, start=START, end=end) for s in (temp, count)]

    from polling import poll_until
    back = poll_until(
        lambda: sync_client.timeseries.retrieve_datapoints_binary(filters),
        lambda read: sum(len(s) for s in read) == sent,
    ).to_polars(wide=True)
    assert back.equals(df)

    # What came back goes back in as it is.
    assert sync_client.timeseries.insert_dataframe_binary(back) == sent
