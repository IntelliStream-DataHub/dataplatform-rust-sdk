"""Binary datapoint read (`POST /timeseries/data/list/binary`) through the Python bindings.

Two halves. The offline one answers the client from a stand-in server with response bodies the
platform's own Java frame writer produced (`src/timeseries/fixtures/`), so it checks the binding
and the Arrow PyCapsule stream without a backend. The live one round-trips through a backend and
skips when that backend does not serve the endpoint yet.
"""
import datetime
import decimal
import http.server
import pathlib
import threading

import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException
from polling import poll_until
from python_tests.fixtures import *  # noqa: F401,F403

FIXTURES = pathlib.Path(__file__).resolve().parent.parent / "src" / "timeseries" / "fixtures"
T0 = datetime.datetime.fromtimestamp(1_700_000_000, tz=datetime.timezone.utc)
MEDIA_TYPE = "application/vnd.intellistream.datapoint-block"


class _StandIn(http.server.BaseHTTPRequestHandler):
    status = 200
    body = b""
    content_type = MEDIA_TYPE
    seen = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        type(self).seen.append((self.path, self.headers.get("Accept"), self.rfile.read(length)))
        self.send_response(type(self).status)
        self.send_header("Content-Type", type(self).content_type)
        self.send_header("Content-Length", str(len(type(self).body)))
        self.end_headers()
        self.wfile.write(type(self).body)

    def log_message(self, *args):
        pass


@pytest.fixture
def stand_in():
    _StandIn.status, _StandIn.content_type, _StandIn.seen = 200, MEDIA_TYPE, []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _StandIn)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    client = intellistream_datahub_sdk.DataHubClient(
        base_url=f"http://127.0.0.1:{server.server_address[1]}", token="test-token"
    )
    yield client, _StandIn
    server.shutdown()


def _filter(ts="rd_float"):
    return intellistream_datahub_sdk.RetrieveFilter(ts=ts, start=T0, limit=1000)


def test_every_value_type_converts_through_the_arrow_stream(stand_in):
    pl = pytest.importorskip("polars")
    client, server = stand_in
    server.body = (FIXTURES / "read_values.bin").read_bytes()

    series = client.timeseries.retrieve_datapoints_binary([_filter("rd_bigint"), _filter("rd_float")])

    path, accept, _ = server.seen[0]
    assert path == "/timeseries/data/list/binary"
    assert accept.startswith(MEDIA_TYPE) and "application/problem+json" in accept
    assert [s.external_id for s in series] == [
        "rd_bigint", "rd_float", "rd_float32", "rd_numeric", "rd_decimal32", "rd_text", "rd_mixed", "rd_mixed_numbers",
    ]
    by_name = {s.external_id: s for s in series}

    bigint = by_name["rd_bigint"]
    assert (bigint.id, bigint.value_type, len(bigint), bigint.is_aggregates) == (11, "bigint", 3, False)
    assert bigint.columns == ["timestamp", "value"]
    frame = pl.DataFrame(bigint)
    assert frame["value"].to_list() == [1, 42, -9_000_000_000]
    assert frame["timestamp"].dtype == pl.Datetime("ms", "UTC")
    assert frame["timestamp"][0] == T0
    # A stream is made per call, so one object converts more than once.
    assert bigint.to_polars().equals(frame)

    assert pl.DataFrame(by_name["rd_float"])["value"].to_list() == [1.5, -2.25]
    assert pl.DataFrame(by_name["rd_float32"])["value"].dtype == pl.Float32
    numeric = pl.DataFrame(by_name["rd_numeric"])["value"]
    assert numeric.dtype == pl.Decimal(18, 6)
    assert numeric.to_list() == [decimal.Decimal("123456789012.345678"), decimal.Decimal("-0.000001")]
    assert pl.DataFrame(by_name["rd_decimal32"])["value"].dtype == pl.Decimal(9, 4)
    assert pl.DataFrame(by_name["rd_text"])["value"].to_list() == ["open", "blåbær"]

    mixed = by_name["rd_mixed"]
    assert mixed.columns == ["timestamp", "value_numeric", "value_text"]
    mixed_frame = pl.DataFrame(mixed)
    assert mixed_frame["value_numeric"].to_list() == [3.5, None, -7.0]
    assert mixed_frame["value_text"].to_list() == [None, "fault", None]


def test_aggregates_convert_to_pandas(stand_in):
    pytest.importorskip("pyarrow")
    pytest.importorskip("pandas")
    client, server = stand_in
    server.body = (FIXTURES / "read_aggregates.bin").read_bytes()

    (series,) = client.timeseries.retrieve_datapoints_binary(_filter("rd_aggregates"))

    assert series.is_aggregates and series.value_type == "float"
    assert series.columns == ["timestamp", "avg", "min", "max", "sum"]
    frame = series.to_pandas()
    assert frame["avg"].tolist() == [1.5, 2.5, 3.5]
    assert frame["sum"].tolist() == [6.0, 10.0, 14.0]
    assert frame["timestamp"].iloc[1] == T0 + datetime.timedelta(hours=1)


def test_no_points_is_no_series(stand_in):
    client, server = stand_in
    server.body = b"DHDE"
    assert client.timeseries.retrieve_datapoints_binary(_filter()) == []


def test_a_response_cut_short_raises_rather_than_returning_fewer_points(stand_in):
    client, server = stand_in
    full = (FIXTURES / "read_values.bin").read_bytes()
    server.body = full[:-4]  # every frame intact, only the end marker missing
    with pytest.raises(DataHubException, match="cut short") as excinfo:
        client.timeseries.retrieve_datapoints_binary(_filter())
    assert excinfo.value.status_code == 502


def test_a_refusal_is_the_problem_the_server_sent(stand_in):
    client, server = stand_in
    server.status, server.content_type = 400, "application/problem+json"
    server.body = (
        b'{"type":"https://intellistream.ai/errors/bad-request","title":"Bad Request","status":400,'
        b'"detail":"The binary read has no cursors; page by moving `start` past the last timestamp read."}'
    )
    with pytest.raises(DataHubException, match="no cursors") as excinfo:
        client.timeseries.retrieve_datapoints_binary(_filter())
    assert excinfo.value.status_code == 400


# --- live ---------------------------------------------------------------------------------------

START = datetime.datetime(2024, 3, 1, tzinfo=datetime.timezone.utc)
TIMESTAMPS = [START + datetime.timedelta(minutes=i) for i in range(120)]


@pytest.fixture
def binary_read(sync_client, make_ts):
    """Skips unless the backend serves the binary read: it is newer than the binary insert."""
    probe = intellistream_datahub_sdk.RetrieveFilter(ts=make_ts(value_type="float"), start=START, limit=1)
    try:
        sync_client.timeseries.retrieve_datapoints_binary(probe)
    except DataHubException as e:
        if e.status_code in (404, 405):
            pytest.skip("the backend does not serve POST /timeseries/data/list/binary")
        raise
    return sync_client.timeseries.retrieve_datapoints_binary


def _wait_for(client, ts, expected):
    rf = intellistream_datahub_sdk.RetrieveFilter(ts=ts, start=START, limit=10_000)
    poll_until(
        lambda: client.timeseries.retrieve_datapoints(rf),
        lambda collections: bool(collections) and len(collections[0].get_datapoints()) >= expected,
    )


def test_binary_read_matches_the_json_read(sync_client, make_ts, binary_read):
    pl = pytest.importorskip("polars")
    ts = make_ts(value_type="float")
    values = [i * 0.5 - 30.0 for i in range(len(TIMESTAMPS))]
    sync_client.timeseries.insert_from_lists_binary(TIMESTAMPS, values, ts)
    _wait_for(sync_client, ts, len(TIMESTAMPS))

    (series,) = binary_read(intellistream_datahub_sdk.RetrieveFilter(ts=ts, start=START, limit=10_000))

    assert series.external_id == ts.external_id and series.value_type == "float"
    frame = series.to_polars()
    assert frame["timestamp"].to_list() == TIMESTAMPS
    assert frame["value"].to_list() == values


def test_binary_read_of_hourly_aggregates(sync_client, make_ts, binary_read):
    pytest.importorskip("pyarrow")
    ts = make_ts(value_type="float")
    sync_client.timeseries.insert_from_lists_binary(TIMESTAMPS, [float(i) for i in range(len(TIMESTAMPS))], ts)
    _wait_for(sync_client, ts, len(TIMESTAMPS))

    rf = intellistream_datahub_sdk.RetrieveFilter(
        ts=ts, start=START, end=TIMESTAMPS[-1] + datetime.timedelta(minutes=1),
        aggregates=["avg", "min", "max", "sum"], granularity="1h",
    )
    (series,) = binary_read(rf)
    json_buckets = sync_client.timeseries.retrieve_datapoints(rf)[0].get_datapoints()

    frame = series.to_pandas()
    assert series.is_aggregates and len(frame) == len(json_buckets) == 2
    # Zero-order hold with carry-in, so the second hour's min is the 59 carried into it.
    assert frame["min"].tolist() == [dp.min for dp in json_buckets]
    assert frame["max"].tolist() == [dp.max for dp in json_buckets]
    assert frame["sum"].tolist() == pytest.approx([dp.sum for dp in json_buckets])
    assert frame["avg"].tolist() == pytest.approx([dp.average for dp in json_buckets])


def test_binary_read_of_an_unknown_series_is_a_bad_request(make_ts, binary_read):
    missing = unique_id("ts_missing")
    with pytest.raises(DataHubException, match=missing) as excinfo:
        binary_read(intellistream_datahub_sdk.RetrieveFilter(ts=missing, start=START))
    assert excinfo.value.status_code == 400
