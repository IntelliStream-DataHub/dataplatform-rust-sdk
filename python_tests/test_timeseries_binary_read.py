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
import struct
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
    assert len(client.timeseries.retrieve_datapoints_binary(_filter())) == 0


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


# --- streaming, offline -----------------------------------------------------------------------


def _frames(body):
    """The frames of a response body, each as its own bytes, without the end marker."""
    frames, pos = [], 0
    while body[pos:pos + 4] == b"DHDP":
        directory, payload = struct.unpack_from("<II", body, pos + 16)
        end = pos + 28 + directory + payload
        frames.append(body[pos:end])
        pos = end
    return frames


VALUES = _frames((FIXTURES / "read_values.bin").read_bytes())
MIXED = VALUES[6:8]  # rd_mixed and rd_mixed_numbers: two series with the same columns


class _Chunked(http.server.BaseHTTPRequestHandler):
    """Answers with a chunked body, flushing each chunk; the chunk at `gate_after` waits for `gate`."""
    protocol_version = "HTTP/1.1"
    chunks = []
    gate_after = None
    gate = threading.Event()
    status = 200
    content_type = MEDIA_TYPE

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        cls = type(self)
        self.send_response(cls.status)
        self.send_header("Content-Type", cls.content_type)
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        for i, chunk in enumerate(cls.chunks):
            if i == cls.gate_after:
                cls.gate.wait(10)
            self.wfile.write(b"%x\r\n%s\r\n" % (len(chunk), chunk))
            self.wfile.flush()
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()

    def log_message(self, *args):
        pass


@pytest.fixture
def chunked():
    _Chunked.chunks, _Chunked.gate_after, _Chunked.status, _Chunked.content_type = [], None, 200, MEDIA_TYPE
    _Chunked.gate = threading.Event()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Chunked)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    client = intellistream_datahub_sdk.DataHubClient(
        base_url=f"http://127.0.0.1:{server.server_address[1]}", token="test-token"
    )
    yield client, _Chunked
    _Chunked.gate.set()
    server.shutdown()


def test_iterating_yields_a_chunk_per_frame_as_it_arrives(chunked):
    client, server = chunked
    server.chunks = [VALUES[0], b"".join(VALUES[1:]) + b"DHDE"]
    server.gate_after = 1  # nothing after the first frame until the test has it

    reader = client.timeseries.retrieve_datapoints_binary_stream([_filter("rd_bigint")])
    first = next(reader)
    assert (first.external_id, len(first)) == ("rd_bigint", 3)
    server.gate.set()
    rest = list(reader)

    assert [c.external_id for c in rest] == ["rd_float", "rd_float32", "rd_numeric", "rd_decimal32", "rd_text", "rd_mixed", "rd_mixed_numbers"]
    assert list(reader.external_ids.values())[:2] == ["rd_bigint", "rd_float"]
    assert rest[4].value_type == "text"  # mixed value types are fine when iterating
    assert list(reader) == []


def test_the_arrow_stream_is_one_long_table(chunked):
    pl = pytest.importorskip("polars")
    client, server = chunked
    server.chunks = MIXED + [b"DHDE"]

    reader = client.timeseries.retrieve_datapoints_binary_stream(_filter("rd_mixed"))
    df = pl.DataFrame(reader)

    assert df.columns == ["timeseries_id", "timestamp", "value_numeric", "value_text"]
    assert df["timeseries_id"].to_list() == [17, 17, 17, 18, 18]
    assert reader.external_ids == {17: "rd_mixed", 18: "rd_mixed_numbers"}
    assert df["timestamp"].dtype == pl.Datetime("ms", "UTC")
    assert df["value_text"].to_list() == [None, "fault", None, None, None]


def test_a_reader_is_read_once(chunked):
    pl = pytest.importorskip("polars")
    client, server = chunked
    server.chunks = MIXED + [b"DHDE"]
    reader = client.timeseries.retrieve_datapoints_binary_stream(_filter("rd_mixed"))
    assert reader.to_polars().height == 5
    with pytest.raises(RuntimeError, match="already been read"):
        reader.to_polars()

    reader = client.timeseries.retrieve_datapoints_binary_stream(_filter("rd_mixed"))
    next(reader)
    with pytest.raises(RuntimeError, match="already been read"):
        pl.DataFrame(reader)


def test_series_of_different_value_types_cannot_share_the_long_table(chunked):
    pl = pytest.importorskip("polars")
    client, server = chunked
    server.chunks = VALUES[:2] + [b"DHDE"]  # bigint, then float
    with pytest.raises(Exception, match="rd_float has columns"):
        pl.DataFrame(client.timeseries.retrieve_datapoints_binary_stream(_filter()))


def test_no_rows_iterates_as_nothing_and_has_no_arrow_schema(chunked):
    pl = pytest.importorskip("polars")
    client, server = chunked
    server.chunks = [b"DHDE"]
    assert list(client.timeseries.retrieve_datapoints_binary_stream(_filter())) == []
    with pytest.raises(ValueError, match="no rows"):
        pl.DataFrame(client.timeseries.retrieve_datapoints_binary_stream(_filter()))


def test_a_stream_cut_short_raises_after_the_chunks_that_arrived(chunked):
    client, server = chunked
    server.chunks = VALUES[:2]  # two whole frames, no end marker
    reader = client.timeseries.retrieve_datapoints_binary_stream(_filter())
    assert [c.external_id for c in (next(reader), next(reader))] == ["rd_bigint", "rd_float"]
    with pytest.raises(DataHubException, match="cut short") as excinfo:
        next(reader)
    assert excinfo.value.status_code == 502


def test_a_refusal_is_raised_before_there_is_a_reader(chunked):
    client, server = chunked
    server.status, server.content_type = 400, "application/problem+json"
    server.chunks = [b'{"type":"https://intellistream.ai/errors/bad-request","status":400,"detail":"no cursors here"}']
    with pytest.raises(DataHubException, match="no cursors") as excinfo:
        client.timeseries.retrieve_datapoints_binary_stream(_filter())
    assert excinfo.value.status_code == 400


# --- one table, buffered or streamed --------------------------------------------------------


def _both(client, server, frames):
    """The same body read buffered and streamed."""
    server.chunks = frames + [b"DHDE"]
    buffered = client.timeseries.retrieve_datapoints_binary(_filter())
    return buffered, lambda: client.timeseries.retrieve_datapoints_binary_stream(_filter())


def test_buffered_and_streamed_give_the_same_tables(chunked):
    pl = pytest.importorskip("polars")
    client, server = chunked
    buffered, streamed = _both(client, server, MIXED)

    assert len(buffered) == 2 and buffered[-1].external_id == "rd_mixed_numbers"
    assert buffered.external_ids == {17: "rd_mixed", 18: "rd_mixed_numbers"}
    long = buffered.to_polars()
    assert long.columns == ["timeseries_id", "timestamp", "value_numeric", "value_text"]
    assert long.equals(streamed().to_polars())
    assert long.equals(pl.DataFrame(buffered))
    assert buffered.to_polars(wide=True).equals(streamed().to_polars(wide=True))


def test_wide_names_several_value_columns_after_their_series(chunked):
    pl = pytest.importorskip("polars")
    client, server = chunked
    buffered, _ = _both(client, server, MIXED)

    wide = buffered.to_polars(wide=True)

    # rd_mixed has three rows, rd_mixed_numbers two: matched on timestamp, the gap left null.
    assert wide.columns == [
        "timestamp",
        "rd_mixed.value_numeric", "rd_mixed.value_text",
        "rd_mixed_numbers.value_numeric", "rd_mixed_numbers.value_text",
    ]
    assert wide["timestamp"].to_list() == [T0 + datetime.timedelta(seconds=s) for s in range(3)]
    assert wide["rd_mixed.value_text"].to_list() == [None, "fault", None]
    assert wide["rd_mixed_numbers.value_numeric"].to_list() == [1.0, 2.0, None]


def test_wide_holds_series_of_different_value_types(chunked):
    pl = pytest.importorskip("polars")
    client, server = chunked
    buffered, streamed = _both(client, server, VALUES[:2] + [VALUES[5]])  # bigint, float, text

    with pytest.raises(ValueError, match="do not share one long table"):
        buffered.to_polars()
    wide = buffered.to_polars(wide=True)
    assert wide.columns == ["timestamp", "rd_bigint", "rd_float", "rd_text"]
    assert wide.schema["rd_bigint"] == pl.Int64 and wide.schema["rd_text"] == pl.String
    assert wide["rd_bigint"].to_list() == [1, 42, -9_000_000_000]
    assert wide["rd_float"].to_list() == [1.5, -2.25, None]
    assert wide["rd_text"].to_list() == ["open", "blåbær", None]
    assert wide.equals(streamed().to_polars(wide=True))


def test_wide_of_no_rows_raises_like_the_long_table(chunked):
    pytest.importorskip("polars")
    client, server = chunked
    buffered, streamed = _both(client, server, [])
    for read in (buffered, streamed()):
        with pytest.raises(ValueError, match="no rows"):
            read.to_polars(wide=True)


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


def test_streaming_read_matches_the_buffered_read(sync_client, make_ts, binary_read):
    pl = pytest.importorskip("polars")
    ts = make_ts(value_type="float")
    values = [i * 0.5 - 30.0 for i in range(len(TIMESTAMPS))]
    sync_client.timeseries.insert_from_lists_binary(TIMESTAMPS, values, ts)
    _wait_for(sync_client, ts, len(TIMESTAMPS))
    rf = intellistream_datahub_sdk.RetrieveFilter(ts=ts, start=START, limit=10_000)

    (buffered,) = binary_read(rf)
    chunks = list(sync_client.timeseries.retrieve_datapoints_binary_stream(rf))
    reader = sync_client.timeseries.retrieve_datapoints_binary_stream(rf)
    long = reader.to_polars()

    assert pl.concat([c.to_polars() for c in chunks]).equals(buffered.to_polars())
    assert long.select("timestamp", "value").equals(buffered.to_polars())
    assert reader.external_ids == {buffered.id: ts.external_id}


def test_wide_from_series_on_the_same_and_on_different_timestamps(sync_client, make_ts, binary_read):
    pl = pytest.importorskip("polars")
    a, b, c = (make_ts(value_type="float") for _ in range(3))
    sync_client.timeseries.insert_from_lists_binary(TIMESTAMPS, [float(i) for i in range(len(TIMESTAMPS))], a)
    sync_client.timeseries.insert_from_lists_binary(TIMESTAMPS, [-float(i) for i in range(len(TIMESTAMPS))], b)
    every_other = TIMESTAMPS[1::2]
    sync_client.timeseries.insert_from_lists_binary(every_other, [0.5] * len(every_other), c)
    for ts, n in ((a, len(TIMESTAMPS)), (b, len(TIMESTAMPS)), (c, len(every_other))):
        _wait_for(sync_client, ts, n)
    rf = lambda *series: [intellistream_datahub_sdk.RetrieveFilter(ts=s, start=START, limit=10_000) for s in series]

    # Same timestamps: the columns side by side.
    same = sync_client.timeseries.retrieve_datapoints_binary(rf(a, b)).to_polars(wide=True)
    assert same.columns == ["timestamp", a.external_id, b.external_id]
    assert same["timestamp"].to_list() == TIMESTAMPS
    assert same[b.external_id].to_list() == [-float(i) for i in range(len(TIMESTAMPS))]

    # Different timestamps: matched up, the gaps null; streamed gives the same table.
    mixed = sync_client.timeseries.retrieve_datapoints_binary(rf(a, c)).to_polars(wide=True)
    assert mixed["timestamp"].to_list() == TIMESTAMPS
    assert mixed[c.external_id].to_list() == [None, 0.5] * (len(TIMESTAMPS) // 2)
    assert mixed.equals(sync_client.timeseries.retrieve_datapoints_binary_stream(rf(a, c)).to_polars(wide=True))
