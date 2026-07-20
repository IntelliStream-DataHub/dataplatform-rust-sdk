"""Timezone handling for datetime inputs across the Python bindings.

Two groups live here:

* In-process tests (the bulk, no backend / no .env) construct SDK objects and read the
  normalized value back, exercising the `datahub_python_bindings/src/datetime.rs` helper
  every datetime input goes through. Any timezone-aware datetime — UTC, a fixed offset, or
  a `ZoneInfo` named zone — must be accepted and normalized to the same UTC instant; naive
  datetimes (and non-datetime types) must be rejected.
* Backend round-trip tests (near the bottom, marked with the `sync_client` fixture)
  confirm a non-UTC offset survives the full write -> read (and delete) cycle: it must land
  as the correct absolute UTC instant, not its naive wall-clock reading.
"""

from datetime import datetime, timedelta, timezone
from time import sleep
from zoneinfo import ZoneInfo

import datahub_sdk
import numpy as np
import pandas as pd
import pytest

from fixtures import (  # noqa: F401  (fixtures are used by name via pytest injection)
    make_dataset,
    make_events,
    make_ts,
    sync_client,
    unique_id,
)

UTC = timezone.utc
OSLO = ZoneInfo("Europe/Oslo")  # DST-aware: +01:00 in winter (CET), +02:00 in summer (CEST)
PLUS2 = timezone(timedelta(hours=2))
MINUS5 = timezone(timedelta(hours=-5))

# All of these denote the same instant: 2025-01-01 12:00:00 UTC. The list spans stdlib
# datetimes and pandas Timestamps (a datetime subclass), across UTC / fixed-offset / named
# zones, so every input path is exercised with real data-science timestamp types.
UTC_NOON = datetime(2025, 1, 1, 12, 0, tzinfo=timezone.utc)
EQUIVALENT_AWARE = [
    UTC_NOON,  # tzinfo == timezone.utc
    datetime(2025, 1, 1, 14, 0, tzinfo=timezone(timedelta(hours=2))),  # fixed +02:00
    datetime(2025, 1, 1, 13, 0, tzinfo=ZoneInfo("Europe/Oslo")),  # named zone, winter = +01:00
    datetime(2025, 1, 1, 7, 0, tzinfo=ZoneInfo("America/New_York")),  # named zone, winter = -05:00
    pd.Timestamp("2025-01-01 12:00", tz="UTC"),
    pd.Timestamp("2025-01-01 14:00+02:00"),  # fixed offset
    pd.Timestamp("2025-01-01 13:00", tz="Europe/Oslo"),  # named zone
    pd.date_range("2025-01-01 12:00", periods=1, tz="UTC")[0],  # DatetimeIndex element
]


@pytest.mark.parametrize("dt", EQUIVALENT_AWARE)
def test_event_constructor_accepts_any_timezone(dt):
    ev = datahub_sdk.Event(external_id="tz_evt", event_time=dt)
    assert ev.event_time == UTC_NOON


@pytest.mark.parametrize("dt", EQUIVALENT_AWARE)
def test_event_time_setter_accepts_any_timezone(dt):
    ev = datahub_sdk.Event(external_id="tz_evt", event_time=UTC_NOON)
    ev.event_time = dt
    assert ev.event_time == UTC_NOON


def test_event_rejects_naive_datetime():
    with pytest.raises(TypeError):
        datahub_sdk.Event(external_id="tz_evt", event_time=datetime(2025, 1, 1, 12, 0))


def test_event_setter_rejects_naive_datetime():
    ev = datahub_sdk.Event(external_id="tz_evt", event_time=UTC_NOON)
    with pytest.raises(TypeError):
        ev.event_time = datetime(2025, 1, 1, 12, 0)


@pytest.mark.parametrize("dt", EQUIVALENT_AWARE)
def test_time_filter_accepts_any_timezone(dt):
    # A single bound (start only) -> TimeFilter::After; must not raise on any tz.
    datahub_sdk.TimeFilter(start=dt)


def test_time_filter_mixed_timezones_compare_correctly():
    # start (Oslo 13:00 = 12:00Z) is before end (New York 08:00 = 13:00Z): valid range.
    start = datetime(2025, 1, 1, 13, 0, tzinfo=ZoneInfo("Europe/Oslo"))
    end = datetime(2025, 1, 1, 8, 0, tzinfo=ZoneInfo("America/New_York"))
    datahub_sdk.TimeFilter(start=start, end=end)


@pytest.mark.parametrize("dt", EQUIVALENT_AWARE)
def test_datapoint_string_accepts_any_timezone(dt):
    # DatapointString stores the timestamp as an epoch-millis string.
    expected_ms = str(int(UTC_NOON.timestamp() * 1000))
    dp = datahub_sdk.DatapointString(dt, "1.0")
    assert dp.timestamp == expected_ms


def test_datapoint_string_rejects_naive_datetime():
    with pytest.raises(TypeError):
        datahub_sdk.DatapointString(datetime(2025, 1, 1, 12, 0), "1.0")


@pytest.mark.parametrize("dt", EQUIVALENT_AWARE)
def test_retrieve_filter_accepts_any_timezone(dt):
    rf = datahub_sdk.RetrieveFilter("some_external_id", start=dt, end=dt)
    assert rf.start == UTC_NOON
    assert rf.end == UTC_NOON


# --- rejections: types that carry no unambiguous instant --------------------- #


def test_naive_pandas_timestamp_is_rejected():
    # pandas Timestamp subclasses datetime, so a naive one hits the same naive guard.
    with pytest.raises(TypeError):
        datahub_sdk.Event(external_id="tz_evt", event_time=pd.Timestamp("2025-01-01 12:00"))


def test_pandas_nat_is_rejected():
    with pytest.raises(TypeError):
        datahub_sdk.Event(external_id="tz_evt", event_time=pd.NaT)


def test_numpy_datetime64_is_rejected_with_helpful_error():
    # numpy datetime64 is not a datetime and has no timezone; it must be rejected with a
    # message that points at the fix, not an opaque AttributeError.
    with pytest.raises(TypeError) as exc:
        datahub_sdk.Event(external_id="tz_evt", event_time=np.datetime64("2025-01-01T12:00"))
    assert "datetime64" in str(exc.value) or "pd.Timestamp" in str(exc.value)


def test_numpy_datetime64_converted_via_pandas_is_accepted():
    # The documented workaround round-trips to the right instant.
    dt = pd.Timestamp(np.datetime64("2025-01-01T12:00")).tz_localize("UTC")
    ev = datahub_sdk.Event(external_id="tz_evt", event_time=dt)
    assert ev.event_time == UTC_NOON


# --- daylight saving time (a named zone's offset depends on the date) -------- #


def test_dst_named_zone_resolves_offset_by_date():
    # The SAME wall-clock time in Europe/Oslo maps to DIFFERENT UTC instants in winter vs
    # summer (+01:00 vs +02:00). A fixed offset can't do this — it proves the zone's DST
    # rules are applied, i.e. the offset is resolved per-date, not frozen.
    winter = datetime(2025, 1, 15, 12, 0, tzinfo=OSLO)  # +01:00 -> 11:00Z
    summer = datetime(2025, 7, 15, 12, 0, tzinfo=OSLO)  # +02:00 -> 10:00Z
    assert datahub_sdk.Event(external_id="w", event_time=winter).event_time == datetime(2025, 1, 15, 11, 0, tzinfo=UTC)
    assert datahub_sdk.Event(external_id="s", event_time=summer).event_time == datetime(2025, 7, 15, 10, 0, tzinfo=UTC)


def test_dst_fall_back_ambiguous_hour_respects_fold():
    # On 2025-10-26 Oslo falls back 03:00->02:00, so 02:30 local occurs twice. PEP 495
    # `fold` disambiguates: fold=0 is the first pass (still +02:00), fold=1 the second (+01:00).
    first = datetime(2025, 10, 26, 2, 30, tzinfo=OSLO, fold=0)   # +02:00 -> 00:30Z
    second = datetime(2025, 10, 26, 2, 30, tzinfo=OSLO, fold=1)  # +01:00 -> 01:30Z
    assert datahub_sdk.Event(external_id="f0", event_time=first).event_time == datetime(2025, 10, 26, 0, 30, tzinfo=UTC)
    assert datahub_sdk.Event(external_id="f1", event_time=second).event_time == datetime(2025, 10, 26, 1, 30, tzinfo=UTC)


# --------------------------------------------------------------------------- #
# Backend round-trips (require a live backend / .env).
#
# The tests above prove the bindings normalize any input to the right UTC instant before
# sending. These prove that instant survives the backend write -> read (and delete) cycle.
# 2025-06-01 is summer time, so Europe/Oslo and a literal +02:00 are both UTC+2.
# --------------------------------------------------------------------------- #


def _retrieve_with_retry(sync_client, ts, start, end, attempts=15, delay=1.0):
    """Poll retrieve_datapoints until points appear (ClickHouse ingest lag)."""
    for _ in range(attempts):
        dps = sync_client.timeseries.retrieve_datapoints(
            datahub_sdk.RetrieveFilter(ts=ts, start=start, end=end, limit=1000)
        )[0].get_datapoints()
        if dps:
            return dps
        sleep(delay)
    return []


def _by_ids_with_retry(sync_client, event, attempts=15, delay=1.0):
    """Poll events.by_ids until the freshly-created event is visible (index lag)."""
    for _ in range(attempts):
        fetched = sync_client.events.by_ids([event])
        if fetched:
            return fetched
        sleep(delay)
    return []


def test_datapoint_non_utc_offset_survives_roundtrip(sync_client, make_ts):
    ts = make_ts(name="tz roundtrip")

    # (input stamped in a non-UTC zone, value, the UTC instant it denotes)
    cases = [
        (datetime(2025, 6, 1, 12, 0, tzinfo=PLUS2), 10.0, datetime(2025, 6, 1, 10, 0, tzinfo=UTC)),
        (datetime(2025, 6, 1, 9, 0, tzinfo=MINUS5), 14.0, datetime(2025, 6, 1, 14, 0, tzinfo=UTC)),
        (datetime(2025, 6, 1, 18, 0, tzinfo=OSLO), 16.0, datetime(2025, 6, 1, 16, 0, tzinfo=UTC)),
    ]
    timestamps = [c[0] for c in cases]
    values = [c[1] for c in cases]

    sync_client.timeseries.insert_from_lists(timestamps=timestamps, values=values, ts=ts)

    dps = _retrieve_with_retry(
        sync_client, ts,
        start=datetime(2025, 6, 1, tzinfo=UTC),
        end=datetime(2025, 6, 2, tzinfo=UTC),
    )
    assert dps, "no datapoints came back after insert"

    got = {dp.timestamp: dp.value for dp in dps}
    for input_dt, value, expected_utc in cases:
        # datetime equality/hashing is by absolute instant for aware datetimes, so a
        # correctly-stored point keys on its UTC instant regardless of the input zone.
        assert expected_utc in got, (
            f"input {input_dt.isoformat()} should store as {expected_utc.isoformat()}; "
            f"got instants {sorted(t.isoformat() for t in got)}"
        )
        assert got[expected_utc] == value


def test_event_non_utc_offset_survives_roundtrip(sync_client, make_dataset, make_events):
    ds = make_dataset()
    event_time = datetime(2025, 6, 1, 18, 0, tzinfo=OSLO)  # summer = +02:00 -> 16:00Z
    expected_utc = datetime(2025, 6, 1, 16, 0, tzinfo=UTC)

    ev = datahub_sdk.Event(
        external_id=unique_id("tz_evt"),
        event_time=event_time,
        data_set_id=ds.id,
    )
    make_events([ev])

    fetched = _by_ids_with_retry(sync_client, ev)
    assert fetched, "event not found after create"
    assert fetched[0].event_time == expected_utc


def test_dst_offset_survives_roundtrip(sync_client, make_ts):
    # Same wall-clock time in a DST zone, winter vs summer, must land as two DISTINCT UTC
    # instants after the backend cycle (11:00Z vs 10:00Z) — the zone's DST rules, not a
    # single frozen offset, applied end to end.
    ts = make_ts(name="tz dst roundtrip")
    winter = datetime(2025, 1, 15, 12, 0, tzinfo=OSLO)  # +01:00 -> 11:00Z
    summer = datetime(2025, 7, 15, 12, 0, tzinfo=OSLO)  # +02:00 -> 10:00Z

    sync_client.timeseries.insert_from_lists(
        timestamps=[winter, summer], values=[1.0, 2.0], ts=ts
    )
    dps = _retrieve_with_retry(
        sync_client, ts,
        start=datetime(2025, 1, 1, tzinfo=UTC),
        end=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert dps, "no datapoints came back after insert"

    got = {dp.timestamp: dp.value for dp in dps}
    assert got.get(datetime(2025, 1, 15, 11, 0, tzinfo=UTC)) == 1.0
    assert got.get(datetime(2025, 7, 15, 10, 0, tzinfo=UTC)) == 2.0


def test_delete_datapoints_non_utc_boundary(sync_client, make_ts):
    ts = make_ts(name="tz delete boundary")

    # Delete boundary given in +02:00: 2025-06-01T12:00:00+02:00 == 10:00:00Z.
    boundary = datetime(2025, 6, 1, 12, 0, tzinfo=PLUS2)
    before = datetime(2025, 6, 1, 9, 0, tzinfo=UTC)   # 09:00Z, strictly before boundary
    after = datetime(2025, 6, 1, 11, 0, tzinfo=UTC)   # 11:00Z, at/after boundary

    sync_client.timeseries.insert_from_lists(
        timestamps=[before, after], values=[1.0, 2.0], ts=ts
    )
    inserted = _retrieve_with_retry(
        sync_client, ts,
        start=datetime(2025, 6, 1, tzinfo=UTC),
        end=datetime(2025, 6, 2, tzinfo=UTC),
    )
    assert len(inserted) == 2, "both points should exist before the delete"

    sync_client.timeseries.delete_datapoints(
        [datahub_sdk.DeleteFilter(ts=ts, inclusive_begin=boundary)]
    )
    sleep(90)  # ClickHouse delete latency

    remaining = {
        dp.timestamp
        for dp in sync_client.timeseries.retrieve_datapoints(
            datahub_sdk.RetrieveFilter(
                ts=ts,
                start=datetime(2025, 6, 1, tzinfo=UTC),
                end=datetime(2025, 6, 2, tzinfo=UTC),
                limit=1000,
            )
        )[0].get_datapoints()
    }
    # inclusive_begin resolves to 10:00Z, so the 11:00Z point is deleted and 09:00Z stays.
    assert before in remaining
    assert after not in remaining
