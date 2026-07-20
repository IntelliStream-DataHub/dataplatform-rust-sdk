"""Timezone acceptance for datetime inputs across the Python bindings.

These are pure in-process tests (no backend / no .env): they only construct SDK
objects and read the normalized value back, exercising the
`datahub_python_bindings/src/datetime.rs` helper that every datetime input now goes
through. Any timezone-aware datetime — UTC, a fixed offset, or a `ZoneInfo` named zone —
must be accepted and normalized to the same UTC instant; naive datetimes must be rejected.
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import datahub_sdk
import numpy as np
import pandas as pd
import pytest

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
