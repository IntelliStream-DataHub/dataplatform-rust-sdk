"""Durable buffering and event UUID v7 tests for the Python bindings.

The first two tests run *offline*: the client points at an unreachable port, so datapoint and event
ingestion buffer to disk instead of raising. The last test needs a live backend (a `.env` with
BASE_URL + auth) and is skipped otherwise.

Run via the repo's wrapper so the bindings are rebuilt first:
    ./run_python_tests.sh python_tests/test_buffering.py
"""

import os
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

import datahub_sdk
import pytest

ENV_FILE = os.path.join(os.path.dirname(__file__), "..", ".env")


def _unreachable_buffered_client(buffer_dir: Path) -> "datahub_sdk.DataHubClient":
    """A client whose base URL refuses connections, with durable buffering pointed at buffer_dir."""
    return datahub_sdk.DataHubClient(
        base_url="http://127.0.0.1:9",
        token="dummy-token",
        enable_buffering=True,
        buffer_dir=str(buffer_dir),
        buffer_retention_secs=3600,
    )


def test_event_buffers_to_disk_with_uuid_v7(tmp_path):
    client = _unreachable_buffered_client(tmp_path)

    # Server unreachable -> create() buffers instead of raising, and confirms no items.
    result = client.events.create(
        [datahub_sdk.Event(external_id="py_buffer_event", event_time=datetime.now(timezone.utc))]
    )
    assert result == []

    events_dir = tmp_path / "events"
    assert events_dir.is_dir(), "event spool directory should exist"

    # The active segment is plain `<ts>\t<json>` NDJSON; find the stamped id and check its version.
    content = ""
    for f in events_dir.glob("*.ndjson"):
        content = f.read_text()
    idx = content.find('"id":"')
    assert idx != -1, "spooled event should carry a stamped id"
    uuid = content[idx + 6 : idx + 6 + 36]
    # In `xxxxxxxx-xxxx-Vxxx-...`, the version nibble V is at index 14.
    assert uuid[14] == "7", f"expected a time-ordered UUID v7 id, got {uuid}"


def test_event_without_event_time_is_rejected():
    # event_time is when the event occurred; there is no now() default. An Event without one is
    # unrepresentable, so this fails at construction rather than at send. Rust callers get a compile
    # error; Python is the only surface where this needs a test.
    with pytest.raises(TypeError):
        datahub_sdk.Event(external_id="py_no_time_event")

    ev = datahub_sdk.Event(
        external_id="py_no_time_event", event_time=datetime.now(timezone.utc)
    )
    with pytest.raises(TypeError):
        ev.event_time = None


def test_datapoints_buffer_to_disk(tmp_path):
    client = _unreachable_buffered_client(tmp_path)

    result = client.timeseries.insert_from_lists(
        timestamps=[datetime.now(timezone.utc)],
        values=[1.0],
        ts="py_buffer_series",
    )
    assert result == []
    assert (tmp_path / "datapoints").is_dir(), "datapoint spool directory should exist"


@pytest.mark.skipif(
    not os.path.exists(ENV_FILE),
    reason="needs a live backend (.env with BASE_URL + auth)",
)
def test_live_event_gets_uuid_v7():
    client = datahub_sdk.DataHubClient.from_envfile(ENV_FILE)
    try:
        created = client.events.create(
            [datahub_sdk.Event(external_id="py_uuid_v7_event", event_time=datetime.now(timezone.utc))]
        )
        assert len(created) == 1
        assert isinstance(created[0].id, UUID)  # binding returns a real uuid.UUID
        assert created[0].id.version == 7
    finally:
        # Events get a fresh uuid per create, so same-external_id inserts pile up
        # instead of overwriting. Delete by external id to remove every copy so
        # re-runs don't accumulate orphaned events.
        try:
            client.events.delete(["py_uuid_v7_event"])
        except Exception:
            pass
