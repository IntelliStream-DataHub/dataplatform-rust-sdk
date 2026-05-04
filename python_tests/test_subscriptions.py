"""Tests for the Python subscriptions module.

Mirrors src/subscriptions/test.rs: a CRUD round-trip and a listen end-to-end test. The
end-to-end listen test needs the backend's Pulsar fan-out consumer running, so it is gated
behind RUN_LISTEN_TESTS=1 (matching the Rust `#[ignore]`).
"""
import os
import time
import uuid
from datetime import datetime, timezone

import datahub_sdk
import pandas as pd
import pytest

from fixtures import sync_client


def _suffix() -> str:
    return uuid.uuid4().hex[:8]


@pytest.fixture(scope="function")
def subscription_timeseries(sync_client):
    """Two timeseries the subscription will be bound to. Cleaned up after the test."""
    suffix = _suffix()
    ts_a_ext = f"sub_test_ts_a_{suffix}"
    ts_b_ext = f"sub_test_ts_b_{suffix}"
    ts_a = datahub_sdk.TimeSeries(
        external_id=ts_a_ext,
        name="Sub Test TS A",
        value_type="decimal",
        unit="Celsius",
        unit_external_id="temperature_deg_c",
    )
    ts_b = datahub_sdk.TimeSeries(
        external_id=ts_b_ext,
        name="Sub Test TS B",
        value_type="decimal",
        unit="Celsius",
        unit_external_id="temperature_deg_c",
    )
    sync_client.timeseries.create([ts_a, ts_b])
    yield ts_a_ext, ts_b_ext
    # Best-effort cleanup. delete() raises if both already gone in some races; swallow.
    try:
        sync_client.timeseries.delete([ts_a, ts_b])
    except Exception:
        pass


def test_create_list_delete(sync_client, subscription_timeseries):
    ts_a_ext, ts_b_ext = subscription_timeseries
    sub_ext = f"sub_test_{_suffix()}"

    sub = datahub_sdk.Subscription(
        external_id=sub_ext,
        name=f"Sub Test {sub_ext}",
        timeseries=[ts_a_ext, ts_b_ext],
    )

    try:
        # Create
        created = sync_client.subscriptions.create([sub])
        assert len(created) == 1
        assert created[0].external_id == sub_ext
        assert created[0].id is not None
        assert created[0].date_created is not None
        assert len(created[0].timeseries) == 2

        # Unfiltered list — backend may carry prior test data, so don't assert exact count.
        all_subs = sync_client.subscriptions.list()
        assert any(s.external_id == sub_ext for s in all_subs)

        # Filter by timeseries via kwargs.
        filtered = sync_client.subscriptions.list(timeseries=[ts_a_ext], limit=100)
        assert any(s.external_id == sub_ext for s in filtered)

        # Same call via an explicit retriever.
        retriever = datahub_sdk.SubscriptionRetriever(
            filter=datahub_sdk.SubscriptionFilter(timeseries=[ts_a_ext]),
            limit=100,
        )
        filtered_via_retriever = sync_client.subscriptions.list(retriever)
        assert any(s.external_id == sub_ext for s in filtered_via_retriever)

        # Delete and verify gone.
        sync_client.subscriptions.delete([sub_ext])
        time.sleep(0.5)
        after = sync_client.subscriptions.list(timeseries=[ts_a_ext])
        assert not any(s.external_id == sub_ext for s in after)
    finally:
        # Best-effort cleanup (delete may have already run in the happy path).
        try:
            sync_client.subscriptions.delete([sub_ext])
        except Exception:
            pass


def test_list_rejects_retriever_and_kwargs_together(sync_client):
    retriever = datahub_sdk.SubscriptionRetriever()
    with pytest.raises(ValueError):
        sync_client.subscriptions.list(retriever, limit=10)


def test_list_default_returns_list(sync_client):
    # Default retriever — caller hasn't passed anything. Should not raise; result type only.
    result = sync_client.subscriptions.list()
    assert isinstance(result, list)


def test_ws_datapoint_as_float():
    """Construct a Subscription so the binding is loaded; then exercise the as_float helper
    via a deserialized SubscriptionMessage we can build by sending a real datapoint over the
    listen stream — covered by the listen test below. Here we only sanity-check that the
    types are exposed."""
    assert hasattr(datahub_sdk, "WsDatapoint")
    assert hasattr(datahub_sdk, "SubscriptionMessage")
    assert hasattr(datahub_sdk, "SubscriptionListener")
    assert hasattr(datahub_sdk, "EventAction")
    assert hasattr(datahub_sdk, "EventObject")


# --- Listen end-to-end -----------------------------------------------------------------

# Skipped by default — needs the backend's Pulsar consumer running so REST datapoint writes
# fan out to the subscription topic. Set RUN_LISTEN_TESTS=1 to enable.
listen_enabled = os.environ.get("RUN_LISTEN_TESTS") == "1"


@pytest.mark.skipif(not listen_enabled, reason="set RUN_LISTEN_TESTS=1 to run live listen tests")
def test_listen_end_to_end(sync_client):
    suffix = _suffix()
    ts_ext = f"sub_listen_ts_{suffix}"
    sub_ext = f"sub_listen_{suffix}"

    ts = datahub_sdk.TimeSeries(
        external_id=ts_ext,
        name="Sub Listen TS",
        value_type="decimal",
        unit="Celsius",
        unit_external_id="temperature_deg_c",
    )
    sync_client.timeseries.create([ts])

    sub = datahub_sdk.Subscription(
        external_id=sub_ext,
        name=f"Sub Listen {suffix}",
        timeseries=[ts_ext],
    )
    sync_client.subscriptions.create([sub])

    received = None
    try:
        # Open the listener before writing — otherwise the fan-out fires before we connect.
        listener = sync_client.subscriptions.listen(sub_ext)

        # Write one datapoint to the bound timeseries.
        ts_obj = sync_client.timeseries.by_ids([ts_ext])[0]
        sync_client.timeseries.insert_from_lists(
            timestamps=[pd.Timestamp.utcnow()],
            values=[42.0],
            ts=ts_obj,
        )

        # __next__ blocks until a frame arrives. Server idle timeout is ~45s; one iteration
        # with a write in flight is enough.
        for msg in listener:
            received = msg
            break

        assert received is not None, "no message arrived before the deadline"
        assert str(received.payload.event_action) == "CREATE"
        assert str(received.payload.event_object) == "DATAPOINTS"

        # Each delivered DataCollectionString carries the datapoints — verify as_float works.
        items = received.payload.items
        assert len(items) >= 1
        dps = items[0].datapoints
        assert len(dps) >= 1
        floats = [d.as_float() for d in dps]
        assert any(abs(v - 42.0) < 1e-9 for v in floats)

        listener.ack([received.message_id])
        listener.close()
    finally:
        try:
            sync_client.subscriptions.delete([sub_ext])
        except Exception:
            pass
        try:
            sync_client.timeseries.delete([ts])
        except Exception:
            pass


@pytest.mark.skipif(not listen_enabled, reason="set RUN_LISTEN_TESTS=1 to run live listen tests")
def test_listen_context_manager_closes_cleanly(sync_client):
    suffix = _suffix()
    ts_ext = f"sub_ctx_ts_{suffix}"
    sub_ext = f"sub_ctx_{suffix}"

    ts = datahub_sdk.TimeSeries(
        external_id=ts_ext,
        name="Sub Ctx TS",
        value_type="decimal",
        unit="a.u",
    )
    sync_client.timeseries.create([ts])
    sub = datahub_sdk.Subscription(
        external_id=sub_ext,
        name=f"Sub Ctx {suffix}",
        timeseries=[ts_ext],
    )
    sync_client.subscriptions.create([sub])

    try:
        with sync_client.subscriptions.listen(sub_ext) as listener:
            assert listener is not None
        # After __exit__, calling close() again should be a no-op (the inner Option is None).
        # The wrapper exposes close() on the Python object so this is safe to invoke.
    finally:
        sync_client.subscriptions.delete([sub_ext])
        sync_client.timeseries.delete([ts])
