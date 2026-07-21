"""Shared pytest fixtures for the Python (PyO3) integration suite.

Prefer the **entity factories** (``make_ts``, ``make_dataset``, ``make_events`` …)
over hand-rolling create/delete with ``try/finally`` in a test: each factory
creates entities and deletes everything it made at fixture teardown, so cleanup
happens automatically once the test (its scope) ends — including when an
assertion fails partway through.
"""
import os
import uuid
from time import sleep

import datahub_sdk
import numpy as np
import pandas as pd
import pytest

# The .env lives at the project root, one directory above python_tests/.
ENV_FILE = os.path.join(os.path.dirname(__file__), "..", ".env")

# Every entity a factory creates carries this prefix in its external id. The
# session janitor (see conftest.py) relies on it to recognise — and reclaim —
# test-owned entities, including ones leaked by an earlier interrupted run.
TEST_PREFIX = "pytest_"


def unique_id(kind: str) -> str:
    """A unique external id for a test entity, e.g. ``pytest_ts_9f3c1a2b4d5e``."""
    return f"{TEST_PREFIX}{kind}_{uuid.uuid4().hex[:12]}"


def _safe_delete_each(delete_fn, entities) -> None:
    """Delete entities one at a time, swallowing errors so a single already-gone
    or undeletable entity doesn't abort the rest of the teardown."""
    for entity in entities:
        try:
            delete_fn([entity])
        except Exception:
            pass


@pytest.fixture(scope="module")
def async_client():
    return datahub_sdk.AsyncDataHubClient.from_envfile(ENV_FILE)


@pytest.fixture(scope="module")
def sync_client():
    return datahub_sdk.DataHubClient.from_envfile(ENV_FILE)


# --------------------------------------------------------------------------- #
# Entity factories — create-and-auto-clean helpers. Prefer these in tests.
# --------------------------------------------------------------------------- #

@pytest.fixture
def make_ts(sync_client):
    """Factory that creates timeseries and deletes them at teardown.

    Defaults give a valid, minimally-populated float series; pass keyword
    overrides for any TimeSeries constructor argument."""
    created = []

    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("ts"))
        kwargs.setdefault("value_type", "float")
        kwargs.setdefault("unit", "a.u")
        ts = datahub_sdk.TimeSeries(**kwargs)
        # ensure a clean slate in case a previous failed run leaked the ext id
        sync_client.timeseries.delete([ts])
        result = sync_client.timeseries.create([ts])[0]
        created.append(result)
        return result

    yield _make

    _safe_delete_each(sync_client.timeseries.delete, created)


@pytest.fixture
def make_dataset(sync_client):
    """Factory that creates datasets and deletes them at teardown."""
    created = []

    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("dataset"))
        ds = datahub_sdk.Dataset(**kwargs)
        sync_client.datasets.delete([ds])
        result = sync_client.datasets.create([ds])[0]
        created.append(result)
        return result

    yield _make

    _safe_delete_each(sync_client.datasets.delete, created)


@pytest.fixture
def make_function(sync_client):
    """Factory that creates functions and deletes them at teardown."""
    created = []

    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("fn"))
        fn = datahub_sdk.Function(**kwargs)
        sync_client.functions.delete([fn.external_id])
        result = sync_client.functions.create([fn])[0]
        created.append(result.external_id)
        return result

    yield _make

    _safe_delete_each(sync_client.functions.delete, created)


@pytest.fixture
def make_events(sync_client):
    """Factory that creates a batch of events and deletes them at teardown.

    Pass a list of ``datahub_sdk.Event`` objects."""
    created = []

    def _make(events):
        sync_client.events.create(events)
        created.extend(events)
        return events

    yield _make

    if created:
        try:
            sync_client.events.delete(created)
        except Exception:
            pass


@pytest.fixture
def make_resource(sync_client):
    """Factory that creates resources (+ optional relations) and deletes the
    nodes at teardown.

    Nodes are torn down in reverse creation order so an edge's END node is
    removed before its START node (the backend refuses to delete the START of an
    edge; removing the END auto-deletes the edge)."""
    created = []

    def _make(resources, relations=None):
        created.extend(r.external_id for r in resources)
        if relations is None:
            return sync_client.resources.create(resources)
        return sync_client.resources.create(resources, relations)

    yield _make

    _safe_delete_each(sync_client.resources.delete, list(reversed(created)))


@pytest.fixture
def make_subscription(sync_client):
    """Factory that creates subscriptions and deletes them at teardown."""
    created = []

    def _make(**kwargs):
        kwargs.setdefault("external_id", unique_id("sub"))
        sub = datahub_sdk.Subscription(**kwargs)
        result = sync_client.subscriptions.create([sub])[0]
        created.append(result.external_id)
        return result

    yield _make

    _safe_delete_each(sync_client.subscriptions.delete, created)


# --------------------------------------------------------------------------- #
# Ready-made timeseries + sample data used across the datapoint tests.
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def ts_float(sync_client):
    ts = datahub_sdk.TimeSeries(
        external_id=unique_id("float"),
        name="test_float",
        value_type="float",
        unit="a.u",
    )
    sync_client.timeseries.delete([ts])
    created_ts = sync_client.timeseries.create([ts])

    yield created_ts[0]
    # Teardown: delete the series (and its datapoints) created for the module.
    try:
        sync_client.timeseries.delete([created_ts[0]])
    except Exception:
        pass


@pytest.fixture(scope="module")
def test_data():
    dates = pd.date_range(start='2023-01-01', periods=100, freq='D', tz='UTC')
    # 2. Generate random "steps" (standard normal distribution)
    seed_value = 42  # Or any other integer
    rng = np.random.default_rng(seed_value)
    steps = rng.standard_normal(100)
    # 3. Create the walk by taking the cumulative sum
    walk = steps.cumsum()
    walk = np.arange(0, 100).astype(float)

    # 4. Combine into a Series or DataFrame
    return pd.Series(walk, index=dates, name="Random Walk")


@pytest.fixture(scope="module")
def inserted_data(sync_client, test_data, ts_float):
    inserted_data = sync_client.timeseries.insert_from_lists(timestamps=test_data.index, values=test_data.values, ts=ts_float)
    sleep(0.5)
    yield inserted_data


@pytest.fixture(scope="function")
def fresh_inserted_data(sync_client, test_data, ts_float):
    inserted_data = sync_client.timeseries.insert_from_lists(timestamps=test_data.index, values=test_data.values, ts=ts_float)
    sleep(0.5)
    yield inserted_data
