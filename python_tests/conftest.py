"""Session-wide cleanup backstop for the integration suite.

The entity factories in ``fixtures.py`` already delete what they create at their
own teardown, which covers the normal path and mid-test assertion failures. What
they *cannot* cover is a hard-killed run (Ctrl-C, ``-x``, a crash, OOM): the
per-test teardown never fires, so any entity created up to that point is orphaned
on the backend. Because those entities carry the shared ``TEST_PREFIX``
(``pytest_``) in their external id, we can reclaim them by re-querying the backend.

The ``_prefix_sweep`` autouse fixture does exactly that — once before the first
test and once after the last: it lists each *enumerable* service and deletes every
entity whose external id starts with ``TEST_PREFIX``. Sweeping at the start
reclaims orphans left by an earlier interrupted run; sweeping at the end tidies up
anything the current run leaked.

Only services whose *sync* client can enumerate are swept: timeseries (``list``),
events (``filter`` by external-id prefix), subscriptions (``list``), and functions
(``list``). Datasets, resources, and files have no usable sync ``list`` endpoint,
so orphaned instances of those are not reclaimable this way — prefer fixed,
self-healing external ids for them (a delete-before-create in the test/factory).
"""
import datahub_sdk
import pytest

from fixtures import ENV_FILE, TEST_PREFIX, _safe_delete_each


def _matching_prefix(items):
    return [
        e.external_id
        for e in items
        if getattr(e, "external_id", None) and e.external_id.startswith(TEST_PREFIX)
    ]


def _sweep(client) -> None:
    """Delete every ``TEST_PREFIX`` entity currently on the enumerable services."""
    # Timeseries — plain list.
    try:
        _safe_delete_each(client.timeseries.delete, _matching_prefix(client.timeseries.list()))
    except Exception:
        pass

    # Events — no list endpoint, but the filter API accepts an external-id prefix.
    try:
        filt = datahub_sdk.EventFilter(
            basic_filter=datahub_sdk.BasicEventFilter(external_id_prefix=TEST_PREFIX)
        )
        _safe_delete_each(client.events.delete, _matching_prefix(client.events.filter(filt)))
    except Exception:
        pass

    # Subscriptions — plain list.
    try:
        _safe_delete_each(client.subscriptions.delete, _matching_prefix(client.subscriptions.list()))
    except Exception:
        pass

    # Functions — plain list.
    try:
        _safe_delete_each(client.functions.delete, _matching_prefix(client.functions.list()))
    except Exception:
        pass


@pytest.fixture(scope="session", autouse=True)
def _prefix_sweep():
    """Reclaim ``TEST_PREFIX`` orphans before and after the session.

    Uses its own client so cleanup is independent of the client fixtures a test
    happened to build. If no backend is configured (e.g. only the offline
    buffering tests run), there is nothing to sweep."""
    try:
        client = datahub_sdk.DataHubClient.from_envfile(ENV_FILE)
    except Exception:
        yield
        return

    _sweep(client)  # reclaim orphans from an earlier interrupted run
    yield
    _sweep(client)  # tidy up anything this run leaked
