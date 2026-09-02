"""Session-wide cleanup backstop for the integration suite.

The entity factories in ``fixtures.py`` already delete what they create at their
own teardown, which covers the normal path and mid-test assertion failures. What
they *cannot* cover is a hard-killed run (Ctrl-C, ``-x``, a crash, OOM, the
backend going down mid-suite): the per-test teardown never fires, so any entity
created up to that point is orphaned. Because those entities carry the shared
``TEST_PREFIX`` (``pytest_``) in their external id, we can reclaim them by
re-querying the backend.

The ``_prefix_sweep`` autouse fixture does exactly that — once before the first
test and once after the last: it deletes every entity whose external id starts
with ``TEST_PREFIX``. Sweeping at the start reclaims orphans left by an earlier
interrupted run; sweeping at the end tidies up anything the current run leaked.

**Nodes go through ``/resources/filter``**, which is the generic node query: it
spans assets, timeseries, data sets, functions and policies alike, narrowable by
``node_type``. This sweep used to skip data sets and resources on the belief that
neither had an enumerable endpoint, and that gap is what let them accumulate —
they were the two largest categories of stranded rows by an order of magnitude,
while the services this file did cover stayed small.

Two things the deletes have to respect:

- **Order.** The backend refuses to delete a node that is the START of an edge,
  and a data set stands above everything that belongs to it. So leaves go first,
  data sets last, and the whole pass repeats while it is still making progress —
  a chain of edges needs one pass per link, and the depth is not known here.
- **Case.** The server matches ``externalId`` patterns case-insensitively, so a
  filter for ``pytest_*`` returns ``PyTest_...`` too. Comparing case-sensitively
  in Python would hand those back as matches and then quietly decline to delete
  them.

Whatever survives the last pass is reported through a pytest warning rather than
swallowed: a leak that nothing prints is a leak nobody fixes.
"""
import warnings

import intellistream_datahub_sdk
import pytest

from fixtures import ENV_FILE, TEST_PREFIX, _safe_delete_each

# `/resources/filter` returns every node as its own type; these are the ones with a typed
# delete endpoint of their own. Anything else is deleted as a plain resource.
#
# Dispatch is on `node_type` rather than on the labels the type is derived from: it is present
# on every node class and says the same thing without the caller re-deriving it.
_TIMESERIES = "timeseries"
_DATASET = "dataset"
_FUNCTION = "function"


def _is_test(value) -> bool:
    return bool(value) and value.lower().startswith(TEST_PREFIX.lower())


def _matching_prefix(items):
    return [
        e.external_id
        for e in items
        if _is_test(getattr(e, "external_id", None))
    ]


def _all_test_nodes(client):
    """Every node carrying the prefix, of every type, paged to the end."""
    found, cursor = [], None
    while True:
        page = client.resources.filter(
            external_id=[f"{TEST_PREFIX}*"], limit=1000, cursor=cursor
        )
        found.extend(page)
        cursor = getattr(page, "next_cursor", None)
        if not cursor:
            return [n for n in found if _is_test(n.external_id)]


def _delete_nodes(client, nodes) -> None:
    """Delete nodes leaves-first, repeating while the count still falls."""
    remaining = list(nodes)
    while remaining:
        by_type = {_TIMESERIES: [], _DATASET: [], _FUNCTION: [], "other": []}
        for node in remaining:
            node_type = getattr(node, "node_type", "resource")
            key = node_type if node_type in by_type else "other"
            by_type[key].append(node.external_id)

        # Data sets last: everything else may belong to one.
        _safe_delete_each(client.timeseries.delete, by_type[_TIMESERIES])
        _safe_delete_each(client.functions.delete, by_type[_FUNCTION])
        _safe_delete_each(client.resources.delete, by_type["other"])
        _safe_delete_each(client.datasets.delete, by_type[_DATASET])

        survivors = _all_test_nodes(client)
        if len(survivors) >= len(remaining):
            remaining = survivors
            break
        remaining = survivors

    if remaining:
        warnings.warn(
            f"{len(remaining)} test-prefixed nodes could not be deleted and remain on the "
            f"backend, e.g. {[n.external_id for n in remaining[:5]]}",
            stacklevel=2,
        )


def _sweep(client) -> None:
    """Delete every ``TEST_PREFIX`` entity the backend still holds."""
    try:
        _delete_nodes(client, _all_test_nodes(client))
    except Exception:
        pass

    # Events — not nodes, so their own filter endpoint and their own id space.
    try:
        events, cursor = [], None
        while True:
            page = client.events.filter(
                filter=intellistream_datahub_sdk.EventFilter(external_id=f"{TEST_PREFIX}*"),
                limit=1000,
                cursor=cursor,
            )
            events.extend(page)
            cursor = getattr(page, "next_cursor", None)
            if not cursor:
                break
        _safe_delete_each(client.events.delete, _matching_prefix(events))
    except Exception:
        pass

    # Subscriptions — plain list.
    try:
        _safe_delete_each(
            client.subscriptions.delete, _matching_prefix(client.subscriptions.list())
        )
    except Exception:
        pass

    # Labels — no external id; the prefix is in the name, which the server upper-cases.
    try:
        stale = [
            label.id
            for label in client.labels.list()
            if _is_test(getattr(label, "name", None))
        ]
        _safe_delete_each(client.labels.delete, stale)
    except Exception:
        pass


@pytest.fixture(scope="session", autouse=True)
def _prefix_sweep():
    """Reclaim ``TEST_PREFIX`` orphans before and after the session.

    Uses its own client so cleanup is independent of the client fixtures a test
    happened to build. If no backend is configured (e.g. only the offline
    buffering tests run), there is nothing to sweep."""
    try:
        client = intellistream_datahub_sdk.DataHubClient.from_envfile(ENV_FILE)
    except Exception:
        yield
        return

    _sweep(client)  # reclaim orphans from an earlier interrupted run
    yield
    _sweep(client)  # tidy up anything this run leaked
