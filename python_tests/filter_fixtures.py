"""A shared, run-unique corpus for the filter-endpoint suites.

Every filter test needs the same thing: a handful of entities whose ids, names, sources, labels,
metadata and data sets are known exactly, and which no other run can collide with. Building that
per test would be slow (each one costs a create round trip plus, for events, the wait for the
ClickHouse projection) and would make the assertions weaker — a test can only assert an exact
result set if it knows the complete population its filter can match.

So the corpus is **module-scoped and token-scoped**. Every external id starts with
``pytest_flt_<token>``, so every assertion can pin the population by filtering on that prefix first
and then compare exact sets. Two runs in parallel, or a run against a tenant full of real data,
cannot disturb each other.

Deliberate details, because filter semantics turn on them:

``_pump_1`` vs ``_pumpX1``
    Two entities differing only in the character where an underscore sits. SQL ``LIKE`` reads ``_``
    as "any single character", so if the api failed to escape it, a filter for ``..._pump_1`` would
    match both. One entity cannot show that; two can.

``sap_<token>`` vs ``sapX<token>``
    The same trick for ``source``, which has no hashed column and is matched purely by pattern.

parent and child data sets
    Joined by a ``BELONGS_TO`` edge so the hierarchy expansion is testable: entities live in the
    child, and filtering by the *parent* must find them. Note the edge is stored ``from = parent,
    to = child`` even though it is named BELONGS_TO — see ``DataSetRepository.findDatasetClosure``,
    which warns against "fixing" the traversal to match the name.

Everything is torn down at module teardown, in dependency order (resources and timeseries before
the data sets that hold them). Data sets are *not* reclaimable by the ``conftest`` prefix janitor —
there is no dataset ``list`` it can sweep — so the teardown here is the only thing standing between
an interrupted run and a permanently orphaned dataset.
"""
import uuid

import pandas as pd
import pytest

import datahub_sdk

from fixtures import TEST_PREFIX
from polling import poll_until


@pytest.fixture(scope="module")
def token():
    """The run-unique tag every corpus entity carries. 10 hex chars: collision-free in practice,
    short enough to keep external ids readable in a failure message."""
    return uuid.uuid4().hex[:10]


@pytest.fixture(scope="module")
def prefix(token):
    """The external-id prefix shared by every corpus entity, e.g. ``pytest_flt_9f3c1a2b4d``."""
    return f"{TEST_PREFIX}flt_{token}"


def _delete_quietly(delete_fn, items):
    for item in items:
        try:
            delete_fn([item])
        except Exception:
            pass


@pytest.fixture(scope="module")
def datasets(sync_client, prefix, token):
    """A parent data set and a child beneath it in the ``BELONGS_TO`` hierarchy.

    Returns ``(parent, child)``. The hierarchy is what makes "filtering on a parent returns the
    child's contents" testable, which is the behaviour every ``dataSetId`` field now promises.
    """
    parent_ext = f"{prefix}_ds_parent"
    child_ext = f"{prefix}_ds_child"
    # Delete-before-create: a previous interrupted run cannot have left these ids behind (the token
    # is new each run), but the factories elsewhere in the suite do the same and it costs nothing.
    _delete_quietly(sync_client.datasets.delete, [child_ext, parent_ext])

    parent = sync_client.datasets.create([
        datahub_sdk.Dataset(external_id=parent_ext, name=f"Filter Parent {token}",
                            metadata={"tier": "gold", f"dsonly_{token}": "yes"})
    ])[0]
    child = sync_client.datasets.create([
        datahub_sdk.Dataset(external_id=child_ext, name=f"Filter Child {token}",
                            metadata={"tier": "silver"})
    ])[0]
    # Stored from = parent, to = child. Reversing it silently produces no hierarchy at all: the
    # closure query walks rel_start -> rel_end and simply finds nothing to descend into.
    sync_client.edges.create([
        datahub_sdk.RelForm(relationship_type="BELONGS_TO",
                            from_external_id=parent.external_id,
                            to_external_id=child.external_id)
    ])

    yield parent, child

    _delete_quietly(sync_client.datasets.delete, [child_ext, parent_ext])


@pytest.fixture(scope="module")
def timeseries_corpus(sync_client, datasets, prefix, token):
    """Three timeseries: two in the child data set, one in the parent.

    Returns a dict keyed by a short name. ``pump_1`` and ``pump_x1`` differ only where the
    underscore sits, which is what pins ``_`` as a literal.
    """
    parent, child = datasets
    specs = {
        "pump_1": dict(
            external_id=f"{prefix}_ts_pump_1", name=f"Pump Alpha {token}",
            unit="bar", unit_external_id="pressure_bar", value_type="float",
            data_set_id=child.id,
            metadata={f"tsk_{token}": "alpha", f"tsshared_{token}": "yes"},
        ),
        "pump_x1": dict(
            external_id=f"{prefix}_ts_pumpX1", name=f"Pump Beta {token}",
            unit="celsius", unit_external_id="temperature_c", value_type="float",
            data_set_id=child.id,
            metadata={f"tsk_{token}": "beta", f"tsshared_{token}": "yes"},
        ),
        "valve": dict(
            external_id=f"{prefix}_ts_valve_1", name=f"Valve Gamma {token}",
            unit="bar", unit_external_id="pressure_bar", value_type="text",
            data_set_id=parent.id,
            metadata={f"tsshared_{token}": "yes"},
        ),
    }
    _delete_quietly(sync_client.timeseries.delete, [s["external_id"] for s in specs.values()])
    created = sync_client.timeseries.create(
        [datahub_sdk.TimeSeries(**spec) for spec in specs.values()]
    )
    by_external_id = {ts.external_id: ts for ts in created}
    corpus = {key: by_external_id[spec["external_id"]] for key, spec in specs.items()}

    yield corpus

    _delete_quietly(sync_client.timeseries.delete, [s["external_id"] for s in specs.values()])


@pytest.fixture(scope="module")
def resource_corpus(sync_client, datasets, prefix, token):
    """Two resources in the child data set: one root, one not.

    Their sources differ only where the underscore sits (``sap_<token>`` vs ``sapX<token>``), and
    their labels overlap on one entry — enough to tell "carries all of these" from "carries any".
    """
    _parent, child = datasets
    specs = {
        "root": dict(
            external_id=f"{prefix}_res_root", name=f"Root Node {token}", is_root=True,
            source=f"sap_{token}", labels=["FLT_ALPHA", "FLT_BETA"],
            data_set_id=child.id, metadata={f"resk_{token}": "one"},
        ),
        "leaf": dict(
            external_id=f"{prefix}_res_leaf", name=f"Leaf Node {token}", is_root=False,
            source=f"sapX{token}", labels=["FLT_ALPHA"],
            data_set_id=child.id, metadata={f"resk_{token}": "two"},
        ),
    }
    _delete_quietly(sync_client.resources.delete, [s["external_id"] for s in specs.values()])
    sync_client.resources.create([datahub_sdk.Resource(**spec) for spec in specs.values()])
    # Read them back so the fixture hands out server-assigned ids.
    stored = {
        r.external_id: r
        for r in sync_client.resources.by_ids([s["external_id"] for s in specs.values()])
    }
    corpus = {key: stored[spec["external_id"]] for key, spec in specs.items()}

    yield corpus

    _delete_quietly(sync_client.resources.delete,
                    [specs["leaf"]["external_id"], specs["root"]["external_id"]])


@pytest.fixture(scope="module")
def event_corpus(sync_client, datasets, prefix, token):
    """Two events, one per data set, differing in type, sub-type, status and source.

    Events land in ClickHouse asynchronously, so this waits for both to be readable before handing
    the corpus over. Without that the first test to run would be the only flaky one, which is the
    hardest kind of flake to attribute.
    """
    parent, child = datasets
    now = pd.Timestamp.now(tz="UTC")
    specs = {
        "alarm": dict(
            external_id=f"{prefix}_ev_alarm_1", type=f"alarm_{token}", sub_type="electrical",
            status="OPEN", source=f"opc_{token}", data_set_id=child.id,
            event_time=now - pd.Timedelta(days=2), metadata={f"evk_{token}": "one"},
        ),
        "warning": dict(
            external_id=f"{prefix}_ev_alarmX1", type=f"warning_{token}", sub_type="mechanical",
            status="CLOSED", source=f"opcX{token}", data_set_id=parent.id,
            event_time=now, metadata={f"evk_{token}": "two"},
        ),
    }
    externals = [s["external_id"] for s in specs.values()]
    sync_client.events.create([datahub_sdk.Event(**spec) for spec in specs.values()])

    # Poll rather than sleep: the projection lag is usually milliseconds and occasionally seconds.
    def visible():
        return sync_client.events.filter(datahub_sdk.EventFilter(
            datahub_sdk.BasicEventFilter(external_id=f"{prefix}*"), limit=50))

    found = poll_until(visible, lambda events: len(events) >= len(specs))
    assert len(found) >= len(specs), (
        f"the event corpus never became visible to /events/filter: {[e.external_id for e in found]}"
    )

    yield specs

    try:
        sync_client.events.delete(externals)
    except Exception:
        pass


@pytest.fixture(scope="module")
def sortable_timeseries(sync_client, datasets, prefix, token):
    """Six timeseries whose name order, external-id order and creation order all disagree.

    That disagreement is the point. A sort test against rows created in the order they sort in
    passes whether or not the server sorted anything — the default order, the requested order and
    the insertion order are all the same sequence. Here each is a different permutation, so only a
    real ``ORDER BY`` produces the expected one.

    Created one call at a time so ``createdTime`` is distinct per row; a batch create stamps them
    all within the same millisecond, and a tie is exactly what the ``id`` tie-breaker exists for —
    tested separately in ``tied_timeseries``.

    Returns the specs keyed by index, each with the ``external_id`` and ``name`` the tests assert on.
    """
    import time

    _parent, child = datasets
    # index -> (name suffix, creation position). Name order A..F is index order; creation order is
    # deliberately shuffled; external ids run _0.._5 in index order.
    creation_order = [3, 0, 5, 1, 4, 2]
    specs = {
        index: {
            "external_id": f"{prefix}_sort_ts_{index}",
            "name": f"Sortable {'ABCDEF'[index]} {token}",
        }
        for index in range(6)
    }
    _delete_quietly(sync_client.timeseries.delete, [s["external_id"] for s in specs.values()])
    for index in creation_order:
        sync_client.timeseries.create([datahub_sdk.TimeSeries(
            external_id=specs[index]["external_id"], name=specs[index]["name"],
            unit="bar", value_type="float", data_set_id=child.id)])
        time.sleep(0.02)

    yield specs

    _delete_quietly(sync_client.timeseries.delete, [s["external_id"] for s in specs.values()])


@pytest.fixture(scope="module")
def null_source_resources(sync_client, datasets, prefix, token):
    """Three resources, one of which has no ``source``.

    ``source`` is nullable — under single-table inheritance a column only some node types use has
    to be — so it is where the null-block rules are observable: nulls last ascending, first
    descending. Only a ``Resource`` can carry a source through this SDK, which is why this lives
    here rather than on the timeseries corpus.
    """
    specs = {
        "a": dict(external_id=f"{prefix}_ns_a", name=f"NullSrc A {token}", source=f"aaa_{token}"),
        "b": dict(external_id=f"{prefix}_ns_b", name=f"NullSrc B {token}", source=f"bbb_{token}"),
        "none": dict(external_id=f"{prefix}_ns_none", name=f"NullSrc None {token}", source=None),
    }
    externals = [s["external_id"] for s in specs.values()]
    _delete_quietly(sync_client.resources.delete, externals)
    sync_client.resources.create([
        datahub_sdk.Resource(labels=["ASSET"], is_root=True, **spec) for spec in specs.values()
    ])

    yield specs

    _delete_quietly(sync_client.resources.delete, list(reversed(externals)))


@pytest.fixture(scope="module")
def sortable_events(sync_client, datasets, prefix, token):
    """Four events whose type, status and event-time orders all disagree.

    Same reasoning as ``sortable_timeseries``: if every attribute sorted the same way, a test could
    not tell a real sort from no sort at all. ``sub_type`` is held constant and at least three
    characters — the server rejects a shorter one — so the null/tie cases stay separate from this.
    """
    import time

    _parent, child = datasets
    base = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)
    # index -> (event-time offset, type letter, status). Event time ascending is 0,1,2,3; type
    # ascending is 3,2,1,0; status ascending (CLOSED before OPEN) is 1,3,0,2.
    specs = {
        0: dict(offset=0, type_letter="d", status="OPEN"),
        1: dict(offset=1, type_letter="c", status="CLOSED"),
        2: dict(offset=2, type_letter="b", status="OPEN"),
        3: dict(offset=3, type_letter="a", status="CLOSED"),
    }
    for index, spec in specs.items():
        spec["external_id"] = f"{prefix}_sort_ev_{index}"
        spec["type"] = f"{spec['type_letter']}_type_{token}"

    sync_client.events.create([
        datahub_sdk.Event(
            external_id=spec["external_id"], type=spec["type"], sub_type="electrical",
            status=spec["status"], source=f"src_{token}", data_set_id=child.id,
            event_time=base + pd.Timedelta(minutes=spec["offset"]),
        )
        for spec in specs.values()
    ])

    def visible():
        return sync_client.events.filter(datahub_sdk.EventFilter(
            datahub_sdk.BasicEventFilter(external_id=f"{prefix}_sort_ev_*"), limit=50))

    found = poll_until(visible, lambda events: len(events) >= len(specs))
    assert len(found) >= len(specs), "the sortable event corpus never became visible"

    yield specs

    try:
        sync_client.events.delete([spec["external_id"] for spec in specs.values()])
    except Exception:
        pass
