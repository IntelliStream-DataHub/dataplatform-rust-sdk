"""Object-level navigation: fetching related entities directly off an object.

Mirrors the Cognite-Python ergonomics added to the bindings: objects *returned by
the API* carry a client, so `resource.neighbors()` / `timeseries.neighbors()` (graph
traversal) and `event.related_resource_nodes()` (resolve the event's related-resource
refs) — plus their `*_async` twins — can call back into the API. Locally-constructed
objects carry no client and raise instead. These tests require a live backend (and its
async projections running); they poll for eventual consistency and then assert, so they
fail — rather than skip — if navigation never returns the expected data.

Note on `depth`: the traversal defaults to the whole connected component (`depth=-1`,
the backend default). Bounded depths are supported but the server's shallow-depth
semantics are quirky (a 1-hop bound can return nothing), so the tests use the default.
"""
import asyncio
import datetime as dt

import datahub_sdk
import pytest
from datahub_sdk import DataHubException, Event, RelForm, Resource, ResourceNetwork, TimeSeries

from fixtures import async_client, make_dataset, make_resource, sync_client, unique_id
from polling import poll_until, poll_until_async


# These navigation reads go through eventually-consistent async projections: the Neo4j
# graph (for `neighbors`) and ClickHouse (for `related_events`, written by an async
# consumer). A create isn't immediately visible to them, so the tests below POLL until
# the expected data lands rather than asserting once. The timeout is generous on purpose:
# these tests verify the navigation actually works end-to-end, so they wait for the
# projection to catch up and then assert — they must NOT skip, or a genuinely broken
# navigation path would masquerade as "propagation lag".
GRAPH_PROPAGATION_TIMEOUT = 60.0


# --------------------------------------------------------------------------- #
# Missing-client: navigation on a locally-constructed object raises.
# --------------------------------------------------------------------------- #

def test_local_objects_have_no_client():
    with pytest.raises(RuntimeError):
        Resource(name="local").neighbors()
    with pytest.raises(RuntimeError):
        TimeSeries(external_id=unique_id("ts")).neighbors()
    with pytest.raises(RuntimeError):
        Event(
            external_id=unique_id("ev"),
            event_time=dt.datetime.now(dt.timezone.utc),
        ).related_resource_nodes()
    with pytest.raises(RuntimeError):
        datahub_sdk.Dataset(external_id=unique_id("ds")).neighbors()
    with pytest.raises(RuntimeError):
        datahub_sdk.Function(external_id=unique_id("fn"), model_name="forecast-ema").neighbors()
    with pytest.raises(RuntimeError):
        datahub_sdk.INode(
            name="local node", external_id=unique_id("nd"), path="/x", size=0,
            related_resources=[1, 2],
        ).related_resource_nodes()
    # reverse lookup (node -> events) also needs a client
    with pytest.raises(RuntimeError):
        Resource(name="local").related_events()
    with pytest.raises(RuntimeError):
        TimeSeries(external_id=unique_id("ts")).related_events()
    with pytest.raises(RuntimeError):
        datahub_sdk.Dataset(external_id=unique_id("ds")).related_events()
    with pytest.raises(RuntimeError):
        datahub_sdk.Function(external_id=unique_id("fn"), model_name="forecast-ema").related_events()


def test_navigation_methods_exist_on_classes():
    # Guards against a future merge silently dropping the navigation methods (as the
    # relations refactor did) — the compiled module must expose them.
    assert hasattr(datahub_sdk.Resource, "neighbors")
    assert hasattr(datahub_sdk.TimeSeries, "neighbors")
    assert hasattr(datahub_sdk.Dataset, "neighbors")
    assert hasattr(datahub_sdk.Function, "neighbors")
    assert hasattr(datahub_sdk.Event, "related_resource_nodes")
    assert hasattr(datahub_sdk.INode, "related_resource_nodes")
    for cls in (datahub_sdk.Resource, datahub_sdk.TimeSeries, datahub_sdk.Dataset,
                datahub_sdk.Function):
        assert hasattr(cls, "neighbors_async")
        assert hasattr(cls, "related_events")
        assert hasattr(cls, "related_events_async")


# --------------------------------------------------------------------------- #
# Resource.related — walk the graph off a resource returned by the API.
# --------------------------------------------------------------------------- #

def test_resource_related(sync_client, make_resource):
    a_ext = unique_id("nav_res_a")
    b_ext = unique_id("nav_res_b")
    ra = Resource(external_id=a_ext, name="Nav Res A", is_root=True, labels=["ASSET"])
    rb = Resource(external_id=b_ext, name="Nav Res B", labels=["ASSET"])
    rel = RelForm.by_external_ids(a_ext, b_ext, "flows_to")
    make_resource([ra, rb], [rel])

    # Fetch A back from the API so it carries a client, then navigate off it.
    a = next(r for r in sync_client.resources.by_ids([a_ext]) if r.external_id == a_ext)

    network = poll_until(
        a.neighbors,
        lambda net: b_ext in {n.external_id for n in net.nodes},
        timeout=GRAPH_PROPAGATION_TIMEOUT,
    )
    assert isinstance(network, ResourceNetwork)
    assert b_ext in {n.external_id for n in network.nodes}, (
        f"neighbor {b_ext} never appeared in the graph traversal off {a_ext} within "
        f"{GRAPH_PROPAGATION_TIMEOUT}s"
    )

    # Nodes returned by navigation carry the client too, so navigation chains.
    b = next(n for n in network.nodes if n.external_id == b_ext)
    assert isinstance(b.neighbors(), ResourceNetwork)


@pytest.mark.asyncio
async def test_resource_related_async(async_client):
    a_ext = unique_id("nav_ares_a")
    b_ext = unique_id("nav_ares_b")
    ra = Resource(external_id=a_ext, name="Nav Async Res A", is_root=True, labels=["ASSET"])
    rb = Resource(external_id=b_ext, name="Nav Async Res B", labels=["ASSET"])
    rel = RelForm.by_external_ids(a_ext, b_ext, "flows_to")

    for ext in (b_ext, a_ext):
        try:
            await async_client.resources.delete([ext])
        except Exception:
            pass
    await async_client.resources.create([ra, rb], [rel])
    try:
        fetched = await async_client.resources.by_ids([a_ext])
        a = next(r for r in fetched if r.external_id == a_ext)

        network = await poll_until_async(
            a.neighbors_async,
            lambda net: b_ext in {n.external_id for n in net.nodes},
            timeout=GRAPH_PROPAGATION_TIMEOUT,
        )
        assert isinstance(network, ResourceNetwork)
        assert b_ext in {n.external_id for n in network.nodes}, (
            f"neighbor {b_ext} never appeared in the async graph traversal off {a_ext} "
            f"within {GRAPH_PROPAGATION_TIMEOUT}s"
        )
    finally:
        for ext in (b_ext, a_ext):
            try:
                await async_client.resources.delete([ext])
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# Event.related_resources — resolve an event's related resource references.
# --------------------------------------------------------------------------- #

def test_event_related_resources(sync_client, make_resource):
    res_ext = unique_id("nav_evres")
    resource = Resource(external_id=res_ext, name="Nav Event Res", is_root=True, labels=["ASSET"])
    make_resource([resource])

    ev = Event(
        external_id=unique_id("nav_ev"),
        event_time=dt.datetime.now(dt.timezone.utc),
        related_resource_external_ids=[res_ext],
    )
    # create() returns the stored event *with a client stamped on it*, so navigation
    # works directly off the result.
    created = sync_client.events.create([ev])[0]
    try:
        related = created.related_resource_nodes()
        assert isinstance(related, list)
        assert res_ext in {r.external_id for r in related}
    finally:
        try:
            sync_client.events.delete([created])
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Resource.related_events — reverse lookup: events that reference this node.
# --------------------------------------------------------------------------- #

def test_resource_related_events(sync_client, make_resource):
    res_ext = unique_id("nav_reev_res")
    resource = Resource(external_id=res_ext, name="Nav RelEvents Res", is_root=True, labels=["ASSET"])
    make_resource([resource])

    ev_ext = unique_id("nav_reev_ev")
    ev = Event(
        external_id=ev_ext,
        event_time=dt.datetime.now(dt.timezone.utc),
        related_resource_external_ids=[res_ext],
    )
    created = sync_client.events.create([ev])[0]
    try:
        r = next(
            x for x in sync_client.resources.by_ids([res_ext]) if x.external_id == res_ext
        )
        events = poll_until(
            r.related_events,
            lambda evs: ev_ext in {e.external_id for e in evs},
            timeout=GRAPH_PROPAGATION_TIMEOUT,
        )
        assert isinstance(events, list)
        assert ev_ext in {e.external_id for e in events}, (
            f"event {ev_ext} referencing {res_ext} never became visible to related_events "
            f"within {GRAPH_PROPAGATION_TIMEOUT}s"
        )

        # Events returned by the reverse lookup carry a client, so they can resolve back.
        e = next(x for x in events if x.external_id == ev_ext)
        assert res_ext in {n.external_id for n in e.related_resource_nodes()}
    finally:
        try:
            sync_client.events.delete([created])
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# TimeSeries.related — the timeseries participates in the unified graph.
# --------------------------------------------------------------------------- #

def test_timeseries_related(sync_client):
    ts = TimeSeries(external_id=unique_id("nav_ts"), value_type="float", unit="a.u")
    created = sync_client.timeseries.create([ts])[0]

    try:
        fetched = sync_client.timeseries.by_ids([created])[0]
        network = fetched.neighbors()
        assert isinstance(network, ResourceNetwork)
    finally:
        try:
            sync_client.timeseries.delete([created])
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# Dataset.related — verifies the client is stamped on API-returned datasets.
# --------------------------------------------------------------------------- #

def test_dataset_related_is_client_stamped(sync_client, make_dataset):
    ds = make_dataset()
    fetched = next(
        d for d in sync_client.datasets.by_ids([ds.external_id])
        if d.external_id == ds.external_id
    )
    # A dataset returned by the API carries a client, so related() must NOT raise the
    # missing-client RuntimeError. It may return a (possibly empty) ResourceNetwork, or raise
    # a DataHubException if the fresh node isn't graph-resolvable yet — both prove the client
    # is attached; only a RuntimeError would mean stamping was lost.
    try:
        network = fetched.neighbors()
        assert isinstance(network, ResourceNetwork)
    except DataHubException:
        pass
