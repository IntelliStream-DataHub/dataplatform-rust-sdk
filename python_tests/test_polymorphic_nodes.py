"""``/resources`` returns nodes typed by their intrinsic label, not one flat shape.

``/resources`` is the generic node query — it spans assets, timeseries, functions, resources,
data sets and policies — but it used to answer with every row coerced into a ``Resource``. A
timeseries came back with no ``unit``, a data set with no ``policies``, and both carried an
``is_root`` flag meaningless for their type. Now each row arrives as its own class.

Two things this suite pins that nothing else can:

* the **dispatch** — which class you get for which node, and that a node with no type-label is a
  plain ``Resource`` rather than an error;
* the **sparseness boundary** — a node read flatly carries its type's full field set, while the
  same node reached through the graph does not, because the graph stores only a column subset.
  Getting this backwards is how a caller ends up trusting a ``value_type`` that is really a
  server-side default.
"""
import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import (
    Asset,
    Dataset,
    Function,
    Resource,
    TimeSeries,
)

from fixtures import TEST_LABEL, async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from polling import poll_until


@pytest.fixture
def corpus(sync_client):
    """One node of each creatable type, all sharing an external-id stem.

    Created through ``/resources/create`` in a single call, which is itself the thing being
    covered: each element of ``nodes`` is dispatched server-side by its own type-label, so one
    heterogeneous list creates four different kinds of node.
    """
    stem = unique_id("poly")
    ds = Dataset(external_id=f"{stem}_ds", name=f"Poly DS {stem}")
    asset = Asset(
        external_id=f"{stem}_asset",
        name=f"Poly Asset {stem}",
        labels=[TEST_LABEL],
        is_root=True,
        geolocation={"type": "Point", "coordinates": [10.75, 59.91]},
    )
    func = Function(external_id=f"{stem}_fn", name=f"Poly Fn {stem}")
    # Every node needs at least one label; a plain resource is the one that carries no *type*
    # label, not one that carries none at all.
    plain = Resource(
        external_id=f"{stem}_plain", name=f"Poly Plain {stem}", labels=[TEST_LABEL]
    )

    created = sync_client.resources.create([ds, asset, func, plain])
    try:
        yield {"stem": stem, "nodes": created.nodes}
    finally:
        for ext in (f"{stem}_plain", f"{stem}_fn", f"{stem}_asset", f"{stem}_ds"):
            try:
                sync_client.resources.delete([ext])
            except Exception:
                pass


def by_ext(nodes):
    return {n.external_id: n for n in nodes}


# --------------------------------------------------------------------------- #
# dispatch
# --------------------------------------------------------------------------- #

def test_one_create_call_builds_four_different_node_types(corpus):
    """The write side dispatches per element, not per request."""
    found = by_ext(corpus["nodes"])
    stem = corpus["stem"]
    assert isinstance(found[f"{stem}_ds"], Dataset)
    assert isinstance(found[f"{stem}_asset"], Asset)
    assert isinstance(found[f"{stem}_fn"], Function)
    assert isinstance(found[f"{stem}_plain"], Resource)


def test_filter_returns_each_node_as_its_own_class(sync_client, corpus):
    stem = corpus["stem"]
    found = by_ext(sync_client.resources.filter(external_id=f"{stem}*", limit=100))
    assert isinstance(found[f"{stem}_ds"], Dataset)
    assert isinstance(found[f"{stem}_asset"], Asset)
    assert isinstance(found[f"{stem}_fn"], Function)
    assert isinstance(found[f"{stem}_plain"], Resource)


def test_node_type_names_the_type_without_an_isinstance_ladder(sync_client, corpus):
    stem = corpus["stem"]
    found = by_ext(sync_client.resources.filter(external_id=f"{stem}*", limit=100))
    assert {ext.rsplit("_", 1)[1]: n.node_type for ext, n in found.items()} == {
        "ds": "dataset",
        "asset": "asset",
        "fn": "function",
        "plain": "resource",
    }


def test_a_node_with_no_type_label_is_a_plain_resource(sync_client, corpus):
    """Absence of a type-label *is* the resource signal — there is no ``RESOURCE`` label."""
    plain = sync_client.resources.filter(external_id=f"{corpus['stem']}_plain")[0]
    assert isinstance(plain, Resource)
    assert not {l.upper() for l in (plain.labels or [])} & {
        "ASSET", "TIMESERIES", "FUNCTION", "DATASET", "POLICY"
    }


def test_a_domain_label_does_not_change_the_type(sync_client, corpus):
    """``TEST`` is an ordinary label; only the five privileged ones name a type."""
    asset = sync_client.resources.filter(external_id=f"{corpus['stem']}_asset")[0]
    assert isinstance(asset, Asset)
    assert TEST_LABEL in (asset.labels or [])


def test_every_node_still_carries_its_type_as_a_label(sync_client, corpus):
    """The label the dispatch reads is not consumed by it — callers can still see it."""
    found = by_ext(sync_client.resources.filter(external_id=f"{corpus['stem']}*", limit=100))
    stem = corpus["stem"]
    assert "DATASET" in (found[f"{stem}_ds"].labels or [])
    assert "ASSET" in (found[f"{stem}_asset"].labels or [])
    assert "FUNCTION" in found[f"{stem}_fn"].labels


# --------------------------------------------------------------------------- #
# per-type fields, which the flat shape used to drop on the floor
# --------------------------------------------------------------------------- #

def test_a_timeseries_read_through_resources_carries_its_timeseries_fields(
    sync_client, corpus
):
    """This is the whole point: `unit` and `value_type` used to be unreachable here."""
    stem = corpus["stem"]
    ext = f"{stem}_ts"
    sync_client.timeseries.create([
        TimeSeries(external_id=ext, name=f"Poly TS {stem}", unit="bar", value_type="float")
    ])
    try:
        node = sync_client.resources.filter(external_id=ext)[0]
        assert isinstance(node, TimeSeries)
        assert node.unit == "bar"
        assert node.value_type == "float"
        assert node.table_engine  # server-assigned, present on a flat read
    finally:
        try:
            sync_client.timeseries.delete([ext])
        except Exception:
            pass


def test_only_assets_and_resources_carry_is_root(sync_client, corpus):
    """A data set has no such column; the flat shape used to give it one anyway."""
    stem = corpus["stem"]
    found = by_ext(sync_client.resources.filter(external_id=f"{stem}*", limit=100))
    assert found[f"{stem}_asset"].is_root is True
    assert found[f"{stem}_plain"].is_root is False
    assert not hasattr(found[f"{stem}_ds"], "is_root")
    assert not hasattr(found[f"{stem}_fn"], "is_root")


def test_an_asset_echoes_its_geometry_where_a_plain_resource_does_not(sync_client, corpus):
    """`geoLocation` is write-only on a resource and read-write on an asset."""
    stem = corpus["stem"]
    found = by_ext(sync_client.resources.filter(external_id=f"{stem}*", limit=100))
    assert found[f"{stem}_asset"].geolocation["type"] == "Point"
    assert found[f"{stem}_plain"].geolocation is None


# --------------------------------------------------------------------------- #
# the sparseness boundary
# --------------------------------------------------------------------------- #

def test_a_node_reached_through_the_graph_is_typed_but_sparse(sync_client, corpus):
    """The graph stores a column subset, so the same timeseries is thinner here.

    Typed either way — that part is the fix. But a caller who reads `unit` off a graph-sourced
    node gets nothing, and must re-read the node by id.
    """
    stem = corpus["stem"]
    ts_ext = f"{stem}_ts_graph"
    sync_client.timeseries.create([
        TimeSeries(external_id=ts_ext, name=f"Graph TS {stem}", unit="bar", value_type="float")
    ])
    try:
        sync_client.edges.create([
            intellistream_datahub_sdk.RelForm(
                relationship_type="MEASURES",
                from_external_id=f"{stem}_asset",
                to_external_id=ts_ext,
            )
        ])
        asset = sync_client.resources.filter(external_id=f"{stem}_asset")[0]

        # The graph projection lags the write, so poll rather than skip: skipping would let this
        # assertion quietly never run, which is the whole point of the test.
        def reached():
            return next(
                (n for n in asset.neighbors(depth=1).nodes if n.external_id == ts_ext), None
            )

        graph_ts = poll_until(reached, lambda n: n is not None)
        assert graph_ts is not None, "the timeseries never appeared in the graph projection"
        assert isinstance(graph_ts, TimeSeries)
        # The graph carries the shared node fields and nothing else, so every type-specific
        # field is absent rather than defaulted. `value_type` is the one that bit: as a required
        # field it made any traversal over a timeseries a hard deserialization error.
        assert graph_ts.unit is None, "the graph does not carry the unit column"
        assert graph_ts.value_type is None
        assert graph_ts.table_engine is None
        assert graph_ts.security_categories is None
    finally:
        try:
            sync_client.timeseries.delete([ts_ext])
        except Exception:
            pass


# --------------------------------------------------------------------------- #
# the write side
# --------------------------------------------------------------------------- #

def test_a_labelled_resource_still_creates_a_typed_node(sync_client):
    """The pre-existing idiom: put the type-label on a bare `Resource`.

    Kept working on purpose — serialization never strips or rewrites a caller's labels — so code
    written before the typed classes existed reads back as the right type.
    """
    ext = unique_id("poly_legacy")
    sync_client.resources.create([Resource(external_id=ext, name="Legacy Asset",
                                           labels=["ASSET"])])
    try:
        assert isinstance(sync_client.resources.filter(external_id=ext)[0], Asset)
    finally:
        try:
            sync_client.resources.delete([ext])
        except Exception:
            pass


def test_two_type_labels_are_refused(sync_client):
    """A node's type is intrinsic; the api will not guess which of two the caller meant."""
    ext = unique_id("poly_ambiguous")
    with pytest.raises(intellistream_datahub_sdk.DataHubException):
        sync_client.resources.create([
            Resource(external_id=ext, name="Ambiguous", labels=["ASSET", "DATASET"])
        ])


def test_a_node_from_filter_can_be_deleted_directly(sync_client):
    """`delete` takes any node object, not just a `Resource`."""
    ext = unique_id("poly_delete")
    sync_client.datasets.create([Dataset(external_id=ext, name="Poly Delete DS")])
    node = sync_client.resources.filter(external_id=ext)[0]
    assert isinstance(node, Dataset)
    sync_client.datasets.delete([node])
    assert sync_client.resources.filter(external_id=ext) == []
