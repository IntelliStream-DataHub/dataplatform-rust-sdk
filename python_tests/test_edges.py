"""Tests for the Python edges bindings.

Mirrors the `live` module in `src/relations/tests.rs`. Edges normally come into being as a side
effect of `resources.create(nodes, relations)`; this service is for linking resources that already
exist, and for reading, deleting or cataloguing relationships on their own.
"""
import intellistream_datahub_sdk
import pytest

from fixtures import async_client, sync_client, unique_id
from polling import poll_until


def _await_graph(sync_client, from_ext, what, predicate):
    """Wait until the graph projection reachable from ``from_ext`` satisfies ``predicate``.

    `edges.delete` refuses anything that would strand a node, and that check runs against the
    graph projection, which lags the write. Acting too early gets the *wrong* answer rather than
    an error — a delete that should be refused succeeds, because the projection cannot yet see
    what the edge was holding up.
    """
    network = poll_until(
        lambda: sync_client.resources.fetch_related(from_ext, depth=-1),
        predicate,
    )
    assert predicate(network), f"graph projection did not catch up: {what}"
    return network


def test_get_by_ids_and_delete(sync_client):
    """Read an edge back, resolve it to its endpoints, then delete it — allowed and refused.

    Mirrors `test_edge_get_by_ids_and_delete` in `src/relations/tests.rs`. The shape is a
    triangle because two nodes cannot express the rule: on a bare ``a -> b`` pair the single edge
    is the only thing holding ``b`` to the graph root, so every delete is refused and the allowed
    case is untestable. With ``a -> b``, ``b -> c``, ``a -> c``, dropping ``a -> c`` leaves ``c``
    reachable through ``b``, and dropping ``b -> c`` afterwards would strand it.
    """
    a, b, c = unique_id("edge_a"), unique_id("edge_b"), unique_id("edge_c")
    created = sync_client.resources.create(
        [
            # `labels` may not be null on create. `is_root` anchors the component to the graph
            # root: without one the projection picks an anchor itself, non-deterministically, so
            # the stranding check below would name a different victim between runs.
            intellistream_datahub_sdk.Resource(
                external_id=a, name="python edge node a", labels=["ASSET"], is_root=True
            ),
            intellistream_datahub_sdk.Resource(
                external_id=b, name="python edge node b", labels=["ASSET"]
            ),
            intellistream_datahub_sdk.Resource(
                external_id=c, name="python edge node c", labels=["ASSET"]
            ),
        ],
        [
            intellistream_datahub_sdk.RelForm(
                relationship_type="SDK_TEST_LINK", from_external_id=a, to_external_id=b
            ),
            intellistream_datahub_sdk.RelForm(
                relationship_type="SDK_TEST_LINK", from_external_id=b, to_external_id=c
            ),
            intellistream_datahub_sdk.RelForm(
                relationship_type="SDK_TEST_LINK", from_external_id=a, to_external_id=c
            ),
        ],
    )
    assert len(created.relations) == 3

    node_id = {n.external_id: n.id for n in created.nodes}
    def edge_between(frm, to):
        for e in created.relations:
            if e.start == node_id[frm] and e.end == node_id[to]:
                return e.id
        raise AssertionError(f"no edge {frm} -> {to} in the create response")

    a_to_c = edge_between(a, c)
    b_to_c = edge_between(b, c)

    try:
        fetched = sync_client.edges.get(a_to_c)
        assert fetched is not None
        assert fetched.id == a_to_c
        assert fetched.relationship_type == "SDK_TEST_LINK"

        # by_ids resolves both endpoints in the same response.
        graph = sync_client.edges.by_ids([a_to_c])
        assert len(graph.relations) == 1
        assert {n.external_id for n in graph.nodes} == {a, c}

        # An EdgeProxy is accepted wherever an id is.
        assert len(sync_client.edges.by_ids([fetched]).relations) == 1

        _await_graph(
            sync_client, a, "all three links visible", lambda n: len(n.edges) >= 3
        )

        # --- delete, allowed: c keeps its route to the root through b ---
        sync_client.edges.delete([a_to_c])
        assert sync_client.edges.get(a_to_c) is None
        # Deleting an id that is already gone is a silent no-op.
        sync_client.edges.delete([a_to_c])
        # The endpoints themselves are untouched by an edge delete.
        assert len(sync_client.resources.by_ids([a, b, c])) == 3

        # --- delete, refused: b -> c is now c's only route to the root ---
        # Wait for the *removal* to land: while the projection still believes `a -> c` exists,
        # `c` looks doubly-connected and the next delete would be allowed.
        _await_graph(
            sync_client,
            a,
            "the deleted link is gone",
            lambda n: all(e.id != a_to_c for e in n.edges),
        )
        with pytest.raises(intellistream_datahub_sdk.DataHubException) as excinfo:
            sync_client.edges.delete([b_to_c])
        message = str(excinfo.value)
        assert "disconnect" in message, message
        assert c in message, f"the refusal should name {c}: {message}"
    finally:
        # One request: deleting these individually is legal — leaf-first works, and a node delete
        # cascades its own edges — but each step would have to wait for the projection first.
        sync_client.resources.delete([a, b, c])


def test_unknown_edge_id_is_none(sync_client):
    # Single fetches 404 server-side; get() absorbs that into None. Batch lookups differ —
    # by_ids answers 200 with an empty graph, which is why it is asserted separately below.
    assert sync_client.edges.get(999_999_999) is None
    graph = sync_client.edges.by_ids([999_999_999])
    assert graph.relations == []
    assert graph.nodes == []


def test_types_catalogue(sync_client):
    types = sync_client.edges.types()
    assert isinstance(types, list)
    assert any(t.name == "BELONGS_TO" for t in types), (
        "BELONGS_TO is created by the platform itself and should always be present"
    )
    for t in types:
        assert isinstance(t.name, str)


def test_create_type_normalises_the_name(sync_client):
    # There is no delete-type endpoint, so this name is seeded once and reused across runs.
    name = "Sdk Py Test Rel Type"
    try:
        sync_client.edges.create_types([intellistream_datahub_sdk.RelTypeForm(name, description="from pytest")])
    except intellistream_datahub_sdk.DataHubException:
        # Already seeded by an earlier run. Once the server returns 409 for a duplicate this is
        # the expected path; today it answers 200 with nothing. Either way the type must exist.
        pass

    assert any(t.name == "SDK_PY_TEST_REL_TYPE" for t in sync_client.edges.types()), (
        "names are normalised to uppercase snake case"
    )


def test_unusable_type_name_is_rejected(sync_client):
    with pytest.raises(intellistream_datahub_sdk.DataHubException):
        sync_client.edges.create_types([intellistream_datahub_sdk.RelTypeForm("!!!")])


def test_create_between_existing_resources(sync_client):
    a, b = unique_id("edge_link_a"), unique_id("edge_link_b")
    sync_client.resources.create(
        [
            intellistream_datahub_sdk.Resource(
                external_id=a, name="python edge link a", labels=["ASSET"], is_root=True
            ),
            intellistream_datahub_sdk.Resource(
                external_id=b, name="python edge link b", labels=["ASSET"]
            ),
        ],
        [],
    )
    form = intellistream_datahub_sdk.RelForm(
        relationship_type="SDK_TEST_LINK", from_external_id=a, to_external_id=b
    )
    try:
        try:
            created = sync_client.edges.create([form])
        except intellistream_datahub_sdk.DataHubException as exc:
            # POST /edges/create is newer than some backends; skip rather than fail there.
            if "405" in str(exc):
                pytest.skip("this backend has no POST /edges/create")
            raise

        assert len(created) == 1
        edge_id = created[0].id
        assert edge_id is not None

        # (start, end, type) is unique, so the same link again conflicts.
        with pytest.raises(intellistream_datahub_sdk.DataHubException):
            sync_client.edges.create([form])

        # No separate edge delete here: this link is b's only route to the graph root, so removing
        # it alone is refused (see test_get_by_ids_and_delete for both sides of that rule).
        # Deleting the resources takes the edge with them.
    finally:
        sync_client.resources.delete([a, b])


@pytest.mark.asyncio
async def test_async_reads(async_client):
    types = await async_client.edges.types()
    assert any(t.name == "BELONGS_TO" for t in types)
    assert await async_client.edges.get(999_999_999) is None
    graph = await async_client.edges.by_ids([999_999_999])
    assert graph.relations == []
