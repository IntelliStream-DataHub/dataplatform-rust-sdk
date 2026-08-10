"""Tests for the Python edges bindings.

Mirrors the `live` module in `src/relations/tests.rs`. Edges normally come into being as a side
effect of `resources.create(nodes, relations)`; this service is for linking resources that already
exist, and for reading, deleting or cataloguing relationships on their own.
"""
import datahub_sdk
import pytest

from fixtures import async_client, sync_client, unique_id


def _node(external_id: str, name: str) -> datahub_sdk.Resource:
    # The create endpoint rejects a node with null labels.
    return datahub_sdk.Resource(external_id=external_id, name=name, labels=["ASSET"])


def test_get_by_ids_and_delete(sync_client):
    a, b = unique_id("edge_a"), unique_id("edge_b")
    created = sync_client.resources.create(
        [_node(a, "python edge node a"), _node(b, "python edge node b")],
        [
            datahub_sdk.RelForm(
                relationship_type="SDK_TEST_LINK",
                from_external_id=a,
                to_external_id=b,
            )
        ],
    )
    assert len(created.relations) == 1
    edge = created.relations[0]
    edge_id = edge.id
    assert edge_id is not None
    assert edge.relationship_type == "SDK_TEST_LINK"

    try:
        fetched = sync_client.edges.get(edge_id)
        assert fetched is not None
        assert fetched.id == edge_id
        assert fetched.start == edge.start
        assert fetched.end == edge.end

        # by_ids resolves both endpoints in the same response.
        graph = sync_client.edges.by_ids([edge_id])
        assert len(graph.relations) == 1
        external_ids = {n.external_id for n in graph.nodes}
        assert {a, b} <= external_ids

        # An EdgeProxy is accepted wherever an id is.
        assert len(sync_client.edges.by_ids([edge]).relations) == 1

        sync_client.edges.delete([edge_id])

        # A deleted edge is a 404 server-side, which the binding absorbs into None. The
        # resources it connected survive.
        assert sync_client.edges.get(edge_id) is None
        # resources.by_ids returns a plain list in the bindings, not a GraphResult.
        assert len(sync_client.resources.by_ids([a, b])) == 2

        # Deleting an unknown id is a silent no-op, not an error.
        sync_client.edges.delete([edge_id])
    finally:
        sync_client.resources.delete([a, b])


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
        sync_client.edges.create_types([datahub_sdk.RelTypeForm(name, description="from pytest")])
    except datahub_sdk.DataHubException:
        # Already seeded by an earlier run. Once the server returns 409 for a duplicate this is
        # the expected path; today it answers 200 with nothing. Either way the type must exist.
        pass

    assert any(t.name == "SDK_PY_TEST_REL_TYPE" for t in sync_client.edges.types()), (
        "names are normalised to uppercase snake case"
    )


def test_unusable_type_name_is_rejected(sync_client):
    with pytest.raises(datahub_sdk.DataHubException):
        sync_client.edges.create_types([datahub_sdk.RelTypeForm("!!!")])


def test_create_between_existing_resources(sync_client):
    a, b = unique_id("edge_link_a"), unique_id("edge_link_b")
    sync_client.resources.create(
        [_node(a, "python edge link a"), _node(b, "python edge link b")], []
    )
    form = datahub_sdk.RelForm(
        relationship_type="SDK_TEST_LINK", from_external_id=a, to_external_id=b
    )
    try:
        try:
            created = sync_client.edges.create([form])
        except datahub_sdk.DataHubException as exc:
            # POST /edges/create is newer than some backends; skip rather than fail there.
            if "405" in str(exc):
                pytest.skip("this backend has no POST /edges/create")
            raise

        assert len(created) == 1
        edge_id = created[0].id
        assert edge_id is not None

        # (start, end, type) is unique, so the same link again conflicts.
        with pytest.raises(datahub_sdk.DataHubException):
            sync_client.edges.create([form])

        sync_client.edges.delete([edge_id])
    finally:
        sync_client.resources.delete([a, b])


@pytest.mark.asyncio
async def test_async_reads(async_client):
    types = await async_client.edges.types()
    assert any(t.name == "BELONGS_TO" for t in types)
    assert await async_client.edges.get(999_999_999) is None
    graph = await async_client.edges.by_ids([999_999_999])
    assert graph.relations == []
