"""Async tests for the Python resources module and graph relations.

The async mirror of `test_resources.py`: exercises `create`/`by_ids`/`delete`/
`search` on `AsyncDataHubClient.resources` against the live API. Skipped if the
backend is unreachable (the fixture takes care of that).
"""
import asyncio

import datahub_sdk
import pytest
from datahub_sdk import DataHubException, GraphResult, RelForm, Resource

from fixtures import async_client, unique_id


# The search index is eventually-consistent with writes; give it a moment.
SEARCH_INDEX_DELAY = 3.0


async def _cleanup_edge(async_client, from_ext: str, to_ext: str) -> None:
    """Best-effort teardown for a from->to edge. The backend blocks deleting a
    node that is the START of an edge, so the END node is removed first (which
    auto-deletes the edge), then the START node."""
    for ext in (to_ext, from_ext):
        try:
            await async_client.resources.delete([ext])
        except Exception:
            pass


@pytest.mark.asyncio
async def test_create_with_flows_to_relation(async_client):
    a_ext = unique_id("async_rel_a")
    b_ext = unique_id("async_rel_b")
    ra = Resource(external_id=a_ext, name="Py SDK Async Rel A", is_root=True, labels=["ASSET"])
    rb = Resource(external_id=b_ext, name="Py SDK Async Rel B", labels=["ASSET"])
    rel = RelForm.by_external_ids(a_ext, b_ext, "flows_to")

    await _cleanup_edge(async_client, a_ext, b_ext)

    try:
        result = await async_client.resources.create([ra, rb], [rel])
        assert isinstance(result, GraphResult)
        assert len(result.nodes) == 2
        assert len(result.relations) == 1

        edge = result.relations[0]
        assert edge.id is not None
        assert edge.start is not None
        assert edge.end is not None
        # server snake-upper-cases the relationship type
        assert edge.relationship_type == "FLOWS_TO"
    finally:
        await _cleanup_edge(async_client, a_ext, b_ext)


@pytest.mark.asyncio
async def test_create_nodes_only(async_client):
    a_ext = unique_id("async_node")
    ra = Resource(external_id=a_ext, name="Py SDK Async Node", is_root=True, labels=["ASSET"])

    await async_client.resources.delete([a_ext])

    try:
        # relations argument is optional
        result = await async_client.resources.create([ra])
        assert isinstance(result, GraphResult)
        assert len(result.nodes) == 1
        assert result.nodes[0].external_id == a_ext
    finally:
        await async_client.resources.delete([a_ext])


@pytest.mark.asyncio
async def test_by_ids_round_trips_created_resource(async_client):
    ext_id = unique_id("async_byids")
    resource = Resource(external_id=ext_id, name="Py SDK Async ByIds", is_root=True, labels=["ASSET"])

    await async_client.resources.delete([ext_id])
    await async_client.resources.create([resource])
    try:
        fetched = await async_client.resources.by_ids([ext_id])
        assert isinstance(fetched, list)
        assert any(r.external_id == ext_id for r in fetched), (
            f"by_ids did not return the created resource {ext_id}"
        )
    finally:
        await async_client.resources.delete([ext_id])


@pytest.mark.asyncio
async def test_search_resources(async_client):
    # Async mirror of `test_resources.py::test_search_resources`: create a
    # resource, then search with a query + limit and assert the matches are
    # bounded and relevant.
    ext_id = unique_id("async_search")
    # The search query rejects underscores, so build the (searched) name from the
    # bare hex tail of ext_id rather than the full underscore-bearing external id.
    token = ext_id.rsplit("_", 1)[-1]
    name = f"py sdk async search resource {token}"
    resource = Resource(external_id=ext_id, name=name, is_root=True, labels=["ASSET"])

    await async_client.resources.delete([ext_id])
    await async_client.resources.create([resource])
    try:
        await asyncio.sleep(SEARCH_INDEX_DELAY)

        form = datahub_sdk.SearchAndFilterForm(query=name, limit=5)
        results = await async_client.resources.search(form)
        assert isinstance(results, list)
        assert len(results) <= 5
        assert any(r.external_id == ext_id for r in results), (
            f"search did not return the created resource {ext_id}"
        )
    finally:
        await async_client.resources.delete([ext_id])


@pytest.mark.asyncio
async def test_api_error_surfaces_status_code(async_client):
    # A resource without labels is rejected by the backend with HTTP 400. The
    # error must reach Python as a DataHubException exposing that status code.
    bad = Resource(external_id=unique_id("async_badreq"), name="Bad Request")
    with pytest.raises(DataHubException) as exc_info:
        await async_client.resources.create([bad])
    assert exc_info.value.status_code == 400
    assert exc_info.value.message  # raw response body is preserved
