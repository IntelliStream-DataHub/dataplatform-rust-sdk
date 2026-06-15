"""Tests for the Python resources module and graph relations.

Mirrors `src/resources/tests.rs`. Exercises constructing `EdgeProxy`/`RelForm`
and creating resources together with relations through the live API. Skipped if
the backend is unreachable (the fixture takes care of that).
"""
import time
import uuid

import datahub_sdk
import pytest
from datahub_sdk import DataHubException, EdgeProxy, GraphResult, RelForm, Resource

from fixtures import sync_client


# The search index is eventually-consistent with writes; give it a moment.
SEARCH_INDEX_DELAY = 3.0


def _suffix() -> str:
    return uuid.uuid4().hex[:8]


def _cleanup_edge(sync_client, from_ext: str, to_ext: str) -> None:
    """Best-effort teardown for a from->to edge. The backend blocks deleting a
    node that is the START of an edge, so the END node is removed first (which
    auto-deletes the edge), then the START node."""
    for ext in (to_ext, from_ext):
        try:
            sync_client.resources.delete([ext])
        except Exception:
            pass


def test_edge_proxy_constructible():
    edge = EdgeProxy(start=1, end=2, relationship_type="FLOWS_TO")
    assert edge.start == 1
    assert edge.end == 2
    assert edge.relationship_type == "FLOWS_TO"
    assert edge.metadata == {}


def test_rel_form_constructors():
    by_ext = RelForm.by_external_ids("a", "b", "flows_to")
    assert by_ext.from_external_id == "a"
    assert by_ext.to_external_id == "b"
    assert by_ext.relationship_type == "flows_to"

    by_ids = RelForm.by_ids(5, 6, "FLOWS_TO")
    assert by_ids.from_id == 5
    assert by_ids.to_id == 6

    kwargs = RelForm(relationship_type="x", from_id=1, to_id=2, metadata={"k": "v"})
    assert kwargs.metadata == {"k": "v"}

    with pytest.raises(TypeError):
        # relationship_type is keyword-required
        RelForm()


def test_create_with_flows_to_relation(sync_client):
    suffix = _suffix()
    a_ext = f"py_sdk_rel_a_{suffix}"
    b_ext = f"py_sdk_rel_b_{suffix}"
    ra = Resource(external_id=a_ext, name="Py SDK Rel A", is_root=True, labels=["ASSET"])
    rb = Resource(external_id=b_ext, name="Py SDK Rel B", labels=["ASSET"])
    rel = RelForm.by_external_ids(a_ext, b_ext, "flows_to")

    _cleanup_edge(sync_client, a_ext, b_ext)

    try:
        result = sync_client.resources.create([ra, rb], [rel])
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
        _cleanup_edge(sync_client, a_ext, b_ext)


def test_create_nodes_only(sync_client):
    suffix = _suffix()
    a_ext = f"py_sdk_node_{suffix}"
    ra = Resource(external_id=a_ext, name="Py SDK Node", is_root=True, labels=["ASSET"])

    sync_client.resources.delete([a_ext])

    try:
        # relations argument is optional
        result = sync_client.resources.create([ra])
        assert isinstance(result, GraphResult)
        assert len(result.nodes) == 1
        assert result.nodes[0].external_id == a_ext
    finally:
        sync_client.resources.delete([a_ext])


def test_search_resources(sync_client):
    # Mirrors `src/resources/tests.rs::test_search_resources`: create a resource,
    # then search with a query + limit and assert the matches are bounded and
    # relevant.
    suffix = _suffix()
    ext_id = f"py_sdk_search_{suffix}"
    name = f"py sdk search resource {suffix}"
    resource = Resource(external_id=ext_id, name=name, is_root=True, labels=["ASSET"])

    sync_client.resources.delete([ext_id])
    sync_client.resources.create([resource])
    try:
        time.sleep(SEARCH_INDEX_DELAY)

        form = datahub_sdk.SearchAndFilterForm(query=name, limit=5)
        results = sync_client.resources.search(form)
        assert isinstance(results, list)
        assert len(results) <= 5
        assert any(r.external_id == ext_id for r in results), (
            f"search did not return the created resource {ext_id}"
        )
    finally:
        sync_client.resources.delete([ext_id])


def test_api_error_surfaces_status_code(sync_client):
    # A resource without labels is rejected by the backend with HTTP 400. The
    # error must reach Python as a DataHubException exposing that status code.
    bad = Resource(external_id=f"py_sdk_badreq_{_suffix()}", name="Bad Request")
    with pytest.raises(DataHubException) as exc_info:
        sync_client.resources.create([bad])
    assert exc_info.value.status_code == 400
    assert exc_info.value.message  # raw response body is preserved
