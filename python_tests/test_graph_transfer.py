"""Tests for `resources.export_graph` / `import_graph`. Mirrors the graph-transfer tests in
`src/resources/tests.rs`."""
import intellistream_datahub_sdk
import pytest

from fixtures import async_client, make_resource, sync_client, unique_id
from polling import poll_until


def _component(make_resource, sync_client):
    root, child = unique_id("export_root"), unique_id("export_child")
    created = make_resource(
        [
            intellistream_datahub_sdk.Resource(
                external_id=root, name="python export root", labels=["ASSET"], is_root=True
            ),
            intellistream_datahub_sdk.Resource(
                external_id=child, name="python export child", labels=["ASSET"]
            ),
        ],
        [
            intellistream_datahub_sdk.RelForm(
                relationship_type="FLOWS_TO", from_external_id=root, to_external_id=child
            )
        ],
    )
    # The export walks the graph projection, which lags the write.
    network = poll_until(
        lambda: sync_client.resources.fetch_related(root, depth=-1),
        lambda n: len(n.nodes) >= 2,
    )
    assert len(network.nodes) >= 2, "graph projection did not catch up"
    return next(n.id for n in created.nodes if n.external_id == root)


def test_export_then_import_into_the_source_tenant_is_a_no_op(sync_client, make_resource, tmp_path):
    root_id = _component(make_resource, sync_client)

    file = sync_client.resources.export_graph(root_id)
    assert isinstance(file, bytes)
    assert file[:2] == b"\x1f\x8b", "the export is gzip"

    result = sync_client.resources.import_graph(file)
    assert result.nodes_created == 0
    assert result.nodes_skipped_existing >= 2

    path = tmp_path / "component.graph"
    written = sync_client.resources.export_graph_to_path(root_id, str(path))
    assert written == path.stat().st_size > 0
    from_disk = sync_client.resources.import_graph_from_path(str(path))
    assert from_disk.nodes_created == 0


def test_export_of_an_unknown_id_raises_404(sync_client):
    with pytest.raises(intellistream_datahub_sdk.DataHubException) as err:
        sync_client.resources.export_graph(2**62)
    assert err.value.status_code == 404


@pytest.mark.asyncio
async def test_async_export_returns_bytes(async_client, sync_client, make_resource):
    root_id = _component(make_resource, sync_client)
    file = await async_client.resources.export_graph(root_id)
    assert isinstance(file, bytes) and file[:2] == b"\x1f\x8b"
