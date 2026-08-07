"""Tests for the Python files module.

Mirrors `src/files/test.rs` (`test_file_upload`, `list_folders`). The clients now
expose the full `FilesServiceSync` / `FilesServiceAsync` with `upload_file`,
`list_root_directory`, and `list_directory_by_path`. The SDK uploads a single
`FileUpload` per call and echoes back the server-assigned metadata as a list.
"""
import os

import datahub_sdk
import pytest

from fixtures import async_client, sync_client


_IMAGE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "resources", "test", "image.jpg"
)


def test_upload_and_list(sync_client):
    # Mirrors `src/files/test.rs::test_file_upload`: upload a real file to a
    # destination directory (the backend requires a destination path), then list
    # that directory and confirm the file is present.
    ext_id = "image_sola_jpg"

    # Best-effort clean slate (folder + file ext-ids the backend assigns).
    for leaked in (ext_id, "datahub_folder_images"):
        try:
            sync_client.files.delete([leaked])
        except Exception:
            pass

    upload = datahub_sdk.FileUpload(
        path=_IMAGE_PATH,
        destination_path="/images/",
        external_id=ext_id,
        name="sola.jpg",
    )
    try:
        uploaded = sync_client.files.upload_file(upload)
        # The /files endpoint echoes the created file as INode(s).
        assert isinstance(uploaded, list)
        assert any(node.external_id == ext_id for node in uploaded)

        roots = sync_client.files.list_root_directory()
        assert isinstance(roots, list)

        listing = sync_client.files.list_directory_by_path("/images/")
        assert isinstance(listing, list)
        assert any(node.name == "sola.jpg" for node in listing)
    finally:
        for leaked in (ext_id, "datahub_folder_images"):
            try:
                sync_client.files.delete([leaked])
            except Exception:
                pass


def test_list_directory_by_path(sync_client):
    inodes = sync_client.files.list_directory_by_path("/")
    assert isinstance(inodes, list)


@pytest.mark.asyncio
async def test_async_list_root_directory(async_client):
    roots = await async_client.files.list_root_directory()
    assert isinstance(roots, list)


@pytest.mark.asyncio
async def test_async_list_directory_by_path(async_client):
    inodes = await async_client.files.list_directory_by_path("/")
    assert isinstance(inodes, list)


def test_get_search_update_download_trash_restore(sync_client, tmp_path):
    # Mirrors `src/files/test.rs::file_lifecycle_get_search_update_download_trash_restore`:
    # upload once, then exercise every read/mutate endpoint against that file.
    ext_id = "py_lifecycle_sola_jpg"
    leaked = (ext_id, "datahub_folder_pylifecycle", "datahub_folder_moved")

    for name in leaked:
        try:
            sync_client.files.delete([name])
        except Exception:
            pass

    with open(_IMAGE_PATH, "rb") as handle:
        source_bytes = handle.read()

    upload = datahub_sdk.FileUpload(
        path=_IMAGE_PATH,
        destination_path="/pylifecycle/",
        external_id=ext_id,
        name="sola.jpg",
    )
    try:
        uploaded = sync_client.files.upload_file(upload)
        node_id = uploaded[0].id
        assert node_id is not None

        by_id = sync_client.files.get_by_id(node_id)
        assert by_id[0].external_id == ext_id

        by_ext = sync_client.files.get_by_external_id(ext_id)
        assert by_ext[0].id == node_id

        found = sync_client.files.search("sola")
        assert any(node.external_id == ext_id for node in found)
        # A blank query is answered with an empty list, not an error.
        assert sync_client.files.search("") == []

        downloaded = sync_client.files.download(node_id)
        assert downloaded.content == source_bytes
        assert len(downloaded) == len(source_bytes)
        assert downloaded.file_name == "sola.jpg"

        destination = tmp_path / "sola_copy.jpg"
        written = sync_client.files.download_to_path(node_id, str(destination))
        assert written == len(source_bytes)
        assert destination.read_bytes() == source_bytes

        updated = sync_client.files.update(
            datahub_sdk.FileUpdate(
                external_id=ext_id,
                name="renamed.jpg",
                path="/pylifecycle/moved",
                description="Updated by the Python test suite",
            )
        )
        assert updated[0].name == "renamed.jpg"
        # Only the folder prefix is asserted — the server's IndexNode transformer doubles a
        # FILE's path tail. See the Rust test for the detail.
        assert updated[0].path.startswith("/pylifecycle/moved/")
        assert updated[0].description == "Updated by the Python test suite"

        sync_client.files.delete([ext_id])

        trashed = [n for n in sync_client.files.list_trash() if n.id == node_id]
        assert trashed, "the deleted file should be in the trash"
        assert trashed[0].external_id.startswith("DELETED_")

        # Restore by numeric id: the trashed `DELETED_...` external id does not round-trip
        # through the server's lowercasing hash. See the Rust test for the detail.
        restored = sync_client.files.restore([node_id])
        assert restored[0].id == node_id
        assert sync_client.files.get_by_id(node_id)[0].external_id == ext_id
    finally:
        for name in leaked:
            try:
                sync_client.files.delete([name])
            except Exception:
                pass


def test_file_update_requires_a_selector():
    with pytest.raises(ValueError):
        datahub_sdk.FileUpdate(name="renamed.jpg")
