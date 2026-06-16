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
