"""Tests for the Python files module.

Mirrors `src/files/test.rs`. The SDK uploads a single `FileUpload` per call and
echoes back the server-assigned metadata as a list. File external ids are stored
verbatim and looked up case-insensitively; a deleted file keeps its external id.
"""
import os

import intellistream_datahub_sdk
import pytest

from fixtures import async_client, make_dataset, make_resource, sync_client, unique_id


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

    upload = intellistream_datahub_sdk.FileUpload(
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

    upload = intellistream_datahub_sdk.FileUpload(
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
            intellistream_datahub_sdk.FileUpdate(
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
        assert trashed[0].external_id == ext_id
        assert trashed[0].deleted_at is not None

        restored = sync_client.files.restore([ext_id])
        assert restored[0].id == node_id
        after = sync_client.files.get_by_id(node_id)[0]
        assert after.external_id == ext_id
        assert after.deleted_at is None
    finally:
        for name in leaked:
            try:
                sync_client.files.delete([name])
            except Exception:
                pass


def _delete_quietly(sync_client, *names):
    for name in names:
        try:
            sync_client.files.delete([name])
        except Exception:
            pass


def test_default_external_id_is_the_file_name():
    # The server's own default when no external id is sent, and stored verbatim like it.
    upload = intellistream_datahub_sdk.FileUpload(_IMAGE_PATH)
    assert upload.external_id == "image.jpg"


def test_external_id_is_stored_verbatim_and_matched_case_insensitively(sync_client):
    ext_id = unique_id("file") + "-Sola.JPG"
    folder = "datahub_folder_pyverbatim"
    upload = intellistream_datahub_sdk.FileUpload(
        path=_IMAGE_PATH, destination_path="/pyverbatim/", external_id=ext_id, name=ext_id
    )
    try:
        node = sync_client.files.upload_file(upload)[0]
        assert node.external_id == ext_id

        for spelling in (ext_id, ext_id.lower(), ext_id.upper()):
            found = sync_client.files.get_by_external_id(spelling)
            assert found[0].id == node.id, spelling
            assert found[0].external_id == ext_id
    finally:
        _delete_quietly(sync_client, ext_id, folder)


def test_external_id_outside_the_charset_is_a_400(sync_client):
    upload = intellistream_datahub_sdk.FileUpload(
        path=_IMAGE_PATH, destination_path="/pycharset/", external_id="has space.jpg"
    )
    with pytest.raises(intellistream_datahub_sdk.DataHubException) as err:
        sync_client.files.upload_file(upload)
    assert err.value.status_code == 400
    fields = (err.value.problem or {}).get("fields", [])
    assert any(f.get("field") == "externalId" for f in fields), err.value.message


def test_restore_by_external_id_takes_the_most_recently_deleted_copy(sync_client):
    # A deleted file keeps its external id, so the trash can hold several copies of one.
    ext_id = unique_id("file_restore")
    folder = "datahub_folder_pyrestore"
    ids = []
    try:
        for _ in range(2):
            upload = intellistream_datahub_sdk.FileUpload(
                path=_IMAGE_PATH, destination_path="/pyrestore/", external_id=ext_id, name=f"{ext_id}.jpg"
            )
            ids.append(sync_client.files.upload_file(upload)[0].id)
            sync_client.files.delete([ext_id])

        trashed = {n.id: n for n in sync_client.files.list_trash() if n.id in ids}
        assert set(trashed) == set(ids)
        assert all(n.external_id == ext_id for n in trashed.values())
        assert trashed[ids[0]].deleted_at <= trashed[ids[1]].deleted_at

        restored = sync_client.files.restore([ext_id])
        assert [n.id for n in restored] == [ids[1]]
        assert sync_client.files.get_by_external_id(ext_id)[0].id == ids[1]
        assert ids[0] in {n.id for n in sync_client.files.list_trash()}
    finally:
        _delete_quietly(sync_client, ext_id, folder)


@pytest.mark.asyncio
async def test_async_trash_and_restore_by_external_id(async_client, sync_client):
    ext_id = unique_id("file_async")
    folder = "datahub_folder_pyasynctrash"
    upload = intellistream_datahub_sdk.FileUpload(
        path=_IMAGE_PATH, destination_path="/pyasynctrash/", external_id=ext_id, name=f"{ext_id}.jpg"
    )
    try:
        node = (await async_client.files.upload_file(upload))[0]
        assert node.deleted_at is None

        await async_client.files.delete([ext_id])
        trashed = [n for n in await async_client.files.list_trash() if n.id == node.id]
        assert trashed and trashed[0].external_id == ext_id
        assert trashed[0].deleted_at is not None

        restored = await async_client.files.restore([ext_id])
        assert restored[0].id == node.id
        assert (await async_client.files.get_by_id(node.id))[0].deleted_at is None
    finally:
        _delete_quietly(sync_client, ext_id, folder)


def test_file_update_requires_a_selector():
    with pytest.raises(ValueError):
        intellistream_datahub_sdk.FileUpdate(name="renamed.jpg")


def test_file_upload_on_a_bad_path_raises_os_errors(tmp_path):
    missing = str(tmp_path / "no_such_file.jpg")
    with pytest.raises(FileNotFoundError):
        intellistream_datahub_sdk.FileUpload(missing)
    with pytest.raises(FileNotFoundError):
        intellistream_datahub_sdk.FileUpload.from_path(missing)
    with pytest.raises(FileNotFoundError):
        intellistream_datahub_sdk.FileUpload.new_with_destination_path(missing, "/x")
    with pytest.raises(IsADirectoryError):
        intellistream_datahub_sdk.FileUpload(str(tmp_path))


# --------------------------------------------------------------------------- #
# FileUpdate, field by field.
#
# `FileUpdate` is not built out of `Field` wrappers: an omitted field means "leave unchanged" and
# there is no `setNull`, so no field here has a clear-it form. The counterpart assertion is that
# an omitted field survives an update that changes its neighbour.
# --------------------------------------------------------------------------- #

@pytest.fixture
def uploaded_file(sync_client):
    """A single uploaded file with a fixed, self-healing external id."""
    ext_id = "py_update_fields_sola_jpg"
    leaked = (ext_id, "datahub_folder_pyupdatefields")

    for name in leaked:
        try:
            sync_client.files.delete([name])
        except Exception:
            pass

    upload = intellistream_datahub_sdk.FileUpload(
        path=_IMAGE_PATH,
        destination_path="/pyupdatefields/",
        external_id=ext_id,
        name="sola.jpg",
        description="original description",
        source="original_source",
        metadata={"a": "1"},
    )
    yield sync_client.files.upload_file(upload)[0]

    for name in leaked:
        try:
            sync_client.files.delete([name])
        except Exception:
            pass


def test_update_source_and_metadata(sync_client, uploaded_file):
    updated = sync_client.files.update(intellistream_datahub_sdk.FileUpdate(
        external_id=uploaded_file.external_id,
        source="updated_source",
        metadata={"b": "2"},
    ))[0]

    assert updated.source == "updated_source"
    # Metadata is a replace, not a merge — `FileUpdate.with_metadata` sets the whole map.
    assert (updated.metadata or {}) == {"b": "2"}


def test_update_data_set_id(sync_client, uploaded_file, make_dataset):
    dataset = make_dataset(name=unique_id("file_upd_ds"))

    updated = sync_client.files.update(intellistream_datahub_sdk.FileUpdate(
        external_id=uploaded_file.external_id, data_set_id=dataset.id
    ))[0]

    assert updated.data_set_id == dataset.id


def test_update_related_resources(sync_client, uploaded_file, make_resource):
    ext = unique_id("file_upd_res")
    created = make_resource([
        intellistream_datahub_sdk.Resource(external_id=ext, name="File update probe",
                             is_root=True, labels=["ASSET"])
    ])
    resource_id = created.nodes[0].id

    updated = sync_client.files.update(intellistream_datahub_sdk.FileUpdate(
        external_id=uploaded_file.external_id, related_resources=[resource_id]
    ))[0]

    assert resource_id in (updated.related_resources or [])


def test_update_leaves_omitted_fields_unchanged(sync_client, uploaded_file):
    """There is no clear-it form; an omitted field is the "leave it alone" instruction."""
    updated = sync_client.files.update(intellistream_datahub_sdk.FileUpdate(
        external_id=uploaded_file.external_id, description="only the description moves"
    ))[0]

    assert updated.description == "only the description moves"
    assert updated.source == "original_source"
    assert (updated.metadata or {}).get("a") == "1"
    assert updated.name == "sola.jpg"
