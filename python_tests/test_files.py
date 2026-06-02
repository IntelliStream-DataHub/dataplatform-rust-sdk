"""Tests for the Python files module.

Only one method is wired on the FileService that the clients expose:
`list_root_directory`. It is implemented with `future_into_py` so it always
returns a Python coroutine — exercise it through the async client.
"""
import datahub_sdk
import pytest

from fixtures import async_client


@pytest.mark.asyncio
async def test_list_root_directory(async_client):
    result = await async_client.files.list_root_directory()
    # The binding currently returns a debug-formatted string of root inodes.
    assert isinstance(result, str)
