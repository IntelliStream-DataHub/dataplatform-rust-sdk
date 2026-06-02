"""Tests for the Python units module.

Units are reference data — there's no create endpoint, only read paths.
Exercises every endpoint on `UnitServiceSync`: list, by_ids, by_external_ids.
"""
import datahub_sdk as dh
import pytest

from fixtures import sync_client


@pytest.fixture(scope="module")
def some_unit(sync_client):
    units = sync_client.units.list()
    if not units:
        pytest.skip("backend has no units configured")
    return units[0]


def test_list_returns_units(sync_client):
    units = sync_client.units.list()
    assert isinstance(units, list)
    assert len(units) > 0


def test_by_ids(sync_client, some_unit):
    result = sync_client.units.by_ids([dh.IdCollection(id=some_unit.id)])
    assert len(result) >= 1
    assert result[0].id == some_unit.id


def test_by_external_ids(sync_client, some_unit):
    result = sync_client.units.by_external_ids(some_unit.external_id)
    assert len(result) >= 1
    assert any(u.external_id == some_unit.external_id for u in result)
