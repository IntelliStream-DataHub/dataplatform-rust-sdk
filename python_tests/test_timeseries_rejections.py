"""What ``timeseries.create`` refuses, and where.

Wrongly *typed* input never leaves Python: the binding declares ``str`` fields, so ``unit=0`` is a
``TypeError`` from the constructor, and ``value_type`` is checked against the SDK's catalogue there
too. Everything else is the api's call, answered with a 400 ``constraint-violation`` whose
``fields`` name each rejected property.

Two things the api accepts that a caller might expect it to refuse, and which are therefore not
pinned here: a ``unit_external_id`` that names no catalogue entry, and one that disagrees with
``unit`` (``unit="Celsius"`` on ``pressure_bar``). REST does not look the id up at all.
"""
import pytest

from intellistream_datahub_sdk import DataHubException, TimeSeries

from fixtures import async_client, make_ts, sync_client, unique_id  # noqa: F401  (fixtures)

ABSENT = object()


def valid(**overrides):
    kwargs = {
        "external_id": unique_id("reject_ts"),
        "name": "rejection probe",
        "value_type": "float",
        "unit": "a.u",
    }
    kwargs.update(overrides)
    return {k: v for k, v in kwargs.items() if v is not ABSENT}


def rejected_fields(sync_client, ts):
    """Create ``ts`` expecting a 400, and return ``{field: code}`` from the problem."""
    try:
        with pytest.raises(DataHubException) as excinfo:
            sync_client.timeseries.create([ts])
    finally:
        # Only reached with something to delete if the api regressed and accepted it.
        sync_client.timeseries.delete([ts.external_id])
    error = excinfo.value
    assert error.status_code == 400, error.message
    assert error.problem_slug == "constraint-violation", error.message
    return {f["field"]: f["code"] for f in error.problem["fields"]}


# --------------------------------------------------------------------------- #
# refused by the constructor
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("overrides", [
    {"unit": 0},
    {"unit_external_id": 0},
    {"external_id": 12},
    {"name": 5},
    {"value_type": None},
    {"metadata": {"count": 1}},
    {"metadata": {"vec": [0, 1, 2]}},
], ids=repr)
def test_a_wrongly_typed_field_is_a_type_error(overrides):
    with pytest.raises(TypeError):
        TimeSeries(**valid(**overrides))


@pytest.mark.parametrize("value_type", ["", "big_int", "strings", "hex"])
def test_an_unknown_value_type_is_a_value_error(value_type):
    with pytest.raises(ValueError, match="ValueType"):
        TimeSeries(**valid(value_type=value_type))


# --------------------------------------------------------------------------- #
# refused by the api
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("unit", [ABSENT, None, "", "   "], ids=["absent", "None", "empty", "blank"])
def test_unit_is_required(sync_client, unit):
    fields = rejected_fields(sync_client, TimeSeries(**valid(unit=unit)))
    assert fields == {"unit": "timeseries.unit.not.blank"}


def test_a_unit_external_id_does_not_stand_in_for_unit(sync_client):
    """Unlike the MCP ``timeseries_create`` tool, REST does not fill ``unit`` in from the catalogue."""
    fields = rejected_fields(
        sync_client, TimeSeries(**valid(unit=ABSENT, unit_external_id="pressure_bar"))
    )
    assert fields == {"unit": "timeseries.unit.not.blank"}


def test_unit_is_at_most_64_characters(sync_client, make_ts):
    assert make_ts(**valid(unit="x" * 64)).unit == "x" * 64

    fields = rejected_fields(sync_client, TimeSeries(**valid(unit="x" * 65)))
    assert set(fields) == {"unit"}


@pytest.mark.parametrize("unit_external_id", ["", "ab"])
def test_unit_external_id_is_at_least_3_characters(sync_client, unit_external_id):
    fields = rejected_fields(
        sync_client, TimeSeries(**valid(unit_external_id=unit_external_id))
    )
    assert set(fields) == {"unitExternalId"}


@pytest.mark.parametrize("name", ["", "ab", "x" * 513], ids=["empty", "2 chars", "513 chars"])
def test_name_is_3_to_512_characters(sync_client, name):
    fields = rejected_fields(sync_client, TimeSeries(**valid(name=name)))
    assert set(fields) == {"name"}


def test_external_id_is_at_least_3_characters(sync_client):
    fields = rejected_fields(sync_client, TimeSeries(**valid(external_id="ab")))
    assert set(fields) == {"externalId"}


def test_every_rejected_field_is_named_at_once(sync_client):
    fields = rejected_fields(
        sync_client, TimeSeries(**valid(name="", unit="", unit_external_id="ab"))
    )
    assert {"name", "unit", "unitExternalId"} <= set(fields)


@pytest.mark.asyncio
async def test_the_async_client_surfaces_the_same_rejection(async_client, sync_client):
    ts = TimeSeries(**valid(unit=""))
    try:
        with pytest.raises(DataHubException) as excinfo:
            await async_client.timeseries.create([ts])
    finally:
        sync_client.timeseries.delete([ts.external_id])
    assert excinfo.value.status_code == 400
    assert excinfo.value.problem_slug == "constraint-violation"
