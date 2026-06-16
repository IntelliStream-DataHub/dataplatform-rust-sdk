"""Pure-unit tests for the entity classes exposed by the `datahub_sdk` bindings.

Unlike the rest of the suite, these tests do **not** touch a live backend or use
the client fixtures. They only exercise the Python surface of the value objects:

* every constructor argument round-trips through its getter,
* documented defaults are applied,
* validation rules raise the expected errors, and
* every settable property can be assigned from Python and reads back the new
  value (including assigning ``None`` to clear optional fields).

Notes on the current build:
* ``FileUpload``, ``ValueType`` and ``RelationFrom`` appear in the ``.pyi`` stub
  but are not registered in the compiled extension, so they cannot be
  constructed from Python yet and are not covered here.
"""
import datetime

import datahub_sdk as dh
import pytest


# --------------------------------------------------------------------------- #
# TimeSeries
# --------------------------------------------------------------------------- #
class TestTimeSeries:
    def test_minimal_constructor_and_defaults(self):
        ts = dh.TimeSeries(external_id="ext1")
        assert ts.external_id == "ext1"
        # name falls back to external_id when omitted
        assert ts.name == "ext1"
        # value_type defaults to the canonical "bigInt"
        assert ts.value_type == "bigInt"
        assert ts.id is None
        assert ts.unit is None
        assert ts.unit_external_id is None
        assert ts.description is None
        assert ts.metadata is None
        assert ts.security_categories is None
        assert ts.data_set_id is None

    def test_full_constructor_round_trips_through_getters(self):
        ts = dh.TimeSeries(
            external_id="ext1",
            name="My TS",
            value_type="float",
            unit="m/s",
            unit_external_id="u-ext",
            description="a description",
            metadata={"k": "v"},
            security_categories=[1, 2],
            data_set_id=99,
        )
        assert ts.external_id == "ext1"
        assert ts.name == "My TS"
        assert ts.value_type == "float"
        assert ts.unit == "m/s"
        assert ts.unit_external_id == "u-ext"
        assert ts.description == "a description"
        assert ts.metadata == {"k": "v"}
        assert ts.security_categories == [1, 2]
        assert ts.data_set_id == 99

    @pytest.mark.parametrize(
        "given, expected",
        [
            ("bigint", "bigInt"),
            ("BigInt", "bigInt"),
            ("BIGINT", "bigInt"),
            ("float", "float"),
            ("FLOAT", "float"),
            ("text", "text"),
            ("TEXT", "text"),
            # "decimal" is an input-only alias normalised to the canonical "float"
            ("decimal", "float"),
            ("DECIMAL", "float"),
        ],
    )
    def test_value_type_is_normalised_in_constructor(self, given, expected):
        ts = dh.TimeSeries(external_id="e", name="n", value_type=given)
        assert ts.value_type == expected

    def test_setters_update_every_field(self):
        ts = dh.TimeSeries(external_id="e", name="n")
        ts.external_id = "ext2"
        ts.name = "renamed"
        ts.unit = "m/s"
        ts.unit_external_id = "u-ext"
        ts.description = "a description"
        ts.metadata = {"k": "v"}
        ts.security_categories = [1, 2, 3]
        ts.data_set_id = 42

        assert ts.external_id == "ext2"
        assert ts.name == "renamed"
        assert ts.unit == "m/s"
        assert ts.unit_external_id == "u-ext"
        assert ts.description == "a description"
        assert ts.metadata == {"k": "v"}
        assert ts.security_categories == [1, 2, 3]
        assert ts.data_set_id == 42

    @pytest.mark.parametrize(
        "given, expected",
        [("bigint", "bigInt"), ("float", "float"), ("decimal", "float"), ("text", "text")],
    )
    def test_value_type_setter_is_normalised(self, given, expected):
        ts = dh.TimeSeries(external_id="e", name="n")
        ts.value_type = given
        assert ts.value_type == expected

    def test_optional_setters_accept_none(self):
        ts = dh.TimeSeries(external_id="e", name="n", unit="m", metadata={"k": "v"}, data_set_id=1)
        ts.unit = None
        ts.metadata = None
        ts.data_set_id = None
        ts.description = None
        ts.unit_external_id = None
        ts.security_categories = None
        assert ts.unit is None
        assert ts.metadata is None
        assert ts.data_set_id is None
        assert ts.security_categories is None

    def test_invalid_value_type_raises(self):
        ts = dh.TimeSeries(external_id="e", name="n")
        with pytest.raises(ValueError):
            ts.value_type = "not_a_type"


# --------------------------------------------------------------------------- #
# Event
# --------------------------------------------------------------------------- #
class TestEvent:
    def test_minimal_constructor_and_defaults(self):
        ev = dh.Event("ev1")
        assert ev.external_id == "ev1"
        assert ev.id is None
        assert ev.type is None
        assert ev.sub_type is None
        assert ev.description is None
        assert ev.status is None
        assert ev.source is None
        assert ev.metadata is None
        assert ev.data_set_id is None
        assert ev.event_time is None
        # list-valued fields default to empty lists, not None
        assert ev.related_resource_ids == []
        assert ev.related_resource_external_ids == []

    def test_full_constructor_round_trips_through_getters(self):
        event_time = datetime.datetime(2023, 6, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)
        ev = dh.Event(
            "ev1",
            type="alarm",
            sub_type="high",
            description="a description",
            status="open",
            source="sensor",
            metadata={"k": "v"},
            data_set_id=7,
            related_resource_ids=[1, 2],
            related_resource_external_ids=["r1", "r2"],
            event_time=event_time,
        )
        assert ev.external_id == "ev1"
        assert ev.type == "alarm"
        assert ev.sub_type == "high"
        assert ev.description == "a description"
        assert ev.status == "open"
        assert ev.source == "sensor"
        assert ev.metadata == {"k": "v"}
        assert ev.data_set_id == 7
        assert ev.related_resource_ids == [1, 2]
        assert ev.related_resource_external_ids == ["r1", "r2"]
        assert ev.event_time == event_time

    def test_external_id_is_required(self):
        with pytest.raises(TypeError):
            dh.Event()

    def test_setters_update_every_field(self):
        event_time = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
        ev = dh.Event("ev1")
        ev.external_id = "ev2"
        ev.type = "alarm"
        ev.sub_type = "high"
        ev.description = "a description"
        ev.status = "open"
        ev.source = "sensor"
        ev.metadata = {"k": "v"}
        ev.data_set_id = 7
        ev.related_resource_ids = [1, 2]
        ev.related_resource_external_ids = ["r1", "r2"]
        ev.event_time = event_time

        assert ev.external_id == "ev2"
        assert ev.type == "alarm"
        assert ev.sub_type == "high"
        assert ev.description == "a description"
        assert ev.status == "open"
        assert ev.source == "sensor"
        assert ev.metadata == {"k": "v"}
        assert ev.data_set_id == 7
        assert ev.related_resource_ids == [1, 2]
        assert ev.related_resource_external_ids == ["r1", "r2"]
        assert ev.event_time == event_time

    def test_optional_setters_accept_none(self):
        ev = dh.Event("ev1", type="alarm", metadata={"k": "v"}, data_set_id=1)
        ev.type = None
        ev.metadata = None
        ev.data_set_id = None
        ev.event_time = None
        assert ev.type is None
        assert ev.metadata is None
        assert ev.data_set_id is None
        assert ev.event_time is None


# --------------------------------------------------------------------------- #
# Dataset
# --------------------------------------------------------------------------- #
class TestDataset:
    def test_minimal_constructor_and_defaults(self):
        ds = dh.Dataset("ds_ext")
        assert ds.external_id == "ds_ext"
        # name falls back to external_id when omitted
        assert ds.name == "ds_ext"
        assert ds.id is None
        assert ds.description is None
        assert ds.policies is None
        # metadata and connected_data_sets are always concrete containers
        assert ds.metadata == {}
        assert ds.connected_data_sets == []

    def test_full_constructor_round_trips_through_getters(self):
        ds = dh.Dataset(
            external_id="ds_ext",
            name="Nice Dataset",
            id=3,
            description="a description",
            policies=["policy-a", "policy-b"],
            metadata={"env": "test"},
            connected_data_sets=[1, 2],
        )
        assert ds.external_id == "ds_ext"
        assert ds.name == "Nice Dataset"
        assert ds.id == 3
        assert ds.description == "a description"
        assert ds.policies == ["policy-a", "policy-b"]
        assert ds.metadata == {"env": "test"}
        assert ds.connected_data_sets == [1, 2]

    def test_external_id_is_required(self):
        with pytest.raises(TypeError):
            dh.Dataset()

    def test_setters_update_every_field(self):
        ds = dh.Dataset("ds_ext")
        ds.external_id = "ds_ext2"
        ds.name = "Renamed"
        ds.id = 5
        ds.description = "a description"
        ds.policies = ["p1", "p2"]
        ds.metadata = {"env": "test"}
        ds.connected_data_sets = [10, 20]

        assert ds.external_id == "ds_ext2"
        assert ds.name == "Renamed"
        assert ds.id == 5
        assert ds.description == "a description"
        assert ds.policies == ["p1", "p2"]
        assert ds.metadata == {"env": "test"}
        assert ds.connected_data_sets == [10, 20]

    def test_optional_setters_accept_none(self):
        ds = dh.Dataset("ds_ext", id=1, description="d", policies=["p"])
        ds.id = None
        ds.description = None
        ds.policies = None
        assert ds.id is None
        assert ds.description is None
        assert ds.policies is None


# --------------------------------------------------------------------------- #
# Resource
# --------------------------------------------------------------------------- #
class TestResource:
    def test_constructor_with_name_only(self):
        r = dh.Resource(name="Pump")
        assert r.name == "Pump"
        assert r.is_root is False
        assert r.id is None

    def test_constructor_with_external_id_only(self):
        r = dh.Resource(external_id="asset1")
        assert r.external_id == "asset1"

    def test_requires_name_or_external_id(self):
        with pytest.raises(ValueError):
            dh.Resource()

    def test_full_constructor_round_trips_through_getters(self):
        r = dh.Resource(
            name="Pump",
            external_id="asset1",
            id=8,
            description="a description",
            is_root=True,
            data_set_id=3,
            source="src",
            labels=["rotating", "critical"],
            metadata={"vendor": "acme"},
            geolocation={"lat": 1.0, "lon": 2.0},
        )
        assert r.name == "Pump"
        assert r.external_id == "asset1"
        assert r.id == 8
        assert r.description == "a description"
        assert r.is_root is True
        assert r.data_set_id == 3
        assert r.source == "src"
        assert r.labels == ["rotating", "critical"]
        assert r.metadata == {"vendor": "acme"}
        assert r.geolocation == {"lat": 1.0, "lon": 2.0}

    def test_setters_update_every_field(self):
        r = dh.Resource(name="Pump", external_id="asset1")
        r.name = "Pump2"
        r.external_id = "asset2"
        r.id = 8
        r.description = "a description"
        r.is_root = True
        r.data_set_id = 3
        r.source = "src"
        r.labels = ["rotating"]
        r.metadata = {"vendor": "acme"}
        r.geolocation = {"lat": 1.0}

        assert r.name == "Pump2"
        assert r.external_id == "asset2"
        assert r.id == 8
        assert r.description == "a description"
        assert r.is_root is True
        assert r.data_set_id == 3
        assert r.source == "src"
        assert r.labels == ["rotating"]
        assert r.metadata == {"vendor": "acme"}
        assert r.geolocation == {"lat": 1.0}

    def test_relations_setter_accepts_edge_proxies(self):
        r = dh.Resource(name="Pump", external_id="asset1")
        assert r.relations is None
        edge = dh.EdgeProxy(relationship_type="connected_to")
        r.relations = [edge]
        assert r.relations is not None
        assert len(r.relations) == 1
        r.relations = None
        assert r.relations is None

    def test_optional_setters_accept_none(self):
        r = dh.Resource(name="Pump", external_id="asset1", id=1, source="s", labels=["l"])
        r.id = None
        r.source = None
        r.labels = None
        r.metadata = None
        r.geolocation = None
        assert r.id is None
        assert r.source is None
        assert r.labels is None


# --------------------------------------------------------------------------- #
# Unit (now exposes a getter and setter for every field)
# --------------------------------------------------------------------------- #
class TestUnit:
    def _make(self):
        return dh.Unit(
            1,
            "deg_c",
            "DegC",
            "degrees Celsius",
            "C",
            "Temperature unit",
            ["celsius", "centigrade"],
            "Temperature",
            {"multiplier": 1.0, "offset": 273.15},
            "qudt",
            "http://qudt.org",
        )

    def test_constructor_round_trips_through_all_getters(self):
        u = self._make()
        assert u.id == 1
        assert u.external_id == "deg_c"
        assert u.name == "DegC"
        assert u.long_name == "degrees Celsius"
        assert u.symbol == "C"
        assert u.description == "Temperature unit"
        assert u.alias_names == ["celsius", "centigrade"]
        assert u.quantity == "Temperature"
        assert u.conversion == {"multiplier": 1.0, "offset": 273.15}
        assert u.source == "qudt"
        assert u.source_reference == "http://qudt.org"

    def test_constructor_requires_all_positional_args(self):
        with pytest.raises(TypeError):
            dh.Unit(1, "deg_c", "DegC")

    def test_setters_update_every_field(self):
        u = self._make()
        u.id = 2
        u.external_id = "kelvin"
        u.name = "K"
        u.long_name = "Kelvin"
        u.symbol = "K"
        u.description = "Absolute temperature"
        u.alias_names = ["absolute"]
        u.quantity = "ThermodynamicTemperature"
        u.conversion = {"multiplier": 1.0}
        u.source = "custom"
        u.source_reference = "http://example.org"

        assert u.id == 2
        assert u.external_id == "kelvin"
        assert u.name == "K"
        assert u.long_name == "Kelvin"
        assert u.symbol == "K"
        assert u.description == "Absolute temperature"
        assert u.alias_names == ["absolute"]
        assert u.quantity == "ThermodynamicTemperature"
        assert u.conversion == {"multiplier": 1.0}
        assert u.source == "custom"
        assert u.source_reference == "http://example.org"
