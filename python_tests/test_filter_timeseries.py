"""``POST /timeseries/filter`` — the refactored contract, end to end.

The filter is mostly the shared node criteria (ids, externalIds, names, sources, labels, metadata,
createdTime, lastUpdatedTime) plus what only a timeseries has: ``dataSetIds``, ``units``,
``unitExternalIds`` and ``valueTypes``. The node half is covered here in depth and only spot-checked
in the resource and dataset suites, since all three go through one ``NodePredicateBuilder``.

What the refactor removed, and why a test would not notice on its own: the scalar ``dataSetId``,
``unit``, ``unitExternalId`` and the ``metadataKey``/``metadataValue`` pair. The api drops unknown
keys silently, so sending any of them today places *no* restriction and returns every timeseries the
caller can read — which reads like a working query. ``timeseries_filter_matches_the_documented_wire_shape``
in ``src/timeseries/test.rs`` pins their absence from the payload; the tests here pin the behaviour
of what replaced them.
"""
import pytest

import datahub_sdk
from datahub_sdk import DataHubException

from fixtures import async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from filter_fixtures import (  # noqa: F401  (fixtures)
    datasets,
    event_corpus,
    prefix,
    resource_corpus,
    timeseries_corpus,
    token,
)


def externals(results):
    """The external ids of a result page, as a set.

    A set, not a list: the endpoint can echo the same row twice while an index catches up, and a
    duplicate is not what any of these tests are about.
    """
    return {t.external_id for t in results}


@pytest.fixture
def flt(sync_client, prefix):
    """Filter within this run's corpus, unless the test says otherwise.

    Every call is scoped to ``<prefix>*`` by default, so an assertion can compare an exact set
    without caring what else lives in the tenant. Pass ``external_ids=...`` to override the scope —
    which the external-id tests do, since the scope is what they are testing.
    """
    def _filter(**criteria):
        criteria.setdefault("external_ids", f"{prefix}*")
        return externals(sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(**criteria)))
    return _filter


# --------------------------------------------------------------------------- #
# externalIds — one list doing exact lookup, prefix, suffix and contains
# --------------------------------------------------------------------------- #

def test_external_ids_exact_match(sync_client, flt, timeseries_corpus, prefix):
    assert flt(external_ids=[f"{prefix}_ts_pump_1"]) == {f"{prefix}_ts_pump_1"}


def test_external_ids_wildcards_at_either_end(sync_client, flt, timeseries_corpus, prefix):
    everything = {ts.external_id for ts in timeseries_corpus.values()}

    assert flt(external_ids=[f"{prefix}*"]) == everything, "trailing wildcard = prefix search"
    assert flt(external_ids=[f"*_ts_valve_1"]) >= {f"{prefix}_ts_valve_1"}, "leading = suffix search"
    assert flt(external_ids=[f"*{prefix[-8:]}_ts_pump*"]) == {
        f"{prefix}_ts_pump_1", f"{prefix}_ts_pumpX1"
    }, "wildcards at both ends = contains search"


def test_percent_is_a_wildcard_too(flt, timeseries_corpus, prefix):
    """``%`` and ``*`` mean the same thing, so a caller who thinks in SQL and one who thinks in
    shell globs both get what they meant."""
    assert flt(external_ids=[f"{prefix}%"]) == flt(external_ids=[f"{prefix}*"])


def test_underscore_is_literal_not_a_single_character_wildcard(flt, timeseries_corpus, prefix):
    """The whole reason the api escapes ``_`` before handing the pattern to SQL.

    ``pump_1`` and ``pumpX1`` differ only at that character. Raw ``LIKE`` would read ``_`` as "any
    one character" and return both, leaving a caller who wanted the one they named with no way to
    ask for it — external ids on this platform are built out of underscores.
    """
    assert flt(external_ids=[f"{prefix}_ts_pump_1"]) == {f"{prefix}_ts_pump_1"}
    assert flt(external_ids=[f"{prefix}_ts_pumpX1"]) == {f"{prefix}_ts_pumpX1"}


def test_external_ids_are_case_insensitive(flt, timeseries_corpus, prefix):
    """Literal entries resolve through a hash of the lowercased id, patterns through ILIKE — so
    both halves of the field agree with how external ids are compared everywhere else."""
    assert flt(external_ids=[f"{prefix}_ts_pump_1".upper()]) == {f"{prefix}_ts_pump_1"}
    assert flt(external_ids=[f"{prefix}_TS_PUMP*".upper()]) == {
        f"{prefix}_ts_pump_1", f"{prefix}_ts_pumpX1"
    }


def test_external_id_entries_or_together(flt, timeseries_corpus, prefix):
    """One list mixes an exact id with a pattern; the entries OR. This is what replaced the
    standalone ``externalIdPrefix``, which could be given once and could not be combined."""
    assert flt(external_ids=[f"{prefix}_ts_valve_1", f"{prefix}_ts_pump*"]) == {
        ts.external_id for ts in timeseries_corpus.values()
    }


# --------------------------------------------------------------------------- #
# names, ids
# --------------------------------------------------------------------------- #

def test_names_match_as_patterns_and_or_together(flt, timeseries_corpus, token):
    assert flt(names=[f"Pump Alpha {token}"]) == {timeseries_corpus["pump_1"].external_id}
    assert flt(names=[f"Pump * {token}"]) == {
        timeseries_corpus["pump_1"].external_id,
        timeseries_corpus["pump_x1"].external_id,
    }
    assert flt(names=[f"Pump Alpha {token}", f"Valve Gamma {token}"]) == {
        timeseries_corpus["pump_1"].external_id,
        timeseries_corpus["valve"].external_id,
    }


def test_names_are_case_insensitive(flt, timeseries_corpus, token):
    assert flt(names=[f"pump alpha {token}".upper()]) == {
        timeseries_corpus["pump_1"].external_id
    }


def test_a_bare_string_means_a_one_element_list(sync_client, timeseries_corpus, token, prefix):
    """The api accepts a scalar wherever it declares a list, and the bindings accept one too.

    Filter fields went plural because one value was rarely enough, but most calls still pass one;
    making the single form a `TypeError` would tax the common case to serve the rare one.
    """
    scalar = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}*", names=f"Pump Alpha {token}"))
    listed = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=[f"{prefix}*"], names=[f"Pump Alpha {token}"]))
    assert externals(scalar) == externals(listed) == {timeseries_corpus["pump_1"].external_id}


def test_ids_match_exactly(flt, timeseries_corpus):
    target = timeseries_corpus["pump_1"]
    assert flt(ids=[target.id]) == {target.external_id}
    assert flt(ids=[target.id, timeseries_corpus["valve"].id]) == {
        target.external_id, timeseries_corpus["valve"].external_id
    }


def test_an_unknown_id_matches_nothing_rather_than_erroring(flt, timeseries_corpus):
    assert flt(ids=[999_999_999_999]) == set()


# --------------------------------------------------------------------------- #
# metadata — the map that replaced metadataKey / metadataValue
# --------------------------------------------------------------------------- #

def test_metadata_key_and_value_must_both_match(flt, timeseries_corpus, token):
    assert flt(metadata={f"tsk_{token}": "alpha"}) == {timeseries_corpus["pump_1"].external_id}
    assert flt(metadata={f"tsk_{token}": "beta"}) == {timeseries_corpus["pump_x1"].external_id}
    assert flt(metadata={f"tsk_{token}": "not-a-value"}) == set()


def test_a_null_metadata_value_matches_the_key_alone(flt, timeseries_corpus, token):
    """``{"key": None}`` is "tagged with this, whatever it says" — what the retired ``metadataKey``
    without a ``metadataValue`` used to mean.

    The node filters previously compared against SQL NULL here, which is never true, so this exact
    body quietly matched nothing. Both timeseries carry ``tsk_<token>`` with *different* values, so
    a key-only match returning both is only possible if the value really is being ignored.
    """
    assert flt(metadata={f"tsk_{token}": None}) == {
        timeseries_corpus["pump_1"].external_id,
        timeseries_corpus["pump_x1"].external_id,
    }


def test_metadata_entries_and_together(flt, timeseries_corpus, token):
    """Several entries mean "has all of these", not "has any"."""
    assert flt(metadata={f"tsk_{token}": "alpha", f"tsshared_{token}": "yes"}) == {
        timeseries_corpus["pump_1"].external_id
    }
    assert flt(metadata={f"tsk_{token}": "alpha", f"tsshared_{token}": "no"}) == set()
    # Mixing an exact entry with a key-only one narrows on both conditions at once.
    assert flt(metadata={f"tsk_{token}": "alpha", f"tsshared_{token}": None}) == {
        timeseries_corpus["pump_1"].external_id
    }


def test_an_unknown_metadata_key_matches_nothing(flt, timeseries_corpus, token):
    assert flt(metadata={f"no_such_key_{token}": None}) == set()


# --------------------------------------------------------------------------- #
# units, unitExternalIds, valueTypes
# --------------------------------------------------------------------------- #

def test_units_match_as_patterns(flt, timeseries_corpus):
    assert flt(units=["bar"]) == {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["valve"].external_id
    }
    assert flt(units=["cels*"]) == {timeseries_corpus["pump_x1"].external_id}
    assert flt(units=["BAR"]) == flt(units=["bar"]), "units match case-insensitively"
    assert flt(units=["watt"]) == set()


def test_units_entries_or_together(flt, timeseries_corpus):
    """The retired scalar ``unit`` needed one call per unit; a list is one call."""
    assert flt(units=["bar", "celsius"]) == {ts.external_id for ts in timeseries_corpus.values()}


def test_unit_external_ids_are_a_pattern_list(flt, timeseries_corpus):
    """It used to be a single exact string, so naming two unit catalogue entries took two calls."""
    assert flt(unit_external_ids=["pressure_bar"]) == {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["valve"].external_id
    }
    assert flt(unit_external_ids=["pressure_*", "temperature_c"]) == {
        ts.external_id for ts in timeseries_corpus.values()
    }


def test_value_types_match_exactly_and_case_insensitively(flt, timeseries_corpus):
    assert flt(value_types=["FLOAT"]) == {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["pump_x1"].external_id
    }
    assert flt(value_types=["float"]) == flt(value_types=["FLOAT"])
    assert flt(value_types=["TEXT"]) == {timeseries_corpus["valve"].external_id}
    assert flt(value_types=["FLOAT", "TEXT"]) == {ts.external_id for ts in timeseries_corpus.values()}


def test_value_types_are_not_patterns(flt, timeseries_corpus):
    """A closed catalogue of seven values, so a wildcard over it would only ever be a way to
    misspell one of them. ``FLOA*`` is not a prefix search — it is a value that does not exist."""
    assert flt(value_types=["FLOA*"]) == set()
    assert flt(value_types=["%"]) == set()


def test_an_unknown_value_type_matches_nothing(flt, timeseries_corpus):
    assert flt(value_types=["NOT_A_VALUE_TYPE"]) == set()


# --------------------------------------------------------------------------- #
# None, empty and garbled input
# --------------------------------------------------------------------------- #

def test_an_absent_filter_places_no_restriction(sync_client, timeseries_corpus):
    """An argument-free form must return the tenant's timeseries, not none of them."""
    everything = sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm())
    assert externals(everything) >= {ts.external_id for ts in timeseries_corpus.values()}


def test_none_valued_criteria_are_omitted_entirely(sync_client, timeseries_corpus, prefix):
    """Passing ``None`` is the same as not passing the argument — it must not reach the wire as a
    ``null`` the server then reads as a restriction."""
    assert externals(sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(
        external_ids=f"{prefix}*", names=None, units=None, value_types=None, metadata=None,
        ids=None, labels=None, sources=None, data_set_ids=None,
    ))) == {ts.external_id for ts in timeseries_corpus.values()}


@pytest.mark.parametrize("empty", [[], ["", "   "]])
def test_an_empty_or_blank_list_places_no_restriction(sync_client, timeseries_corpus, prefix, empty):
    """An empty ``IN`` is not valid SQL, and a caller who built a list and found nothing to put in
    it means "no restriction" far more often than "match nothing". Blank entries are dropped on the
    same reasoning, so an all-blank list behaves like an empty one.

    ``data_set_ids`` is the documented exception and is covered separately.
    """
    scoped = externals(sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(
        external_ids=f"{prefix}*", names=empty, units=empty, labels=empty, value_types=empty)))
    assert scoped == {ts.external_id for ts in timeseries_corpus.values()}


def test_a_garbled_pattern_matches_nothing_without_erroring(flt, timeseries_corpus):
    """Punctuation soup is a value that happens not to exist, not a malformed request. The
    escaping means none of it reaches SQL as syntax."""
    for garbage in ["!!!##$$^&()", "' OR 1=1 --", "\\", "%%%%_____", "😀"]:
        assert flt(external_ids=[garbage]) == set(), f"{garbage!r} should match nothing"


def test_a_bare_wildcard_matches_everything_in_scope(flt, timeseries_corpus, prefix):
    """``*`` on its own is a legitimate "any" rather than a no-op."""
    assert flt(names=["*"]) == {ts.external_id for ts in timeseries_corpus.values()}


# --------------------------------------------------------------------------- #
# dataSetIds — the field where None and [] mean opposite things
# --------------------------------------------------------------------------- #

def test_data_set_scope_by_id_and_by_external_id(flt, timeseries_corpus, datasets):
    """Both spellings name the same data set. The timeseries filter used to take a single numeric
    ``dataSetId`` and nothing else."""
    _parent, child = datasets
    in_child = {timeseries_corpus["pump_1"].external_id, timeseries_corpus["pump_x1"].external_id}
    assert flt(data_set_ids=[child.id]) == in_child
    assert flt(data_set_ids=[child.external_id]) == in_child
    assert flt(data_set_ids=[datahub_sdk.IdCollection(id=child.id)]) == in_child


def test_a_parent_data_set_stands_in_for_its_children(flt, timeseries_corpus, datasets):
    """Naming a parent covers everything beneath it in the ``BELONGS_TO`` hierarchy — the same
    expansion a *grant* on that data set applies, so a filter can never see rows an ACL would not
    have let through, or miss rows it would."""
    parent, _child = datasets
    assert flt(data_set_ids=[parent.id]) == {ts.external_id for ts in timeseries_corpus.values()}


def test_an_explicit_empty_data_set_scope_matches_nothing(sync_client, timeseries_corpus, prefix):
    """The one list where empty is not "no restriction". ``[]`` says "narrow to no data sets", and
    dropping the predicate instead would widen the query to everything the caller can read — the
    opposite of what they asked for.
    """
    assert externals(sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(
        external_ids=f"{prefix}*", data_set_ids=[]))) == set()


def test_an_omitted_data_set_scope_places_no_restriction(flt, timeseries_corpus):
    """The other half of the pair above: absent means unrestricted."""
    assert flt() == {ts.external_id for ts in timeseries_corpus.values()}


def test_a_data_set_external_id_naming_nothing_contributes_nothing(flt, timeseries_corpus, datasets):
    """An unresolvable reference is dropped, so a scope of *only* unknown data sets resolves to
    the empty set and matches nothing — it does not fall back to "unrestricted"."""
    _parent, child = datasets
    assert flt(data_set_ids=["no_such_data_set_at_all"]) == set()
    # Mixed with a real one, the unknown entry simply drops out.
    assert flt(data_set_ids=["no_such_data_set_at_all", child.external_id]) == {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["pump_x1"].external_id
    }


# --------------------------------------------------------------------------- #
# labels, sources
# --------------------------------------------------------------------------- #

def test_labels_narrow_to_the_intrinsic_type_label(flt, timeseries_corpus):
    """The SDK cannot set labels on a timeseries, but the server always stamps the intrinsic
    ``TIMESERIES`` one — enough to prove the inherited field is wired up here at all. It was absent
    from this filter before the refactor, not because the column differs but because the filter was
    written separately. Label semantics themselves are covered in ``test_filter_resources.py``.
    """
    assert flt(labels=["TIMESERIES"]) == {ts.external_id for ts in timeseries_corpus.values()}
    assert flt(labels=["NO_SUCH_LABEL_ANYWHERE"]) == set()


def test_sources_is_accepted_and_narrows(flt, timeseries_corpus):
    """``TimeSeries`` has no ``source`` attribute in this SDK, so there is nothing to match
    positively — but the criterion must still narrow rather than be ignored. If it were dropped,
    this would come back with the whole corpus."""
    assert flt(sources=["definitely_not_a_source"]) == set()


# --------------------------------------------------------------------------- #
# time windows
# --------------------------------------------------------------------------- #

def test_created_time_window_bounds_the_result(flt, timeseries_corpus):
    import pandas as pd

    created = timeseries_corpus["pump_1"].created_time
    assert created is not None, "the server should stamp createdTime on create"
    everything = {ts.external_id for ts in timeseries_corpus.values()}

    window = pd.Timedelta(minutes=10)
    assert flt(created_time=datahub_sdk.TimeFilter(start=created - window)) == everything
    assert flt(created_time=datahub_sdk.TimeFilter(end=created + window)) == everything
    assert flt(created_time=datahub_sdk.TimeFilter(
        start=created - window, end=created + window)) == everything
    # A window that closes before the corpus existed excludes all of it.
    assert flt(created_time=datahub_sdk.TimeFilter(end=created - window)) == set()


# --------------------------------------------------------------------------- #
# combining criteria, and the limit
# --------------------------------------------------------------------------- #

def test_separate_criteria_and_together(flt, timeseries_corpus, token):
    """Within a field, entries OR; across fields, they AND."""
    assert flt(units=["bar"], value_types=["FLOAT"]) == {
        timeseries_corpus["pump_1"].external_id
    }
    assert flt(units=["bar"], value_types=["FLOAT"], names=[f"Valve * {token}"]) == set()


def test_limit_caps_the_page(sync_client, timeseries_corpus, prefix):
    capped = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}*", limit=2))
    assert len(capped) == 2


def test_limit_zero_falls_back_to_the_default(sync_client, timeseries_corpus, prefix):
    """SQL reads ``LIMIT 0`` as "return nothing", which is indistinguishable from "nothing matched"
    — so the server treats a non-positive limit as unset instead."""
    assert externals(sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(
        external_ids=f"{prefix}*", limit=0))) == {ts.external_id for ts in timeseries_corpus.values()}


def test_a_limit_above_the_ceiling_is_refused(sync_client, prefix):
    """10000 is the cap, and exceeding it is a 400 rather than a silently clamped page."""
    with pytest.raises(DataHubException) as excinfo:
        sync_client.timeseries.filter(
            datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}*", limit=10_001))
    assert excinfo.value.status_code == 400


def test_a_negative_limit_is_rejected_client_side(sync_client, prefix):
    """The wire contract says a non-positive limit falls back to the default, but the SDK types
    ``limit`` as unsigned, so a negative one cannot leave the process at all. Pinned so the
    difference between "the server tolerates it" and "you cannot send it" stays visible.
    """
    with pytest.raises(OverflowError):
        sync_client.timeseries.filter(
            datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}*", limit=-5))


@pytest.mark.asyncio
async def test_async_filter_matches_the_sync_one(async_client, sync_client, timeseries_corpus, prefix):
    from_async = await async_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}*", units=["bar"]))
    from_sync = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}*", units=["bar"]))
    assert externals(from_async) == externals(from_sync)
