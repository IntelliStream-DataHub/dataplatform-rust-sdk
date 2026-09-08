"""``POST /datasets/filter`` — the refactored contract.

The dataset filter is now exactly the shared node criteria, *minus* ``dataSetId``: a data set is
the thing other nodes are scoped by, so the field would be asking which data set a data set belongs
to. What distinguishes it from the generic node query (``/resources/filter``) is only the node type
it answers for.

What this suite owns:

* the retirement of ``externalIdPrefix``, folded into ``externalId`` as a trailing wildcard — one
  field that now does exact lookup, prefix, suffix and contains, and can be given more than once;
* ``source``, which used to be one exact string and is a pattern list now — singular in name
  because it still takes a bare value, plural in what it accepts;
* the removal of ``writeProtected`` / ``deactivated``, which leaves this filter as the shared node
  criteria and nothing else — what distinguishes it from the generic node query is now only the
  node type it answers for;
* the page-size contract, which used to differ per entity (two filters defaulted to 100 and two to
  1000, so which page size you got depended on what you were asking about).

``GET /datasets`` is the same handler with an empty filter, so it is checked here too.
"""
import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException

from fixtures import async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from filter_fixtures import datasets, prefix, token  # noqa: F401  (fixtures)


def externals(results):
    return {d.external_id for d in results}


@pytest.fixture
def flt(sync_client, prefix):
    """Filter within this run's corpus unless the test overrides ``external_id``."""
    def _filter(limit=None, **criteria):
        criteria.setdefault("external_id", f"{prefix}*")
        form = dict(filter=intellistream_datahub_sdk.DatasetFilter(**criteria), limit=limit)
        return externals(sync_client.datasets.filter(**form))
    return _filter


@pytest.fixture
def both(datasets):
    parent, child = datasets
    return {parent.external_id, child.external_id}


# --------------------------------------------------------------------------- #
# externalIds — what absorbed externalIdPrefix
# --------------------------------------------------------------------------- #

def test_external_ids_do_exact_lookup_and_prefix_search_in_one_field(flt, datasets, both, prefix):
    parent, child = datasets
    assert flt(external_id=[parent.external_id]) == {parent.external_id}
    assert flt(external_id=[f"{prefix}*"]) == both
    assert flt(external_id=[f"{prefix}_ds_%"]) == both, "`%` is a wildcard too"


def test_external_ids_can_be_given_more_than_once(flt, datasets, both, token):
    """The retired ``externalIdPrefix`` was a single value that could not be combined with the
    exact list — the two ANDed. Here they OR inside one field.

    The wildcard entry carries the run token: a bare ``*_ds_child`` would also match the child of
    every *other* run, which is not hypothetical — a run whose teardown fails leaves its data sets
    behind, and nothing sweeps them (there is no dataset ``list`` for the conftest janitor to use).
    """
    parent, child = datasets
    assert flt(external_id=[parent.external_id, f"*{token}_ds_child"]) == both


def test_a_suffix_and_contains_search(flt, datasets, prefix):
    parent, child = datasets
    assert flt(external_id=["*_ds_parent"]) >= {parent.external_id}
    assert flt(external_id=[f"*{prefix[-8:]}_ds_*"]) == {parent.external_id, child.external_id}


def test_underscore_is_literal(flt, datasets, prefix):
    """``pytest_flt_<token>_ds_parent`` with the underscores taken literally matches one row; read
    as single-character wildcards they would also match ids differing at those positions."""
    parent, _child = datasets
    assert flt(external_id=[parent.external_id]) == {parent.external_id}
    assert flt(external_id=[parent.external_id.replace("_ds_", "Xds_")]) == set()


def test_external_ids_are_case_insensitive(flt, datasets):
    parent, _child = datasets
    assert flt(external_id=[parent.external_id.upper()]) == {parent.external_id}


# --------------------------------------------------------------------------- #
# names, ids, metadata
# --------------------------------------------------------------------------- #

def test_names_match_as_patterns_and_or_together(flt, datasets, both, token):
    parent, child = datasets
    assert flt(name=[f"Filter Parent {token}"]) == {parent.external_id}
    assert flt(name=[f"Filter * {token}"]) == both
    assert flt(name=[f"Filter Parent {token}", f"Filter Child {token}"]) == both
    assert flt(name=["no such name"]) == set()


def test_ids_match_exactly(flt, datasets, both):
    parent, child = datasets
    assert flt(id=[parent.id]) == {parent.external_id}
    assert flt(id=[parent.id, child.id]) == both
    assert flt(id=[999_999_999_999]) == set()


def test_metadata_entries_must_all_be_present(flt, datasets, both, token):
    parent, child = datasets
    assert flt(metadata={"tier": "gold"}) == {parent.external_id}
    assert flt(metadata={"tier": "silver"}) == {child.external_id}
    assert flt(metadata={"tier": None}) == both, "a null value matches the key alone"
    assert flt(metadata={"tier": "gold", f"dsonly_{token}": "yes"}) == {parent.external_id}
    assert flt(metadata={"tier": "silver", f"dsonly_{token}": "yes"}) == set()


# --------------------------------------------------------------------------- #
# sources and labels — accepted, and narrowing
# --------------------------------------------------------------------------- #

def test_sources_and_labels_narrow_rather_than_being_ignored(flt, datasets, both):
    """``Dataset`` exposes neither ``source`` nor ``labels`` in this SDK, so there is nothing to
    match positively — but a criterion that were dropped server-side would return the whole corpus
    here instead of nothing, which is the failure mode this refactor exists to remove.
    """
    assert flt(source=["definitely_not_a_source"]) == set()
    assert flt(labels=["NO_SUCH_LABEL_XYZ"]) == set()


# `datasets.filter(labels=["DATASET"])` matches nothing — the shared DATASET label row carries a
# hash encoded with the wrong algorithm. It is not dataset-specific (ASSET, POLICY and FUNCTION are
# affected too, TIMESERIES is not), so it is pinned once, tenant-wide, by
# `test_every_type_label_is_matchable` in test_filter_resources.py rather than per endpoint here.


# --------------------------------------------------------------------------- #
# the flags that were removed
# --------------------------------------------------------------------------- #

def test_the_retired_flags_are_not_accepted(sync_client):
    """``write_protected`` and ``deactivated`` are gone.

    They were removed server-side as inert, so a filter carrying one looked like it was narrowing
    and was not. A ``TypeError`` from the binding is the useful failure: the api drops unknown keys
    silently, so the alternative is a query that quietly ignores the criterion.
    """
    for retired in ["write_protected", "deactivated"]:
        with pytest.raises(TypeError):
            intellistream_datahub_sdk.DatasetFilter(**{retired: False})


# --------------------------------------------------------------------------- #
# None, empty, garbled
# --------------------------------------------------------------------------- #

def test_an_argument_free_filter_returns_everything(sync_client, datasets, both):
    """The same thing ``GET /datasets`` does — the listing is this call with no criteria, and
    shares the retriever's own limit handling so the two cannot disagree about the page size."""
    from_filter = externals(sync_client.datasets.filter())
    assert from_filter >= both
    assert externals(sync_client.datasets.list(limit=10_000)) >= both


@pytest.mark.parametrize("empty", [[], ["", "  "]])
def test_empty_and_blank_lists_place_no_restriction(flt, both, empty):
    assert flt(name=empty, source=empty, labels=empty) == both


def test_none_valued_criteria_are_omitted(flt, both):
    assert flt(id=None, name=None, source=None, labels=None, metadata=None) == both


def test_garbled_criteria_match_nothing_without_erroring(flt):
    for garbage in ["!!!##$$^&()", "' OR 1=1 --", "\\", "%%%%_____", "😀"]:
        assert flt(name=[garbage]) == set(), f"{garbage!r} should match nothing"


def test_an_unmatchable_criterion_gives_an_empty_result_not_an_unfiltered_one(flt):
    assert flt(external_id=["ds_does_not_exist_xyz"]) == set()


# --------------------------------------------------------------------------- #
# the page-size contract
# --------------------------------------------------------------------------- #

def test_limit_caps_the_page(sync_client, datasets, prefix):
    form = dict(
        filter=intellistream_datahub_sdk.DatasetFilter(external_id=f"{prefix}*"), limit=1)
    assert len(sync_client.datasets.filter(**form)) == 1


def test_limit_zero_falls_back_to_the_default(flt, both):
    assert flt(limit=0) == both


def test_a_limit_above_the_ceiling_is_refused(sync_client, prefix):
    form = dict(
        filter=intellistream_datahub_sdk.DatasetFilter(external_id=f"{prefix}*"), limit=10_001)
    with pytest.raises(DataHubException) as excinfo:
        sync_client.datasets.filter(**form)
    assert excinfo.value.status_code == 400


def test_the_retired_criteria_are_not_accepted(sync_client):
    """``external_id_prefix`` and the plural spellings the criteria briefly carried are gone from
    the binding as well as the wire, so a caller still passing one gets a ``TypeError`` rather than
    a query that quietly ignores it. (The removed flags have their own test above.)"""
    for retired, value in [("external_id_prefix", "sap_"), ("ids", [1]),
                           ("external_ids", ["sap_*"]), ("names", ["SAP*"]), ("sources", ["sap"])]:
        with pytest.raises(TypeError):
            intellistream_datahub_sdk.DatasetFilter(**{retired: value})


@pytest.mark.asyncio
async def test_async_filter_matches_the_sync_one(async_client, sync_client, datasets, prefix):
    form = dict(filter=intellistream_datahub_sdk.DatasetFilter(external_id=f"{prefix}*"))
    assert externals(await async_client.datasets.filter(**form)) == externals(
        sync_client.datasets.filter(**form))
