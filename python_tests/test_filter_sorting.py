"""``sort`` on the four filter endpoints.

Sorting is new on the three node filters and rebuilt on events. Before it, ``/resources/filter`` had
no ``ORDER BY`` at all, so with a ``limit`` the rows you got back were an arbitrary subset and two
identical requests could disagree about which — a result that reads as data changing underneath you
rather than as a missing clause.

The shape is the same everywhere: **one** property with a direction, and ``id`` appended behind it.
The tie-breaker is not decoration. A sort column alone is not a position unless it is unique, so a
page boundary falling inside a run of equal values repeats or drops exactly those rows — which is
why ``test_ties_are_broken_by_id`` matters as much as the ordering tests.

Where the endpoints differ:

* **nodes** default to ``createdTime`` descending and can sort by ``id``, ``externalId``, ``name``,
  ``source``, ``description``, ``createdTime``, ``lastUpdatedTime`` or ``dataSetId``. Nulls form
  their own block — last ascending, first descending.
* **events** default to ``eventTime`` *ascending* — the order the keyset pages in, so paging does
  not silently change the order — and add ``type``, ``subType`` and ``status``.

An unrecognised property falls back to the default rather than failing, so a misspelling returns
the default order: visibly not what was asked for, which is the point.
"""
import pytest

import datahub_sdk

from fixtures import async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from filter_fixtures import (  # noqa: F401  (fixtures)
    datasets,
    null_source_resources,
    prefix,
    sortable_events,
    sortable_timeseries,
    token,
)
from polling import poll_until


def ids_of(page):
    return [item.external_id for item in page]


@pytest.fixture
def ts_sorted(sync_client, prefix):
    """The sortable timeseries corpus, in the order the server returns it."""
    def _sorted(**paging):
        return ids_of(sync_client.timeseries.filter(
            datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*", **paging)))
    return _sorted


@pytest.fixture
def by_index(sortable_timeseries):
    """External ids in index order, which is also name order (Sortable A..F)."""
    return [sortable_timeseries[i]["external_id"] for i in range(6)]


# --------------------------------------------------------------------------- #
# node filters — direction, properties, and the default
# --------------------------------------------------------------------------- #

def test_sorting_by_name_runs_both_ways(ts_sorted, by_index):
    """Names are ``Sortable A`` … ``Sortable F``, so name order is index order — and the rows were
    *created* in a different order, so this only holds if the server really sorted."""
    assert ts_sorted(sort_by="name", sort_order="asc") == by_index
    assert ts_sorted(sort_by="name", sort_order="desc") == list(reversed(by_index))


def test_sorting_by_external_id(ts_sorted, by_index):
    assert ts_sorted(sort_by="externalId", sort_order="asc") == by_index
    assert ts_sorted(sort_by="externalId", sort_order="desc") == list(reversed(by_index))


def test_sorting_by_id(ts_sorted, by_index, sync_client, prefix):
    """``id`` is both a sortable property and the implicit tie-breaker. Ids are assigned in
    creation order, which is deliberately not index order — so this is a third distinct sequence."""
    rows = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*", sort_by="id",
                                         sort_order="asc"))
    numeric = [ts.id for ts in rows]
    assert numeric == sorted(numeric)
    assert set(ids_of(rows)) == set(by_index)

    descending = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*", sort_by="id",
                                         sort_order="desc"))
    assert [ts.id for ts in descending] == sorted(numeric, reverse=True)


def test_sorting_by_created_time_is_creation_order(ts_sorted, sync_client, prefix):
    rows = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*",
                                         sort_by="createdTime", sort_order="asc"))
    stamps = [ts.created_time for ts in rows]
    assert stamps == sorted(stamps), f"not ascending by createdTime: {stamps}"
    assert len(stamps) == 6


def test_the_default_order_is_newest_created_first(ts_sorted, sync_client, prefix):
    """What the node filters returned before they could be sorted, kept as the default."""
    unsorted = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*"))
    explicit = sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*",
                                         sort_by="createdTime", sort_order="desc"))
    assert ids_of(unsorted) == ids_of(explicit)

    stamps = [ts.created_time for ts in unsorted]
    assert stamps == sorted(stamps, reverse=True)


def test_anything_but_desc_sorts_ascending(ts_sorted, by_index):
    """A malformed direction degrades predictably rather than silently reversing the page."""
    ascending = ts_sorted(sort_by="name", sort_order="asc")
    for order in ["ASC", "ascending", "", "sideways", None]:
        assert ts_sorted(sort_by="name", sort_order=order) == ascending, f"order={order!r}"


def test_desc_is_matched_case_insensitively(ts_sorted, by_index):
    assert ts_sorted(sort_by="name", sort_order="DESC") == list(reversed(by_index))


def test_an_unsortable_property_falls_back_to_the_default(ts_sorted, sync_client, prefix):
    """The whitelist exists because a column name reaching a query from a request body is an
    injection point. An unknown name is dropped rather than rejected, so the caller gets the
    default order — visibly not what they asked for."""
    default = ts_sorted()
    for property_name in ["notAProperty", "unit", "metadata", "'; DROP TABLE node;--"]:
        assert ts_sorted(sort_by=property_name) == default, f"sort_by={property_name!r}"


def test_the_first_recognised_property_wins(ts_sorted, by_index):
    """``sort.property`` is a list on the wire but only one entry is used — the first the server
    recognises, so an unknown name in front of a known one does not disable the sort."""
    assert ts_sorted(sort_by=["notAProperty", "name"], sort_order="asc") == by_index


def test_a_bare_string_is_a_one_element_property_list(ts_sorted, by_index):
    assert ts_sorted(sort_by="name", sort_order="asc") == ts_sorted(
        sort_by=["name"], sort_order="asc")


def test_sorting_and_filtering_compose(ts_sorted, by_index, sortable_timeseries, prefix):
    """A sort is applied to whatever the criteria matched, not to the table."""
    narrowed = ts_sorted(sort_by="name", sort_order="desc")
    assert narrowed == list(reversed(by_index))


# --------------------------------------------------------------------------- #
# the id tie-breaker
# --------------------------------------------------------------------------- #

def test_ties_are_broken_by_id(sync_client, prefix, sortable_timeseries):
    """Every row here has the same ``unit``, so sorting by a column they all share leaves the order
    entirely to the tie-breaker — and it has to be *stable*, or paging through the run would repeat
    or drop rows at the boundary.

    Sorted by ``dataSetId``, which is identical for all six.
    """
    def page():
        return ids_of(sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(
            external_ids=f"{prefix}_sort_ts_*", sort_by="dataSetId", sort_order="asc")))

    first, second = page(), page()
    assert first == second, "a tied sort must still be deterministic"

    by_id = {ts.external_id: ts.id for ts in sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*"))}
    assert [by_id[external_id] for external_id in first] == sorted(by_id.values()), \
        "ties should fall back to id ascending"


def test_the_tie_breaker_follows_the_sort_direction(sync_client, prefix, sortable_timeseries):
    """Descending means descending all the way down, or the two halves of the order disagree and a
    keyset boundary lands in the wrong place."""
    descending = ids_of(sync_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(
        external_ids=f"{prefix}_sort_ts_*", sort_by="dataSetId", sort_order="desc")))
    by_id = {ts.external_id: ts.id for ts in sync_client.timeseries.filter(
        datahub_sdk.TimeSeriesFilterForm(external_ids=f"{prefix}_sort_ts_*"))}
    assert [by_id[external_id] for external_id in descending] == sorted(
        by_id.values(), reverse=True)


# --------------------------------------------------------------------------- #
# nulls
# --------------------------------------------------------------------------- #

def test_nulls_sort_last_ascending_and_first_descending(sync_client, null_source_resources, prefix):
    """Most node columns are nullable — under single-table inheritance a column only some types use
    has to be — so nulls are a block in the order, not an edge case.

    Last ascending and first descending, matching how a Postgres btree stores them, so a plain
    index on the column can serve the ordering in either direction.
    """
    def page(order):
        return ids_of(sync_client.resources.filter(
            external_ids=f"{prefix}_ns_*", node_types=["asset", "resource"],
            sort_by="source", sort_order=order))

    without_source = null_source_resources["none"]["external_id"]

    ascending = page("asc")
    assert len(ascending) == 3, ascending
    assert ascending[-1] == without_source, f"nulls should sort last ascending: {ascending}"

    descending = page("desc")
    assert descending[0] == without_source, f"nulls should sort first descending: {descending}"
    # The non-null block is simply reversed between the two.
    assert descending[1:] == list(reversed(ascending[:-1]))


# --------------------------------------------------------------------------- #
# datasets and resources — the same contract, briefly
# --------------------------------------------------------------------------- #

def test_datasets_sort_by_name(sync_client, datasets, prefix, token):
    parent, child = datasets

    def page(order):
        return ids_of(sync_client.datasets.filter(datahub_sdk.DatasetFilter(
            datahub_sdk.BasicDatasetFilter(external_ids=f"{prefix}_ds_*"),
            sort_by="name", sort_order=order)))

    # "Filter Child" sorts before "Filter Parent".
    assert page("asc") == [child.external_id, parent.external_id]
    assert page("desc") == [parent.external_id, child.external_id]


def test_resources_sort_by_name(sync_client, null_source_resources, prefix):
    def page(order):
        return ids_of(sync_client.resources.filter(
            external_ids=f"{prefix}_ns_*", node_types=["asset", "resource"],
            sort_by="name", sort_order=order))

    ascending = page("asc")
    assert ascending == sorted(ascending), "names A, B, None sort in that order"
    assert page("desc") == list(reversed(ascending))


# --------------------------------------------------------------------------- #
# events — a different default, and three extra properties
# --------------------------------------------------------------------------- #

@pytest.fixture
def ev_sorted(sync_client, prefix, sortable_events):
    def _sorted(**paging):
        request = datahub_sdk.EventFilter(
            datahub_sdk.BasicEventFilter(external_ids=f"{prefix}_sort_ev_*"), limit=50, **paging)
        return poll_until(
            lambda: ids_of(sync_client.events.filter(request)),
            lambda found: len(found) >= 4,
            timeout=15.0,
        )
    return _sorted


@pytest.fixture
def ev_by_index(sortable_events):
    return [sortable_events[i]["external_id"] for i in range(4)]


def test_the_event_default_is_event_time_ascending(ev_sorted, ev_by_index):
    """Different from the node filters' newest-created-first, and deliberately so: it is the order
    the keyset cursor pages in, so starting to page does not silently change the order.

    The corpus is created in one batch with event times one minute apart, so event-time order is
    index order while creation order is not.
    """
    assert ev_sorted() == ev_by_index
    assert ev_sorted(sort_by="eventTime", sort_order="asc") == ev_by_index


def test_events_sort_by_event_time_descending(ev_sorted, ev_by_index):
    assert ev_sorted(sort_by="eventTime", sort_order="desc") == list(reversed(ev_by_index))


def test_events_sort_by_type(ev_sorted, sortable_events):
    """Types are ``a_type`` … ``d_type`` on indices 3, 2, 1, 0 — the reverse of event-time order, so
    this cannot pass by accident."""
    expected = [sortable_events[i]["external_id"] for i in [3, 2, 1, 0]]
    assert ev_sorted(sort_by="type", sort_order="asc") == expected
    assert ev_sorted(sort_by="type", sort_order="desc") == list(reversed(expected))


def test_events_sort_by_status(ev_sorted, sortable_events):
    """CLOSED before OPEN ascending. Two events share each status, so this also exercises the id
    tie-breaker within each block."""
    ascending = ev_sorted(sort_by="status", sort_order="asc")
    status_of = {spec["external_id"]: spec["status"] for spec in sortable_events.values()}
    assert [status_of[external_id] for external_id in ascending] == [
        "CLOSED", "CLOSED", "OPEN", "OPEN"]

    descending = ev_sorted(sort_by="status", sort_order="desc")
    assert [status_of[external_id] for external_id in descending] == [
        "OPEN", "OPEN", "CLOSED", "CLOSED"]


def test_events_sort_by_external_id(ev_sorted, ev_by_index):
    assert ev_sorted(sort_by="externalId", sort_order="asc") == ev_by_index


def test_an_unsortable_event_property_falls_back_to_the_default(ev_sorted, ev_by_index):
    for property_name in ["notAProperty", "description", "metadata"]:
        assert ev_sorted(sort_by=property_name) == ev_by_index, f"sort_by={property_name!r}"


def test_events_sort_by_a_nullable_property(ev_sorted, sortable_events):
    """``subType`` and ``status`` may be null, so they are sortable but — separately — not pageable.
    Sorting alone has to keep working; the paging refusal is in ``test_filter_paging.py``."""
    every = {spec["external_id"] for spec in sortable_events.values()}
    assert set(ev_sorted(sort_by="subType", sort_order="asc")) == every
    assert set(ev_sorted(sort_by="status", sort_order="desc")) == every


@pytest.mark.asyncio
async def test_async_sorting_matches_the_sync_client(async_client, sync_client, prefix,
                                                     sortable_timeseries, by_index):
    from_async = await async_client.timeseries.filter(datahub_sdk.TimeSeriesFilterForm(
        external_ids=f"{prefix}_sort_ts_*", sort_by="name", sort_order="asc"))
    assert ids_of(from_async) == by_index
