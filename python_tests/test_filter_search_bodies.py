"""The ``filter`` on the four ``/search`` endpoints.

Each search endpoint declares a ``filter`` in its request schema, of the same type its own
``/filter`` endpoint takes. Only **one** of the four reads it.

``/timeseries/search`` applies it, and the refactor is what made that true for the whole filter
rather than a hand-picked few fields: ``narrowToFilter`` now re-runs the search hits through the
filter endpoint's own query instead of re-implementing three predicates in Java. The fields it did
not hand-roll — ids, externalIds, names, and later everything inherited from the node base — were
accepted and quietly dropped, so a search narrowed by ``name`` returned rows that did not match it.

``/resources/search``, ``/datasets/search`` and ``/events/search`` still accept a filter and ignore
it. Those cases are ``xfail(strict=True)``: they encode the behaviour a caller reading the OpenAPI
document would expect, so they turn green the day the gap closes rather than having to be rewritten.
Asserting the *current* behaviour instead would mean writing a test that has to be deleted to fix
the bug, and that reads as though the gap were deliberate.

Free-text search is also ranked and fuzzy where a filter is exact, so these tests assert membership
("the filter removed the row it should have") rather than exact result sets.
"""
import pytest

import intellistream_datahub_sdk

from fixtures import async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from filter_fixtures import (  # noqa: F401  (fixtures)
    datasets,
    event_corpus,
    prefix,
    resource_corpus,
    timeseries_corpus,
    token,
)
from polling import poll_until


def externals(results):
    return {item.external_id for item in results}


# --------------------------------------------------------------------------- #
# /timeseries/search — the one that honours its filter
# --------------------------------------------------------------------------- #

def test_timeseries_search_finds_the_corpus_before_any_filtering(sync_client, timeseries_corpus, token):
    """The baseline every test below narrows from. If this is empty the search index has not caught
    up and the narrowing assertions would pass for the wrong reason."""
    hits = poll_until(
        lambda: sync_client.timeseries.search(intellistream_datahub_sdk.SearchAndFilterForm(query=f"Pump Alpha {token}")),
        bool,
    )
    assert timeseries_corpus["pump_1"].external_id in externals(hits)


def test_timeseries_search_filter_narrows_on_an_inherited_node_field(sync_client, timeseries_corpus, token):
    """``name`` is inherited from the node base, and is exactly the kind of field the old
    hand-rolled post-filter did not implement — it was accepted and dropped."""
    query = f"Pump {token}"
    unfiltered = poll_until(
        lambda: sync_client.timeseries.search(intellistream_datahub_sdk.SearchAndFilterForm(query=query)),
        lambda hits: len(externals(hits)) >= 2,
    )
    assert externals(unfiltered) >= {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["pump_x1"].external_id
    }

    narrowed = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(name=[f"Pump Alpha {token}"]),
    )
    assert externals(narrowed) == {timeseries_corpus["pump_1"].external_id}


def test_timeseries_search_filter_narrows_on_a_timeseries_only_field(sync_client, timeseries_corpus, token):
    query = f"Pump {token}"
    by_unit = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(unit=["celsius"]),
    )
    assert externals(by_unit) == {timeseries_corpus["pump_x1"].external_id}

    by_value_type = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(value_type=["TEXT"]),
    )
    assert externals(by_value_type) == set(), "both Pump series are FLOAT"


def test_timeseries_search_filter_narrows_by_metadata_and_labels(sync_client, timeseries_corpus, token):
    query = f"Pump {token}"
    by_metadata = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(metadata={f"tsk_{token}": "beta"}),
    )
    assert externals(by_metadata) == {timeseries_corpus["pump_x1"].external_id}

    by_label = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(labels=["NO_SUCH_LABEL_XYZ"]),
    )
    assert externals(by_label) == set()


def test_timeseries_search_filter_narrows_by_data_set(sync_client, timeseries_corpus, datasets, token):
    """``dataSetId`` is pushed into the search query itself rather than applied afterwards, so it
    takes a different path from the rest of the filter and is worth its own case.

    The Valve series lives in the parent data set and the two Pump series in the child.
    """
    parent, child = datasets
    query = f"{token}"

    in_child = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(data_set_id=[child.id]),
    )
    assert timeseries_corpus["valve"].external_id not in externals(in_child)

    # Naming the parent covers the child, so the whole corpus is back in scope.
    under_parent = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(data_set_id=[parent.id]),
    )
    assert externals(under_parent) >= {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["valve"].external_id
    }


def test_timeseries_search_filter_is_optional(sync_client, timeseries_corpus, token):
    """Omitting it must place no restriction — the search has to keep working for callers who never
    pass one."""
    with_none = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=f"Pump Alpha {token}"), filter=None)
    assert timeseries_corpus["pump_1"].external_id in externals(with_none)


def test_timeseries_search_ranking_survives_the_filter(sync_client, timeseries_corpus, token):
    """The narrowing re-runs the hits through the filter query, which orders by ``dateCreated``.
    The result must keep the *search's* ranking — relevance is the whole point of having searched —
    so this checks the filtered page is a subsequence of the unfiltered one, not a re-sort of it.
    """
    query = f"{token}"
    unfiltered = [ts.external_id for ts in poll_until(
        lambda: sync_client.timeseries.search(intellistream_datahub_sdk.SearchAndFilterForm(query=query)),
        lambda hits: len(hits) >= 2,
    )]
    filtered = [ts.external_id for ts in sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(value_type=["FLOAT"]),
    )]

    kept = [external_id for external_id in unfiltered if external_id in set(filtered)]
    assert filtered == kept, f"filtering reordered the hits: {unfiltered} -> {filtered}"


@pytest.mark.asyncio
async def test_timeseries_search_filter_works_on_the_async_client(
    async_client, sync_client, timeseries_corpus, token
):
    narrowed = await async_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=f"Pump {token}"),
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(name=[f"Pump Alpha {token}"]),
    )
    assert externals(narrowed) == {timeseries_corpus["pump_1"].external_id}


# --------------------------------------------------------------------------- #
# The three that accept a filter and ignore it
# --------------------------------------------------------------------------- #

@pytest.mark.xfail(
    reason="ResourceService.search reads only search.query and limit; the ResourceSearch body's "
           "`filter` is declared in the OpenAPI schema and never looked at, so a caller who "
           "narrows a resource search is silently not narrowed. Server-side.",
    strict=True,
)
def test_resource_search_honours_its_filter(sync_client, resource_corpus, token):
    query = f"Node {token}"
    unfiltered = poll_until(
        lambda: sync_client.resources.search(intellistream_datahub_sdk.SearchAndFilterForm(query=query)),
        lambda hits: len(externals(hits)) >= 2,
    )
    assert externals(unfiltered) >= {r.external_id for r in resource_corpus.values()}

    narrowed = sync_client.resources.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=query),
        filter=intellistream_datahub_sdk.ResourceFilter(labels=["FLT_BETA"]),
    )
    assert externals(narrowed) == {resource_corpus["root"].external_id}


@pytest.mark.xfail(
    reason="DataSetService.search passes only form.getSearch().getQuery() and the limit to the "
           "repository; the Data Set Search body's `filter` is declared and ignored. Server-side.",
    strict=True,
)
def test_dataset_search_honours_its_filter(sync_client, datasets, token):
    parent, child = datasets
    query = f"Filter {token}"
    unfiltered = poll_until(
        lambda: sync_client.datasets.search(query),
        lambda hits: len(externals(hits)) >= 2,
    )
    assert externals(unfiltered) >= {parent.external_id, child.external_id}

    narrowed = sync_client.datasets.search(
        query, filter=intellistream_datahub_sdk.BasicDatasetFilter(metadata={"tier": "gold"}))
    assert externals(narrowed) == {parent.external_id}


@pytest.mark.xfail(
    reason="EventService.search passes only the query and the limit to ClickHouseEventService."
           "search; the EventSearch body's `filter` is declared and ignored. Server-side.",
    strict=True,
)
def test_event_search_honours_its_filter(sync_client, event_corpus, prefix, token):
    query = f"alarm {token}"
    unfiltered = poll_until(
        lambda: sync_client.events.search(intellistream_datahub_sdk.EventSearch(query)), bool)
    assert externals(unfiltered), "the event search index never returned the corpus"

    narrowed = sync_client.events.search(intellistream_datahub_sdk.EventSearch(
        query, filter=intellistream_datahub_sdk.BasicEventFilter(status=["CLOSED"])))
    assert externals(narrowed) == {f"{prefix}_ev_alarmX1"}


def test_the_ignored_filters_are_at_least_accepted(sync_client, resource_corpus, datasets, token):
    """Until the three above are fixed, passing a filter must still not *break* the search.

    Worth pinning separately: "silently ignored" and "rejected as an unknown field" are different
    failures, and a caller migrating to the new contract hits this before they hit the xfails.
    """
    parent, _child = datasets
    assert sync_client.resources.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=f"Node {token}"),
        filter=intellistream_datahub_sdk.ResourceFilter(labels=["FLT_BETA"]),
    ) is not None
    assert sync_client.datasets.search(
        f"Filter {token}", filter=intellistream_datahub_sdk.BasicDatasetFilter(metadata={"tier": "gold"})
    ) is not None
    assert sync_client.events.search(intellistream_datahub_sdk.EventSearch(
        f"alarm {token}", filter=intellistream_datahub_sdk.BasicEventFilter(status=["CLOSED"]))
    ) is not None
