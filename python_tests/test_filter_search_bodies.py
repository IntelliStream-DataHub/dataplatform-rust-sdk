"""The ``filter`` on the four ``/search`` endpoints.

Each search endpoint declares a ``filter`` of the same type its own ``/filter`` endpoint takes, and
all four now apply it. Three of them used to accept one and drop it on the floor — the tests below
were ``xfail(strict=True)`` for exactly that, encoding the contract a caller reading the OpenAPI
document would expect, and they went green when the server closed the gap rather than having to be
rewritten.

The contract they pin: the phrase decides which rows are candidates, the filter only ever *removes*
some of them, and ``limit`` caps what survives. So a filter can never widen a search, and omitting
it returns the phrase's hits as found.

Free-text search is ranked and fuzzy where a filter is exact, so these tests assert membership
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
# /timeseries/search — the one that has always honoured its filter
# --------------------------------------------------------------------------- #

def test_timeseries_search_finds_the_corpus_before_any_filtering(sync_client, timeseries_corpus, token):
    """The baseline every test below narrows from. If this is empty the search index has not caught
    up and the narrowing assertions would pass for the wrong reason."""
    hits = poll_until(
        lambda: sync_client.timeseries.search(f"Pump Alpha {token}"),
        bool,
    )
    assert timeseries_corpus["pump_1"].external_id in externals(hits)


def test_timeseries_search_filter_narrows_on_an_inherited_node_field(sync_client, timeseries_corpus, token):
    """``name`` is inherited from the node base, and is exactly the kind of field the old
    hand-rolled post-filter did not implement — it was accepted and dropped."""
    query = f"Pump {token}"
    unfiltered = poll_until(
        lambda: sync_client.timeseries.search(query),
        lambda hits: len(externals(hits)) >= 2,
    )
    assert externals(unfiltered) >= {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["pump_x1"].external_id
    }

    narrowed = sync_client.timeseries.search(
        query,
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(name=[f"Pump Alpha {token}"]),
    )
    assert externals(narrowed) == {timeseries_corpus["pump_1"].external_id}


def test_timeseries_search_filter_narrows_on_a_timeseries_only_field(sync_client, timeseries_corpus, token):
    query = f"Pump {token}"
    by_unit = sync_client.timeseries.search(
        query,
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(unit=["celsius"]),
    )
    assert externals(by_unit) == {timeseries_corpus["pump_x1"].external_id}

    by_value_type = sync_client.timeseries.search(
        query,
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(value_type=["TEXT"]),
    )
    assert externals(by_value_type) == set(), "both Pump series are FLOAT"


def test_timeseries_search_filter_narrows_by_metadata_and_labels(sync_client, timeseries_corpus, token):
    query = f"Pump {token}"
    by_metadata = sync_client.timeseries.search(
        query,
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(metadata={f"tsk_{token}": "beta"}),
    )
    assert externals(by_metadata) == {timeseries_corpus["pump_x1"].external_id}

    by_label = sync_client.timeseries.search(
        query,
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
        query,
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(data_set_id=[child.id]),
    )
    assert timeseries_corpus["valve"].external_id not in externals(in_child)

    # Naming the parent covers the child, so the whole corpus is back in scope.
    under_parent = sync_client.timeseries.search(
        query,
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(data_set_id=[parent.id]),
    )
    assert externals(under_parent) >= {
        timeseries_corpus["pump_1"].external_id, timeseries_corpus["valve"].external_id
    }


def test_timeseries_search_filter_is_optional(sync_client, timeseries_corpus, token):
    """Omitting it must place no restriction — the search has to keep working for callers who never
    pass one."""
    with_none = sync_client.timeseries.search(
        f"Pump Alpha {token}", filter=None)
    assert timeseries_corpus["pump_1"].external_id in externals(with_none)


def test_timeseries_search_ranking_survives_the_filter(sync_client, timeseries_corpus, token):
    """The narrowing re-runs the hits through the filter query, which orders by ``dateCreated``.
    The result must keep the *search's* ranking — relevance is the whole point of having searched —
    so this checks the filtered page is a subsequence of the unfiltered one, not a re-sort of it.
    """
    query = f"{token}"
    unfiltered = [ts.external_id for ts in poll_until(
        lambda: sync_client.timeseries.search(query),
        lambda hits: len(hits) >= 2,
    )]
    filtered = [ts.external_id for ts in sync_client.timeseries.search(
        query,
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(value_type=["FLOAT"]),
    )]

    kept = [external_id for external_id in unfiltered if external_id in set(filtered)]
    assert filtered == kept, f"filtering reordered the hits: {unfiltered} -> {filtered}"


@pytest.mark.asyncio
async def test_timeseries_search_filter_works_on_the_async_client(
    async_client, sync_client, timeseries_corpus, token
):
    narrowed = await async_client.timeseries.search(
        f"Pump {token}",
        filter=intellistream_datahub_sdk.TimeSeriesFilterForm(name=[f"Pump Alpha {token}"]),
    )
    assert externals(narrowed) == {timeseries_corpus["pump_1"].external_id}


# --------------------------------------------------------------------------- #
# The three that used to accept a filter and ignore it
# --------------------------------------------------------------------------- #

def test_resource_search_honours_its_filter(sync_client, resource_corpus, token):
    query = f"Node {token}"
    unfiltered = poll_until(
        lambda: sync_client.resources.search(query),
        lambda hits: len(externals(hits)) >= 2,
    )
    assert externals(unfiltered) >= {r.external_id for r in resource_corpus.values()}

    narrowed = sync_client.resources.search(
        query,
        filter=intellistream_datahub_sdk.ResourceFilter(labels=["FLT_BETA"]),
    )
    assert externals(narrowed) == {resource_corpus["root"].external_id}


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


def test_event_search_honours_its_filter(sync_client, event_corpus, prefix, token):
    query = f"alarm {token}"
    unfiltered = poll_until(
        lambda: sync_client.events.search(query), bool)
    assert externals(unfiltered), "the event search index never returned the corpus"

    narrowed = sync_client.events.search(
        query, filter=intellistream_datahub_sdk.BasicEventFilter(status=["CLOSED"]))
    assert externals(narrowed) == {f"{prefix}_ev_alarmX1"}
