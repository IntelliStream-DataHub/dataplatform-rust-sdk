"""Search coverage for the TimeSeries service.

Mirrors `src/timeseries/test.rs::test_search_timeseries`. Searches hit a backend search index that
lags writes, so we sleep briefly before querying.

The endpoint used to take `name`, `query` and `description` and honour exactly one of them. Only
`query` survives: the phrase already covers the description column, and a name is matched through
the filter's `name` pattern list — which is a case-insensitive pattern rather than the exact
equality the old `name` field did.
"""
import time
import uuid

import intellistream_datahub_sdk
from python_tests.fixtures import *  # noqa: F401,F403  (sync_client fixture)


# The search index is eventually-consistent with writes; give it a moment.
SEARCH_INDEX_DELAY = 3.0


def _uid(prefix="search"):
    return unique_id(prefix)


def _token():
    return uuid.uuid4().hex[:12]


def test_search_finds_created_series_by_query(sync_client, make_ts):
    token = _token()
    ext_id = _uid("query")
    make_ts(external_id=ext_id, name=f"Py SDK Search {token}",
            description=f"description for {token}")

    time.sleep(SEARCH_INDEX_DELAY)

    results = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=f"Py SDK Search {token}"))
    assert isinstance(results, list)
    assert any(t.external_id == ext_id for t in results), (
        f"free-text search did not return the created series {ext_id}"
    )


def test_the_phrase_covers_the_description_column(sync_client, make_ts):
    """What the retired ``description`` field was for: the phrase already matches it."""
    token = _token()
    ext_id = _uid("desc")
    make_ts(external_id=ext_id, name=f"Py SDK Search {token}",
            description=f"unmistakable description {token}")

    time.sleep(SEARCH_INDEX_DELAY)

    results = sync_client.timeseries.search(
        intellistream_datahub_sdk.SearchAndFilterForm(query=f"unmistakable description {token}"))
    assert any(t.external_id == ext_id for t in results), (
        f"the phrase did not match the description of {ext_id}"
    )


def test_a_query_may_contain_an_external_id(sync_client, make_ts):
    """Searching for a tag or id is the obvious thing to try, and this platform's ids are
    underscore-heavy. ``search.query`` used to be ``^[\\p{IsLatin}\\p{Zs}\\p{Nd}]+`` — letters,
    spaces and digits — which rejected every one of them, and every non-Latin script with them."""
    token = _token()
    ext_id = _uid("punct")
    make_ts(external_id=ext_id, name=f"Py SDK Search {token}",
            description=f"tagged as {ext_id}")

    time.sleep(SEARCH_INDEX_DELAY)

    results = sync_client.timeseries.search(intellistream_datahub_sdk.SearchAndFilterForm(query=ext_id))
    assert any(t.external_id == ext_id for t in results)


def test_a_name_is_matched_through_the_filter(sync_client, make_ts):
    """The replacement for the retired ``name`` search field, and strictly more than it could do:
    a pattern list rather than one exact string."""
    token = _token()
    ext_id = _uid("name")
    unique_name = f"Py SDK Search {token}"
    make_ts(external_id=ext_id, name=unique_name)

    time.sleep(SEARCH_INDEX_DELAY)

    exact = sync_client.timeseries.filter(
        intellistream_datahub_sdk.TimeSeriesFilterForm(name=unique_name))
    assert any(t.external_id == ext_id for t in exact), (
        f"filtering by name did not return {ext_id}"
    )

    as_pattern = sync_client.timeseries.filter(
        intellistream_datahub_sdk.TimeSeriesFilterForm(name=f"Py SDK Search {token[:6]}*"))
    assert any(t.external_id == ext_id for t in as_pattern), (
        "the name filter is a pattern list, so a trailing wildcard must match"
    )
