"""Tests for the event vocabulary endpoints.

Mirrors the `vocabulary` module in `src/events/tests.rs`. These answer "what values does this
tenant actually use?" for the four categorical event fields, so they back filter dropdowns and
autocompletes. They return plain strings, not events.
"""
from datetime import datetime, timezone

import intellistream_datahub_sdk
import pytest

from fixtures import TEST_PREFIX, async_client, sync_client, unique_id
from polling import poll_until


DIMENSIONS = [
    intellistream_datahub_sdk.EventDimension.TYPE,
    intellistream_datahub_sdk.EventDimension.SUB_TYPE,
    intellistream_datahub_sdk.EventDimension.STATUS,
    intellistream_datahub_sdk.EventDimension.SOURCE,
]

# Fixed, not unique: a dimension row outlives the events that put it there, so a fresh name per
# run would add a vocabulary entry per run.
SEEDED_TYPES = [f"{TEST_PREFIX}vocab_type_a", f"{TEST_PREFIX}vocab_type_b"]


@pytest.fixture(scope="module")
def seeded_types(sync_client):
    events = [
        intellistream_datahub_sdk.Event(
            type=t, external_id=unique_id("vocab_event"), event_time=datetime.now(timezone.utc)
        )
        for t in SEEDED_TYPES
    ]
    sync_client.events.create(events)
    try:
        found = poll_until(
            lambda: sync_client.events.search_types(f"{TEST_PREFIX}vocab_type"),
            lambda found: set(SEEDED_TYPES) <= set(found),
        )
        assert set(SEEDED_TYPES) <= set(found), found
        yield SEEDED_TYPES
    finally:
        sync_client.events.delete(events)


def test_list_dimensions_are_distinct(sync_client):
    for dimension in DIMENSIONS:
        values = sync_client.events.list_dimension(dimension)
        assert isinstance(values, list)
        assert all(isinstance(v, str) for v in values)

        # Distinctness is the guarantee worth pinning. The endpoint also documents alphabetical
        # ordering, but that ordering comes from the database collation — case-insensitive, and
        # handling punctuation differently from a byte comparison — so asserting it exactly would
        # make this fail on a differently-configured database rather than on a real regression.
        assert len(set(values)) == len(values), dimension


def test_named_helpers_match_the_generic_form(sync_client):
    assert sync_client.events.list_types() == sync_client.events.list_dimension(
        intellistream_datahub_sdk.EventDimension.TYPE
    )
    assert sync_client.events.list_sub_types() == sync_client.events.list_dimension(
        intellistream_datahub_sdk.EventDimension.SUB_TYPE
    )
    assert sync_client.events.list_statuses() == sync_client.events.list_dimension(
        intellistream_datahub_sdk.EventDimension.STATUS
    )
    assert sync_client.events.list_sources() == sync_client.events.list_dimension(
        intellistream_datahub_sdk.EventDimension.SOURCE
    )


def test_limit_caps_results(sync_client, seeded_types):
    assert len(sync_client.events.list_types(limit=1)) == 1


def test_limit_is_clamped_not_rejected(sync_client):
    # The server clamps to 1..=10000 rather than rejecting, so absurd values still succeed.
    assert isinstance(sync_client.events.list_types(limit=999_999), list)
    assert isinstance(sync_client.events.list_types(limit=0), list)


def test_search_is_case_insensitive_substring(sync_client, seeded_types):
    sample = seeded_types[0]

    assert sample in sync_client.events.search_types(sample)
    assert sample in sync_client.events.search_types(sample.upper())
    assert sample in sync_client.events.search_types(sample[:-1])

    # Search filters the vocabulary; it never invents values.
    hits = sync_client.events.search_types(sample[:-1])
    assert all(sample[:-1] in v.lower() for v in hits), hits


def test_no_match_is_empty_not_an_error(sync_client):
    assert sync_client.events.search_types("zzz_no_such_type_zzz") == []


def test_every_search_route_resolves(sync_client):
    # The route segments differ between the two families — plural to list, singular to search.
    # A typo there is a 404 that only shows up at runtime, so exercise all eight.
    for call in (
        sync_client.events.search_types,
        sync_client.events.search_sub_types,
        sync_client.events.search_statuses,
        sync_client.events.search_sources,
    ):
        assert isinstance(call("a", limit=1), list)
    for call in (
        sync_client.events.list_types,
        sync_client.events.list_sub_types,
        sync_client.events.list_statuses,
        sync_client.events.list_sources,
    ):
        assert isinstance(call(limit=1), list)


@pytest.mark.asyncio
async def test_async_vocabulary(async_client):
    types = await async_client.events.list_types()
    assert isinstance(types, list)

    assert types == await async_client.events.list_dimension(
        intellistream_datahub_sdk.EventDimension.TYPE
    )

    if types:
        assert types[0] in await async_client.events.search_types(types[0])
    assert await async_client.events.search_sources("zzz_no_such_source_zzz") == []
