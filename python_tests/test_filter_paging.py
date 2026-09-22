"""Keyset paging: ``cursor`` in, ``next_cursor`` out.

Nothing here used ``OFFSET``, and nothing here should: ``OFFSET n`` makes the database produce and
discard n rows on every page, so cost grows with depth, and a row written before the current
position shifts every later one — the next page then repeats or skips one. A cursor names a
*position in the order* instead, which is a range the index seeks straight to and which writes
elsewhere cannot shift.

Before this landed the node filters could not page at all, and events accepted a ``cursor`` while
never returning one — so the feature existed only from the outside, with callers reverse-engineering
the encoding from the last row of the previous page. ``next_cursor`` on the response is what made it
usable, and it is why the SDK's ``filter`` returns a ``Page`` rather than a bare list.

Two rules carry the weight, and neither fails loudly when it is wrong:

* **A cursor belongs to the sort that produced it.** Continuing it under a different order asks
  "everything after X" of a sequence no longer in that order, and answers with a page that is
  silently short.
* **A cursor is opaque, and an unreadable one is rejected.** It encodes the sort, the boundary and
  the id. A malformed cursor used to decode to "no cursor", which means the first page — so a client
  that pages by echoing back what it was handed looped on page one forever, never advancing and
  never told anything was wrong. It is a 400 now — on all four endpoints, from one
  ``@ControllerAdvice``, with a message naming what is wrong. An *absent* cursor is not this: no
  cursor is the start of a walk, not an error in one.
"""
import base64
import string

import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException

from fixtures import async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from filter_fixtures import (  # noqa: F401  (fixtures)
    datasets,
    prefix,
    sortable_events,
    sortable_timeseries,
    token,
)
from polling import poll_until


def ids_of(page):
    return [item.external_id for item in page]


def forge_cursor(template, property_name, row_id, value):
    """Rewrite a server-minted cursor's boundary — only ever to test what the server does with a
    bad one.

    ``template`` is a real ``next_cursor`` produced by the very sort the forgery will be sent
    under, and only the id and value segments are replaced. Whatever sits in front of the sort —
    a version tag, say — travels through exactly as the server minted it.

    Building one from scratch is what this deliberately does not do. A cursor is opaque so that
    its encoding can change, and a test that spells the encoding out turns any such change into a
    red run *here* rather than in the platform's own ``PageCursor`` tests. It also fails the wrong
    way round: a forgery the server rejects as structurally malformed answers 400 just like one
    rejected for its boundary, so a stale shape leaves the tests below passing while testing
    nothing.

    The segment layout still has to be known this far: ``<property>|<asc|desc>|<id>|v<value>``,
    with ``n`` for a null boundary and the value last because it may contain the separator.
    """
    raw = base64.urlsafe_b64decode(template + "=" * (-len(template) % 4)).decode()
    parts = raw.split("|")
    assert property_name in parts, f"cursor does not carry its sort property: {raw!r}"
    index = parts.index(property_name)
    # Only a version-style prefix may precede the sort. Further left means the name was matched
    # inside the boundary value instead, which is not a segment this may rewrite.
    assert index <= 1, f"unexpected cursor layout, forge_cursor needs updating: {raw!r}"
    assert len(parts) >= index + 4, \
        f"cursor has too few segments, forge_cursor needs updating: {raw!r}"
    direction = parts[index + 1]
    assert direction in ("asc", "desc"), \
        f"cursor does not carry its direction where expected, forge_cursor needs updating: {raw!r}"

    tagged = "n" if value is None else f"v{value}"
    forged = "|".join([*parts[:index], property_name, direction, str(row_id), tagged])
    return base64.urlsafe_b64encode(forged.encode()).decode().rstrip("=")


@pytest.fixture
def ts_page(sync_client, prefix):
    """One page of the sortable timeseries corpus."""
    def _page(**paging):
        return sync_client.timeseries.filter(external_id=f"{prefix}_sort_ts_*", **paging)
    return _page


@pytest.fixture
def by_index(sortable_timeseries):
    return [sortable_timeseries[i]["external_id"] for i in range(6)]


def walk(fetch_page, **paging):
    """Page through everything, returning the rows in order and how many requests it took.

    This is the loop the api documents — "keep going while ``next_cursor`` is present" — written
    once so every test below walks the same way a caller would.
    """
    rows, requests, cursor = [], 0, None
    while True:
        page = fetch_page(cursor=cursor, **paging)
        requests += 1
        rows += ids_of(page)
        cursor = page.next_cursor
        if cursor is None:
            return rows, requests
        assert requests < 50, "the walk did not terminate"


# --------------------------------------------------------------------------- #
# the Page object itself
# --------------------------------------------------------------------------- #

def test_a_page_behaves_like_a_list(ts_page, by_index):
    """``filter`` used to return a list, and code written against that must keep working — the only
    reason it is a ``Page`` now is that a list cannot carry the cursor."""
    page = ts_page(sort_by="name", sort_order="asc")

    assert len(page) == 6
    assert page[0].external_id == by_index[0]
    assert page[-1].external_id == by_index[-1]
    assert [ts.external_id for ts in page] == by_index
    assert ids_of(page[1:3]) == by_index[1:3], "slicing works"
    assert page[0] in page
    assert bool(page) is True
    assert page == page.items, "equal to the plain list of the same rows"
    assert isinstance(page.items, list)


def test_an_empty_page_is_falsey(sync_client, prefix):
    page = sync_client.timeseries.filter(external_id="no_such_external_id_at_all")
    assert len(page) == 0
    assert bool(page) is False
    assert page == []
    assert page.next_cursor is None


# --------------------------------------------------------------------------- #
# next_cursor: when it is there, and when it is not
# --------------------------------------------------------------------------- #

def test_a_full_page_carries_a_cursor_and_a_short_one_does_not(ts_page):
    """The end-of-walk signal is the absence of a cursor, so a short page must not carry one.

    A *full* page may still be the last — the server does not count the rows twice — which is why
    the walk ends with one request that comes back empty rather than with a count.
    """
    assert ts_page(limit=4, sort_by="name").next_cursor is not None, "4 of 6 is a full page"
    assert ts_page(limit=10, sort_by="name").next_cursor is None, "6 of 10 is short: the end"


def test_a_full_last_page_costs_one_extra_empty_request(ts_page, by_index):
    """Six rows at three per page: two full pages, then an empty third that ends the walk."""
    rows, requests = walk(ts_page, limit=3, sort_by="name", sort_order="asc")
    assert rows == by_index
    assert requests == 3, "two full pages plus the empty one that proves there are no more"


def test_an_exactly_divisible_walk_returns_every_row_once(ts_page, by_index):
    rows, _requests = walk(ts_page, limit=2, sort_by="name", sort_order="asc")
    assert rows == by_index
    assert len(rows) == len(set(rows)), "no row may appear on two pages"


def test_a_walk_with_an_indivisible_page_size(ts_page, by_index):
    rows, requests = walk(ts_page, limit=4, sort_by="name", sort_order="asc")
    assert rows == by_index
    assert requests == 2, "4 + 2: the short second page ends it"


@pytest.mark.parametrize("page_size", [1, 2, 3, 5, 6, 7])
def test_every_page_size_yields_the_same_sequence(ts_page, by_index, page_size):
    """Paging must not change *what* comes back or in what order — only how it is delivered."""
    rows, _requests = walk(ts_page, limit=page_size, sort_by="name", sort_order="asc")
    assert rows == by_index


def test_paging_follows_the_sort_direction(ts_page, by_index):
    rows, _requests = walk(ts_page, limit=2, sort_by="name", sort_order="desc")
    assert rows == list(reversed(by_index))


def test_paging_the_default_order(ts_page, sync_client, prefix):
    """No explicit sort, so the walk runs in the default newest-created-first order."""
    rows, _requests = walk(ts_page, limit=2)
    expected = ids_of(sync_client.timeseries.filter(external_id=f"{prefix}_sort_ts_*", limit=100))
    assert rows == expected


def test_paging_across_a_run_of_tied_values(ts_page, sync_client, prefix):
    """The case the ``id`` tie-breaker exists for.

    Every row shares a ``dataSetId``, so without a total order a page boundary falling inside the
    run would repeat or drop exactly the rows around it. Two pages of three must still be six
    distinct rows.
    """
    rows, _requests = walk(ts_page, limit=3, sort_by="dataSetId", sort_order="asc")
    assert len(rows) == 6
    assert len(set(rows)) == 6, f"a tied boundary repeated or dropped rows: {rows}"

    unpaged = ids_of(sync_client.timeseries.filter(
        external_id=f"{prefix}_sort_ts_*", sort_by="dataSetId", sort_order="asc", limit=100))
    assert rows == unpaged


# --------------------------------------------------------------------------- #
# the cursor is opaque
# --------------------------------------------------------------------------- #

def test_the_cursor_is_opaque(ts_page):
    """Asserted only as far as a caller can rely on it: base64url, unpadded, so it survives a
    query string, a JSON body and a copy-paste without escaping.

    What the encoding *says* is deliberately not asserted here. It has already changed once — it
    carried a ``v1`` version tag and no longer does — and pinning that from this repository is what
    made an internal change to the api's ``PageCursor`` fail an SDK suite, on an unrelated platform
    pull request, rather than the platform's own tests for it. The two properties that matter to a
    caller are covered behaviourally instead: that a cursor belongs to the sort that produced it
    (``test_continuing_a_cursor_under_a_different_sort_is_refused``) and that an unreadable one is
    refused rather than silently restarting the walk
    (``test_an_unreadable_cursor_never_silently_returns_page_one``).
    """
    cursor = ts_page(limit=2, sort_by="name", sort_order="asc").next_cursor
    assert cursor and "|" not in cursor and " " not in cursor and "=" not in cursor
    # Deliberately not the base64url alphabet: a signed or dot-delimited token would satisfy
    # everything a caller needs and fail that, which is the coupling this test just shed.
    assert set(cursor) <= set(string.ascii_letters + string.digits + "-._~"), cursor


UNREADABLE_CURSORS = [
    pytest.param("not-a-cursor", id="not-base64"),
    pytest.param("!!!", id="punctuation"),
    pytest.param("djE6", id="truncated"),
    pytest.param(base64.urlsafe_b64encode(b"v9|junk").decode(), id="right-alphabet-wrong-shape"),
]


@pytest.mark.parametrize("nonsense", UNREADABLE_CURSORS)
def test_an_unreadable_cursor_is_rejected(ts_page, nonsense):
    """A cursor that cannot be read is a caller mistake, and has to be said out loud.

    Ignoring it returns the *first* page, and a client paging by echoing back what it was handed
    then receives page one forever: never advancing, never finishing, never told anything is wrong.

    The message has to name what is wrong, because the cursor is opaque — a caller cannot inspect
    one to work out why it was refused.
    """
    with pytest.raises(DataHubException) as excinfo:
        ts_page(limit=3, sort_by="name", sort_order="asc", cursor=nonsense)
    assert excinfo.value.status_code == 400
    assert "cursor" in excinfo.value.message.lower(), excinfo.value.message


def test_every_filter_endpoint_rejects_an_unreadable_cursor(sync_client, prefix, datasets):
    """All four, because they did not agree.

    The rejection was thrown for a long time before it was rendered: three endpoints answered 200
    with an empty body — telling a client its request had succeeded and matched nothing — and
    ``/datasets/filter``'s catch-all turned the caller's mistake into a **500**, a server fault
    reported for a bad request. One shape now, from one ``@ControllerAdvice``.
    """
    calls = {
        "timeseries": lambda: sync_client.timeseries.filter(limit=2, cursor="not-a-cursor"),
        "resources": lambda: sync_client.resources.filter(limit=2, cursor="not-a-cursor"),
        "datasets": lambda: sync_client.datasets.filter(limit=2, cursor="not-a-cursor"),
        "events": lambda: sync_client.events.filter(limit=2, cursor="not-a-cursor"),
    }
    for endpoint, call in calls.items():
        with pytest.raises(DataHubException) as excinfo:
            call()
        assert excinfo.value.status_code == 400, \
            f"{endpoint} answered {excinfo.value.status_code}: {excinfo.value.message[:120]}"


@pytest.mark.parametrize("nonsense", UNREADABLE_CURSORS)
def test_an_unreadable_cursor_never_silently_returns_page_one(ts_page, nonsense):
    """The invariant the rejection exists for, asserted so it holds either way.

    Whether the server refuses the cursor (the intended 400) or drops the page on the floor (what
    it does today), what must never happen is page one coming back as though the cursor had been
    honoured — that is the shape that loops a client forever.
    """
    first_page = ids_of(ts_page(limit=3, sort_by="name", sort_order="asc"))
    assert first_page, "the corpus should fill a first page"
    try:
        got = ids_of(ts_page(limit=3, sort_by="name", sort_order="asc", cursor=nonsense))
    except DataHubException as error:
        assert error.status_code == 400
        return
    assert got != first_page, f"a bad cursor silently restarted the walk: cursor={nonsense!r}"


def test_an_absent_cursor_is_the_start_of_a_walk_not_an_error(ts_page):
    """``None`` and blank are not "unreadable" — asking for the first page is exactly what no
    cursor means, so they must keep working while nonsense is refused."""
    first_page = ids_of(ts_page(limit=3, sort_by="name", sort_order="asc"))
    assert ids_of(ts_page(limit=3, sort_by="name", sort_order="asc", cursor=None)) == first_page
    assert ids_of(ts_page(limit=3, sort_by="name", sort_order="asc", cursor="")) == first_page


def test_a_malformed_boundary_is_a_400_and_not_a_500(sync_client, prefix, sortable_timeseries):
    """A cursor is opaque but unsigned, so a caller can put anything in one.

    A boundary that is not a number used to reach ``Long.parseLong`` on a numeric or temporal sort
    column and come back as a **500** — a server error raised by a value the caller supplied.
    Numeric and temporal columns validate their boundary now, so it is a 400 like any other
    unusable cursor rather than a server fault.
    """
    def page(**paging):
        return sync_client.timeseries.filter(external_id=f"{prefix}_sort_ts_*", **paging)

    for property_name in ["id", "dataSetId", "createdTime", "lastUpdatedTime"]:
        template = page(limit=1, sort_by=property_name).next_cursor
        assert template, f"{property_name}: no cursor to forge from"
        # The template unmodified has to be accepted, or the refusal below would be about the
        # forgery's *shape* rather than about its boundary — which is a 400 either way, and so a
        # test that cannot tell the two apart passes without asserting anything.
        try:
            page(limit=1, sort_by=property_name, cursor=template)
        except DataHubException as error:
            raise AssertionError(
                f"{property_name}: the server refused a cursor it minted itself, so nothing below "
                f"can be attributed to the boundary: {error.message[:120]}") from error

        cursor = forge_cursor(template, property_name, "5", "not-a-number")
        with pytest.raises(DataHubException) as excinfo:
            page(limit=2, sort_by=property_name, cursor=cursor)
        assert excinfo.value.status_code == 400, \
            f"{property_name}: {excinfo.value.status_code} {excinfo.value.message[:100]}"


def test_an_injection_payload_in_the_cursor_boundary_is_data(sync_client, prefix,
                                                             sortable_timeseries, by_index):
    """The cursor boundary is the one caller-supplied value that reaches a comparison rather than
    a pattern, so it is worth showing it is bound rather than interpolated. (The sort *property* is
    the other half of that question, and is a whitelist — see
    ``test_an_unsortable_property_falls_back_to_the_default``.)

    Each payload rides behind row C's name, which is what makes the answer knowable: the boundary
    lands between C and D because C sorts before D, and nothing the payload contains can move it.
    A payload sent as the whole boundary cannot be read that way — where a punctuation-led string
    sorts is exactly what collations disagree about. glibc ignores punctuation at the primary level
    and ICU does not, so ``' UNION SELECT 1 --`` sorts *before* this corpus under one and *after*
    it under the other, and the test would be asserting the database's locale rather than the
    server's parameter binding.

    The equivalence is the point: a bound value gives D, E, F, while a boundary reaching the query
    as syntax gives an error or a different set (``' OR 1=1 --`` would return all six). "It
    returned no rows" would distinguish neither, and nor would "it did not crash".
    """
    anchor = sortable_timeseries[2]["name"]
    template = sync_client.timeseries.filter(
        external_id=f"{prefix}_sort_ts_*", limit=1, sort_by="name",
        sort_order="asc").next_cursor
    assert template, "no cursor to forge a boundary into"

    def page_after(boundary):
        # Cursor id 0: below every real id, so the tie-break never decides which rows come back.
        return ids_of(sync_client.timeseries.filter(
            external_id=f"{prefix}_sort_ts_*", limit=10, sort_by="name", sort_order="asc",
            cursor=forge_cursor(template, "name", "0", boundary)))

    # The anchor alone is row C's own name, so the id tie-break decides C, and 0 is below every
    # real id: C is still returned. Appending anything at all moves the boundary strictly past it.
    assert page_after(anchor) == by_index[2:]
    for payload in ["' OR 1=1 --", "'; DROP TABLE node;--", "' UNION SELECT 1 --", "{x:String}"]:
        assert page_after(f"{anchor} {payload}") == by_index[3:], f"payload={payload!r}"

    # And the table is still there afterwards.
    assert ids_of(sync_client.timeseries.filter(
        external_id=f"{prefix}_sort_ts_*", sort_by="name", sort_order="asc")) == by_index


# --------------------------------------------------------------------------- #
# a cursor belongs to the sort that produced it
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("mismatch", [
    pytest.param(dict(sort_by="externalId", sort_order="asc"), id="different-property"),
    pytest.param(dict(sort_by="name", sort_order="desc"), id="different-direction"),
    pytest.param({}, id="no-sort-at-all"),
])
def test_continuing_a_cursor_under_a_different_sort_is_refused(ts_page, mismatch):
    """A cursor is a position in one particular order.

    Continuing it under another asks "everything after X" of a sequence that is no longer in that
    order, so the honest answer is a refusal — the alternative is a page that looks fine and is
    silently short. The previous behaviour was to ignore ``sort`` whenever a cursor was present,
    which only looked harmless while events had exactly one possible order.

    ``no-sort-at-all`` is a mismatch too: omitting the sort does not mean "whatever the cursor
    says", it means the default order, which is a different order.
    """
    cursor = ts_page(limit=2, sort_by="name", sort_order="asc").next_cursor
    assert cursor is not None

    with pytest.raises(DataHubException) as excinfo:
        ts_page(limit=2, cursor=cursor, **mismatch)
    assert excinfo.value.status_code == 400


def test_the_mismatch_refusal_names_both_sorts(ts_page):
    """The cursor is opaque, so the caller cannot see which sort it belongs to — the refusal has to
    tell them, and tell them what they asked for instead, or they cannot tell which half to fix."""
    cursor = ts_page(limit=2, sort_by="name", sort_order="asc").next_cursor
    with pytest.raises(DataHubException) as excinfo:
        ts_page(limit=2, sort_by="externalId", sort_order="asc", cursor=cursor)

    message = excinfo.value.message
    assert "name asc" in message, message
    assert "externalId asc" in message, message


def test_a_cursor_continued_with_its_own_sort_is_accepted(ts_page, by_index):
    """The other side of the rule — the same sort must keep working, or paging is unusable."""
    first = ts_page(limit=2, sort_by="name", sort_order="asc")
    second = ts_page(limit=2, sort_by="name", sort_order="asc", cursor=first.next_cursor)
    assert ids_of(second) == by_index[2:4]


# --------------------------------------------------------------------------- #
# datasets and resources
# --------------------------------------------------------------------------- #

def test_datasets_page(sync_client, datasets, prefix):
    parent, child = datasets

    def page(cursor=None):
        return sync_client.datasets.filter(
            intellistream_datahub_sdk.DatasetFilter(external_id=f"{prefix}_ds_*"),
            limit=1, sort_by="name", sort_order="asc", cursor=cursor)

    rows, requests = walk(lambda cursor=None, **_: page(cursor))
    assert rows == [child.external_id, parent.external_id]
    assert requests == 3, "two full pages of one, then the empty one that ends the walk"


def test_resources_page(sync_client, sortable_timeseries, prefix):
    """The generic node query pages too. Narrowed to timeseries so the population is the sortable
    corpus rather than every node sharing the prefix."""
    def page(cursor=None):
        return sync_client.resources.filter(
            external_id=f"{prefix}_sort_ts_*", node_type=["timeseries"],
            limit=2, sort_by="externalId", sort_order="asc", cursor=cursor)

    rows, _requests = walk(lambda cursor=None, **_: page(cursor))
    assert rows == [f"{prefix}_sort_ts_{i}" for i in range(6)]


# --------------------------------------------------------------------------- #
# events
# --------------------------------------------------------------------------- #

@pytest.fixture
def ev_page(sync_client, prefix, sortable_events):
    def _page(**paging):
        request = dict(
            filter=intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}_sort_ev_*"), **paging)
        return sync_client.events.filter(**request)
    return _page


@pytest.fixture
def ev_by_index(sortable_events):
    return [sortable_events[i]["external_id"] for i in range(4)]


def test_events_page_in_the_default_order(ev_page, ev_by_index, sync_client, prefix):
    # Wait for the corpus first, so a projection lag cannot be mistaken for the end of the walk.
    poll_until(lambda: ev_page(limit=50), lambda page: len(page) >= 4, timeout=15.0)

    rows, _requests = walk(ev_page, limit=2)
    assert rows == ev_by_index


def test_events_page_under_an_explicit_sort(ev_page, sortable_events):
    poll_until(lambda: ev_page(limit=50), lambda page: len(page) >= 4, timeout=15.0)

    rows, _requests = walk(ev_page, limit=2, sort_by="type", sort_order="asc")
    assert rows == [sortable_events[i]["external_id"] for i in [3, 2, 1, 0]]


def test_an_event_walk_covers_every_row_once(ev_page, ev_by_index):
    poll_until(lambda: ev_page(limit=50), lambda page: len(page) >= 4, timeout=15.0)

    rows, _requests = walk(ev_page, limit=1, sort_by="eventTime", sort_order="asc")
    assert rows == ev_by_index
    assert len(rows) == len(set(rows))


def test_an_event_short_page_carries_no_cursor(ev_page):
    poll_until(lambda: ev_page(limit=50), lambda page: len(page) >= 4, timeout=15.0)
    assert ev_page(limit=50).next_cursor is None
    assert ev_page(limit=2).next_cursor is not None


@pytest.mark.parametrize("property_name", ["subType", "status"])
def test_a_nullable_event_sort_pages_all_the_way_through(ev_page, sortable_events, property_name):
    """Sorting events by a nullable column used to produce a first page with **no cursor**, however
    many rows remained — so a client following the documented loop stopped there and reported
    success, having seen one page of an arbitrary number.

    Two decisions combined to cause it: nullable columns were excluded from paging, and the
    exclusion was only enforced when a cursor was already supplied — so the first request, which by
    definition has none, passed the check and then had no value to build a cursor from. The
    exclusion was the wrong half to keep: the null block is placed explicitly instead, as the node
    filters already did.
    """
    poll_until(lambda: ev_page(limit=50), lambda page: len(page) >= 4, timeout=15.0)
    every = [spec["external_id"] for spec in sortable_events.values()]

    first = ev_page(limit=2, sort_by=property_name, sort_order="asc")
    assert first.next_cursor is not None, "a full page on a nullable sort must still page"

    rows, _requests = walk(ev_page, limit=2, sort_by=property_name, sort_order="asc")
    assert sorted(rows) == sorted(every), f"the walk lost rows: {rows}"
    assert len(rows) == len(set(rows)), f"the walk repeated rows: {rows}"


def test_an_exhausted_event_walk_omits_the_cursor_rather_than_nulling_it(ev_page, ev_by_index):
    """The loop's terminating condition. A client tests for *presence*, so an exhausted response
    that sends ``nextCursor: null`` instead of omitting the field never stops.

    In Python both spellings arrive as ``None``, so this asserts the SDK-visible half; the wire
    half is pinned by ``next_cursor_is_read_from_a_response_and_never_serialized`` in
    ``src/filters.rs`` and by the api's own ``DataWrapperCursorTest``.
    """
    poll_until(lambda: ev_page(limit=50), lambda page: len(page) >= 4, timeout=15.0)
    rows, _requests = walk(ev_page, limit=2, sort_by="eventTime", sort_order="asc")
    assert rows == ev_by_index

    exhausted = ev_page(limit=50)
    assert exhausted.next_cursor is None


@pytest.mark.asyncio
async def test_async_paging_matches_the_sync_client(async_client, sync_client, prefix,
                                                    sortable_timeseries, by_index):
    first = await async_client.timeseries.filter(
        external_id=f"{prefix}_sort_ts_*", limit=2, sort_by="name", sort_order="asc")
    assert ids_of(first) == by_index[:2]
    assert first.next_cursor is not None

    second = await async_client.timeseries.filter(
        external_id=f"{prefix}_sort_ts_*", limit=2, sort_by="name", sort_order="asc",
        cursor=first.next_cursor)
    assert ids_of(second) == by_index[2:4]
