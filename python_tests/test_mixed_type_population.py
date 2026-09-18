"""Every node type shares one table, so a typed query must narrow *before* it limits.

Assets, timeseries, functions, resources, data sets and policies are one table with single-table
inheritance. That makes every typed read a filtered read, and gives it a failure mode no
single-type test can reach: if the type restriction is applied *after* the row limit — a
``LIMIT 100`` in SQL and a ``.filter(instanceof)`` in Java, say — then ``timeseries.filter(limit=100)``
against a tenant holding 100 assets and 100 timeseries takes 100 mixed rows, throws most of them
away, and answers with a short page that looks exactly like a tenant that only has that many series.
Nothing about the response says rows were dropped, and ``nextCursor`` is absent on a short page, so
a caller's paging loop ends there too.

Which rows survive depends on where the other types happen to sit in the scan, so the corpus here
is built to make that dependence visible rather than incidental:

* **ids interleave.** One ``/resources/create`` call lists the five types round-robin, so the ids
  come out ``asset, timeseries, dataset, function, resource, asset, …``. The fixture asserts this
  rather than assuming it — a corpus whose types landed in contiguous id blocks would let a broken
  typed query pass by luck, and the assertion is what stops that from being invisible.
* **names put the timeseries last.** ``Mixed Alpha`` … ``Mixed Tango`` sort in type order, so all
  ``4N`` other nodes sort ahead of every timeseries. A typed query that limits before it narrows
  returns *nothing* under ``sort_by="name"``, which is an unambiguous signal rather than a count
  that has to be reasoned about.
* **one phrase matches all of them.** Every name carries the run token, so a single search phrase
  is a candidate set spanning all five types — the search-side version of the same question.

Events are deliberately absent: they live in their own ClickHouse table with their own id space, so
they cannot dilute a node query. Policies are absent for a duller reason — a ``Policy`` built here
and sent to ``/resources/create`` comes back 400 ``unreadable-request-body``, so one cannot be put
in the corpus until that is chased down. The five types below are every type this SDK can create.

The other half of the contract is the generic query: ``/resources/*`` spans all six types on
purpose, so here it must return *every* type rather than the dominant one, and answer each row in
its own class. ``test_polymorphic_nodes.py`` pins that dispatch on one node of each type; this
suite pins that it survives a population where the types compete for the same page.
"""
import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import (
    Asset,
    Dataset,
    Function,
    Resource,
    ResourceFilter,
    TimeSeries,
    TimeSeriesFilter,
)

from fixtures import TEST_LABEL, sync_client, unique_id  # noqa: F401  (fixtures)
from polling import poll_until

# Per type. Large enough that a page diluted by four other types is obviously short, small enough
# that the whole corpus is one create call and one read.
N = 12

# node_type -> (class, name word). The words sort in this order, which is what puts every
# timeseries behind every other node under ``sort_by="name"``.
KINDS = {
    "asset": (Asset, "Alpha"),
    "dataset": (Dataset, "Delta"),
    "function": (Function, "Foxtrot"),
    "resource": (Resource, "Romeo"),
    "timeseries": (TimeSeries, "Tango"),
}

TOTAL = N * len(KINDS)


def externals(rows):
    return {row.external_id for row in rows}


def _build(kind, index, stem, token):
    cls, word = KINDS[kind]
    common = dict(
        external_id=f"{stem}_{word.lower()}_{index:02d}",
        name=f"Mixed {word} {index:02d} {token}",
    )
    if kind == "asset":
        return Asset(labels=[TEST_LABEL], is_root=True, **common)
    if kind == "resource":
        return Resource(labels=[TEST_LABEL], **common)
    if kind == "timeseries":
        return TimeSeries(unit="bar", value_type="float", **common)
    return cls(**common)


def _delete(sync_client, ids):
    """Batch delete, falling back to one call per id so one undeletable node cannot strand the rest."""
    try:
        sync_client.resources.delete(ids)
    except Exception:
        for one in ids:
            try:
                sync_client.resources.delete([one])
            except Exception:
                pass


@pytest.fixture(scope="module")
def corpus(sync_client):
    """``N`` nodes of each of the five creatable types, interleaved by id.

    One heterogeneous ``/resources/create`` call: each element is dispatched server-side by its own
    type-label, and the ids are handed out in list order, so a round-robin list is a round-robin id
    sequence. Both properties are asserted below — they are the premise every test here rests on,
    and a corpus that quietly lost one would make the suite pass without testing anything.
    """
    token = unique_id("mix").rsplit("_", 1)[1]
    stem = f"pytest_mix_{token}"
    ids_by_kind = {
        kind: [f"{stem}_{word.lower()}_{i:02d}" for i in range(N)]
        for kind, (_cls, word) in KINDS.items()
    }

    nodes = [_build(kind, i, stem, token) for i in range(N) for kind in KINDS]
    sync_client.resources.create(nodes)
    try:
        rows = sync_client.resources.filter(external_id=f"{stem}*", limit=TOTAL * 2)
        assert len(rows) == TOTAL, (
            f"the corpus did not come back whole: {len(rows)} of {TOTAL}; every test below "
            f"compares against a complete population"
        )
        for kind, expected in ids_by_kind.items():
            assert externals(r for r in rows if r.node_type == kind) == set(expected)

        # A type whose ids form one contiguous block cannot show a limit applied before the type
        # predicate — the first page would be that type's rows either way.
        ordered = [row.node_type for row in sorted(rows, key=lambda r: int(r.id))]
        head, tail = ordered[:N], ordered[-N:]
        for kind in KINDS:
            assert head.count(kind) < N and tail.count(kind) < N, (
                f"{kind} ids are contiguous, so this corpus cannot expose the bug it exists for"
            )

        yield {
            "stem": stem,
            "token": token,
            "query": f"Mixed {token}",
            "ids": ids_by_kind,
            "all": {e for ids in ids_by_kind.values() for e in ids},
        }
    finally:
        _delete(sync_client, [e for k, ids in ids_by_kind.items() if k != "dataset" for e in ids])
        _delete(sync_client, ids_by_kind["dataset"])


# --------------------------------------------------------------------------- #
# the typed filters — a full page of one type, out of a table holding five
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("service,kind", [("timeseries", "timeseries"), ("datasets", "dataset")])
def test_a_typed_filter_fills_its_page_with_its_own_type(sync_client, corpus, service, kind):
    """``limit=N`` with exactly ``N`` rows of that type present must return all ``N``.

    The limit is the population size on purpose: any larger and the four other types could be
    dropped from an over-wide page without the count changing, which is the whole failure this
    asserts against.
    """
    found = getattr(sync_client, service).filter(
        external_id=f"{corpus['stem']}*", limit=N
    )
    assert externals(found) == set(corpus["ids"][kind])


@pytest.mark.parametrize("service,kind", [("timeseries", "timeseries"), ("datasets", "dataset")])
def test_a_typed_filter_is_not_diluted_when_the_other_types_sort_first(
    sync_client, corpus, service, kind
):
    """Sorted by name, the other ``4N`` nodes all sort ahead of the timeseries.

    So a limit applied before the type predicate returns the leading block, discards it as the
    wrong type, and answers with **nothing** — a result no amount of arithmetic about page sizes
    can explain away. Data sets sit in the middle of the name order, which catches the same fault
    from the other side: they would come back partial rather than empty.
    """
    leading = sync_client.resources.filter(
        external_id=f"{corpus['stem']}*", sort_by="name", sort_order="asc", limit=N
    )
    assert kind not in {row.node_type for row in leading}, (
        f"premise failed: the first {N} nodes in name order must hold no {kind}, or a typed "
        f"filter that limited before it narrowed would pass this anyway"
    )

    found = getattr(sync_client, service).filter(
        external_id=f"{corpus['stem']}*", sort_by="name", sort_order="asc", limit=N
    )
    assert externals(found) == set(corpus["ids"][kind])


def test_a_short_limit_still_spends_every_row_on_the_requested_type(sync_client, corpus):
    """A page smaller than the population is still a *full* page of the right type."""
    found = sync_client.timeseries.filter(external_id=f"{corpus['stem']}*", limit=5)
    assert len(found) == 5
    assert externals(found) <= set(corpus["ids"]["timeseries"])


# --------------------------------------------------------------------------- #
# /resources/filter — the generic node query, narrowed and unnarrowed
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("kind", list(KINDS))
def test_node_type_narrows_to_a_whole_page_of_that_type(sync_client, corpus, kind):
    found = sync_client.resources.filter(
        external_id=f"{corpus['stem']}*", node_type=[kind], limit=N
    )
    assert externals(found) == set(corpus["ids"][kind])
    assert {row.node_type for row in found} == {kind}


def test_a_two_type_narrowing_returns_both_types_whole(sync_client, corpus):
    """``node_type`` entries OR together, and neither type may crowd the other off the page."""
    found = sync_client.resources.filter(
        external_id=f"{corpus['stem']}*", node_type=["asset", "timeseries"], limit=2 * N
    )
    assert externals(found) == set(corpus["ids"]["asset"]) | set(corpus["ids"]["timeseries"])
    counted = {kind: sum(row.node_type == kind for row in found) for kind in ("asset", "timeseries")}
    assert counted == {"asset": N, "timeseries": N}


def test_the_unnarrowed_filter_returns_every_type_at_once(sync_client, corpus):
    """No ``node_type`` means no restriction — all five types, ``N`` of each, each in its own class."""
    found = sync_client.resources.filter(external_id=f"{corpus['stem']}*", limit=TOTAL)
    assert externals(found) == corpus["all"]
    assert {kind: sum(row.node_type == kind for row in found) for kind in KINDS} == {
        kind: N for kind in KINDS
    }
    for row in found:
        expected_class, _word = KINDS[row.node_type]
        assert isinstance(row, expected_class), f"{row.external_id} came back as {type(row)}"


def test_one_limit_is_spent_across_the_types_not_handed_to_each(sync_client, corpus):
    """``limit`` caps the page, not each type's share of it.

    The generic search used to run one query per type and concatenate the results, so every type
    got the caller's whole limit and a request for ``N`` could answer with ``5N``. The filter side
    has to hold the same line: one page, one limit, whatever mix of types fills it.
    """
    found = sync_client.resources.filter(external_id=f"{corpus['stem']}*", limit=N)
    assert len(found) == N
    assert externals(found) <= corpus["all"]


def test_an_unknown_node_type_beside_a_known_one_narrows_to_the_known_one(sync_client, corpus):
    """Unknown names are dropped per entry, so the known one still restricts.

    The neighbouring outcome — the whole list being discarded, leaving no restriction at all — is
    what makes this worth pinning: it would return the entire corpus and read like a working query.
    """
    found = sync_client.resources.filter(
        external_id=f"{corpus['stem']}*", node_type=["timeseries", "nosuchtype"], limit=TOTAL
    )
    assert externals(found) == set(corpus["ids"]["timeseries"])


def test_an_unknown_node_type_matches_nothing_rather_than_everything(sync_client, corpus):
    """A list of only unknown names is a restriction that nothing satisfies.

    Worth pinning next to the rest: the api drops unknown *keys* silently, so the neighbouring
    failure — an unrecognised value quietly becoming "no restriction" — would return the whole
    corpus and read like a working query.
    """
    assert sync_client.resources.filter(
        external_id=f"{corpus['stem']}*", node_type=["nosuchtype"], limit=TOTAL
    ) == []


# --------------------------------------------------------------------------- #
# paging — a mixed batch created inside one millisecond
# --------------------------------------------------------------------------- #

def _walk(fetch, page_size):
    """Page to exhaustion, returning every row in order — duplicates included, so they show."""
    seen, cursor = [], None
    while True:
        page = fetch(page_size, cursor)
        seen.extend(row.external_id for row in page)
        cursor = getattr(page, "next_cursor", None)
        if not cursor:
            return seen


def test_paging_a_mixed_population_visits_every_type_exactly_once(sync_client, corpus):
    """A walk crossing many page boundaries must not lose a *type* at one of them.

    The type predicate is not carried in the cursor — it is re-derived from the request on every
    page — so the thing that could go wrong here is a boundary compared against a column only some
    of the types populate, which would strand whichever type straddles it.

    Under the default sort on purpose. The corpus is one create call, so its ``createdTime`` values
    tie across types in every millisecond it spans — the case that dropped rows before v2 cursors
    carried timestamps in microseconds (``test_filter_paging.py``'s tied-timestamp tests).
    """
    walked = _walk(
        lambda limit, cursor: sync_client.resources.filter(
            external_id=f"{corpus['stem']}*", limit=limit, cursor=cursor
        ),
        7,
    )
    assert len(walked) == len(set(walked)), "the walk returned the same node on two pages"
    assert set(walked) == corpus["all"]


def test_paging_a_typed_filter_never_leaves_the_type(sync_client, corpus):
    """Every page of a typed walk is drawn from the same type, not just the first one."""
    walked = _walk(
        lambda limit, cursor: sync_client.timeseries.filter(
            external_id=f"{corpus['stem']}*", limit=limit, cursor=cursor
        ),
        5,
    )
    assert len(walked) == len(set(walked))
    assert set(walked) == set(corpus["ids"]["timeseries"])


# --------------------------------------------------------------------------- #
# the plain listings — the criteria-free read over the same shared table
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize(
    "service,kind", [("timeseries", "timeseries"), ("datasets", "dataset"), ("functions", "function")]
)
def test_a_typed_listing_never_leaks_another_node_type(sync_client, corpus, service, kind):
    """``GET /<collection>?limit=`` has no criteria at all, so its type restriction is the only
    thing between it and the rest of the table.

    The corpus is the newest ``5N`` nodes and the listings read newest-first, so a listing that
    forgot its restriction returns *our* assets — the assertion does not depend on what else the
    tenant holds. ``isinstance`` would not catch it: the binding builds the service's own class out
    of whatever row arrives, so a leaked asset deserializes into a ``TimeSeries`` with empty fields.
    """
    listed = getattr(sync_client, service).list(limit=N)
    assert len(listed) == N
    foreign = corpus["all"] - set(corpus["ids"][kind])
    assert externals(listed) & foreign == set()


def test_the_resource_listing_types_every_row_of_a_mixed_page(sync_client, corpus):
    """``resources.list`` spans all six types, so unlike its typed siblings it *should* be mixed —
    and each row still has to arrive as its own class."""
    listed = sync_client.resources.list(limit=1000)
    by_external_id = {row.external_id: row for row in listed}
    assert corpus["all"] <= set(by_external_id), (
        "the corpus is the newest 5N nodes, so all of it belongs on the newest-first page"
    )
    for kind, ids in corpus["ids"].items():
        expected_class, _word = KINDS[kind]
        for external_id in ids:
            assert isinstance(by_external_id[external_id], expected_class)


def test_functions_by_ids_finds_every_function_among_the_other_types(sync_client, corpus):
    """``functions.by_ids`` has no ``/byids`` endpoint behind it — it filters a plain listing
    client-side, and that listing is drawn from the shared table. Every other type in it is a row
    the lookup has to skip without losing a function to the listing's own cap."""
    found = sync_client.functions.by_ids(corpus["ids"]["function"])
    assert externals(found) == set(corpus["ids"]["function"])


# --------------------------------------------------------------------------- #
# the searches — one phrase, five types of candidate
# --------------------------------------------------------------------------- #

def test_a_typed_search_is_not_diluted_by_other_types_matching_the_phrase(sync_client, corpus):
    """The phrase matches all ``5N`` names; ``limit=N`` must still be ``N`` timeseries.

    Search ranks before it limits, so this is the one place where the ordering question is not
    hypothetical: every candidate scores the same here, and if the type restriction ran after the
    ranking cut the page to ``N``, roughly four in five of those rows would be discarded.
    """
    expected = set(corpus["ids"]["timeseries"])
    hits = poll_until(
        lambda: sync_client.timeseries.search(corpus["query"], limit=N),
        lambda rows: externals(rows) == expected,
    )
    assert externals(hits) == expected


def test_a_search_filter_narrows_a_mixed_candidate_set_to_one_type(sync_client, corpus):
    """Same question through the generic search, where the type is a filter rather than the route."""
    expected = set(corpus["ids"]["timeseries"])
    hits = poll_until(
        lambda: sync_client.resources.search(
            corpus["query"], filter=ResourceFilter(node_type=["timeseries"]), limit=N
        ),
        lambda rows: externals(rows) == expected,
    )
    assert externals(hits) == expected
    assert {row.node_type for row in hits} == {"timeseries"}


def test_the_generic_search_spans_every_type_and_types_each_hit(sync_client, corpus):
    hits = poll_until(
        lambda: sync_client.resources.search(corpus["query"], limit=TOTAL),
        lambda rows: externals(rows) >= corpus["all"],
    )
    assert externals(hits) >= corpus["all"]
    by_external_id = {row.external_id: row for row in hits}
    for kind, ids in corpus["ids"].items():
        expected_class, _word = KINDS[kind]
        for external_id in ids:
            assert isinstance(by_external_id[external_id], expected_class)


def test_the_generic_search_spends_one_limit_across_all_types(sync_client, corpus):
    """``limit=N`` over a phrase matching all five types is ``N`` hits, not ``N`` per type.

    This one is a named regression: the endpoint used to run five per-type queries and concatenate
    them, so each type received the caller's whole limit and a request for 50 could come back with
    250. It is one ranked query now, and the corpus gives the phrase candidates of every type.
    """
    hits = poll_until(
        lambda: sync_client.resources.search(corpus["query"], limit=N),
        lambda rows: len(rows) == N,
    )
    assert len(hits) == N
    assert externals(hits) <= corpus["all"]


def test_a_search_filter_cannot_widen_a_typed_search(sync_client, corpus):
    """The route already fixed the type, so a filter naming the corpus cannot pull in the rest of it.

    The guard against "fixing" dilution by making the typed search consult its filter for the type
    instead of the route — that would make a filter able to widen a search, which no filter may do.
    """
    hits = sync_client.timeseries.search(
        corpus["query"],
        filter=TimeSeriesFilter(external_id=f"{corpus['stem']}*"),
        limit=TOTAL,
    )
    assert externals(hits) <= set(corpus["ids"]["timeseries"])
