"""``POST /events/filter`` — the refactored contract.

The event filter is deliberately not a node filter: events live in ClickHouse, their id is a UUID
where a node's is a long, and the table has no ``name`` column. What it does instead is match the
node base field for field wherever ClickHouse can back it, so ``externalId``, ``source``,
``metadata``, ``createdTime``, ``lastUpdatedTime`` and ``dataSetId`` carry the same names and
semantics they have there — including the ``*`` / ``%`` wildcards, the literal ``_`` and the
case-insensitive OR-within-a-list matching. This suite checks that parity holds in practice, not
just on paper.

What the refactor changed here:

* ``type``, ``subType`` and ``status`` were single exact strings while the rest of the filter took
  lists, which made the most-used event criteria the least capable ones — "alarms and warnings"
  needed two calls. They keep their names and take pattern lists now; a bare string still works.
* ``externalIdPrefix`` is gone; a trailing ``*`` in ``externalId`` says the same thing and composes
  with exact ids.
* ``id`` is gone. It was typed as a long while an event's id is a UUID string, and nothing read it,
  so filtering by it silently did nothing.
* ``description`` is gone — there is nothing on the events table for it to match. Use
  ``/events/search``.
* ``dataSetId`` now expands the hierarchy. It used to match the listed ids exactly, so filtering
  on a parent returned none of its children's events while the same filter against timeseries
  returned them.

Events reach the filter projection asynchronously, so reads here poll rather than assume.
"""
import pandas as pd
import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException

from fixtures import async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from filter_fixtures import datasets, event_corpus, prefix, token  # noqa: F401  (fixtures)
from polling import poll_until


def externals(results):
    return {e.external_id for e in results}


@pytest.fixture
def flt(sync_client, prefix):
    """Filter within this run's corpus unless the test overrides ``external_id``.

    Polls until the result stops being empty *or* the timeout elapses, then returns what it has —
    so a test expecting an empty set pays the timeout only when it is about to fail anyway, and a
    test expecting rows rides out projection lag.
    """
    def _filter(limit=None, expect_rows=True, **criteria):
        criteria.setdefault("external_id", f"{prefix}*")
        request = dict(
            filter=intellistream_datahub_sdk.EventFilter(**criteria), limit=limit or 100)

        def fetch():
            return externals(sync_client.events.filter(**request))

        if not expect_rows:
            return fetch()
        return poll_until(fetch, bool, timeout=10.0)
    return _filter


@pytest.fixture
def both(prefix):
    return {f"{prefix}_ev_alarm_1", f"{prefix}_ev_alarmX1"}


@pytest.fixture
def alarm(prefix):
    return f"{prefix}_ev_alarm_1"


@pytest.fixture
def warning(prefix):
    return f"{prefix}_ev_alarmX1"


# --------------------------------------------------------------------------- #
# types, subTypes, statuses — the fields that went plural
# --------------------------------------------------------------------------- #

def test_types_match_exactly_and_as_patterns(flt, event_corpus, alarm, warning, token):
    assert flt(type=[f"alarm_{token}"]) == {alarm}
    assert flt(type=["alarm_*"]) == {alarm}
    assert flt(type=["*_" + token]) == {alarm, warning}
    assert flt(type=["no_such_type"], expect_rows=False) == set()


def test_type_entries_or_together(flt, event_corpus, alarm, warning, token):
    """"Alarms and warnings" is one call now. It needed two before, even though the aggregation
    endpoint's own ``groupBy=type`` hands you several at once."""
    assert flt(type=[f"alarm_{token}", f"warning_{token}"]) == {alarm, warning}
    assert flt(type=["no_such_type", f"alarm_{token}"]) == {alarm}


def test_types_are_case_insensitive(flt, event_corpus, alarm, token):
    assert flt(type=[f"ALARM_{token}".upper()]) == {alarm}


def test_sub_types_and_statuses_are_pattern_lists_too(flt, event_corpus, alarm, warning):
    assert flt(sub_type=["electrical"]) == {alarm}
    assert flt(sub_type=["electr*"]) == {alarm}
    assert flt(sub_type=["electrical", "mechanical"]) == {alarm, warning}

    assert flt(status=["OPEN"]) == {alarm}
    assert flt(status=["open"]) == {alarm}, "statuses match case-insensitively"
    assert flt(status=["OPEN", "CLOSED"]) == {alarm, warning}
    assert flt(status=["NO_SUCH_STATUS"], expect_rows=False) == set()


# --------------------------------------------------------------------------- #
# externalIds and sources — the node-filter parity fields
# --------------------------------------------------------------------------- #

def test_external_ids_wildcard_search(flt, event_corpus, alarm, warning, prefix):
    """The pattern half of the field; the literal half is below."""
    assert flt(external_id=[f"{prefix}*"]) == {alarm, warning}
    assert flt(external_id=["*_ev_alarm_1"]) >= {alarm}
    assert flt(external_id=[f"{prefix}_ev_alarm%"]) == {alarm, warning}
    assert flt(external_id=[f"*{prefix[-8:]}_ev_*"]) == {alarm, warning}


def test_an_exact_external_id_matches_the_event(flt, event_corpus, alarm, warning):
    """An external id with no wildcard is an exact lookup — the highest-value case in the field.

    Regression test. When the literal half of this field was first routed through the indexed hash
    column it matched *nothing*: the filter derived the hash with the node algorithm (a 64-bit xx3
    of the lowercased id) while an event's ``external_id_hash`` is BLAKE3 over
    ``externalId + tenantId`` — different algorithm, different width, salted per tenant, so the
    comparison could never be true. A wire DTO has no tenant and cannot produce that value at all,
    which is why the literals now travel unhashed and are compared as text.
    """
    assert flt(external_id=[alarm]) == {alarm}
    assert flt(external_id=[warning]) == {warning}
    assert flt(external_id=[alarm, warning]) == {alarm, warning}
    assert flt(external_id=["no_such_external_id"], expect_rows=False) == set()


def test_an_exact_external_id_is_case_sensitive_where_a_pattern_is_not(flt, event_corpus, alarm):
    """The two halves of ``externalId`` disagree about case, deliberately.

    An event's ``external_id_hash`` is BLAKE3 over the external id **verbatim**, so a literal entry
    can only match an exactly-equal string; the wildcard branch beside it goes through ILIKE and
    does not care. Nodes lowercase before hashing, so ``/timeseries/filter`` and friends are
    case-insensitive on both halves — see ``test_external_ids_are_case_insensitive`` in
    ``test_filter_timeseries.py``.

    Pinned rather than treated as a bug because the server states the choice, but it is the one
    place the event filter does *not* match the node contract it otherwise mirrors, and it is
    invisible until someone upper-cases an id.
    """
    assert flt(external_id=[alarm]) == {alarm}
    assert flt(external_id=[alarm.upper()], expect_rows=False) == set(), \
        "a literal entry is compared against the verbatim hash"
    # The same id with a trailing wildcard takes the ILIKE path, which folds case.
    assert flt(external_id=[f"{alarm.upper()}*"]) == {alarm}


def test_an_exact_id_and_a_pattern_mix_in_one_list(flt, event_corpus, alarm, warning, prefix):
    """The two halves of the field are separate query paths — literals by equality, wildcards by
    ILIKE — OR'd together. A list carrying both has to return the union, not one or the other."""
    assert flt(external_id=[alarm, f"{prefix}_ev_alarmX*"]) == {alarm, warning}


def test_external_id_underscore_is_literal(flt, event_corpus, alarm, warning, prefix):
    """``_ev_alarm_1`` and ``_ev_alarmX1`` differ only where the underscore sits.

    Written with a trailing ``*`` so the comparison goes down the ILIKE path — the one that has to
    escape ``_``. The literal path compares whole strings and cannot confuse the two.
    """
    assert flt(external_id=[f"{prefix}_ev_alarm_1*"]) == {alarm}
    assert flt(external_id=[f"{prefix}_ev_alarmX1*"]) == {warning}


def test_external_ids_are_case_insensitive(flt, event_corpus, alarm, prefix):
    assert flt(external_id=[f"{prefix}_EV_ALARM_1*".upper()]) == {alarm}


def test_external_id_entries_or_together(flt, event_corpus, alarm, warning, prefix):
    """This is what replaced ``externalIdPrefix``, which could be given once and not combined.

    Both entries carry the run's prefix. A leading wildcard (``*_ev_alarmX1``) would also match the
    events every previous run left behind, so the assertion failed against accumulated state rather
    than against the OR.
    """
    assert flt(external_id=[f"{prefix}_ev_alarm_1*", f"{prefix}*_ev_alarmX1"]) == {alarm, warning}


def test_sources_match_as_patterns_with_a_literal_underscore(flt, event_corpus, alarm, warning, token):
    assert flt(source=[f"opc_{token}"]) == {alarm}
    assert flt(source=[f"opcX{token}"]) == {warning}
    assert flt(source=["opc*"]) == {alarm, warning}
    assert flt(source=[f"OPC_{token}".upper()]) == {alarm}


# --------------------------------------------------------------------------- #
# metadata
# --------------------------------------------------------------------------- #

def test_metadata_key_and_value_and_the_key_only_form(flt, event_corpus, alarm, warning, token):
    assert flt(metadata={f"evk_{token}": "one"}) == {alarm}
    assert flt(metadata={f"evk_{token}": "two"}) == {warning}
    assert flt(metadata={f"evk_{token}": None}) == {alarm, warning}, \
        "a null value matches the key alone — the meaning the event filter has always given it"
    assert flt(metadata={f"evk_{token}": "three"}, expect_rows=False) == set()
    assert flt(metadata={f"no_such_key_{token}": None}, expect_rows=False) == set()


# --------------------------------------------------------------------------- #
# dataSetIds — hierarchy expansion, and the null/empty split
# --------------------------------------------------------------------------- #

def test_data_set_scope_by_id_and_by_external_id(flt, event_corpus, datasets, alarm):
    _parent, child = datasets
    assert flt(data_set_id=[child.id]) == {alarm}
    assert flt(data_set_id=[child.external_id]) == {alarm}
    assert flt(data_set_id=[intellistream_datahub_sdk.IdCollection(id=child.id)]) == {alarm}


def test_a_parent_data_set_stands_in_for_its_children(flt, event_corpus, datasets, alarm, warning):
    """The alarm lives in the child, the warning in the parent. Filtering on the parent must return
    both — it used to return only the warning, because events matched the listed ids exactly while
    every other filter expanded the hierarchy."""
    parent, _child = datasets
    assert flt(data_set_id=[parent.id]) == {alarm, warning}


def test_empty_and_absent_data_set_scopes_are_opposites(sync_client, event_corpus, prefix, both):
    """``None`` is "no data set restriction", ``[]`` is "narrow to no data sets". Opposite answers,
    so the SDK has to keep them apart all the way onto the wire."""
    def run(data_set_ids):
        return externals(sync_client.events.filter(
            intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}*", data_set_id=data_set_ids)))

    assert run([]) == set()
    assert poll_until(lambda: run(None), bool, timeout=10.0) == both


def test_a_data_set_reference_naming_nothing_contributes_nothing(flt, event_corpus, datasets, alarm):
    _parent, child = datasets
    assert flt(data_set_id=["no_such_data_set_at_all"], expect_rows=False) == set()
    assert flt(data_set_id=["no_such_data_set_at_all", child.external_id]) == {alarm}


# --------------------------------------------------------------------------- #
# relatedResources
# --------------------------------------------------------------------------- #

def test_related_resources_must_all_be_attached(sync_client, datasets, token):
    """Each entry may name a resource by id, external id, or both, and the event must be attached
    to **all** of them.

    Uses its own external-id prefix rather than the corpus one. Its two extra events would
    otherwise fall inside every other test's ``<prefix>*`` scope, and a ClickHouse delete is not
    synchronous — so the tests that follow would see them for a while after this one cleaned up.
    """
    _parent, child = datasets
    own_prefix = f"pytest_rr_{token}"
    res_a, res_b = f"{own_prefix}_a", f"{own_prefix}_b"
    ev_both, ev_one = f"{own_prefix}_ev_both", f"{own_prefix}_ev_one"

    for external_id in (res_a, res_b):
        try:
            sync_client.resources.delete([external_id])
        except Exception:
            pass
    created = sync_client.resources.create([
        intellistream_datahub_sdk.Resource(external_id=res_a, name=f"RR A {token}", labels=["ASSET"],
                             is_root=True, data_set_id=child.id),
        intellistream_datahub_sdk.Resource(external_id=res_b, name=f"RR B {token}", labels=["ASSET"],
                             is_root=True, data_set_id=child.id),
    ])
    resource_id = {r.external_id: r.id for r in created.nodes}

    now = pd.Timestamp.now(tz="UTC")
    sync_client.events.create([
        intellistream_datahub_sdk.Event(external_id=ev_both, type=f"rr_{token}", event_time=now,
                          data_set_id=child.id,
                          related_resources=[intellistream_datahub_sdk.IdCollection(external_id=res_a),
                                             intellistream_datahub_sdk.IdCollection(external_id=res_b)]),
        intellistream_datahub_sdk.Event(external_id=ev_one, type=f"rr_{token}", event_time=now,
                          data_set_id=child.id,
                          related_resources=[intellistream_datahub_sdk.IdCollection(external_id=res_a)]),
    ])
    try:
        def run(related):
            return externals(sync_client.events.filter(
                intellistream_datahub_sdk.EventFilter(external_id=f"{own_prefix}_ev_*",
                                             related_resources=related)))

        by_a = [intellistream_datahub_sdk.IdCollection(external_id=res_a)]
        assert poll_until(lambda: run(by_a), lambda r: len(r) >= 2, timeout=15.0) == {ev_both, ev_one}

        # Both required: only the event carrying both qualifies.
        by_both = [intellistream_datahub_sdk.IdCollection(external_id=res_a),
                   intellistream_datahub_sdk.IdCollection(external_id=res_b)]
        assert run(by_both) == {ev_both}

        # A numeric id names the same resource as its external id.
        assert run([intellistream_datahub_sdk.IdCollection(id=resource_id[res_b])]) == {ev_both}
    finally:
        try:
            sync_client.events.delete([ev_both, ev_one])
        except Exception:
            pass
        for external_id in (res_b, res_a):
            try:
                sync_client.resources.delete([external_id])
            except Exception:
                pass


# --------------------------------------------------------------------------- #
# time windows — three of them, all one shape
# --------------------------------------------------------------------------- #

def test_event_time_window_bounds_the_result(flt, event_corpus, alarm, warning):
    """``eventTime`` is when the event happened; ``createdTime`` is when the platform ingested it.
    The corpus sets them two days apart, so a window can tell them apart.

    All three windows are the same ``TimeFilter`` type now — the api had two empty subclasses of it
    that added no field and no behaviour, and ``eventTime`` was typed as one of them, a third
    meaning wearing the name of the first.
    """
    now = pd.Timestamp.now(tz="UTC")
    assert flt(event_time=intellistream_datahub_sdk.TimeFilter(start=now - pd.Timedelta(hours=1))) == {warning}
    assert flt(event_time=intellistream_datahub_sdk.TimeFilter(end=now - pd.Timedelta(hours=1))) == {alarm}
    assert flt(event_time=intellistream_datahub_sdk.TimeFilter(
        start=now - pd.Timedelta(days=3), end=now + pd.Timedelta(hours=1))) == {alarm, warning}


def test_created_time_is_ingest_time_not_event_time(flt, event_corpus, both):
    """Both events were ingested just now, however far apart their event times are."""
    now = pd.Timestamp.now(tz="UTC")
    assert flt(created_time=intellistream_datahub_sdk.TimeFilter(start=now - pd.Timedelta(minutes=10))) == both
    assert flt(created_time=intellistream_datahub_sdk.TimeFilter(end=now - pd.Timedelta(days=1)),
               expect_rows=False) == set()


# --------------------------------------------------------------------------- #
# None, empty and garbled
# --------------------------------------------------------------------------- #

def test_an_absent_filter_places_no_restriction(sync_client, event_corpus, both):
    """An argument-free filter returns the tenant's events, not none of them."""
    everything = poll_until(
        lambda: externals(sync_client.events.filter(limit=1000)),
        lambda found: found >= both,
        timeout=15.0,
    )
    assert everything >= both


@pytest.mark.parametrize("empty", [[], ["", "  "]])
def test_empty_and_blank_lists_place_no_restriction(sync_client, event_corpus, prefix, both, empty):
    scoped = poll_until(
        lambda: externals(sync_client.events.filter(
            intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}*", type=empty,
                                         sub_type=empty, status=empty, source=empty))),
        bool,
        timeout=10.0,
    )
    assert scoped == both


def test_none_valued_criteria_are_omitted(flt, event_corpus, both):
    assert flt(type=None, sub_type=None, status=None, source=None,
               metadata=None, data_set_id=None) == both


def test_garbled_criteria_match_nothing_without_erroring(flt, event_corpus):
    for garbage in ["!!!##$$^&()", "' OR 1=1 --", "\\", "%%%%_____", "😀"]:
        assert flt(type=[garbage], expect_rows=False) == set(), f"{garbage!r} should match nothing"


def test_a_bare_string_means_a_one_element_list(sync_client, event_corpus, prefix, alarm, token):
    scalar = sync_client.events.filter(
        intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}*", type=f"alarm_{token}"))
    listed = sync_client.events.filter(
        intellistream_datahub_sdk.EventFilter(external_id=[f"{prefix}*"], type=[f"alarm_{token}"]))
    assert externals(scalar) == externals(listed) == {alarm}


# --------------------------------------------------------------------------- #
# limit, ordering, and the retired fields
# --------------------------------------------------------------------------- #

def test_limit_caps_the_page(sync_client, event_corpus, prefix):
    capped = poll_until(
        lambda: sync_client.events.filter(
            intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}*"), limit=1),
        bool,
        timeout=10.0,
    )
    assert len(capped) == 1


def test_a_limit_above_the_ceiling_is_refused(sync_client, prefix):
    with pytest.raises(DataHubException) as excinfo:
        sync_client.events.filter(
            intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}*"), limit=10_001)
    assert excinfo.value.status_code == 400


def _ordered_page(sync_client, prefix, both, **sort):
    return [
        event.external_id
        for event in poll_until(
            lambda: sync_client.events.filter(
                intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}*"), limit=100, **sort),
            lambda found: externals(found) >= both,
            timeout=10.0,
        )
    ]


def test_an_explicit_sort_orders_the_page_both_ways(sync_client, event_corpus, prefix, both,
                                                    alarm, warning):
    """The corpus events are two days apart in ``eventTime``, so the direction is unambiguous."""
    assert _ordered_page(sync_client, prefix, both,
                         sort_by=["eventTime"], sort_order="asc") == [alarm, warning]
    assert _ordered_page(sync_client, prefix, both,
                         sort_by=["eventTime"], sort_order="desc") == [warning, alarm]


def test_an_unsortable_property_falls_back_rather_than_erroring(sync_client, event_corpus,
                                                                prefix, both):
    """Sort properties go through a fixed table — a column name interpolated from a request body is
    an injection point — and one that is not in it is dropped rather than rejected."""
    page = _ordered_page(sync_client, prefix, both, sort_by=["notASortableProperty"])
    assert set(page) == both


def test_the_default_order_is_event_time_then_id_ascending(sync_client, datasets, token):
    """Unordered is the one thing the default may not be.

    Without an ``ORDER BY`` the rows come back in whatever order ClickHouse produced, and with a
    ``limit`` that makes the *rows themselves* arbitrary rather than just their sequence — two
    identical requests may disagree about which, a wrong answer that reads as data shifting
    underneath you. The refactor's default is ``(event_time, id)`` ascending, chosen to match the
    order the keyset cursor pages in: a default that disagreed with the cursor would silently
    change the ordering the moment a caller started paging.

    Six events with distinct event times, because two can come back in the right order by chance.

    **Not marked xfail.** The ordering is present in the backend working tree but absent from the
    jar currently serving this suite, so this goes green on the next backend rebuild — where an
    ``xfail`` would flip to a strict-XPASS failure and have to be removed by hand.
    """
    _parent, child = datasets
    own_prefix = f"pytest_ord_{token}"
    base = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)
    # Created in an order unrelated to their event times, so "as inserted" and "sorted" differ.
    order_of_creation = [3, 0, 5, 1, 4, 2]
    externals_by_offset = {i: f"{own_prefix}_{i}" for i in range(6)}

    for offset in order_of_creation:
        sync_client.events.create([intellistream_datahub_sdk.Event(
            external_id=externals_by_offset[offset], type=f"ord_{token}",
            event_time=base + pd.Timedelta(minutes=offset), data_set_id=child.id)])
    try:
        expected = [externals_by_offset[i] for i in range(6)]
        page = poll_until(
            lambda: sync_client.events.filter(
                intellistream_datahub_sdk.EventFilter(external_id=f"{own_prefix}*"), limit=100),
            lambda found: externals(found) == set(expected),
            timeout=20.0,
        )
        assert [event.external_id for event in page] == expected
    finally:
        try:
            sync_client.events.delete(list(externals_by_offset.values()))
        except Exception:
            pass


def test_the_retired_criteria_are_not_accepted(sync_client):
    """``external_id_prefix``, ``description``, ``id`` and the plural spellings the pattern fields
    briefly carried are gone from the binding as well as the wire. A ``TypeError`` beats the
    alternative: the api drops unknown keys silently, so a leftover ``types=`` would place no
    restriction and return the whole tenant while looking like a narrowed query.
    """
    for retired, value in [("external_id_prefix", "p"), ("description", "x"), ("id", 1),
                           ("external_ids", "p*"), ("types", ["alarm"]), ("sub_types", ["x"]),
                           ("statuses", ["OPEN"]), ("sources", ["sap"]), ("data_set_ids", [1])]:
        with pytest.raises(TypeError):
            intellistream_datahub_sdk.EventFilter(**{retired: value})


@pytest.mark.asyncio
async def test_async_filter_matches_the_sync_one(async_client, sync_client, event_corpus, prefix):
    request = dict(filter=intellistream_datahub_sdk.EventFilter(external_id=f"{prefix}*"))
    assert externals(await async_client.events.filter(**request)) == externals(
        sync_client.events.filter(**request))
