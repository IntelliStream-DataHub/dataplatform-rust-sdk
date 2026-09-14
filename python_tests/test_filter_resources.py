"""``POST /resources/filter`` — the refactored contract.

The shared node criteria are exercised in depth in ``test_filter_timeseries.py``; all three node
filters go through one ``NodePredicateBuilder``, so repeating every wildcard case here would test
the same code three times. What this suite owns is what the resource filter alone can show:

* the endpoint's **breadth**: it is the generic node query, spanning every node type, narrowed by
  ``node_type``. It behaved this way before by omission — no discriminator, and single-table
  inheritance did the rest — so the breadth could not be narrowed and was not stated.
* ``source`` and ``labels`` with real values — a ``Resource`` can carry both, where the SDK's
  ``TimeSeries`` and ``Dataset`` cannot, so this is the only place their semantics are observable.
* ``isRoot``, which exists because only resources form the graph roots a tree view hangs off.
* ``dataSetId`` accepting an **external id**. It took numeric ids only before the refactor, so a
  caller holding an external id had to resolve it first — and the event filter, given the same
  concept, accepted both. One shape now.

It also pins the removal of the singular ``id``/``externalId``/``name``/``source``. Those were six
fields for three concepts, and the two forms ANDed rather than merged, so sending both narrowed the
query in a way no caller intended.
"""
import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException

from fixtures import async_client, sync_client, unique_id  # noqa: F401  (fixtures)
from filter_fixtures import (  # noqa: F401  (fixtures)
    datasets,
    prefix,
    resource_corpus,
    timeseries_corpus,
    token,
)


def externals(results):
    return {r.external_id for r in results}


@pytest.fixture
def flt(sync_client, prefix):
    """Filter within this run's corpus, narrowed to resources unless the test says otherwise.

    ``node_type`` defaults to ``["resource"]`` because this endpoint is the *generic* node query:
    without it, ``external_id=<prefix>*`` would also match the corpus's data sets and timeseries,
    which share the prefix. The breadth is covered on purpose further down.
    """
    def _filter(**criteria):
        criteria.setdefault("external_id", f"{prefix}*")
        criteria.setdefault("node_type", ["resource"])
        return externals(sync_client.resources.filter(**criteria))
    return _filter


# --------------------------------------------------------------------------- #
# sources — a pattern list, with no hashed column to fall back on
# --------------------------------------------------------------------------- #

def test_sources_match_exactly_and_as_patterns(flt, resource_corpus, token):
    assert flt(source=[f"sap_{token}"]) == {resource_corpus["root"].external_id}
    assert flt(source=["sap*"]) == {r.external_id for r in resource_corpus.values()}
    assert flt(source=["SAP_" + token.upper()]) == {resource_corpus["root"].external_id}, \
        "sources match case-insensitively"
    assert flt(source=["not_a_source"]) == set()


def test_sources_underscore_is_literal(flt, resource_corpus, token):
    """``sap_<token>`` and ``sapX<token>`` differ only where the underscore sits.

    Every entry in this field is a pattern — there is no hashed source column to resolve a literal
    through — so if ``_`` were not escaped, asking for ``sap_<token>`` would return both.
    """
    assert flt(source=[f"sap_{token}"]) == {resource_corpus["root"].external_id}
    assert flt(source=[f"sapX{token}"]) == {resource_corpus["leaf"].external_id}


def test_source_entries_or_together(flt, resource_corpus, token):
    """The retired singular ``source`` needed one call per value."""
    assert flt(source=[f"sap_{token}", f"sapX{token}"]) == {
        r.external_id for r in resource_corpus.values()
    }


def test_the_pattern_is_not_upper_cased_before_matching(flt, resource_corpus, token):
    """``ResourceService`` used to upper-case the caller's pattern before matching, which the other
    two filters did not. Invisible for ASCII under a case-insensitive comparison, but it was one
    filter in a family of three behaving differently for no stated reason."""
    assert flt(source=[f"sap_{token}"]) == flt(source=[f"SAP_{token.upper()}"])


# --------------------------------------------------------------------------- #
# labels — all of them, canonicalised
# --------------------------------------------------------------------------- #

def test_labels_must_all_be_present(flt, resource_corpus):
    """Unlike every other list on the filter, label entries AND rather than OR: a label is a set a
    node belongs to or does not, so "tagged PUMP and CRITICAL" is the question worth asking.

    ``root`` carries FLT_ALPHA and FLT_BETA; ``leaf`` carries only FLT_ALPHA.
    """
    both = {r.external_id for r in resource_corpus.values()}
    assert flt(labels=["FLT_ALPHA"]) == both
    assert flt(labels=["FLT_BETA"]) == {resource_corpus["root"].external_id}
    assert flt(labels=["FLT_ALPHA", "FLT_BETA"]) == {resource_corpus["root"].external_id}


def test_a_label_naming_nothing_matches_no_resources(flt, resource_corpus):
    """Not "ignored" — an unknown label in an all-of set makes the whole set unsatisfiable."""
    assert flt(labels=["NO_SUCH_LABEL_XYZ"]) == set()
    assert flt(labels=["FLT_ALPHA", "NO_SUCH_LABEL_XYZ"]) == set()


def test_label_names_are_canonicalised_before_matching(flt, resource_corpus):
    """Labels are stored in SNAKE_UPPER_CASE, so ``"flt alpha"`` and ``"Flt-Alpha"`` are the same
    label as ``FLT_ALPHA``. The lookup canonicalises the caller's spelling the same way the writer
    did — two implementations of that rule is how a filter ends up matching nothing and looking
    like an empty result rather than a bug."""
    both = {r.external_id for r in resource_corpus.values()}
    assert flt(labels=["flt alpha"]) == both
    assert flt(labels=["Flt-Alpha"]) == both


def test_labels_are_not_patterns(flt, resource_corpus):
    """A label is matched by the hash of its canonical name, so a wildcard is just a name that
    canonicalises to something no label uses."""
    assert flt(labels=["FLT_*"]) == set()


def test_a_blank_only_label_list_restricts_nothing(flt, resource_corpus):
    """Every supplied name was blank, so there is nothing to require — which is "no restriction",
    not "carries the empty label"."""
    assert flt(labels=["", "  "]) == {r.external_id for r in resource_corpus.values()}


# --------------------------------------------------------------------------- #
# isRoot
# --------------------------------------------------------------------------- #

def test_is_root_selects_either_side(flt, resource_corpus):
    assert flt(is_root=True) == {resource_corpus["root"].external_id}
    assert flt(is_root=False) == {resource_corpus["leaf"].external_id}


def test_is_root_omitted_places_no_restriction(flt, resource_corpus):
    """``None`` is the third state, and it has to be distinguishable from ``False``."""
    assert flt(is_root=None) == {r.external_id for r in resource_corpus.values()}


# --------------------------------------------------------------------------- #
# dataSetIds — now accepting external ids, and expanding the hierarchy
# --------------------------------------------------------------------------- #

def test_data_set_scope_accepts_ids_external_ids_and_collections(flt, resource_corpus, datasets):
    _parent, child = datasets
    both = {r.external_id for r in resource_corpus.values()}
    assert flt(data_set_id=[child.id]) == both
    assert flt(data_set_id=[child.external_id]) == both, \
        "an external id is a valid data set reference here now; it used to be ids only"
    assert flt(data_set_id=[intellistream_datahub_sdk.IdCollection(external_id=child.external_id)]) == both


def test_a_parent_data_set_stands_in_for_its_children(flt, resource_corpus, datasets):
    """Filtering on a parent used to return nothing from its children even though the caller could
    read them — while timeseries, given the same filter, returned them. One concept, two answers."""
    parent, _child = datasets
    assert flt(data_set_id=[parent.id]) == {r.external_id for r in resource_corpus.values()}


def test_empty_and_absent_data_set_scopes_are_opposites(sync_client, resource_corpus, prefix):
    assert externals(sync_client.resources.filter(
        external_id=f"{prefix}*", node_type=["resource"], data_set_id=[])) == set()
    assert externals(sync_client.resources.filter(
        external_id=f"{prefix}*", node_type=["resource"], data_set_id=None)) == {
        r.external_id for r in resource_corpus.values()}


def test_a_data_set_scope_of_only_unknown_references_matches_nothing(flt, resource_corpus):
    assert flt(data_set_id=["no_such_data_set_at_all"]) == set()


# --------------------------------------------------------------------------- #
# the shared node criteria, spot-checked
# --------------------------------------------------------------------------- #

def test_external_ids_and_names_are_patterns(flt, resource_corpus, prefix, token):
    assert flt(external_id=[f"{prefix}_res_root"]) == {resource_corpus["root"].external_id}
    assert flt(external_id=[f"{prefix}_res_*"]) == {r.external_id for r in resource_corpus.values()}
    assert flt(name=[f"Root Node {token}"]) == {resource_corpus["root"].external_id}
    assert flt(name=["* Node *"]) == {r.external_id for r in resource_corpus.values()}


def test_ids_and_metadata(flt, resource_corpus, token):
    assert flt(id=[resource_corpus["root"].id]) == {resource_corpus["root"].external_id}
    assert flt(metadata={f"resk_{token}": "one"}) == {resource_corpus["root"].external_id}
    assert flt(metadata={f"resk_{token}": None}) == {
        r.external_id for r in resource_corpus.values()}, "a null value matches the key alone"


def test_criteria_and_together(flt, resource_corpus, token):
    assert flt(labels=["FLT_ALPHA"], is_root=True) == {resource_corpus["root"].external_id}
    assert flt(labels=["FLT_BETA"], is_root=False) == set()


def test_an_argument_free_filter_places_no_restriction(sync_client, resource_corpus):
    assert externals(sync_client.resources.filter()) >= {
        r.external_id for r in resource_corpus.values()}


# --------------------------------------------------------------------------- #
# nodeTypes — what makes this the generic node query
# --------------------------------------------------------------------------- #

def test_without_node_types_the_query_spans_every_node_type(
    sync_client, resource_corpus, timeseries_corpus, datasets, prefix
):
    """One endpoint over the whole node table. The data sets, timeseries and resources of this
    run's corpus all share a prefix, so an unrestricted query returns all three."""
    parent, child = datasets
    everything = externals(sync_client.resources.filter(external_id=f"{prefix}*"))
    assert everything >= {r.external_id for r in resource_corpus.values()}
    assert everything >= {ts.external_id for ts in timeseries_corpus.values()}
    assert everything >= {parent.external_id, child.external_id}


def test_node_types_narrows_to_the_types_named(
    sync_client, resource_corpus, timeseries_corpus, datasets, prefix
):
    parent, child = datasets

    def by_type(node_types):
        return externals(sync_client.resources.filter(
            external_id=f"{prefix}*", node_type=node_types))

    assert by_type(["resource"]) == {r.external_id for r in resource_corpus.values()}
    assert by_type(["timeseries"]) == {ts.external_id for ts in timeseries_corpus.values()}
    assert by_type(["dataset"]) == {parent.external_id, child.external_id}
    # Entries OR together.
    assert by_type(["dataset", "resource"]) == (
        {parent.external_id, child.external_id} | {r.external_id for r in resource_corpus.values()}
    )
    assert by_type("resource") == by_type(["resource"]), "a bare string is a one-element list"


def test_node_types_are_case_insensitive(sync_client, resource_corpus, prefix):
    assert externals(sync_client.resources.filter(
        external_id=f"{prefix}*", node_type=["RESOURCE"])) == {
        r.external_id for r in resource_corpus.values()}


def test_a_list_of_only_unknown_node_types_matches_nothing(sync_client, resource_corpus, prefix):
    """Not "no restriction": the caller asked to be narrowed to those types, and answering with
    every type would be the opposite of what they asked for."""
    assert externals(sync_client.resources.filter(
        external_id=f"{prefix}*", node_type=["no_such_type"])) == set()


def test_an_unknown_node_type_beside_a_known_one_is_dropped(sync_client, resource_corpus, prefix):
    assert externals(sync_client.resources.filter(
        external_id=f"{prefix}*", node_type=["no_such_type", "resource"])) == {
        r.external_id for r in resource_corpus.values()}


def test_every_node_carries_its_type_as_a_label(sync_client, resource_corpus, prefix):
    """The breadth is only usable if a caller can tell what came back — which is what the intrinsic
    type-label is for."""
    nodes = sync_client.resources.filter(external_id=f"{prefix}*", node_type=["timeseries"])
    assert nodes, "expected the corpus timeseries"
    for node in nodes:
        assert "TIMESERIES" in (node.labels or []), f"{node.external_id} has labels {node.labels}"


@pytest.mark.parametrize("type_label,node_type", [
    ("ASSET", "asset"),
    ("DATASET", "dataset"),
    ("TIMESERIES", "timeseries"),
    ("FUNCTION", "function"),
    ("POLICY", "policy"),
])
def test_every_type_label_is_matchable(sync_client, type_label, node_type):
    """A node reports its type-label on every read, so filtering by it must find that node.

    Asserted tenant-wide rather than against this run's corpus: a type-label is one shared row per
    name, so whether it matches is a property of that row, not of any node a fixture can create.
    The failure mode is silent either way — an empty result is indistinguishable from "nothing is
    tagged that way" — which is what makes it worth pinning.

    Four of these five were broken by hash drift until V37: commit 5c22b485 dropped the line writing
    `label.hash` with XXH64 and left `Label.setName`'s XXH3 as the only writer, so every row written
    before that date carried a hash no current code could reproduce. TIMESERIES was written after
    the cutoff and always worked, which is why the bug first looked dataset-specific.

    POLICY was a different fault and stayed red after V37: there was no POLICY row in the label
    table at all, so a policy node reported `labels: ['POLICY']` from the denormalised
    `node.labels` column while the filter's join on the label hash had nothing to join to — the
    label visible and unsearchable, with no row to repair. Fixed server-side; it is a plain case
    here now rather than a strict xfail.
    """
    of_type = sync_client.resources.filter(node_type=[node_type], limit=1000)
    if not of_type:
        pytest.skip(f"no {node_type} nodes in this tenant to match")

    by_label = sync_client.resources.filter(labels=[type_label], limit=1000)
    assert by_label, f"{len(of_type)} {node_type} nodes exist but none match labels=[{type_label}]"


def test_garbled_criteria_match_nothing_without_erroring(flt, resource_corpus):
    for garbage in ["!!!##$$^&()", "' OR 1=1 --", "\\", "😀"]:
        assert flt(source=[garbage]) == set(), f"{garbage!r} should match nothing"


# --------------------------------------------------------------------------- #
# limit, and the request body's shape
# --------------------------------------------------------------------------- #

def test_limit_caps_the_page_and_zero_falls_back_to_the_default(sync_client, resource_corpus, prefix):
    assert len(sync_client.resources.filter(external_id=f"{prefix}*", limit=1)) == 1
    assert externals(sync_client.resources.filter(
        external_id=f"{prefix}*", node_type=["resource"], limit=0)) == {
        r.external_id for r in resource_corpus.values()}


def test_a_limit_above_the_ceiling_is_refused(sync_client, prefix):
    with pytest.raises(DataHubException) as excinfo:
        sync_client.resources.filter(external_id=f"{prefix}*", limit=10_001)
    assert excinfo.value.status_code == 400


def test_the_retired_plural_criteria_are_not_accepted(sync_client, prefix):
    """The plural spellings the criteria briefly carried are gone from the binding as well as the
    wire. A ``TypeError`` here is the point: the alternative is the argument being accepted and
    silently dropped, which is exactly how the old scalar-plus-plural pair of forms misbehaved.
    """
    for retired, value in [("ids", [1]), ("external_ids", ["x"]), ("names", ["x"]),
                           ("sources", ["x"]), ("node_types", ["resource"]),
                           ("data_set_ids", [1])]:
        with pytest.raises(TypeError):
            sync_client.resources.filter(**{retired: value})


@pytest.mark.asyncio
async def test_async_filter_matches_the_sync_one(async_client, sync_client, resource_corpus, prefix):
    from_async = await async_client.resources.filter(external_id=f"{prefix}*", labels=["FLT_ALPHA"])
    from_sync = sync_client.resources.filter(external_id=f"{prefix}*", labels=["FLT_ALPHA"])
    assert externals(from_async) == externals(from_sync)
