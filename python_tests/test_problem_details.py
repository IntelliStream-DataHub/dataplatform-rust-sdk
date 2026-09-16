"""``DataHubException`` carries the API's RFC 9457 problem document.

The Rust side (``src/problem.rs``, ``src/problem_integration.rs``) pins the parsing and the wire
contract in depth. This covers the one thing those cannot: that the document survives the PyO3
boundary and lands on the exception a Python caller actually catches.

Branch on ``problem_slug``, never on ``title``/``detail`` — those are prose and the RFC says so.
``problem`` is the whole body as a dict, so an extension member the bindings have no accessor for is
still reachable from Python.

The API's ``errors/*`` series has landed, so every refusal here answers with a problem document and
these are regression guards rather than a progress report. The one exception is a ``GET`` by id,
which still carries no ``type``; ``src/problem_integration.rs`` → ``pending`` tracks that, and the
404 test below tolerates it explicitly rather than silently. See ``AGENTS.md`` → Errors.
"""

import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import DataHubException

from fixtures import make_ts, sync_client, unique_id  # noqa: F401  (fixtures)


def refusal(call) -> DataHubException:
    """Run ``call``, expecting the API to refuse it, and hand back the exception."""
    with pytest.raises(DataHubException) as excinfo:
        call()
    return excinfo.value


def test_a_missing_id_arrives_as_a_problem_dict(sync_client):
    """A 404 is a problem document, and its standard members reach Python."""
    # Not every Python read raises on a miss — `labels.get` absorbs a 404 into None (see
    # `none_on_404` in the bindings). `resources.get_by_id` propagates it, which is what makes it
    # the right probe here.
    error = refusal(lambda: sync_client.resources.get_by_id(999_999_999))

    assert error.status_code == 404
    assert error.problem is not None, f"expected a problem document, got {error.message!r}"
    assert error.problem["status"] == 404
    assert error.problem["instance"] == "/resources/999999999"
    # `detail` is prose: asserted present, never matched on.
    assert error.problem.get("detail")
    # A by-id miss is the one refusal still without a `type`, so the slug is None here and must
    # not be invented as "not-found". Tracked as red-on-purpose in
    # `src/problem_integration.rs` → `pending`; when the api fixes it that test goes green and
    # this tolerance can become an equality.
    assert error.problem_slug in (None, "not-found")


def test_a_bad_cursor_is_identified_by_type_not_by_wording(sync_client):
    """``problem_slug`` is the contract; the message wording is not."""
    error = refusal(lambda: sync_client.timeseries.filter(cursor="!!!not-a-cursor!!!", limit=5))

    assert error.status_code == 400
    assert error.problem_slug == "malformed-cursor", (
        f"expected the malformed-cursor type, got {error.problem_type!r} "
        f"from body {error.message!r}"
    )
    assert error.problem_type == "https://intellistream.ai/errors/malformed-cursor"


def test_a_bad_filter_expression_says_where_it_broke(sync_client):
    """``advancedFilter`` is parsed server-side, so only the API can locate the failure."""
    error = refusal(
        lambda: sync_client.events.filter(advanced_filter="type NOT LIKE (((", limit=5)
    )

    assert error.status_code == 400
    assert error.problem_slug == "filter-expression", (
        f"got {error.problem_type!r} from body {error.message!r}"
    )
    assert "offset" in error.problem, (
        f"a long expression is only fixable if the API says where it broke: {error.problem}"
    )


def test_an_over_cap_limit_names_the_offending_field(sync_client):
    """This was refused as bare plain text before the unification; now it names ``limit``."""
    error = refusal(lambda: sync_client.timeseries.list(limit=99_999))

    assert error.status_code == 400
    assert error.problem is not None, (
        f"an over-cap limit should be a problem document, got {error.message!r}"
    )
    assert any(
        field.get("field") == "limit" for field in error.problem.get("fields", [])
    ), f"the rejected field should be named: {error.problem}"


def test_the_attributes_are_none_rather_than_absent_when_there_is_no_problem():
    """``problem`` is ``None``, not missing, so an ``except`` block needs no ``hasattr``.

    Raised locally rather than fetched: now that the API answers every refusal with a document, a
    client-side failure is the honest way to exercise the no-problem path, and it does not depend
    on some endpoint continuing to misbehave.
    """
    error = DataHubException("boom")

    assert getattr(error, "problem", None) is None
    assert getattr(error, "problem_slug", None) is None
    assert getattr(error, "problem_type", None) is None


def test_a_duplicate_external_id_is_a_conflict(sync_client, make_ts):
    """409 was already right; the body is a ``duplicate`` problem naming the value now."""
    created = make_ts()
    duplicate = intellistream_datahub_sdk.TimeSeries(
        external_id=created.external_id,
        name="problem duplicate probe",
        unit="a.u",
        value_type="float",
    )

    error = refusal(lambda: sync_client.timeseries.create([duplicate]))
    assert error.status_code == 409
    assert error.problem is not None, (
        f"a duplicate should be a problem document, got {error.message!r}"
    )
    assert error.problem_slug == "duplicate", (
        f"got {error.problem_type!r} from body {error.message!r}"
    )
    assert any(
        created.external_id in entry.values()
        for entry in error.problem.get("duplicated", [])
    ), f"the colliding value should be named: {error.problem}"


def test_an_unknown_field_is_refused_by_the_bindings_before_the_wire(sync_client):
    """The Python filter takes keywords, so an unknown one cannot reach the API at all.

    Worth pinning as the counterpart to the Rust ``green`` test that sends ``bogusField`` raw: the
    two languages refuse it in different places, and only the Rust side can exercise the API's
    ``unreadable-request-body`` problem. Here the guarantee is that it fails *early*, with a
    ``TypeError`` naming the keyword rather than a 400 from the server.
    """
    with pytest.raises(TypeError):
        sync_client.timeseries.filter(bogus_field=["x"])
