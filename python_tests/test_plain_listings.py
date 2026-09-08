"""``GET /<collection>?limit=`` — one plain listing shape for every node type.

The api used to spell "give me what I have" four different ways: resources, assets and events had
no listing at all, so it meant composing a POST body; timeseries and datasets had ``GET /x?limit=``;
policies returned every row unordered and uncapped; and functions spelled it ``GET /x/list``, also
uncapped. Datasets additionally had a ``POST /datasets/list`` that took the filter's own body and
ran the filter's own handler — one operation under two names, on the only collection that had them.

They converge here, and the convergence is what this file pins. Each service's ``list`` is the same
call against a different collection: no criteria, newest created first, capped by one shared
``limit`` contract — absent means the server's 1000, above 10000 is a rejection rather than a
silent clamp, and no cursor ever comes back.

Two of those claims have an exception each, and both are here rather than in a docstring somewhere:
``events.list`` reads *oldest*-first, and ``resources.list`` answers each row as its own class.
"""
import pytest

import intellistream_datahub_sdk
from intellistream_datahub_sdk import Asset

from fixtures import (  # noqa: F401  (fixtures)
    make_events,
    make_resource,
    sync_client,
    unique_id,
)

# Every collection that answers the plain listing. Labels, units and subscriptions are separate
# tables with their own shapes and are deliberately not in the family.
LISTINGS = ["resources", "timeseries", "datasets", "events", "functions"]


@pytest.mark.parametrize("collection", LISTINGS)
def test_listing_caps_at_the_limit_it_was_given(sync_client, collection):
    listed = getattr(sync_client, collection).list(limit=1)
    assert isinstance(listed, list)
    assert len(listed) <= 1


@pytest.mark.parametrize("collection", LISTINGS)
def test_listing_rejects_a_limit_above_the_maximum(sync_client, collection):
    """10001 is a 400, not 10000 rows.

    A silent clamp is worse than it looks: a caller who asked for 50000 and received 10000 cannot
    tell that from a tenant holding exactly 10000, so it reads as a complete answer.
    """
    with pytest.raises(intellistream_datahub_sdk.DataHubException):
        getattr(sync_client, collection).list(limit=10_001)


@pytest.mark.parametrize("collection", LISTINGS)
def test_listing_returns_a_plain_list_not_a_page(sync_client, collection):
    """No cursor, so no ``Page``.

    ``filter()`` returns a ``Page`` carrying ``next_cursor``; the listings return a plain list,
    because a walk needs a sort and a cursor and both live on the filter body. Handing out a cursor
    an endpoint cannot accept back invites a paging loop that silently never advances.
    """
    listed = getattr(sync_client, collection).list(limit=1)
    assert not hasattr(listed, "next_cursor")


def test_resource_listing_answers_each_row_as_its_own_class(sync_client, make_resource):
    """The plain listing dispatches on the type-label like every other ``/resources`` read.

    ``/resources`` spans every node type, so a listing that flattened its rows to ``Resource``
    would hand back an asset with no geometry and a timeseries with no unit — the exact failure
    the polymorphic-node work removed from ``filter`` and ``by_ids``.
    """
    ext_id = unique_id("listing_asset")
    make_resource([Asset(external_id=ext_id, name=ext_id)])

    # Newest created first, and the default page is 1000, so a node created a moment ago is on it.
    listed = sync_client.resources.list()
    mine = next((n for n in listed if n.external_id == ext_id), None)
    assert mine is not None, "a resource created a moment ago should be on the newest-first page"
    assert isinstance(mine, Asset)


def test_event_listing_is_oldest_first(sync_client):
    """Events are the one member of the family that does not read newest-first.

    ``GET /events`` runs the event filter with an empty body, and that body's default sort is
    ``eventTime`` ascending — the order the keyset cursor pages in. So this is the start of the
    tenant's history, not what just happened, which is the opposite of what the endpoint's own
    description promises and the opposite of what the three node listings beside it do.

    Needs no fixture: it asserts the order of whatever the tenant already holds.
    """
    times = [e.event_time for e in sync_client.events.list(limit=50)]
    assert times == sorted(times), (
        "the plain event listing is eventTime ascending, not newest-first"
    )
