"""Search coverage for the TimeSeries service.

Mirrors `src/timeseries/test.rs::test_search_timeseries`, which searches a freshly
created series by name, by free-text query, and by description. Searches hit a
backend search index that lags writes, so we sleep briefly before querying.
"""
import time
import uuid

import pytest

import datahub_sdk
from python_tests.fixtures import *  # noqa: F401,F403  (sync_client fixture)


# The search index is eventually-consistent with writes; give it a moment.
SEARCH_INDEX_DELAY = 3.0


def _uid(prefix="search"):
    return f"pytest_{prefix}_{uuid.uuid4().hex[:12]}"


@pytest.mark.parametrize("field", ["name", "query", "description"])
def test_search_finds_created_series(sync_client, make_ts, field):
    ext_id = _uid(field)
    # Make name/description unique enough that the search can pick this row out.
    unique_name = f"Py SDK Search {ext_id}"
    unique_description = f"description for {ext_id}"
    make_ts(external_id=ext_id, name=unique_name, description=unique_description)

    time.sleep(SEARCH_INDEX_DELAY)

    if field == "name":
        form = datahub_sdk.SearchAndFilterForm(name=unique_name)
    elif field == "query":
        form = datahub_sdk.SearchAndFilterForm(query=unique_name)
    else:
        form = datahub_sdk.SearchAndFilterForm(description=unique_description)

    results = sync_client.timeseries.search(form)
    assert isinstance(results, list)
    assert any(t.external_id == ext_id for t in results), (
        f"search by {field} did not return the created series {ext_id}"
    )
