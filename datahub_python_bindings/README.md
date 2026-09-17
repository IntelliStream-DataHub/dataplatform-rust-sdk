# intellistream-datahub-sdk

Python bindings for the IntelliStream DataHub SDK — a client for the DataHub REST API, implemented
in Rust and exposed to Python with [PyO3](https://pyo3.rs).

```bash
pip install intellistream-datahub-sdk
```

```python
from datetime import datetime, timezone

import intellistream_datahub_sdk as dh

client = dh.DataHubClient.from_envfile(".env")

# Create a time series, then ingest a datapoint into it.
client.timeseries.create([dh.TimeSeries(external_id="sensor_1", name="Sensor 1", value_type="float")])
client.timeseries.insert_from_lists(
    timestamps=[datetime.now(timezone.utc)],
    values=[21.5],
    ts="sensor_1",
)

# Find the alarms.
alarms = client.events.filter(type="ALARM", limit=100)

# The same criteria, kept as an object, so one definition can be filtered and searched with.
ALARMS = dh.EventFilter(type="ALARM")
recent = client.events.filter(filter=ALARMS, limit=100, sort_by="eventTime", sort_order="desc")
matches = client.events.search("bearing", filter=ALARMS)
```

Every `filter()` takes either the criteria as keywords or a prepared `filter=` object — passing
both is a `TypeError`. `limit`, `sort_by`, `sort_order` and `cursor` are always arguments of the
call rather than fields of the filter, so a stored filter carries no paging state into its next use.

Both a synchronous and an asynchronous client are available — `DataHubClient` and
`AsyncDataHubClient`. The async one exposes the same services with awaitable methods.

Anywhere an entity is identified you may pass its external id, its numeric id, an `IdCollection`,
or the entity itself.

## What it covers

| Service | Highlights |
|---|---|
| `timeseries` | time-series CRUD, datapoint ingestion and retrieval |
| `events` | event CRUD, filter/search, and the type/sub-type/status/source vocabularies |
| `resources` | the generic node query across every node type, and creating nodes with the edges between them |
| `edges` | linking existing resources, reading or deleting an edge, and the relationship-type catalogue |
| `files` | upload, download, directory listing, move/rename, trash and restore |
| `datasets` | dataset CRUD and search |
| `labels` | label CRUD |
| `subscriptions` | subscription CRUD and WebSocket listening |
| `functions` | function registration and lookup |
| `units` | unit lookup |

## Configuration

The client reads its configuration from a `.env` file or the environment:

- `BASE_URL` — the backend root, e.g. `https://datahub.example.com`
- either `TOKEN` — a bearer token used as-is
- or `CLIENT_ID`, `CLIENT_SECRET` and `TOKEN_URI` — an OAuth2 client-credentials exchange
- `SCOPE` — added to `openid`, which is always requested. A realm using Keycloak Organizations
  needs `organization:*`, or `organization:<alias>` to pin one tenant; without it every call fails
  `401 invalid_token`.
- `AUDIENCE`, and the `ASSERTION*` variables for exchanging a token from another identity provider
  (such as Entra ID) — see the [Rust SDK's README](https://github.com/IntelliStream-DataHub/dataplatform-rust-sdk#configuration),
  which reads the same variables.

Every variable is also a keyword argument of `DataHubClient(...)` and `AsyncDataHubClient(...)`
(`base_url=`, `token=`, `scope=`, …; `TOKEN_URI` is `token_url=`), for configuring the client
without an environment.

## Errors

A refused call raises `DataHubException`, carrying `status_code` and the raw `message`. When the API
explains itself with an RFC 9457 problem document, `problem` is that document as a dict and
`problem_slug` names its type — branch on the slug, not on the prose in `title` or `detail`:

```python
try:
    client.edges.delete([edge_id])
except dh.DataHubException as e:
    if e.problem_slug == "would-strand":
        for blocker in e.problem["blockedBy"]:
            print(blocker["externalId"])
```

## Type hints

The package ships `py.typed` and a full `.pyi` stub, so mypy and IDE completion work out of the box.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

Source: <https://github.com/IntelliStream-DataHub/dataplatform-rust-sdk>
