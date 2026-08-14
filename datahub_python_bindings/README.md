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
client.timeseries.create([dh.TimeSeries(external_id="sensor_1", name="Sensor 1")])
client.timeseries.insert_from_lists(
    timestamps=[datetime.now(timezone.utc)],
    values=[21.5],
    ts="sensor_1",
)

# Find the alarms.
alarms = client.events.filter(
    dh.EventFilter(basic_filter=dh.BasicEventFilter(type="ALARM"), limit=100)
)
```

Both a synchronous and an asynchronous client are available — `DataHubClient` and
`AsyncDataHubClient`. The async one exposes the same services with awaitable methods.

Anywhere an entity is identified you may pass its external id, its numeric id, an `IdCollection`,
or the entity itself.

## What it covers

| Service | Highlights |
|---|---|
| `timeseries` | time-series CRUD, datapoint ingestion and retrieval |
| `events` | event CRUD, filter/search, and the type/sub-type/status/source vocabularies |
| `resources` | hierarchical assets, plus relationship `edges` between them |
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

## Type hints

The package ships `py.typed` and a full `.pyi` stub, so mypy and IDE completion work out of the box.

## License

Apache License 2.0 — see [LICENSE](LICENSE).

Source: <https://github.com/IntelliStream-DataHub/dataplatform-rust-sdk>
