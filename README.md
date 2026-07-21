# DataHub Rust SDK

An async Rust client (reqwest + Tokio) for the **DataHub Platform** REST API, with an
optional blocking client and Python bindings built from the same core.

Services are fields on the `ApiService` returned by `create_api_service()`:
`time_series` (including datapoint ingestion), `units`, `events`, `resources`, `datasets`,
`files`, `subscriptions` (including WebSocket listening), and `functions`.

## Quick start

```rust
use dataplatform_rust_sdk::create_api_service;

#[tokio::main]
async fn main() {
    let api = create_api_service(); // reads configuration from .env / the environment
    let series = api.time_series.search_by_name("engine").await.unwrap();
    for ts in series.get_items() {
        println!("{}", ts.external_id);
    }
}
```

### Blocking client

Enable the `blocking` cargo feature for a synchronous mirror of the async API — the same
split as `reqwest` / `reqwest::blocking`. Every call delegates to the async implementation
on a runtime owned by the client, so behavior is identical. Do not use it from inside an
async context; use the async `ApiService` there.

```rust
use dataplatform_rust_sdk::blocking;

let api = blocking::create_api_service();
let series = api.time_series.search_by_name("engine").unwrap();
```

## Configuration

`create_api_service()` loads a local `.env` file (via `dotenv`) and the process
environment:

- `BASE_URL` — backend root, e.g. `http://localhost:8081` (required)
- Either `TOKEN` (bearer token used as-is, never considered expired), **or** the OAuth2
  client-credentials set: `CLIENT_ID`, `CLIENT_SECRET`, `TOKEN_URI`
- `PROJECT_NAME` — optional
- `SCOPE`, `AUDIENCE` — optional, sent with the token request only when set. Keycloak needs
  neither; Entra ID requires `SCOPE=api://<app-id-uri>/.default`, Auth0 requires `AUDIENCE`.

Setting an assertion source switches the request at `TOKEN_URI` to the RFC 7523 `jwt-bearer`
grant, exchanging a JWT from one issuer for a token from another — how an Entra ID service
principal reaches a Keycloak-backed API. `CLIENT_ID`/`CLIENT_SECRET`/`TOKEN_URI` then describe
the client performing the exchange:

- `ASSERTION` — a ready-made JWT. Never refreshed; prefer the credentials below.
- `ASSERTION_CLIENT_ID`, `ASSERTION_CLIENT_SECRET`, `ASSERTION_TOKEN_URI` — fetch the assertion
  with client credentials from another provider (all three required).
- `ASSERTION_SCOPE`, `ASSERTION_AUDIENCE` — narrow the assertion request.

The same options are available on the builder as `set_scope`, `set_audience`, `set_assertion`,
`set_assertion_credentials`, `set_assertion_scope` and `set_assertion_audience`.

## Durable ingest buffering

Optionally, datapoint/event ingestion that can't get through spools to disk and is flushed
automatically on a later ingest call. Off by default; enable via the environment
(`ENABLE_BUFFERING=true`, or set any bound — `BUFFER_RETENTION_SECS`, `BUFFER_MAX_BYTES`,
`BUFFER_DIR`) or programmatically (`set_buffer_dir`, `set_buffer_retention_secs`, …).
Defaults when enabled: 72 h retention window, 5 GiB size cap, `.datahub-spool` directory.

The spool is a segmented, zstd-compressed, newline-delimited-JSON log. It is memory-safe —
sealed at a ~50 MiB rollover and drained one segment at a time — so even a multi-gigabyte
spool never loads into memory, and a torn trailing line from an unclean shutdown is skipped
on read.

## Python bindings

`datahub_python_bindings/` wraps this SDK as the Python package `datahub-sdk` (import name
`datahub_sdk`) using PyO3 and maturin. The Python test suite in `python_tests/` runs against
the **compiled** module — always use the wrapper script, which rebuilds the bindings first:

```bash
./run_python_tests.sh                # build + run the whole suite
./run_python_tests.sh -k timeseries  # extra args are forwarded to pytest
```

## Building and testing

```bash
cargo build
cargo test                 # most tests are integration tests against a live backend (.env)
cargo test -- --ignored    # long-running datapoint tests
```

## License

Apache License 2.0 — see [LICENSE](LICENSE).
