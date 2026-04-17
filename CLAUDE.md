# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build / Test

```
cargo build
cargo test                               # runs all non-ignored tests
cargo test <name>                        # substring match on test name
cargo test -- --ignored                  # run tests marked #[ignore] (e.g. long-running datapoint tests)
cargo test <path>::tests::<name>         # e.g. `events::tests::test_events_full`
cargo test -- --nocapture                # show println! from tests (the SDK prints response bodies)
```

Most tests are integration tests that call a live backend via `create_api_service()`. They read configuration from a local `.env` file (gitignored). Required:

- `BASE_URL` — backend root, e.g. `http://localhost:8081`
- Either `TOKEN` (bearer token used as-is, no expiry) **or** the full OAuth2 client-credentials set: `CLIENT_ID`, `CLIENT_SECRET`, `AUTH_URI`, `TOKEN_URI`, `REDIRECT_URI` (optional: `PROJECT_NAME`)

Tests that mutate backend state (create/delete) often `sleep` a few seconds between operations and are sensitive to race conditions — prefer running them serially or isolating by unique external IDs.

## Backend

The DataHub REST API this SDK targets is a separate Spring Boot project; the HTTP endpoints are defined in its `datahub-api` module. Check that source when endpoint shapes, field names, or error responses are unclear. If you have a local checkout, record its path in `CLAUDE.local.md` (gitignored) so future Claude sessions on your machine can read it directly.

## Architecture

This crate is a thin async HTTP SDK around a DataHub-style REST API. Entry point is `create_api_service()` in `src/lib.rs`, which returns an `Rc<ApiService>` built with `Rc::new_cyclic` so each subservice holds a `Weak<ApiService>` back-reference. Subservices are fields on `ApiService`:

- `time_series` (`src/timeseries/`) — `TimeSeries` + datapoint ingestion/retrieval
- `units` (`src/unit/`)
- `events` (`src/events/`)
- `resources` (`src/resources/`) — hierarchical asset-like entities
- `files` (`src/files/`) — multipart upload via `execute_file_upload_request`

### The `ApiServiceProvider` trait (`src/generic.rs`)

Every subservice implements `ApiServiceProvider`, which owns the HTTP plumbing: token acquisition, `execute_get_request`, `execute_post_request`, `execute_file_upload_request`. Subservice methods should go through these helpers rather than calling `reqwest` directly — a few early methods (e.g. `TimeSeriesService::list`) still bypass the trait and should be migrated when touched.

### Response shape: `DataWrapper<T>`

The API wraps collections in `{ "items": [...] }`. `DataWrapper<T>` mirrors that and carries the HTTP status code + raw error body alongside items. Deserialization goes through the `DataWrapperDeserialization` trait, which tolerates 204/empty bodies and stores non-2xx bodies in `error_body` instead of failing. When adding new endpoint methods, return `Result<DataWrapper<T>, ResponseError>`.

### Entity → request-body conversion

`DataHubEntity` is a marker trait (`ext_id()` + `Clone + Serialize`) that unlocks generic `From` impls so a `T`, `&T`, `Vec<T>`, or `&Vec<T>` can be passed to service methods and auto-wrapped into `DataWrapper<T>`. Service methods accept `&I where for<'a> &'a I: Into<DataWrapper<Event>>` (see `EventsService::create`) — implement `DataHubEntity` on new entity types to get this ergonomics for free. `IdAndExtId` has its own parallel set of `From` impls for delete/byids endpoints.

### Auth (`src/datahub.rs`)

`DataHubApi` holds `Rc<RwLock<AuthState>>`. `get_api_token()` reads the cached token; if missing or expired, `refresh_token()` uses the OAuth2 refresh token when present, otherwise does a client-credentials exchange. A `TOKEN` passed via env is stored with `expire_time: None` (never considered expired by the `is_expired` check — a user-supplied token is assumed to be managed externally). OAuth2 client is `None` unless all five OAuth env vars are present.

### Errors

Two error types, used in different layers:
- `DataHubError` (`src/errors.rs`) — config/auth/setup errors from `DataHubApi`
- `ResponseError` (`src/http.rs`) — HTTP errors surfaced to callers of service methods; carries `StatusCode` + message

`get_token()` in `ApiServiceProvider` converts `DataHubError` → `ResponseError(401)` so service methods can return a single error type.

### Filters (`src/filters.rs`)

Two parallel filter styles coexist: `BasicEventFilter` (builder-style, legacy) and `EventFilter` + `AdvancedFilter` (richer, added more recently). When adding endpoints, prefer the advanced filter type — some advanced-filter endpoints are not yet wired up server-side and are currently tested only via serde round-trips.

## Conventions

- `#[serde(rename = "camelCase")]` or explicit `#[serde(rename = "...")]` on fields — the backend is camelCase, Rust is snake_case.
- `externalId` (string, user-supplied) and numeric `id` are both valid identifiers across the API. `IdAndExtId` / `IdAndExtIdCollection` model this choice.
- `process_response` (`src/http.rs`) prints response bodies to stdout (truncated to 2000 chars). This is deliberate for debugging — don't silently remove it.
- Tests that depend on backend state being empty are brittle; recent fixes moved away from exact-count assertions (see commit `7f0a059`). Don't add new ones.
