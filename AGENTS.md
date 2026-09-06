# AGENTS.md
Guidance for AI coding agents working in this repository. Claude Code reads it through the `CLAUDE.md` import alongside it.

## Build / Test

```
cargo build
cargo test                               # runs all non-ignored tests
cargo test <name>                        # substring match on test name
cargo test -- --ignored                  # run tests marked #[ignore] (e.g. long-running datapoint tests)
cargo test <path>::tests::<name>         # e.g. `events::tests::test_events_full`
cargo test -- --nocapture                # show println! from tests (the SDK prints response bodies)
./run_python_tests.sh                    # Python-bindings suite (rebuilds the PyO3 module first — see below)
./run_c_tests.sh                         # C-bindings suite: cargo test + the C smoke test, no backend needed (see below)
```

Most tests are integration tests that call a live backend via `create_api_service()`. They read configuration from a local `.env` file (gitignored). Required:

- `BASE_URL` — backend root, e.g. `http://localhost:8081`
- Either `TOKEN` (bearer token used as-is, no expiry) **or** the OAuth2 client-credentials set: `CLIENT_ID`, `CLIENT_SECRET`, `TOKEN_URI` (optional: `PROJECT_NAME`)

Tests that mutate backend state (create/delete) often `sleep` a few seconds between operations and are sensitive to race conditions — prefer running them serially or isolating by unique external IDs.

### Multi-tenant / ACL tests

`src/multi_tenant_integration.rs` (Rust) and `python_tests/test_multi_tenant_acl.py` cover what happens across organizations: a principal belonging to more than one, tenant isolation, per-dataset read/write grants, and how an ACL denial interacts with durable buffering.

```
cargo test multi_tenant_integration -- --ignored --nocapture --test-threads=1
./run_python_tests.sh -k multi_tenant
```

They need six purpose-built Keycloak principals beyond the usual `.env` (a two-organization one, read-only/write-only/no-grant ones, and a full-access one per organization), configured through `MT_*` variables. Each test skips with a printed note when its fixture is absent, so a checkout without that realm setup is unaffected. **The Rust module's `//!` doc is the reference** — it carries the env contract, the organization-group naming convention, the realm-level mapper prerequisites, and the reason 401s here can only be asserted on status (the API drops the explanation before it reaches any client).

## Backend

The DataHub REST API this SDK targets is a separate Spring Boot project; the HTTP endpoints are defined in its `datahub-api` module. Check that source when endpoint shapes, field names, or error responses are unclear. If you have a local checkout, record its path in `CLAUDE.local.md` (gitignored) so future Claude sessions on your machine can read it directly.

## Architecture

This crate is a thin async HTTP SDK around a DataHub-style REST API. Entry point is `create_api_service()` in `src/lib.rs`, which returns an `Arc<ApiService>` built with `Arc::new_cyclic` so each subservice holds a `Weak<ApiService>` back-reference. Subservices are fields on `ApiService`:

- `time_series` (`src/timeseries/`) — `TimeSeries` + datapoint ingestion/retrieval. Neither `TimeSeries` nor `TimeSeriesUpdate` has **`securityCategories`**: it was stored, writable and returned, but nothing ever read it — no part in access control (dataset grants are Keycloak organization groups), no query filtering on it, and the backend silently dropped any id that did not already exist, so the field never round-tripped. It has been removed server-side along with its join table, and the api reads request bodies strictly, so sending it is now a 400. Files keep their own `securityCategories` (`INode` in `src/generic.rs`) — separate entity, separate question. `ListFieldU64` went with it: it was the only field of that type, so the Python wrapper class is gone too (`ListFieldStr` and `ListFieldIdCollection` remain).
- `units` (`src/unit/`)
- `events` (`src/events/`) — event CRUD, filter/search, plus the vocabulary endpoints (`list_types`/`search_types` and the same pair for sub-types, statuses and sources, over `EventDimension`). Those answer "what values does this tenant actually use" for the four categorical fields and back filter dropdowns; they read small server-side dimension tables rather than scanning events, so they are cheap but *eventually consistent* with the events. Note the route asymmetry the SDK hides: `/events/list/{plural}` but `/events/search/{singular}`. `EventUpdate` has **no `event_time`**: an event's time is immutable after creation — the events table is partitioned by it, so ClickHouse refuses the mutation outright, and the api used to validate the field, echo the new value back with a 200 and then fail to apply it. It has been dropped from the update form, so sending it is now a 400. Record a corrected time as a new event.
- `resources` (`src/resources/`) — the generic node service. Its reads span **every** node type and answer with [`Node`](#the-polymorphic-node-type) rather than one flat shape; relationship edges live in `src/relations/` (`EdgeProxy`, `RelForm`, `RelatedNode`)
- `edges` (`src/relations/service.rs`) — the `/edges` endpoints: `get`/`by_ids`/`create`/`delete` plus the relationship-type catalogue (`types`/`create_types`). Edges normally come into being through `resources.create(nodes, relations)`; this service is for linking resources that already exist and for reading or deleting an edge on its own. `get` answers an unknown id with 404 and a `problem+json` body; `by_ids`, like every batch lookup, answers 200 with the found subset and silently omits what is missing. (`get` used to be 200-and-nothing despite documenting a 404 — api #275 made single-resource by-id GETs consistently 404 and deliberately left batch lookups alone.) Two further behaviours are worth knowing, and are documented at each call site:

  - `create_types` fails silently on a duplicate name — the unique-hash collision surfaces at commit, after the handler returned, so the caller gets a 200 with an empty *body*, and in a batch the valid new types are rolled back with it. This one does contradict the OpenAPI: `test_duplicate_relationship_type_conflicts` encodes the intended 409 and is red until the server-side fix lands.
  - `delete` will not remove an edge that is an endpoint's only route to the graph root: `ResourceService.delete` refuses rather than orphan the node, answering **400** with the stranded resource named in `fields` (`{"type": "strandedResource", "externalId": …}`) and a message saying to include it in the deletion or keep a connecting path. Practically, an edge is separately deletable only when both endpoints stay reachable without it; otherwise it goes away with the resources. The check reads the graph projection, which lags the write, so deleting too soon after creating the edge gets the *wrong answer* rather than an error — the refusal does not fire and the node is stranded. `relations::tests` measured 6/6 wrongly allowed immediately after create, 6/6 refused 500ms later; its `await_graph` helper is what the live tests wait on.
- `datasets` (`src/datasets/`)
- `files` (`src/files/`) — raw-`PUT` upload via `execute_file_upload_request` (content is the body, metadata rides in `X-Datahub-*` headers), plus directory listing, get/search, `FileUpdate` (rename/move/re-dataset), trash + restore, delete, and download (`download` in memory, `download_to_path` streamed)
- `subscriptions` (`src/subscriptions/`) — subscription CRUD, plus `listen.rs`: WebSocket listening against the api's subscription-listen endpoint (`tokio-tungstenite`)
- `functions` (`src/functions/`)
- `labels` (`src/labels/`) — label CRUD (`list`/`get`/`create`/`update`/`delete`). Note the entity type is `labels::Label`, deliberately *not* re-exported at the crate root because `resources::*` already brings a different graph-DTO `Label` there.

### The polymorphic node type (`src/nodes.rs`)

`/resources` spans six node types, and its reads answer with each row in the shape of its own
kind. `Node` is that: an enum over `Asset`, `TimeSeries`, `Function`, `Resource`, `Dataset` and
`Policy`. `filter`/`search`/`get_by_id` return `DataWrapper<Node>`, `by_ids`/`create` and
`EdgesService::by_ids` return `GraphDataWrapper<Node>`, and `ResourceNetwork::nodes` is `Vec<Node>`.

**The discriminator is a label, not a field.** There is no `nodeType` key on the wire. A node's
type is the intrinsic type-label the api forces into `labels` on every read — `ASSET`,
`TIMESERIES`, `FUNCTION`, `DATASET`, `POLICY` — and a plain resource carries **none of them**, so
absence is the `RESOURCE` signal. Serde has no mode for a tag inside an array field, so `Node`
hand-writes `Deserialize`: buffer into `serde_json::Value`, canonicalize each label the way the
api's `TextValidator.toSnakeUpperCased` does, dispatch. More than one type-label is an **error**,
mirroring the api's `NodeModelDeserializer`; zero is a `Resource`. Serializing goes the other way,
emitting the variant's own shape and appending its type-label if the caller has not — it never
strips a conflicting one, because a body labelled both `ASSET` and `POLICY` earns the api's 400
naming both, and quietly picking one for the caller would be worse.

`Node` is `#[non_exhaustive]`, so a seventh node type is additive.

Behaviours worth knowing, each pinned by a test in `src/nodes.rs`:

- **Flat reads never populate `related_resources`.** `get_by_id`, `by_ids`, `filter` and `search`
  all answer `[]`; only the graph reads and the create echo fill it.
- **Graph reads are typed but sparse.** Neo4j stores a column subset, so a `TimeSeries` from
  `fetch_related` carries **none** of its type-specific fields — the payload is the shared node
  keys and nothing else, so `unit`, `value_type`, `table_engine` and `security_categories` are all
  `None`. That is why `TimeSeries::value_type` is `Option<String>`: it is always present on a flat
  read and never on a graph one, and a required field made any traversal over a timeseries a hard
  deserialization error. `metadata` is empty rather than absent. An asset's geometry is
  reconstructed as a Point, so a stored Polygon comes back wrong.
- **`update` still echoes flat `Resource`s**, whatever the node's real type — the one read/write
  asymmetry left, owned by the api's `NODE_UPDATE_REFACTOR.md`. `ResourceService::update` is
  therefore the one method here that does *not* return `Node`.
- **Policies never carry `value`, `template_id` or `data_set_id`** on a read, and their `metadata`
  can be outright `null`.
- **`Resource::geolocation` is write-only** server-side: accepted on create, never echoed. Assets
  carry it.
- **Every type is creatable through `/resources/create`, timeseries included** — each element of
  `nodes` is dispatched by its own labels. `DATASET` and `POLICY` need the all-datasets manage
  grant (403 without), and their `data_set_id` is silently dropped. A duplicate `external_id`
  surfaces as a constraint violation rather than the clean 409 `/timeseries/create` gives.

In Python each variant maps to its own pyclass, so `isinstance(node, TimeSeries)` works and an
object from `resources.filter()` behaves exactly like one from `timeseries.by_ids()`. The dispatch
is a hand-written `IntoPyObject` on a non-pyclass `PyNode` wrapper (`datahub_python_bindings/src/nodes.rs`)
— the first such impl in the bindings — which is what lets `Vec<PyNode>` and `Page` stay generic.
Every node class also exposes `node_type` for data-driven dispatch.

### Blocking client (`src/blocking.rs`)

Synchronous mirror of the async API behind the `blocking` cargo feature — the same split as `reqwest` / `reqwest::blocking`. Every wrapper delegates to the async implementation on a dedicated Tokio runtime owned by the client, so there is exactly one implementation of each call. It must not be constructed or called from inside an async context (building its runtime there panics); use the async `ApiService` instead.

### Durable ingest buffering (`src/buffer.rs`, integration tests in `src/buffer_integration.rs`)

`TimeSeriesService::flush_buffer` / `EventsService::flush_buffer` drain a spool without ingesting anything new (for a controlled shutdown, or a host retrying on its own clock); `buffered_count` reports what is held. Retention is measured on each record's own timestamp, so a backfill older than the window is not kept.

When a datapoint/event send can't get through, ingestion spools to a segmented, zstd-compressed NDJSON log on disk and flushes automatically on a later ingest call. Invariants to preserve: memory use is bounded by a single segment (plain append-only active segment, zstd-sealed at ~50 MiB rollover via temp file + atomic rename, drained oldest-first one segment at a time); bounded by time retention (whole segments past the window dropped, expired records skipped on read) and a size cap (oldest segment deleted); a torn trailing line from an unclean shutdown is skipped on read. Each on-disk line is `<epoch_millis>\t<json>`; the spool is content-agnostic.

### The `ApiServiceProvider` trait (`src/generic.rs`)

Every subservice implements `ApiServiceProvider`, which owns the HTTP plumbing: token acquisition, `execute_get_request`, `execute_post_request`, `execute_file_upload_request`, `execute_get_stream_request`. Subservice methods should go through these helpers rather than calling `reqwest` directly — a few early methods (e.g. `TimeSeriesService::list`) still bypass the trait and should be migrated when touched.

`execute_get_stream_request` is the odd one out: it returns the raw `reqwest::Response` instead of a `DataWrapper`, for endpoints that answer with bytes (currently only `/files/download/{id}`). It also overrides the client's default `Accept: application/json` with `*/*` — that endpoint only `produces` `application/octet-stream`, and Spring answers a JSON-only `Accept` with 406 before the handler runs.

### Response shape: `DataWrapper<T>`

The API wraps collections in `{ "items": [...] }`. `DataWrapper<T>` mirrors that and carries the HTTP status code + raw error body alongside items. Deserialization goes through the `DataWrapperDeserialization` trait, which tolerates 204/empty bodies and stores non-2xx bodies in `error_body` instead of failing. When adding new endpoint methods, return `Result<DataWrapper<T>, ResponseError>`.

### Entity → request-body conversion

`DataHubEntity` is a marker trait (`ext_id()` + `Clone + Serialize`) that unlocks generic `From` impls so a `T`, `&T`, `Vec<T>`, or `&Vec<T>` can be passed to service methods and auto-wrapped into `DataWrapper<T>`. Service methods accept `&I where for<'a> &'a I: Into<DataWrapper<Event>>` (see `EventsService::create`) — implement `DataHubEntity` on new entity types to get this ergonomics for free. `IdAndExtId` has its own parallel set of `From` impls for delete/byids endpoints.

### Auth (`src/datahub.rs`)

`DataHubConfig` holds `Arc<tokio::sync::RwLock<AuthState>>`. `get_api_token()` reads the cached token; if missing or expired, `refresh_token()` uses the OAuth2 refresh token when present, otherwise does a client-credentials exchange — or, when an assertion source is configured, the RFC 7523 assertion exchange (`exchange_assertion`). A `TOKEN` passed via env is stored with `expire_time: None` (never considered expired by the `is_expired` check — a user-supplied token is assumed to be managed externally). OAuth2 client is `None` unless `CLIENT_ID`, `CLIENT_SECRET`, and `TOKEN_URI` are all present — only the client-credentials, refresh-token and `jwt-bearer` flows are supported, so `AUTH_URI` and `REDIRECT_URI` are not consumed.

`openid` is **always** sent in the token request, and `SCOPE` **adds** to it rather than replacing it — so `SCOPE=organization:acme` asks for `openid organization:acme`. It is unconditional because the API resolves dataset grants by calling the IdP's UserInfo endpoint with the caller's own token, and Keycloak answers UserInfo with 403 for a token whose scope omits it — surfacing as a *503 "identity provider is unreachable"* on every permission-checked endpoint, which names the wrong cause. Safe to force because this scope only ever goes to `TOKEN_URI`, the issuer the API validates against; the provider that rejects `openid` alongside other scopes (Entra ID's `.default`) belongs to `ASSERTION_SCOPE`, which is untouched. Two server-side traps worth knowing when setting it: an unknown organization alias is *silently dropped* (the token comes back with no `organization` claim, so it fails 401 without naming the alias), and the bare name `organization` — no `:*`, no alias — makes Keycloak answer `{"error":"unknown_error"}`. `AUDIENCE` is appended to the token request only when set. The `ASSERTION*` variables select `jwt-bearer`: either a ready-made `ASSERTION`, or `ASSERTION_CLIENT_ID`/`ASSERTION_CLIENT_SECRET`/`ASSERTION_TOKEN_URI` for the SDK to fetch one (narrowed by `ASSERTION_SCOPE`/`ASSERTION_AUDIENCE`). The `oauth2` crate has no `jwt-bearer` builder, so that path posts the form directly via `post_token_form` and deserializes into `BasicTokenResponse`. The assertion is never cached — providers commonly reject a replayed one.

For the assertion exchange (`exchange_assertion`), `CLIENT_SECRET` is optional: with it the exchange is the jwt-bearer grant over basic auth; without it the SDK switches to federated client authentication — the assertion is sent as the RFC 7523 `client_assertion`, matching Keycloak's "Signed JWT - Federated" client authenticator (client resolved from assertion issuer + subject, so no Keycloak-issued secret exists). In that secretless mode `ASSERTION_GRANT` selects the grant: `client_credentials` (default — token for the client's service account) or `jwt-bearer` (identity chaining — token for the user linked to the assertion's subject). Requires Keycloak ≥26.6 with the `federated-jwt` execution present in the realm's client-auth flow — realms created on older versions lack it and must add it to a copy of the built-in `clients` flow. Full setup walkthrough: `docs/entra-federated-auth.md`.

### Errors

Two error types, used in different layers:
- `DataHubError` (`src/errors.rs`) — config/auth/setup errors from `DataHubConfig`
- `ResponseError` (`src/http.rs`) — HTTP errors surfaced to callers of service methods; carries `StatusCode` + message

`get_token()` in `ApiServiceProvider` converts `DataHubError` → `ResponseError(401)` so service methods can return a single error type.

### Filters (`src/filters.rs`)

**Two types per entity, named the same way every time.** `XFilter` is the *criteria* — the fields a
row is matched on. `XFilterForm` is the *request body*, wrapping those criteria with `limit`, `sort`
and `cursor` (and, for events only, `advancedFilter`). So `EventFilter` goes inside
`EventFilterForm`, `DatasetFilter` inside `DatasetFilterForm`, and likewise for resources,
timeseries and subscriptions. The same `XFilter` is what `SearchAndFilterForm<F>` narrows a search
by. This convention is recent: the criteria for events and datasets used to be called
`BasicEventFilter`/`BasicDatasetFilter`, because the plain name had been spent on the request body,
and the request bodies themselves answered to `DatasetFilter`, `EventFilter`, `ResourceRetreiver`
and `TimeSeriesFilterForm` — four spellings of one idea, which is what made `Basic` necessary in the
first place. The backend still calls the request bodies `XRetreiver` (its own spelling of
*Retriever*); the wire format is unaffected either way, since none of these names is serialized.

**That is the Rust surface. Python does not mirror it, deliberately.** There is no `XFilterForm`
in the bindings: `filter()` takes either the criteria as keywords — `client.timeseries.filter(
name=["Pump*"], limit=100)` — or a prepared `filter=` object, and passing both is a `TypeError`.
`limit`, `sort_by`, `sort_order` and `cursor` are always arguments of the call, never fields of the
filter, so one `XFilter` can be reused across `filter()` and `search()` without carrying paging
state between them. Before 0.3.0 the Python side had three shapes at once — an envelope for events
and datasets, a flat `TimeSeriesFilterForm` that was really the criteria, and bare keywords on
resources — and `timeseries.search` silently discarded the paging fields of the form it was handed.

The four `/{entity}/filter` endpoints share one contract. `NodeFilter` (`src/filters.rs`) is the criteria every node type can be filtered by — `id`, `externalId`, `name`, `source`, `labels`, `metadata`, `createdTime`, `lastUpdatedTime` — and `ResourceFilter`, `TimeSeriesFilter` and `DatasetFilter` each `#[serde(flatten)]` it, so on the wire its fields sit alongside the type-specific ones. `EventFilter` deliberately does **not** extend it (events are not nodes: no `name` column, a UUID id) but matches it field for field wherever ClickHouse can back it.

The rules, which every one of them obeys:

- **Patterns.** `externalId`, `name`, `source` — plus `unit`/`unitExternalId` on timeseries and `type`/`subType`/`status` on events — are pattern lists. `*` and `%` are both wildcards; `_` is **literal**, because identifiers here are built out of underscores and raw `LIKE` would make `sap_work_orders` also match `sapXwork_orders`. Matching is case-insensitive. An entry with no wildcard matches exactly, and resolves through the indexed hash where one exists.
- **Singular names, list values.** Every criterion above is a list, and every one is named in the singular, because the api declares them `@SingleOrList`: a bare value is accepted wherever a list is, and one value is the common case. `labels` and `relatedResources` keep their plurals — their entries AND rather than OR, so those fields really are about a set. The SDK always sends the list form.
- **AND across fields, OR within a list** — except `labels` and `metadata`, where every entry must be present.
- **Empty means no restriction**, and so do blank entries and `None`: an empty `IN` is not valid SQL, and a caller who built a list and found nothing to put in it means "no restriction" far more often than "match nothing".
- **`dataSetId` is the one exception to that.** `None`/absent is "no data set restriction"; an explicit `[]` is "narrow to no data sets" and matches nothing. Opposite answers, so the SDK skips the key when `None` rather than emitting `null`. Entries name a data set by id *or* external id, and a data set stands in for everything beneath it in the `BELONGS_TO` hierarchy — the same expansion its ACL grant applies.
- **A `None` metadata value matches the key alone.** Hence `MetadataFilter = HashMap<String, Option<String>>` rather than a map of `String`.
- **`valueType` is not a pattern list** — a closed catalogue (`BIGINT`, `FLOAT`, `FLOAT32`, `NUMERIC`, `DECIMAL32`, `TEXT`, `MIXED`), matched exactly and case-insensitively.
- **`limit`** defaults to 1000 and is capped at 10000 (400 above it); `<= 0` falls back to the default. The SDK types it `u64`, so a negative one cannot be sent at all.

Names the refactors removed — `externalIdPrefix`, `metadataKey`/`metadataValue`, `description` on the event filter, the dataset `writeProtected`/`deactivated` flags, and the plural spellings `ids`/`externalIds`/`names`/`sources`/`types`/`subTypes`/`statuses`/`units`/`unitExternalIds`/`valueTypes`/`nodeTypes`/`dataSetIds` the fields briefly carried — are gone from the SDK rather than kept as aliases. **The backend drops unknown keys silently**, so a leftover one places no restriction and returns everything the caller can read, which reads like a working query. The serde tests in `src/filters.rs`, `src/datasets/tests.rs`, `src/resources/tests.rs` and `src/timeseries/test.rs` assert their absence from the payload; the live behaviour is covered by `python_tests/test_filter_{timeseries,resources,datasets,events}.py`.

Related resources are one field, not two: both `Event` and `EventFilter` carry `relatedResources`, an array of the backend's `IdCollection` (`[{"id": "34"}, {"externalId": "sensor_abc"}]`, modelled by `IdAndExtId`). An entry may name a resource by id, external id, or both; the backend resolves the missing side and returns both. `dataSetId` on the filter uses the same shape. There are no aliases for the retired flat `relatedResourceIds` / `relatedResourceExternalIds` arrays.

`EventFilterForm` + `AdvancedEventFilter` remain the richer style for events; some advanced-filter endpoints are not yet wired up server-side and are tested only via serde round-trips.

#### Sorting and paging

All four filters take `sort` and a keyset `cursor`, and answer with `nextCursor` on the envelope.
In Rust that is `PageRequest` (flattened into the three node retrievers; the event filter declares
the two fields itself because it also carries `advancedFilter`) plus `DataWrapper::next_cursor`. In
Python it is `sort_by` / `sort_order` / `cursor` keywords, and `filter()` returns a **`Page`** —
list-like, so existing code is unaffected, but carrying `.next_cursor`.

- **One** sort property, plus `id` appended. The tie-breaker is what makes the order *total*: a
  sort column alone is not a position unless it is unique, so a page boundary inside a run of equal
  values repeats or drops exactly those rows. An unrecognised property falls back to the default
  rather than erroring; anything that is not exactly `desc` sorts ascending.
- **Defaults differ.** Nodes: `createdTime` descending, sortable by `id`, `externalId`, `name`,
  `source`, `description`, `createdTime`, `lastUpdatedTime`, `dataSetId`. Events: `eventTime`
  **ascending** — the order the cursor pages in — sortable also by `type`, `subType`, `status`.
- **Nulls are a block**: last ascending, first descending.
- **Cursors are opaque** (base64 of a versioned encoding of sort + boundary + id). Never build one;
  echo back `next_cursor`. An unreadable cursor restarts from page one rather than erroring.
- **A cursor belongs to its sort.** Continuing it under another is *meant* to be a 400; today it is
  a 200 with a zero-byte body — see `python_tests/test_filter_paging.py`. Paging a nullable event
  sort (`subType`, `status`) is refused the same way.
- `nextCursor` is absent on a short page, so "keep going while it is present" is the whole loop. A
  full page may still be the last, so a walk ends with one empty request.

#### `/resources/filter` is the generic node query

It spans **every** node type — assets, timeseries, functions, resources, data sets, policies —
narrowed by `nodeType` (`["resource", "timeseries"]`, case-insensitive; omitted = all; a list of
only unknown names matches *nothing*). It behaved this way before by omission, with no discriminator
and single-table inheritance doing the rest; the breadth is now stated and narrowable. What comes
back is typed per row — see [`Node`](#the-polymorphic-node-type). The other three endpoints stay
typed. `DatasetFilter` is consequently just the shared criteria — its `writeProtected` and
`deactivated` flags were removed server-side as inert.

#### The four `/search` endpoints share one contract

`{ "search": { "query": … }, "filter": … , "limit": … }` — the same shape on all four, so one type covers them: `SearchAndFilterForm<F>`, generic over the filter alone, mirroring the api's `SearchBody<F>`. `DatasetSearch` and `EventSearch` were the same three fields written twice more and are gone; the duplication is what let the dataset one keep claiming its filter was ignored for a release after that stopped being true. In Python there is no form class at all — every `search` takes `query`, `filter` and `limit` directly, the way `datasets.search` always did.

- **The phrase selects, the filter only removes, `limit` caps what survives.** A filter can never widen a search, and omitting it (`None`) returns the phrase's hits as found; it is skipped rather than sent empty. `search.query` is the one field that is *not* optional — see below.
- **All four apply it.** Resources, datasets and events used to accept a filter and drop it on the floor; `python_tests/test_filter_search_bodies.py` carried those as `xfail(strict=True)` and they went green when the gap closed.
- **`search.query` is the only free-text field, and it is required** — 3–140 characters, and nothing else. It used to carry a charset too (`^[\p{IsLatin}\p{Zs}\p{Nd}]+`), which rejected the `._:+=-` that external ids are built from and every non-Latin script with them; it was never a safety control (the phrase is a bound parameter) but a way of guaranteeing the tsquery at least one lexeme, and the empty case is guarded directly now. `name` and `description` used to sit beside it, honoured only by the timeseries search and only one of the three at a time, with `name` matching by *exact equality* under an endpoint documented as full-text. Both are gone from the api: `query` already covers the description column, and the filter's `name` is a case-insensitive pattern list, which is strictly more than the old field could do. `TimeSeriesService::search_by_name`/`search_by_description` went with them — match a name through `filter`.
- **Hits are ranked.** The three node searches order by `ts_rank` and tie-break on id, so the first item is the best match and repeating a request returns the same page rather than a different slice of an equally-scored block. Ranking costs the index's early exit — every match is scored before `limit` applies. `/events/search` is the exception: it is newest-first (`eventTime` descending), not scored.
- **`limit` defaults to 100, caps at 1000** (the *filter* endpoints default to 1000 and cap at 10000 — different numbers, easy to conflate), and `<= 0` falls back to the default rather than returning nothing.

#### Building a dataset hierarchy in a test

`Dataset.connected_data_sets` does not create the hierarchy — create the edge explicitly, and note the direction: the row is stored `from = parent, to = child` even though the relationship is named `BELONGS_TO`, and the closure query descends `rel_start -> rel_end`. Reversing it produces no hierarchy and no error. See `python_tests/filter_fixtures.py`.

## MCP tools (`src/mcp_integration.rs`)

The api publishes its entity surface as **37 MCP tools** (`timeseries_create`, `event_filter`, …)
over Spring AI's *stateless* WebMVC transport at `POST /mcp`, behind the same JWT +
`ROLE_DATAHUB_ACCESS` gate as REST — no MCP-specific bypass. Stateless means no `initialize`
handshake and no session id: each request is a self-contained JSON-RPC call. Tool sources live in the
backend's `datahub-api/src/main/java/ai/intellistream/datahub/api/mcp/tools/`.

```
cargo test mcp_                          # the whole MCP suite (~30s)
cargo test mcp_full_tool_surface         # just the tool sweep (~8s)
```

**These tests are in Rust, not `python_tests/`, on purpose.** The SDK has no MCP client, so the
module carries a private `McpClient` — about a hundred lines of JSON-RPC over the service's own
`http_client`, with the token from `DataHubConfig::get_api_token`. Auth with refresh and TLS against
the OS trust store (`reqwest`'s `rustls-tls-native-roots`) are already wired here and must not be
reimplemented per language: a Python version needed a hand-rolled token exchange plus a CA-bundle
probe, because a venv's certifi does not carry the dev IdP's issuer while the OS store does.

Two transport details are easy to get wrong and cost a confusing failure each:

- **`Accept` must be `application/json, text/event-stream`, byte for byte.** The transport compares
  it with `MediaType.equals`, so offering only `application/json` is a bare 400 with nothing pointing
  at the header.
- **The response must not be double-encoded.** This has regressed to the envelope being written as a
  `String` which Spring then serializes *as JSON*, so the body is a quoted, escaped document and the
  obvious `parse(body)["result"]` yields a string. `unwrap_envelope` **rejects** that rather than
  parsing twice. Accommodating it would leave the suite green against a wire format no conformant MCP
  client can read, so while it is present nearly every test here fails — which is the accurate
  report, because no tool is reachable. `mcp_response_is_a_json_object` is what names the cause; the
  handful that still pass are the ones that never parse an envelope (the auth rejections, the
  malformed-body check).

### Cleanup

Every entity the sweep creates is covered by a `tests::cleanup` guard, armed *before* the create, so a
panicking assertion still tears its data down. Three rules the module follows, each learned from a
stray it actually left behind:

- **Hold a guard for as long as the entity is needed.** The label guard lives in
  `mcp_full_tool_surface`, not in `sweep_reference_data` that creates it. Dropping it when that helper
  returned deleted the label mid-sweep, `resource_create` silently re-created it further down, and the
  re-created one had no guard — one leaked label per run.
- **Arm the guard with every name the entity can take.** `timeseries_update`, `event_update` and
  `resource_update` all rename their subject, and a guard armed only with the original external id
  looks for something that no longer exists.
- **Only `disarm` when the delete actually landed.** `McpClient::quietly` returns whether the tool
  accepted it, so call sites disarm conditionally. An unconditional `disarm` after a best-effort
  delete is how the secondary dataset kept surviving: `dataset_delete` does not cascade, it failed
  while a resource still pointed at it, and the guard that would have caught that was already off.

### Every tool is driven once; every parameter is not

`mcp_full_tool_surface` calls all 37 tools and reads each write back. Each `sweep_*` helper owns its
entities and removes them through the MCP delete tools, which is also how those get exercised. It is
one sequential test rather than one per entity group because the helpers share reference data — a
dataset, a label, a relationship type — and a relationship type cannot be deleted once created, so
minting a set per test would grow the tenant's catalogue on every run.

It used to also **audit its own field coverage**: `try_call_tool` recorded every `(tool, field)` pair
sent and the test ended by diffing that against the live `tools/list` schema, so a parameter added
server-side failed the run until something drove it. That was removed. It made an additive api change
fail a test that had nothing to say about whether the new field works, and the number it enforced
(125 fields across 37 tools) moved with the api rather than with the SDK. Consequence worth knowing:
nothing here notices a server-side parameter addition any more — same as on the REST side.

Behaviours worth knowing, each pinned by an assertion:

- **Unknown vocabulary is created on the fly, not rejected.** `edge_create` creates a missing
  `relationshipType` (its description claims the type "must already exist" — that sentence is wrong),
  and `resource_create` does the same for a missing entry in `labels`. Intended, and convenient, but
  a typo becomes a permanent catalogue entry rather than an error.
- **Labels are reclaimable; relationship types are not.** `cleanup_labels` deletes by name through
  `/labels/delete` and works. Relationship types have **no delete at all** — not in the MCP surface,
  not in `EdgesService` — so every run that creates one leaves it behind for good, and the tenant's
  catalogue only grows. That is the one stray the suite cannot clean up after itself.
- **Delete order in the graph is not free.** `resource_delete` and `edge_delete` refuse to strand a
  node (see the `edges` notes above), so the sweep builds a triangle and drops `b -> c` — the one edge
  whose endpoints both stay reachable — then deletes `b`, then `c`, then `a`. Getting this order wrong
  surfaces as a 400 naming the stranded resource, in teardown, after the assertions passed.
- **Reads lag writes.** Events and datapoints (ClickHouse), the graph (Neo4j) and the search indexes
  all settle after the call returns, so poll with `tests::polling::poll_until`. A renamed event can
  surface under its new `externalId` *before* the rest of the update propagates, so poll on the field
  under test, not on mere existence.
- **`event_search` omits `items` entirely on a miss** rather than returning an empty list; `items()`
  in the module is tolerant for that reason.
- **`event_filter` accepts a `source` that `event_create` cannot set**, so a source is filterable but
  never settable through MCP. A genuine gap in the tool surface.
- **Ids come back as strings from most tools but as JSON numbers from `label_create` and
  `edge_create_type`** — hence `id_of`, which accepts either.
- **`timeseries_create` takes `unit` (free text) or `unitExternalId` (a catalogue entry), and needs
  one of them.** Supplying only `unitExternalId` fills `unit` in from the catalogue
  (`pressure_bar` -> `bar`); supplying both keeps the free-text `unit` as given and does not reconcile
  it against the referenced unit, so a caller can end up with `unit: "Celsius"` on
  `unitExternalId: "pressure_bar"`. Take the id from `unit_list` in tests — one that is merely absent
  from the tenant fails as "Unknown unit externalId", which is a different path from supplying neither.

### Tests that are red on purpose

Two encode intended behaviour the api does not yet provide, in the same spirit as
`test_duplicate_relationship_type_conflicts`: they stay red until the server-side fix lands rather
than being softened to match the bug.

- `mcp_event_update_by_uuid_reindexes_the_external_id` — renaming an event identified by **UUID**
  writes the new `externalId` but never reindexes it. The update returns the new value, yet the event
  stays reachable under the *old* externalId and never under the new one, while `event_get` by UUID
  reports the new one — two identifiers disagreeing about one row. The same update by `externalId`
  reindexes within about half a second.
- `mcp_response_is_a_json_object` — whenever the double-encoding regression above is present, along
  with every other test that parses an envelope. Both directions are the same underlying fault: the
  transport moves the JSON-RPC payload as a `String` and lets content negotiation's JSON converter
  handle it. Reading, the converter is asked to bind an object *into* a `String` and refuses — a
  **500 on the spec-mandated `Content-Type: application/json`**, which locks out every off-the-shelf
  client, guarded by `mcp_accepts_application_json`. Writing, it is handed a `String` to emit *as*
  `application/json` and escapes it. Note the api's own MockMvc test (`McpEndpointTest`) asserts only
  on the security gate, so neither direction was covered there.

## Python bindings (`datahub_python_bindings/`)

A PyO3 crate (built with maturin) that wraps this SDK as the Python package `intellistream-datahub-sdk` (import name `intellistream_datahub_sdk`). Binding modules in `datahub_python_bindings/src/` mirror the Rust subservices; the pure-Python side lives in `datahub_python_bindings/python/intellistream_datahub_sdk`.

The Python test suite in `python_tests/` imports the **compiled** `intellistream_datahub_sdk` module, not the Rust sources — a stale `.so` silently masks source changes. Always run it through `./run_python_tests.sh`, which rebuilds via `maturin develop` first. Extra args are forwarded to pytest (`./run_python_tests.sh -k timeseries`); `--release`, `--no-build`, and `--no-deps` are consumed by the script itself.

Every entity a test creates carries `TEST_PREFIX` — `pytest_` in Python, `rust_sdk_` in Rust — and
`python_tests/conftest.py` sweeps that prefix off the backend at session start and end, which is
what catches a run killed before its fixtures could tear down. Nodes go through `/resources/filter`,
the generic node query, so resources and data sets are covered along with timeseries and functions;
the sweep skipped those two for a long time and they were, by a wide margin, what accumulated.
Deletes repeat while they make progress — the backend refuses to delete the START of an edge, and a
data set stands above everything that belongs to it — and whatever survives is reported as a
warning rather than swallowed. **Give a new entity a `unique_id()`**: a fixed external id strands on
the first run that dies mid-test and collides on every run after.

**Labels are the exception to that** — tag with the shared `TEST_LABEL` (`"TEST"`). A label is a
dictionary row rather than an entity: the server creates it on first use, so it needs no seeding,
and refuses to delete it while anything still carries it. A unique label per run therefore adds a
row per run *and* cannot be dropped until its resource is, so one stranded resource strands a label
behind it — which is exactly how the label table filled up. Mint a unique name only when the test
owns the definition's lifecycle (create/rename/delete), where a shared row would be pulled out from
under another test, and use a fixed *pair* when a test has to tell two labels apart.

## C bindings (`datahub_c_bindings/`)

A thin FFI crate that builds the SDK as `libintellistream_datahub` (cdylib + staticlib) with a
cbindgen-generated header at `datahub_c_bindings/include/intellistream_datahub.h`. `docs/c-sdk-design.md`
records why it exists and what is deliberately not in it. Things to know when touching it:

- **Every export is written out by hand.** cbindgen does not expand `macro_rules!`, so a
  macro-generated `extern "C"` function ends up in the library but not in the header — and a C
  caller cannot see it. Share bodies through private helper functions instead.
- **The header is generated by `build.rs` and committed.** `cargo build` in the crate rewrites it;
  CI runs `./run_c_tests.sh --check-header`, which fails on a diff. Commit the regenerated header
  with the change that caused it. The version macros and `DATAHUB_TIME_UNSET` are added by
  `build.rs`, not by cbindgen.
- **Every export runs inside `error::guard`** (`catch_unwind`): a panic becomes `DATAHUB_PANIC`
  with the message in the thread-local `datahub_last_error()`. Never let a panic reach C.
- **Runtime model:** a `datahub_client` owns a Tokio runtime and `block_on`s the async
  `ApiService` directly — not the blocking client, which has no subscriptions. A listener shares
  the runtime (`Arc`), so client and listener may be freed in either order.
- **Typed on the datapoint hot path, JSON everywhere else.** A `..._json` function takes exactly
  the REST request body and returns exactly the response body (`items` plus `nextCursor`, which
  `DataWrapper` itself skips when serializing); `datahub_request_json` is the raw authenticated
  escape hatch for any endpoint without a dedicated function.
- **Core switches it depends on:** `http::set_debug_output(false)` (the core's stdout/stderr
  tracing, on by default for Rust and Python users), `DataHubConfig::from_map` (one config path
  for env, env file and typed setters; the C layer never reads `.env` from the host's cwd), and
  `TimeSeriesService::flush_buffer` / `EventsService::flush_buffer` behind `datahub_client_flush`.
- **Tests:** unit tests in the crate; `tests/offline.rs` (boundary and spool paths, no network);
  `tests/mock_api.rs` (a tiny HTTP mock asserting what goes on the wire); `tests/live.rs` (skips
  without a `BASE_URL`); `tests/c/smoke.c`, compiled and run by `run_c_tests.sh`. The spool's
  retention window is measured on each record's own timestamp, so a test that expects a record
  to survive in the spool must stamp it with a recent time.
- The crate's version is locked to the other three manifests by the CI `versions` job.

## Conventions

- `#[serde(rename = "camelCase")]` or explicit `#[serde(rename = "...")]` on fields — the backend is camelCase, Rust is snake_case.
- **A request body naming a field the api does not have is a 400.** Jackson used to drop unknown properties, so a stale or misspelled key was answered with 200 and no effect; a strict converter now rejects the body and names every offender alongside the fields the endpoint accepts. Two consequences for this SDK: a struct that doubles as request *and* response must `#[serde(skip_serializing)]` its response-only fields — `GraphDataWrapper`'s `errorBody`/`httpStatusCode` reached `/resources/create` and made every resource and function create and update a 400 — and one Rust type may not stand in for two endpoints that disagree on their fields (see the search forms above). Reading is unaffected: responses stay lenient in both directions.
- `externalId` (string, user-supplied) and numeric `id` are both valid identifiers across the API. `IdAndExtId` / `IdAndExtIdCollection` model this choice.
- `process_response` (`src/http.rs`) prints response bodies to stdout (truncated to 2000 chars). This is deliberate for debugging — don't silently remove it. It and the other request-path prints go through the `debug_println!`/`debug_eprintln!` macros, gated by `http::set_debug_output` (on by default; the C bindings turn it off, since a library must not write to streams it does not own).
- Tests that depend on backend state being empty are brittle; recent fixes moved away from exact-count assertions (see commit `7f0a059`). Don't add new ones.
