# API coverage audit

Endpoint-by-endpoint comparison of the DataHub REST API against this SDK.

Baselines:
- Backend: `datahub-platform` **`origin/master`** @ `2fd92d2c`, all `@RestController` mappings under
  `datahub-api/.../api/controllers/` plus the handlers registered in `api/websocket/WebSocketConfig.java`.
- SDK: this branch, merged up to `origin/main` @ `5632fbb`.

`/stream/**` is deliberately out of scope and not listed.

The main table reflects `origin/master` only. Unpushed backend work that will change it is in
[In-flight backend work](#in-flight-backend-work) at the end — read that before acting on this.

## Summary

| Controller | Endpoints | Covered |
|---|---|---|
| `/datasets` | 7 | 3 |
| `/edges` | 5 | 0 |
| `/events` | 16 | 8 |
| `/files` | 9 | 9 |
| `/functions` | 3 | 3 |
| `/governance` | 2 | 0 |
| `/labels` | 5 | 5 |
| `/policies` | 7 | 0 |
| `/resources` | 9 | 6 |
| `/stats` | 1 | 0 |
| `/subscriptions` | 3 | 3 |
| `/tenant` | 1 | 0 |
| `/timeseries` | 12 | 11 |
| `/units` | 3 | 3 |
| WebSocket | 2 | 1 |
| **Total** | **85** | **52** |

## Broken, not merely missing

These are worse than a gap — the SDK exposes a method that cannot work.

| SDK | Problem |
|---|---|
| `datasets.filter()` | POSTs `/datasets/filter`. No such route. The backend's structured-list endpoint is `POST /datasets/list`. The body the SDK already sends (`DatasetFilter` → `{filter, cursor, limit}`) matches the server's `DataSetRetreiver` exactly — only the path is wrong. |
| `datasets.search()` | Also POSTs `/datasets/filter`, not `/datasets/search`, so search is unreachable even though the backend implements it. |
| `datasets.list()` | `todo!()` — panics. |
| `datasets.update()` | `todo!()` — panics, while `POST /datasets/update` exists. |
| `datasets.policies()` | `todo!()` — panics, while `GET /datasets/policies` exists. |

`blocking.rs:242` already notes the dataset stubs, so the blocking mirror inherits the same holes.
These five are the only remaining stubs — the event stubs (`retrieve`/`search`/`update`) were
replaced with real calls in `add-event-endpoints` (#60).

## Entirely uncovered controllers

| Route | Purpose |
|---|---|
| `GET /edges/{id}` | Fetch one relationship edge |
| `POST /edges/byids` | Batch-fetch edges |
| `GET /edges/types` | List relationship types |
| `POST /edges/types/create` | Create relationship types |
| `POST\|DELETE /edges/delete` | Delete edges |
| `GET /policies` | List policies |
| `GET /policies/types` | List policy types |
| `POST /policies/create` | Create policy |
| `POST /policies/update` | Update policy |
| `POST\|DELETE /policies/delete` | Delete policies |
| `GET /policies/{policyNodeId}` | Get policy by id |
| `POST /policies/naming/check` | Validate a name against naming policy |
| `GET /governance/templates` | List governance templates |
| `GET /governance/templates/{templateId}` | Get template by id |
| `GET /stats` | Instance stats (optional `keys` param) |
| `GET /tenant/features` | Tenant feature flags |

`src/relations/` holds the edge *DTOs* (`EdgeProxy`, `RelForm`) used when creating resources and
reading `fetch-related` results, but there is no service that talks to `/edges` directly — edges can
only be created as a side effect of `resources.create()`/`update()`, never listed, fetched, or
deleted on their own. Relationship *types* cannot be created or enumerated at all.

## Partial gaps

### `/events` — 8 of 16

Covered: `byids`, `filter`, `search`, `create`, `update`, `delete`, `GET /{id}`, `count`.

Missing: the eight metadata-enumeration routes — `GET /list/{types,sub-types,statuses,sources}`
(distinct values) and `GET /search/{type,sub-type,status,source}` (typeahead over the same
vocabularies). All take a `limit` param, default 1000, capped at 10000.

### `/resources` — 6 of 9

Covered: `byids`, `search`, `fetch-related`, `create`, `update`, `delete`.

Missing: `GET /resources/{id}` (single fetch by numeric id), `POST /resources/filter` (structured
filtering — note the SDK ships `AdvancedFilter` machinery in `src/filters.rs` with no resource
endpoint wired to it), `POST /resources/fetch-nearest`.

### `/timeseries` — 11 of 12

Only `GET /timeseries/recommend-value-type/{unitExternalId}` is missing (returns a
`ValueTypeRecommendation` for a unit).

`POST /timeseries/filter` **is** covered and correct: `TimeSeriesFilterForm` / `TimeSeriesFilter`
serialize to exactly the server's `TimeseriesRetreiver` / `TimeseriesFilter`
(`{limit, filter:{dataSetId, unit, unitExternalId, metadataKey, metadataValue}}`).

### WebSocket

| Route | Status |
|---|---|
| `/timeseries/datapoints/subscription/listen/**` | ✅ `subscriptions.listen()` |
| `/timeseries/datapoints/listen` | ❌ live, cursorless datapoint stream — no SDK support |

The second handler is a separate, non-durable live feed (`DatapointListenWebSocketHandler`); it is
`permitAll` at the security-config level and validates its token internally.

## Fully covered

`/files` (9/9), `/units` (3/3), `/labels` (5/5), `/functions` (3/3), `/subscriptions` (3/3).

`/files` was completed in this branch — `get_by_id`, `get_by_external_id`, `search`, `list_trash`,
`restore`, `update`, `download` and `download_to_path` were added alongside the existing upload,
list and delete, and mirrored into the blocking client and the Python bindings. Three things worth
knowing, found while testing against a live backend:

- **`delete` returned the wrong type.** `FileService::delete` was declared
  `Result<DataWrapper<Event>, _>` — a copy-paste from the events service. It is now
  `DataWrapper<INode>`. The endpoint answers 204 with no body, so nothing deserialized either way,
  but the signature was a lie. This is a breaking change for anyone who named the type.
- **Downloads need an `Accept` override.** The shared `http_client` sets a default
  `Accept: application/json`, and `/files/download/{id}` only `produces`
  `application/octet-stream` — so Spring returned 406 before the handler ran. The new
  `execute_get_stream_request` helper sends `Accept: */*`.
- **A file's `path` comes back doubled.** `FileTransformer.transform` in the backend sets a FILE's
  DTO path to `node.getPath() + "/" + node.getName()`, but a file's stored path already ends in its
  name — so `/lifecycle/sola.jpg` is returned as `/lifecycle/sola.jpg/sola.jpg`. This affects every
  file endpoint that returns an `IndexNode`, not just the new ones, and is a **backend** bug. The
  tests assert the folder prefix rather than the full path so they survive the fix.
- **Restore only works by numeric id.** `POST /files/restore` hashes a supplied external id through
  `ExternalIds.hash`, which lowercases, while a trashed id is `DELETED_<checksum>_<id>_<epoch>` with
  the stored hash not lowercased — so the lookup misses and the call 404s. Also a **backend** bug;
  restoring by id sidesteps the hash entirely and works.

## Suggested priority

1. **Fix `datasets.filter()` / `datasets.search()`** — they point at a route that doesn't exist, so
   dataset filtering and search are silently unusable today. The bodies are already right; this is a
   two-line path change plus tests.
2. **Replace the three remaining `todo!()` stubs** (`datasets.list/update/policies`) with real calls
   or remove them; a panicking public method is worse than an absent one.
3. ~~`GET /files/download/{id}`~~ — **done**, along with the rest of `/files`.
4. **`/edges` service** — needed for any relationship management that isn't a resource-create side
   effect, and for discovering relationship types.
5. **`/resources/filter`** — the `AdvancedFilter` types already exist; only the call is missing.
6. **`/policies` and `/governance`** — newer surfaces; wire up when the platform needs them.
7. Lower value: events vocabulary enumeration, `/stats`, `/tenant/features`,
   `recommend-value-type`, the live datapoint WebSocket.

## In-flight backend work

Three branches in the primary backend checkout are ahead of `origin/master` and unpushed. Two of
them change the contract this SDK depends on.

### `fix/files-upload-indexnode-and-eventtime-schema` (+1)

No endpoint added or removed, but it changes what `PUT /files` **returns**: today the handler leaks
the raw JPA entity `INode` instead of the `IndexNode` DTO every other file endpoint uses.

This already affects us. `files.upload_file()` deserializes into `crate::generic::INode`, which is
shaped like the server's `IndexNode` (`type`, `parentId`, string id). Against current master the
response instead carries nested `nodeType`/`parent` objects, a **base64** checksum, and a numeric
id — so today upload silently yields `type: None` and `parent_id: None`, and a `checksum` in the
wrong encoding. It does not error: every affected field is `Option`, and `opt_string_id` accepts a
JSON number as well as a string. The pending fix aligns the response with what the SDK already
expects, so no SDK change is needed — but any test asserting on those fields will flip behaviour
when it lands.

The same branch corrects `EventModel.eventTime`'s OpenAPI schema from a (never-emitted) numeric
epoch to `string`/`date-time`. No wire change.

### `fix/events-related-resources` (+2)

Collapses `EventModel`'s parallel `relatedResourceIds: List<Long>` and
`relatedResourceExternalIds: List<String>` into a single `relatedResources: List<IdCollection>`
(`[{"id": 34, "externalId": "sensor_abc"}]`), with the API resolving whichever side is omitted and
always returning both. `EventFilter.relatedResources` already had that shape; this makes the entity
match, and tightens the filter's documented semantics to "related to **all** of these".

The Rust SDK's `Event` still carries the two parallel fields (`get_related_resource_ids()` /
`get_related_resource_external_ids()`). The matching SDK-side work is already underway on the
`feature/event-related-resources-single-field` branch in the sibling worktree.

### `bugfix/datasets-in-event-filter-broken` (+3)

The only branch that changes the endpoint set. It adds a `PolicyFindingController` (also mapped
under `/policies`) with two new endpoints and relocates a third:

| Route | Change |
|---|---|
| `GET /policies/findings` | **new** — list policy findings |
| `POST /policies/findings/{findingId}/resolve` | **new** — resolve a finding |
| `POST /policies/naming/check` | moved from `PolicyController` to `PolicyFindingController` |

When this lands `/policies` becomes **9 endpoints, still 0 covered**. The branch also deletes the
console's `ChatApiController` and trims `ResourceController`/`TimeseriesController`, but without
changing any mapping path.

### Reference

The backend carries an uncommitted `datahub-api-model/WIRE_FORMAT_AUDIT.md` cataloguing wire-contract
inconsistencies (the two fixes above are its B1 and B2), plus structural issues left unfixed. Worth
reading before making assumptions about envelope or field-level shapes.
