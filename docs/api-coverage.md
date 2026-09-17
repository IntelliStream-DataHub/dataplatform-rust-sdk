# API coverage audit

Endpoint-by-endpoint comparison of the DataHub REST API against this SDK.

Baselines:
- Backend: `datahub-platform` **`origin/main`** @ `3973bdf0` (GitHub remote; its default branch is
  `main` — the older `intellistream` remote's `master` is a separate line and not an ancestor). All
  `@RestController` mappings under `datahub-api/.../api/controllers/` plus the handlers registered
  in `api/websocket/WebSocketConfig.java`.
- SDK: `origin/main` @ `5b206a9`.

Backend references below are relative to `datahub-api/src/main/java/ai/intellistream/datahub/api/`
and give the line of the mapping annotation. SDK references give the line of the `pub async fn`.

Out of scope, and not counted:
- `POST /mcp` — Spring AI's stateless MCP transport (`datahub-api/src/main/resources/application.yml:9`).
  The SDK does not wrap MCP on purpose; `src/mcp_integration.rs` drives it from tests only.
- `/error` — `controllers/errors/ProblemErrorController.java:33`, the framework error page.
- Controllers in other deployables: `datahub-analysis` (`AnalysisController`),
  `datahub-rvm-converter` (`ConversionController`) and `datahub-console`.
- `/stream/**` — excluded by earlier audits; no such mapping exists in `datahub-api` on `main`.

Coverage is route-level: a method counts as covering an endpoint when it calls that verb and path.
Body shapes are not systematically diffed — the live suites pin those — and the parameter gaps
found in passing are under [Gaps inside covered routes](#gaps-inside-covered-routes).

## Summary

| Controller | Endpoints | Covered |
|---|---|---|
| `/assets` | 8 | 0 |
| `/datasets` | 9 | 8 |
| `/edges` | 6 | 6 |
| `/events` | 17 | 17 |
| `/files` | 9 | 9 |
| `/functions` | 5 | 3 |
| `/governance` | 2 | 0 |
| `/labels` | 5 | 5 |
| `/policies` | 7 | 0 |
| `/resources` (`ResourceController`) | 10 | 10 |
| `/resources` (`GraphTransferController`) | 2 | 0 |
| `/stats` | 1 | 0 |
| `/subscriptions` | 4 | 4 |
| `/tenant` | 1 | 0 |
| `/tenant/settings` | 3 | 0 |
| `/timeseries` | 13 | 11 |
| `/timeseries/data/binary` | 1 | 1 |
| `/units` | 3 | 3 |
| WebSocket | 2 | 1 |
| **Total** | **108** | **78** |

`/files/list` and `/files/list/**` are one mapping and counted once; a `POST|DELETE` mapping is one
endpoint.

## Broken, not merely missing

None. Every route the SDK calls exists on `origin/main` with the verb the SDK uses, and no
`todo!()` or `unimplemented!()` remains outside tests (`src/resources/mod.rs:269` is a comment).

## Entirely uncovered controllers

| Route | Backend | Purpose |
|---|---|---|
| `POST /assets/create` | `controllers/AssetController.java:84` | Create asset |
| `GET /assets/{id}` | `controllers/AssetController.java:113` | One asset by id |
| `POST /assets/byids` | `controllers/AssetController.java:134` | Assets by id or externalId |
| `GET /assets` | `controllers/AssetController.java:174` | List (`limit`) |
| `POST /assets/filter` | `controllers/AssetController.java:210` | Filter |
| `POST /assets/search` | `controllers/AssetController.java:251` | Search |
| `POST /assets/update` | `controllers/AssetController.java:268` | Update |
| `POST\|DELETE /assets/delete` | `controllers/AssetController.java:312` | Delete |
| `GET /policies` | `controllers/PolicyController.java:89` | List (`limit`) |
| `GET /policies/types` | `controllers/PolicyController.java:108` | Distinct policy types |
| `POST /policies/create` | `controllers/PolicyController.java:158` | Create |
| `POST\|DELETE /policies/delete` | `controllers/PolicyController.java:240` | Delete |
| `GET /policies/{policyNodeId}` | `controllers/PolicyController.java:257` | One policy by id |
| `POST /policies/update` | `controllers/PolicyController.java:281` | Update |
| `POST /policies/naming/check` | `controllers/PolicyController.java:349` | Check external ids against the naming policy |
| `GET /governance/templates` | `controllers/GovernanceController.java:37` | List governance templates |
| `GET /governance/templates/{templateId}` | `controllers/GovernanceController.java:54` | One template |
| `GET /resources/export/{id}` | `controllers/GraphTransferController.java:85` | Export a resource's graph component as a file |
| `POST /resources/import` | `controllers/GraphTransferController.java:162` | Import an exported graph file |
| `GET /stats` | `controllers/StatsController.java:34` | Instance stats (`keys`); `@Hidden` from the OpenAPI |
| `GET /tenant/features` | `controllers/TenantController.java:33` | Tenant feature flags |
| `GET /tenant/settings/permissions` | `controllers/TenantSettingsController.java:55` | What the caller may do with settings |
| `GET /tenant/settings/llm` | `controllers/TenantSettingsController.java:68` | Organization model configuration |
| `PUT /tenant/settings/llm` | `controllers/TenantSettingsController.java:84` | Change it |

No SDK source names any of these prefixes: no service in `src/lib.rs:131-141` is built on
`/assets`, `/policies`, `/governance`, `/stats` or `/tenant`, and `src/resources/mod.rs` builds no
`export`/`import` path.

Assets and policies are still reachable *as nodes*: `resources.create` dispatches an `ASSET` or
`POLICY` node by its label (`src/resources/mod.rs:62`), and every `/resources` read answers them as
`Node::Asset`/`Node::Policy`. What is missing is the type-specific surface — asset update and
geometry, policy types, naming checks. `datasets.policies()` (`src/datasets/mod.rs:163`) reads
`GET /datasets/policies`, not `/policies`.

## Partial gaps

### `/datasets` — 8 of 9

Covered: `GET /datasets`, `byids`, `filter`, `search`, `create`, `update`, `delete`, `policies`.

Missing: `GET /datasets/{id}` (`controllers/DataSetController.java:106`). `src/datasets/mod.rs`
builds no `{base}/{id}` path; `by_ids` (`:86`) is the only lookup.

### `/functions` — 3 of 5

Covered: `create`, `GET /functions`, `delete`.

| Route | Backend | SDK |
|---|---|---|
| `GET /functions/{id}` | `controllers/FunctionController.java:131` | none — `src/functions/mod.rs` builds no `{base}/{id}` path |
| `POST /functions/update` | `controllers/FunctionController.java:148` | none |

`FunctionsService::by_ids` (`src/functions/mod.rs:68`) and `by_external_id` (`:100`) are not
routes: there is no `/functions/byids`, so they filter `list(Some(10_000))` client-side and miss
the oldest functions of a tenant past 10000. `GET /functions/{id}` now makes the by-id half of that
unnecessary.

### `/timeseries` — 11 of 13

Covered: `GET /timeseries`, `byids`, `filter`, `search`, `create`, `update`, `delete`, `data`,
`data/list`, `data/delete`, `data/latest`.

| Route | Backend | SDK |
|---|---|---|
| `GET /timeseries/{id}` | `controllers/TimeseriesController.java:176` | none — `src/timeseries/mod.rs` builds no `{base}/{id}` path |
| `GET /timeseries/recommend-value-type/{unitExternalId}` | `controllers/TimeseriesController.java:208` | none |

### WebSocket — 1 of 2

| Route | Backend | Status |
|---|---|---|
| `/timeseries/datapoints/subscription/listen/**` | `websocket/WebSocketConfig.java:45` | covered — `subscriptions.listen()` (`src/subscriptions/mod.rs:91`, URL built in `src/subscriptions/listen.rs:414`) |
| `/timeseries/datapoints/listen` | `websocket/WebSocketConfig.java:50` | uncovered — nothing in `src/` builds this path |

The second is the live, cursorless tail (`DatapointListenWebSocketHandler`). It is `permitAll` at
the filter chain and takes its token as a `?token=` query parameter on `main` — which an unmerged
branch changes, see [In-flight backend work](#in-flight-backend-work).

### Gaps inside covered routes

Parameters the backend accepts that the SDK method cannot send:

| Route | Backend | SDK |
|---|---|---|
| `GET /timeseries?dataSetId=` | `controllers/TimeseriesController.java:112` | `list(limit)` sends `limit` only (`src/timeseries/mod.rs:68`) |
| `GET /files/search?limit=` | `controllers/FileController.java:543` | `search(query)` sends `q` only (`src/files/mod.rs:96`) |
| `POST /subscriptions/filter` `cursor` | `SubscriptionRetriever.java:60`¹ | `SubscriptionFilterForm` has `filter`/`limit`/`sort` only (`src/subscriptions/mod.rs:160`) |
| `POST /subscriptions/filter` `filter.{id,externalId,name,createdTime,lastUpdatedTime}` | `SubscriptionFilter.java:46-87`¹ | `SubscriptionFilter` has `timeseries` only (`src/subscriptions/mod.rs:142`) |

¹ `datahub-api-model/src/main/java/ai/intellistream/datahub/subscription/`.

And one the SDK can send that the backend does not accept: the subscriptions module's own
`DataSort` carries `nulls` (`src/subscriptions/mod.rs:149`), which the backend's `DataSort` does not
have (`datahub-api-model/.../models/DataSort.java:26-27` — `property`, `order`). It is skipped when
`None`; setting it should be a 400 under the strict body converter (inferred from source, not
observed live).

## Fully covered

`/edges` (6/6), `/events` (17/17), `/files` (9/9), `/labels` (5/5), `/resources` (10/10),
`/subscriptions` (4/4), `/timeseries/data/binary` (1/1), `/units` (3/3).

| Route | Backend | SDK |
|---|---|---|
| `GET /edges/{id}` | `controllers/EdgeController.java:82` | `edges.get` `src/relations/service.rs:39` |
| `POST /edges/byids` | `:115` | `edges.by_ids` `:53` |
| `POST /edges/create` | `:189` | `edges.create` `:79` |
| `GET /edges/types` | `:235` | `edges.types` `:128` |
| `POST /edges/types/create` | `:269` | `edges.create_types` `:149` |
| `POST\|DELETE /edges/delete` | `:327` | `edges.delete` `:119` |
| `GET /events/{id}` | `controllers/EventController.java:81` | `events.get` `src/events/mod.rs:210` |
| `POST /events/byids` | `:104` | `events.by_ids` `:199` |
| `GET /events` | `:160` | `events.list` `:188` |
| `POST /events/filter` | `:248` | `events.filter` `:194` |
| `POST /events/search` | `:304` | `events.search` `:233` |
| `POST /events/create` | `:371` | `events.create` `:37` |
| `POST /events/update` | `:452` | `events.update` `:221` |
| `POST\|DELETE /events/delete` | `:506` | `events.delete` `:163` |
| `GET /events/count` | `:550` | `events.count` `:244` |
| `GET /events/list/{types,sub-types,statuses,sources}` | `:578`, `:598`, `:618`, `:638` | `events.list_dimension` `:263` (`list_types` `:290`, `list_sub_types` `:308`, `list_statuses` `:327`, `list_sources` `:346`) |
| `GET /events/search/{type,sub-type,status,source}` | `:660`, `:682`, `:704`, `:726` | `events.list_dimension` `:263` (`search_types` `:298`, `search_sub_types` `:317`, `search_statuses` `:336`, `search_sources` `:355`) |
| `PUT /files` | `controllers/FileController.java:185` | `files.upload_file` `src/files/mod.rs:32` |
| `GET /files/list`, `/files/list/**` | `:429` | `files.list_root_directory` `:44`, `list_directory_by_path` `:51` |
| `GET /files?id=\|externalId=` | `:482` | `files.get_by_id` `:73`, `get_by_external_id` `:81` |
| `GET /files/search` | `:535` | `files.search` `:96` |
| `GET /files/download/{id}` | `:594` | `files.download` `:147`, `download_to_path` `:174` |
| `POST /files/delete` | `:806` | `files.delete` `:60` |
| `GET /files/trash` | `:863` | `files.list_trash` `:108` |
| `POST /files/restore` | `:908` | `files.restore` `:125` |
| `POST /files/update` | `:991` | `files.update` `:138` |
| `GET /labels/{id}` | `controllers/LabelController.java:69` | `labels.get` `src/labels/mod.rs:37` |
| `GET /labels` | `:90` | `labels.list` `:32` |
| `POST /labels/create` | `:120` | `labels.create` `:44` |
| `POST /labels/update` | `:156` | `labels.update` `:54` |
| `POST\|DELETE /labels/delete` | `:203` | `labels.delete` `:65` |
| `GET /resources/{id}` | `controllers/ResourceController.java:100` | `resources.get_by_id` `src/resources/mod.rs:153` |
| `POST /resources/fetch-related` | `:140` | `resources.fetch_related` `:116` |
| `POST /resources/fetch-nearest` | `:177` | `resources.fetch_nearest` `:201` |
| `POST /resources/byids` | `:199` | `resources.by_ids` `:79` |
| `GET /resources` | `:253` | `resources.list` `:174` |
| `POST /resources/filter` | `:338` | `resources.filter` `:185` |
| `POST /resources/search` | `:413` | `resources.search` `:99` |
| `POST /resources/create` | `:542` | `resources.create` `:62` |
| `POST /resources/update` | `:682` | `resources.update` `:135` |
| `POST\|DELETE /resources/delete` | `:773` | `resources.delete` `:89` |
| `POST /subscriptions/create` | `controllers/SubscriptionController.java:81` | `subscriptions.create` `src/subscriptions/mod.rs:33` |
| `POST /subscriptions/filter` | `:156` | `subscriptions.filter` `:69` |
| `GET /subscriptions` | `:237` | `subscriptions.list` `:49` |
| `POST\|DELETE /subscriptions/delete` | `:304` | `subscriptions.delete` `:78` |
| `POST /timeseries/data/binary` | `controllers/DatapointBinaryController.java:87` | `time_series.insert_datapoints_binary` `src/timeseries/binary.rs:768` (path built at `:821`, content type `application/vnd.intellistream.datapoint-block` at `:31`; resolves series through `POST /timeseries/byids` at `:864`) |
| `GET /units` | `controllers/UnitController.java:50` | `units.list` `src/unit/mod.rs:25` |
| `GET /units/{externalId}` | `:67` | `units.by_external_id` `:29` |
| `POST /units/byids` | `:86` | `units.by_ids` `:34` |

Covered routes on partially covered controllers:

| Route | Backend | SDK |
|---|---|---|
| `GET /datasets` | `controllers/DataSetController.java:187` | `datasets.list` `src/datasets/mod.rs:67` |
| `POST /datasets/byids` | `:129` | `datasets.by_ids` `:86` |
| `POST /datasets/filter` | `:251` | `datasets.filter` `:78` |
| `POST /datasets/create` | `:326` | `datasets.create` `:37` |
| `POST /datasets/update` | `:409` | `datasets.update` `:142` |
| `POST\|DELETE /datasets/delete` | `:484` | `datasets.delete` `:47` |
| `POST /datasets/search` | `:565` | `datasets.search` `:113` |
| `GET /datasets/policies` | `:606` | `datasets.policies` `:163` |
| `POST /functions/create` | `controllers/FunctionController.java:70` | `functions.create` `src/functions/mod.rs:36` |
| `GET /functions` | `:106` | `functions.list` `:54` |
| `POST\|DELETE /functions/delete` | `:195` | `functions.delete` `:114` |
| `GET /timeseries` | `controllers/TimeseriesController.java:107` | `time_series.list` `src/timeseries/mod.rs:68` |
| `POST /timeseries/byids` | `:236` | `time_series.by_ids` `:127` |
| `POST /timeseries/filter` | `:312` | `time_series.filter` `:153` |
| `POST /timeseries/create` | `:386` | `time_series.create` `:74` (`create_one` `:83`, `create_from_list` `:92`) |
| `POST /timeseries/update` | `:472` | `time_series.update` `:118` |
| `POST /timeseries/search` | `:547` | `time_series.search` `:138` (`search_by_query` `:166`) |
| `POST\|DELETE /timeseries/delete` | `:632` | `time_series.delete` `:110` |
| `POST /timeseries/data` | `:701` | `time_series.insert_datapoints` `:201` (`insert_datapoint` `:173`; path built at `:213` buffered, `:328` unbuffered) |
| `POST /timeseries/data/list` | `:784` | `time_series.retrieve_datapoints` `:427` |
| `POST\|DELETE /timeseries/data/delete` | `:856` | `time_series.delete_datapoints` `:444` |
| `POST /timeseries/data/latest` | `:910` | `time_series.retrieve_latest_datapoint` `:453` |

The blocking client (`src/blocking.rs:90-101`) mirrors every service above except
`subscriptions`, which is async-only.

## Suggested priority

1. **`GET /{id}` reads** — `/timeseries/{id}`, `/datasets/{id}`, `/functions/{id}`. Small, and the
   last makes `FunctionsService::by_ids`'s capped client-side scan avoidable for numeric ids.
2. **`POST /functions/update`** — functions are the one covered node type with no update.
3. **`/assets`** — the type-specific surface for a node type the SDK already reads and creates.
4. **`/policies`, `/governance`** — wire up when the platform needs them.
5. Lower value: `/tenant/features`, `/tenant/settings/*`, `/stats` (hidden from the OpenAPI),
   `recommend-value-type`, graph export/import, the live datapoint WebSocket (wait for its auth
   change below), and the parameter gaps under [Gaps inside covered routes](#gaps-inside-covered-routes).

## In-flight backend work

No unmerged branch on the `main` line — pushed or local — removes or renames a mapping
`origin/main` has. Two pushed branches change the contract of a route listed above; three stale
local branches would add a controller. Branches from the older `intellistream` remote
(`master`, `feat/edges-create`, …) share no merge base with `origin/main` and are not considered.

### `fix/live-tail-token-subprotocol` (pushed, +1, checked out in `dh-wt4`)

Moves the `/timeseries/datapoints/listen` access token out of the `?token=` query parameter and
into `Sec-WebSocket-Protocol`, as `datahub.bearer.<jwt>` offered alongside `datahub.v1`. Breaking
for any client of that socket, and the one the SDK does not yet cover — build against the
subprotocol form rather than the query parameter.

### `node-reads/search-paging` (pushed, +30, 13 not yet on `main` by patch)

Makes `POST /resources/search` page with a `nextCursor` instead of truncating at `limit`, and
refuses a cursor minted by a filter walk. `SearchAndFilterForm` carries no `cursor`, so the SDK
would need one to continue a search. Last touched 2026-09-09.

### `feat/files-download-range` (local, +1)

Byte ranges and conditional reads on `GET /files/download/{id}`. Additive; `files.download` is
unaffected.

### `feat/autonomous-agents`, `feat/external-mcp-tools`, `feat/tenant-agents` (local, unpushed)

Add an `AgentController` at `/agents` (list, `/tools`, and `GET|PUT|DELETE /{externalId}`). Based on
`fd52f4b7` (2026-08-31) and untouched since 2026-09-02. `feat/tenant-agents` also puts
`/permissions` and `/llm` on `TenantController` — superseded on `main` by
`TenantSettingsController` at `/tenant/settings/*`, so treat the whole set as stale until rebased.

### Not endpoint changes

`errors/8-docs-links` (`dh-wt1`), `errors/6-delete-response-error` (`dh-wt3`),
`fix/masked-error-statuses`, `docs/openapi-422-accuracy`, `docs/related-resources-both-ids`,
`refactor/files-verbatim-external-ids` and `refactor/files-deleted-at` edit controllers without
changing a mapping. `chore/consolidate-datasets-list` is already on `main` by patch. `dh-wt2` is
detached at a commit already on `main`; `dh-wt5` (`fix/commons-event-external-id-test`) only moves
a test. All worktrees are clean.
