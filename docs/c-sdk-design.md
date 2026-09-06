# A C SDK, built as an FFI crate over this one

Design note. Nothing in it is implemented; it records what a C SDK for DataHub should be, where
it should live, and what has to be true before anyone starts on it, so that the scope is agreed
once rather than rediscovered mid-build.

Baseline: this repository at `origin/main` @ `79173af` (crate `intellistream-datahub-sdk` 0.3.0).

## The decision

**Build the C SDK as a third crate in this repository — `datahub_c_bindings/` beside
`datahub_python_bindings/` — exposing a small, ingest-first C ABI over the existing Rust core.
Do not start it until a named consumer exists.**

Three questions were asked; the answers, in order:

1. **Does the Rust SDK already cover C?** No. There is no `extern "C"` surface, no `cbindgen`,
   and the only `cdylib` in the tree is the PyO3 crate. A Rust `rlib` is consumable from Rust
   only. Nothing written in C, C++, C#, Go, MATLAB or LabVIEW can touch this SDK today.
2. **Should there be one?** Yes, on condition. The parts of the SDK worth having from C are the
   parts nobody should reimplement in C: token acquisition and refresh across four OAuth2 flows
   (client credentials, refresh token, RFC 7523 `jwt-bearer`, and the secretless federated
   exchange), the durable zstd disk spool that keeps ingest alive through an outage, and the
   WebSocket subscription listener with reconnect. A one-off datapoint push from C can be done
   with libcurl and a JSON library; the SDK earns its keep once any of those three is needed.
3. **Where?** Here. The whole point is *one implementation, several ABIs* — exactly the
   arrangement the Python bindings already prove. A hand-written C client would be a third copy
   of the auth and buffering logic, and would drift.

### Who it is for

A C ABI is the universal bridge, which is the strongest argument for building it once:

| Consumer | How it loads the library |
|---|---|
| Edge gateways and PLC-adjacent daemons in C or C++ | Link `libintellistream_datahub` directly |
| C++ applications | Same header; an RAII wrapper is a few dozen lines the consumer can own |
| .NET | P/Invoke |
| Go | cgo |
| LabVIEW, MATLAB | Call Library Function Node / `loadlibrary` with the header |
| Swift, Zig, Free Pascal, Ada, … | Their native C interop |

Consumers it does **not** target: Java (the platform ships `datahub-java-sdk`), Python (PyO3
is a better fit than going through C), and Node, which should use `napi-rs` directly from Rust
if it is ever wanted rather than a C-to-N-API shim.

It is a **hosted-OS** library — Linux (glibc and musl), Windows, macOS — not a bare-metal one.
reqwest, rustls and Tokio need an operating system; a microcontroller without one is out of
scope and would need a different (much smaller, `no_std`) design that shares nothing with this
SDK beyond the wire format.

### The go/no-go condition

Do not build this "for completeness". The Python bindings are ~8k lines because they mirror
every service; an unscoped C SDK would be the same size again, in the one language where every
line of surface is also a line of ownership and lifetime contract to get wrong, with zero known
callers. Start when one of the following is concrete: a gateway or firmware team that will link
it, a .NET or Go integration, or a LabVIEW/MATLAB deployment. The first consumer also decides
what goes into the first slice beyond the ingest core described below.

## What exists today, and what the C layer builds on

- **The async core** (`src/lib.rs`, `ApiService`) with every service as a field, driven by
  Tokio. The C crate wraps this directly.
- **The blocking client** (`src/blocking.rs`): owns a `tokio::runtime::Runtime` and `block_on`s
  each async call, so there is exactly one implementation of every call. That is the runtime
  model the C layer copies. It does **not** wrap the blocking client itself, for one reason:
  the blocking client deliberately has no `subscriptions` field (`async_api()` is the escape
  hatch for it), and the listener is one of the three things the C SDK exists for. The FFI
  crate therefore holds an `Arc<ApiService>` plus its own runtime and drives the async API the
  way `blocking::ApiService::wrap` does.
- **Durable buffering** (`src/buffer.rs`): `TimeSeriesService::insert_datapoints` drains the
  on-disk backlog first, posts in ≤100k-datapoint chunks, and spools to disk on a transient
  failure; retries are safe because the backend dedups on `(series, timestamp)`. Configured
  through `DataHubConfig::enable_buffering` / `set_buffer_dir` / `set_buffer_retention_secs` /
  `set_buffer_max_bytes` or the `ENABLE_BUFFERING` / `BUFFER_*` variables. This is the feature
  an edge device with flaky connectivity is buying.
- **TLS is already rustls** (`rustls-tls-native-roots` on both reqwest and tokio-tungstenite).
  That matters more for a C library than for the crate: a `cdylib` loaded into an arbitrary host
  process must not dynamically link a `libssl` whose version the host also has opinions about.
  Keep it that way. `native-roots` reads the OS trust store, which a stripped embedded image may
  not have; the library must document `SSL_CERT_FILE` / `SSL_CERT_DIR` as the fix.
- **Auth diagnostics** (`src/auth_diagnostics.rs`) reconstructs the reason for an otherwise
  blank 401 from the token's `organization` claim. The C layer should surface that text through
  its error string; it is the single most common support question and C callers have even less
  to go on than Rust ones.

### Two things in the core that must change first

Both are small, both are core changes rather than FFI-crate workarounds, and both are
blockers for shipping a shared library:

1. **`process_response` prints response bodies to stdout** (`src/http.rs`). That is a
   documented, deliberate debugging aid in a Rust crate a developer runs on purpose. Inside a
   `cdylib` loaded into someone's gateway daemon it is a library writing to a stream it does not
   own. It has to become opt-in — a config flag or cargo feature, default off for the C crate —
   before the first release. AGENTS.md says not to silently remove it; this note is the
   non-silent request to gate it.
2. **`create_api_service()` reads `.env` from the current directory** via `dotenv`. A shared
   library picking up a dotfile from the *host process's* working directory is a surprise the
   host did not ask for. The C constructor that reads configuration from the environment uses
   `DataHubConfig::from_env()` (process environment only); loading a file is a separate,
   explicit `datahub_config_load_envfile(path)` built on `from_envfile`.

Also worth noting, not a blocker: `insert_datapoint` panics when given neither an id nor an
external id. The FFI validates arguments before calling into the core, and `catch_unwind` at
the boundary is the backstop, never the plan.

## Shape of the crate

```
datahub_c_bindings/
  Cargo.toml          package datahub_c_bindings, publish = false, version locked to the others
  cbindgen.toml
  build.rs            runs cbindgen; CI fails if the committed header is stale
  include/
    intellistream_datahub.h     generated, committed — C users must not need cbindgen
  src/
    lib.rs            handle types, runtime, error state, catch_unwind wrapper
    config.rs         datahub_config_*
    client.rs         datahub_client_*
    datapoints.rs     datahub_datapoints_*
    json.rs           the JSON pass-through functions
    listener.rs       datahub_listener_* / datahub_message_*
  tests/
    c/smoke.c         compiled and run in CI against the built library, no backend needed
```

```toml
[lib]
name = "intellistream_datahub"          # libintellistream_datahub.{so,dylib,a}, intellistream_datahub.dll
crate-type = ["cdylib", "staticlib"]
```

Symbol prefix `datahub_`, types `datahub_client`, `datahub_config`, `datahub_listener`,
`datahub_message`, `datahub_datapoint`, status enum `datahub_status`. Every handle is an opaque
pointer to a Rust `Box`; C never sees a Rust struct layout except the two `#[repr(C)]` datapoint
structs below.

**Workspace.** This will be the third crate with its own `Cargo.toml` and its own target
directory. Landing it is the moment to make the repository a Cargo workspace (root `[workspace]
members = ["datahub_python_bindings", "datahub_c_bindings"]`) so there is one lockfile, one
target dir, and one `cargo build --workspace` in CI. The root package already `exclude`s the
binding directories from the published crate, so `cargo publish` is unaffected. Separable from
the C work; recommended alongside it.

## The ABI

### Runtime and threading

- A `datahub_client` owns one multi-thread Tokio runtime, created in `datahub_client_new`,
  dropped in `datahub_client_free`. Every call is `runtime.block_on(...)`, exactly as
  `blocking.rs` does. The calling C thread blocks for the duration of the call.
- `datahub_client` is `Send + Sync`: any number of C threads may call it concurrently. The
  client must be freed after every listener created from it.
- `datahub_listener` and `datahub_message` are single-owner: one thread at a time.
- Nothing may be called from inside a Tokio runtime thread. That is only possible if the host
  is itself Rust, in which case it should use the crate, not the C ABI.

### Errors

Every function that can fail returns a `datahub_status`:

```c
typedef enum datahub_status {
    DATAHUB_OK = 0,
    DATAHUB_BUFFERED,        /* accepted into the disk spool; will be sent on a later call */
    DATAHUB_TIMEOUT,         /* datahub_listener_next: nothing arrived within the timeout  */
    DATAHUB_CLOSED,          /* datahub_listener_next: the stream has ended               */
    DATAHUB_INVALID_ARGUMENT,/* NULL where a value was required, bad UTF-8, empty id, …   */
    DATAHUB_CONFIG,          /* DataHubError: missing BASE_URL, incomplete credential set */
    DATAHUB_AUTH,            /* token acquisition failed, or 401/403 from the api         */
    DATAHUB_HTTP,            /* any other non-2xx; see datahub_last_http_status()         */
    DATAHUB_IO,              /* spool directory unwritable, TLS setup, DNS, socket        */
    DATAHUB_PANIC,           /* a Rust panic was caught at the boundary; report it        */
} datahub_status;
```

`DATAHUB_BUFFERED` is the important non-error. It is the answer an edge device wants when the
network is down: the datapoints are safe on disk and the call returns immediately. Callers that
must know whether data has *reached* the server check for `DATAHUB_OK` specifically.

Detail travels out of band, thread-locally, so the enum stays small and the message can be as
long as it likes:

```c
const char *datahub_last_error(void);        /* borrowed; valid until the next failing call on this thread */
int         datahub_last_http_status(void);  /* 0 when the last failure was not an HTTP response */
```

The message is the `ResponseError`/`DataHubError` text, with the `auth_diagnostics` explanation
appended for a 401. Every failing call overwrites it; a successful call leaves it alone.

Every exported function body runs inside `std::panic::catch_unwind`. A panic becomes
`DATAHUB_PANIC` with the panic message as the last error. Unwinding into C is undefined
behaviour and is never allowed to happen, whatever the core does.

### Memory

- Strings cross the boundary as NUL-terminated UTF-8. Input strings are borrowed for the
  duration of the call only; the library copies what it keeps.
- Every pointer the library hands out has a matching free: `datahub_string_free`,
  `datahub_config_free`, `datahub_client_free`, `datahub_listener_close`,
  `datahub_message_free`. C `free()` on a Rust allocation is undefined behaviour; the header
  says so next to every out-parameter.
- Arrays the library returns come with their length in an out-parameter and are freed as a unit.
- The datapoint structs are plain C values with no hidden ownership:

```c
typedef struct datahub_datapoint {
    int64_t timestamp_ms;   /* Unix epoch milliseconds, UTC */
    double  value;
} datahub_datapoint;

/* Read side. Absent aggregates are NaN, so a plain read never needs a presence flag. */
typedef struct datahub_datapoint_agg {
    int64_t timestamp_ms;
    double  value, min, max, average, sum;
} datahub_datapoint_agg;
```

### Typed on the hot path, JSON everywhere else

The datapoint path is typed — arrays of the structs above — because it is the path a gateway
calls a thousand times a second and the one where a C caller should never touch a JSON library.
Everything else crosses the boundary as **JSON text**: the same request body the REST endpoint
accepts and the same response body it returns.

```c
datahub_status datahub_events_create_json(datahub_client *c, const char *request_json, char **response_json);
```

This is not a raw pass-through. The body is deserialized into the SDK's typed structs and sent
through the same service method Rust and Python callers use — so the durable buffer, chunking
and auth apply, and a body whose fields have the wrong type is rejected before any request is made —
then the typed response is serialized back. The point is that the *C surface* stays small and
stable while the SDK's own types can keep changing underneath it, and the REST API reference
doubles as the documentation for every JSON function. A C++ caller uses whatever JSON library
it already has; a C caller on the hot path never needs one.

### Version 1 surface

Ingest-first: what a device that produces datapoints and events, and reacts to subscription
messages, needs. Nothing else.

| Group | Functions |
|---|---|
| Version | `datahub_version()` returning the crate version string; `DATAHUB_VERSION_{MAJOR,MINOR,PATCH}` macros in the header |
| Config | `datahub_config_new` / `_free`; `_from_env` (process environment only); `_load_envfile(path)`; setters for base URL, bearer token, client credentials (id, secret, token URI), scope, audience, the assertion set, and the three buffer bounds plus `_enable_buffering` |
| Client | `datahub_client_new(config, &client)`, `datahub_client_free`; `datahub_client_flush` to drain the spool on demand (before a controlled shutdown), `datahub_client_spool_bytes` |
| Time series | `datahub_timeseries_get_by_external_id` → id, name, unit, value type; `datahub_timeseries_create_json`; `datahub_timeseries_search_json` |
| Datapoints | `datahub_datapoints_insert(client, external_id, points, n)`; `datahub_datapoints_insert_str` for string-valued series; `datahub_datapoints_latest`; `datahub_datapoints_retrieve(client, external_id, start_ms, end_ms, limit, &out, &n)` (raw); aggregated reads via `_retrieve_json` |
| Events | `datahub_events_create_json`, `datahub_events_filter_json` |
| Subscriptions | `datahub_listener_open(client, ids, n, &l)`; `datahub_listener_next(l, timeout_ms, &msg)`; `datahub_listener_ack` / `_nack(l, ids, n)`; `datahub_listener_subscribe` / `_unsubscribe`; `datahub_listener_close` |
| Messages | `datahub_message_id`, `datahub_message_json` (borrowed, valid until `_free`), `datahub_message_datapoints(msg, &points, &n)` for the typed fast path when the message is a datapoint batch; `datahub_message_free` |

`datahub_client_flush` needs a public "drain now" method on the core's timeseries and events
services; today draining only happens as a side effect of the next insert. Small addition, made
in the core, not the FFI crate — the rule throughout is that anything the C layer needs and the
core lacks is added to the core, so Rust and Python callers get it too.

The listener is **pull-based** with a timeout, mirroring `SubscriptionListener::next`, rather
than callback-based. A callback API has to define which thread the callback runs on, what the
callback may call, and what happens if it blocks; a `next(timeout)` loop defines none of that
and is what every C event loop already knows how to integrate. A callback variant can be added
later on top of the same handle if a consumer needs it.

### Deliberately not in version 1

Resources and the graph, datasets, files, labels, functions, edges, policies, units. Each is a
JSON function pair away when a consumer asks — that is what the JSON convention buys — but not
before. Also out: a C++ header, async/callback APIs, and any registry packaging (vcpkg, Conan);
the library ships as release tarballs, below.

## Illustration

```c
#include <intellistream_datahub.h>

datahub_config *cfg = datahub_config_new();
datahub_config_set_base_url(cfg, "https://datahub.example.com");
datahub_config_set_client_credentials(cfg, client_id, client_secret, token_uri);
datahub_config_set_scope(cfg, "organization:*");
datahub_config_set_buffer_dir(cfg, "/var/lib/gateway/datahub-spool");   /* also enables buffering */

datahub_client *client = NULL;
if (datahub_client_new(cfg, &client) != DATAHUB_OK) {
    fprintf(stderr, "datahub: %s\n", datahub_last_error());
    datahub_config_free(cfg);
    return 1;
}
datahub_config_free(cfg);                 /* the client copied what it needs */

datahub_datapoint points[] = { { .timestamp_ms = now_ms(), .value = 21.5 } };
switch (datahub_datapoints_insert(client, "pump-1/temperature", points, 1)) {
    case DATAHUB_OK:       break;                                   /* on the server */
    case DATAHUB_BUFFERED: break;                                   /* on disk, will flush */
    default: fprintf(stderr, "datahub: %s\n", datahub_last_error()); /* fix and retry */
}

const char *subs[] = { "pump-1-alarms" };
datahub_listener *l = NULL;
if (datahub_listener_open(client, subs, 1, &l) == DATAHUB_OK) {
    datahub_message *msg = NULL;
    for (;;) {
        datahub_status st = datahub_listener_next(l, 5000, &msg);
        if (st == DATAHUB_TIMEOUT) continue;            /* idle; check a stop flag here */
        if (st != DATAHUB_OK) break;                    /* DATAHUB_CLOSED or an error */
        handle(datahub_message_json(msg));
        const char *id = datahub_message_id(msg);
        datahub_listener_ack(l, &id, 1);
        datahub_message_free(msg);
    }
    datahub_listener_close(l);
}

datahub_client_flush(client);             /* push any spooled backlog before exit */
datahub_client_free(client);
```

## Build, test, release

**Header.** `cbindgen` generates `include/intellistream_datahub.h` from the `#[no_mangle]
extern "C"` items and `#[repr(C)]` types. The generated file is **committed**, so a C user
clones or downloads and has a header; CI regenerates it and fails on a diff, the same way a
stale `.so` is treated on the Python side.

**Tests.** Three layers, cheapest first:

1. Rust unit tests in the crate for the boundary itself: NULL handling, invalid UTF-8, the
   panic-to-status conversion, thread-local error state, every `_free` on every path.
2. `tests/c/smoke.c`, compiled with the system C compiler against `target/<profile>` and run in
   CI **without a backend**: `datahub_version`, config construction and its error cases, and the
   offline buffering path — point the client at an unreachable base URL with a temp spool
   directory, insert, assert `DATAHUB_BUFFERED` and that a spool segment exists. That exercises
   the most important behaviour of the whole library with no network.
3. Live-backend tests following the Rust suite's `.env` convention and `rust_sdk_`-style
   prefixing, skipped when `.env` is absent, as `multi_tenant_integration.rs` does.

**Versions.** The CI `versions` job compares three manifests today; the C crate's
`Cargo.toml` becomes the fourth. A `vX.Y.Z` tag releases all four together, so the C library
version always names the crate version it was built from.

**Artifacts.** The C SDK is not published to a registry. `release.yml` gains a job that builds,
per target, a tarball of the header plus the shared and static libraries, and attaches them to
the GitHub Release. The target list is the one `build-wheels.yml` already proves works, which
is exactly the list that matters for edge hardware:

| OS | Targets |
|---|---|
| Linux glibc | x86_64, i686, aarch64, armv7 |
| Linux musl | x86_64, aarch64 |
| Windows | x64, arm64 (`.dll` + import library) |
| macOS | x86_64, arm64 |

A static library built from reqwest + rustls + Tokio is several megabytes and links against
`pthread`, `dl` and `m` on Linux; the tarball ships a `pkg-config` file so the consumer does not
have to know that. Fine for a gateway, wrong for a microcontroller — see the hosted-OS note
above.

**Licence.** The repository is Apache-2.0 (`LICENSE`, `NOTICE`, `Cargo.toml`), which is what a
library statically linked into a customer's firmware has to be; the C crate inherits it.

## Open points to settle before starting

- **First consumer and their first slice.** The version-1 surface above is a floor. Whoever
  links this first tells us which JSON pairs to add on day one.
- **Symbol prefix.** `datahub_` is short and unlikely to collide. The alternative,
  `intellistream_datahub_`, matches the library and Python names but is unpleasant at every
  call site. Decide once; it cannot change later.
- **Gating `process_response`'s stdout printing.** Feature flag or runtime config, and whether
  the Python bindings should also default it off. Needs a decision in the core before any of
  this ships.
- **Windows toolchain.** MSVC-built `.dll` + `.lib` only, or a MinGW build too. Default to
  MSVC only until someone asks.
