# IntelliStream DataHub — C bindings

`libintellistream_datahub` is the [DataHub Rust SDK](../README.md) built as a C library: a shared
and a static library plus one header, [`include/intellistream_datahub.h`](include/intellistream_datahub.h).
It is for C and C++ directly, and for anything with C interop — .NET P/Invoke, Go cgo, LabVIEW's
Call Library Function Node, MATLAB's `loadlibrary`, Swift, Zig.

There is exactly one implementation of every call: the Rust core. This crate adds no HTTP, auth,
buffering or WebSocket code of its own, so token refresh across every supported OAuth2 flow, the
durable on-disk spool, and the reconnecting subscription listener all behave exactly as they do
for Rust and Python callers.

It is a **hosted-OS** library — Linux (glibc and musl), Windows, macOS. It needs an operating
system; a microcontroller without one is out of scope.

## Build

```bash
cd datahub_c_bindings
cargo build --release
```

produces, under `target/release/`:

| File | What |
|---|---|
| `libintellistream_datahub.so` / `.dylib` / `intellistream_datahub.dll` | shared library |
| `libintellistream_datahub.a` / `intellistream_datahub.lib` | static library |

and regenerates `include/intellistream_datahub.h` from the sources. The header is committed, so a
download of the repository has one without building anything.

Link the shared library the usual way:

```bash
cc gateway.c -I datahub_c_bindings/include -L target/release -lintellistream_datahub -o gateway
```

The static library also needs the system libraries the Rust runtime links against — on Linux
`-lpthread -ldl -lm` — and is several megabytes, since it carries reqwest, rustls and Tokio.

TLS is rustls with the OS trust store (no OpenSSL dependency, so the library does not care which
`libssl` the host process has). On a stripped-down image with no CA bundle, point it at one with
`SSL_CERT_FILE` or `SSL_CERT_DIR`.

## Conventions

- **Handles are opaque pointers** — `datahub_config`, `datahub_client`, `datahub_listener`,
  `datahub_message`, `datahub_timeseries` — each released with its own `_free` (or `_close`) and
  never any other way.
- **Strings are NUL-terminated UTF-8.** Input strings are borrowed for the duration of the call.
  A `char **` out-parameter hands you an owned string: release it with `datahub_string_free`. A
  `const char *` returned by an accessor is borrowed and valid until the handle it came from is
  freed. Never pass a library allocation to `free()`.
- **Every function that can fail returns a `datahub_status`.** Anything other than `DATAHUB_OK`
  and `DATAHUB_BUFFERED` leaves a message in `datahub_last_error()` (per thread; overwritten by
  the next failure on that thread) and, for HTTP failures, the code in `datahub_last_http_status()`.
  `datahub_status_name()` gives the enum's name for logging.
- **A `datahub_client` may be used from any number of threads at once.** A listener and a message
  belong to one thread at a time.
- **A Rust panic never reaches you.** It becomes `DATAHUB_PANIC` with the message in
  `datahub_last_error()`; please report one, it is a bug in the SDK.

### Status codes

| Status | Meaning |
|---|---|
| `DATAHUB_OK` | Success. For an ingest call, the data reached the server. |
| `DATAHUB_BUFFERED` | The data went to the on-disk spool (server unreachable, or credential refused) and will be sent on a later ingest call or `datahub_client_flush`. Not an error. |
| `DATAHUB_TIMEOUT` | `datahub_listener_next`: nothing arrived within the timeout. |
| `DATAHUB_CLOSED` | `datahub_listener_next`: the stream has ended. |
| `DATAHUB_NOT_FOUND` | The api answered with no item where one was asked for. |
| `DATAHUB_INVALID_ARGUMENT` | NULL where a value was required, invalid UTF-8, an empty id, a non-finite value, a JSON body of the wrong shape. Nothing was sent. |
| `DATAHUB_CONFIG` | Incomplete or contradictory configuration (no `BASE_URL`, malformed URL, …). |
| `DATAHUB_AUTH` | A token could not be obtained, or the api answered 401/403. For a 401 the message carries the SDK's diagnosis of the token's `organization` claim when it has one. |
| `DATAHUB_HTTP` | Another non-2xx answer, or no answer at all; see `datahub_last_http_status()`. A transport failure (refused, DNS, timeout) is reported as 503, the same way the core treats it: retryable. |
| `DATAHUB_IO` | A local failure: unreadable env file, runtime could not start, WebSocket lost for good. |
| `DATAHUB_SUBSCRIPTION` | The server rejected one subscription (unknown id, no read access); the connection stays open, call `datahub_listener_next` again. |
| `DATAHUB_PANIC` | A Rust panic was caught at the boundary. |

## Ingesting datapoints

```c
#include <intellistream_datahub.h>
#include <stdio.h>

int main(void) {
    datahub_config *cfg = datahub_config_new();
    datahub_config_set_base_url(cfg, "https://datahub.example.com");
    datahub_config_set_client_credentials(cfg, "gateway-7", "…secret…",
                                          "https://sso.example.com/realms/datahub/protocol/openid-connect/token");
    datahub_config_set_scope(cfg, "organization:*");          /* Keycloak Organizations realms need it */
    datahub_config_set_buffer_dir(cfg, "/var/lib/gateway/datahub-spool");   /* also enables buffering */

    datahub_client *client = NULL;
    if (datahub_client_new(cfg, &client) != DATAHUB_OK) {
        fprintf(stderr, "datahub: %s\n", datahub_last_error());
        datahub_config_free(cfg);
        return 1;
    }
    datahub_config_free(cfg);                 /* the client copied what it needs */

    datahub_datapoint points[] = {
        { .timestamp_ms = 1789000000000, .value = 21.5 },
        { .timestamp_ms = 1789000001000, .value = 21.6 },
    };
    switch (datahub_datapoints_insert(client, "pump-1/temperature", points, 2)) {
        case DATAHUB_OK:       break;                                  /* on the server */
        case DATAHUB_BUFFERED: break;                                  /* on disk; sent later */
        default: fprintf(stderr, "datahub: %s\n", datahub_last_error()); /* fix and retry */
    }

    datahub_client_flush(client);             /* push any backlog before a controlled shutdown */
    datahub_client_free(client);
    return 0;
}
```

Configuration can also come from the process environment (`datahub_config_from_env()`), from a
dotenv-style file (`datahub_config_load_envfile`), or from `datahub_config_set(cfg, "KEY", "value")`
with any of the keys below. Nothing ever reads a `.env` file from the working directory: a library
must not pick up a dotfile from its host's cwd.

| Key | Setter | Meaning |
|---|---|---|
| `BASE_URL` | `datahub_config_set_base_url` | The api's root URL. Required. |
| `TOKEN` | `datahub_config_set_token` | A bearer token used as-is, never refreshed. |
| `CLIENT_ID`, `CLIENT_SECRET`, `TOKEN_URI` | `datahub_config_set_client_credentials` | OAuth2 client credentials; tokens are minted and refreshed automatically. |
| `SCOPE` | `datahub_config_set_scope` | Added to the token request. `organization:*` (or `organization:<alias>`) on Keycloak Organizations realms. |
| `AUDIENCE` | `datahub_config_set_audience` | Token request audience (Auth0). |
| `ASSERTION`, `ASSERTION_CLIENT_ID`, `ASSERTION_CLIENT_SECRET`, `ASSERTION_TOKEN_URI`, `ASSERTION_SCOPE`, `ASSERTION_AUDIENCE`, `ASSERTION_GRANT` | `datahub_config_set_assertion*` | The RFC 7523 `jwt-bearer` and federated flows; see the crate README. |
| `ENABLE_BUFFERING`, `BUFFER_DIR`, `BUFFER_RETENTION_SECS`, `BUFFER_MAX_BYTES` | `datahub_config_enable_buffering`, `datahub_config_set_buffer_*` | Durable ingest buffering: off by default; 72 h window and 5 GiB cap when on. |

### What buffering does and does not do

With buffering on, an ingest that cannot reach the server — or whose credential is refused, which
is recoverable out of band — returns `DATAHUB_BUFFERED` and the data is on disk. The next ingest
call, or `datahub_client_flush`, sends the backlog first, so ordering holds; retries are safe
because the server dedups datapoints on (series, timestamp) and events on their client-stamped id.
`datahub_client_buffered_count` reports what is held. The spool survives the process: a new
client on the same directory picks the backlog up.

The retention window is measured on each record's **own** timestamp, not on when it was spooled.
A backfill older than the window is reported `DATAHUB_BUFFERED` but does not survive in the spool.
Backfill old data with buffering off, or widen the window with `datahub_config_set_buffer_retention_secs`.

Reads are never buffered.

## Reading

```c
datahub_timeseries *ts = NULL;
if (datahub_timeseries_get_by_external_id(client, "pump-1/temperature", &ts) == DATAHUB_OK) {
    printf("%s (%s), unit %s\n", datahub_timeseries_name(ts), datahub_timeseries_external_id(ts),
           datahub_timeseries_unit(ts) ? datahub_timeseries_unit(ts) : "-");
    datahub_timeseries_free(ts);
}

datahub_datapoint_agg latest;
if (datahub_datapoints_latest(client, "pump-1/temperature", &latest) == DATAHUB_OK) {
    printf("latest: %lld -> %g\n", (long long)latest.timestamp_ms, latest.value);
}

datahub_datapoint_agg *points = NULL;
size_t count = 0;
if (datahub_datapoints_retrieve(client, "pump-1/temperature",
                                start_ms, DATAHUB_TIME_UNSET, 1000, &points, &count) == DATAHUB_OK) {
    for (size_t i = 0; i < count; i++) { /* points[i].value; aggregates are NaN on a raw read */ }
    datahub_datapoints_free(points, count);
}
```

## Everything else: JSON

The datapoint path is typed because it is the path a gateway calls a thousand times a second.
Everything else crosses the boundary as **JSON text**: a `..._json` function takes exactly the
request body the REST endpoint takes and hands back exactly the response body it answers with
(`items`, plus `nextCursor` when there is another page). The REST API reference is therefore the
documentation for every one of them, and a C++ caller uses whatever JSON library it already has.

```c
char *response = NULL;
datahub_status st = datahub_events_create_json(client,
    "{\"items\":[{\"externalId\":\"alarm-17\",\"type\":\"Alarm\",\"eventTime\":\"2026-09-06T10:00:00Z\"}]}",
    &response);
if (st == DATAHUB_OK || st == DATAHUB_BUFFERED) { /* … */ }
datahub_string_free(response);
```

These are not raw pass-throughs: the body is parsed into the SDK's own types and sent through the
same service method Rust and Python use (so buffering applies to `datahub_events_create_json`, and
a body whose fields have the wrong type is rejected as `DATAHUB_INVALID_ARGUMENT` before anything
is sent).

For any endpoint without a dedicated function there is `datahub_request_json(client, "POST",
"/resources/filter", body, &response)`: an authenticated raw call, no buffering, response body
returned as sent. `GET` and `POST` are supported; the path is relative to the base URL and may
carry a query string.

## Listening to subscriptions

```c
const char *subs[] = { "pump-1-alarms" };
datahub_listener *listener = NULL;
if (datahub_listener_open(client, subs, 1, &listener) != DATAHUB_OK) { /* … */ }

datahub_message *msg = NULL;
for (;;) {
    switch (datahub_listener_next(listener, 5000, &msg)) {
        case DATAHUB_TIMEOUT:       continue;                 /* idle; check a stop flag here */
        case DATAHUB_SUBSCRIPTION:  fprintf(stderr, "%s\n", datahub_last_error()); continue;
        case DATAHUB_OK:            break;
        default:                    goto done;                /* DATAHUB_IO after failed reconnects */
    }
    if (datahub_message_series_count(msg) > 0) {              /* a DATAPOINTS message */
        const datahub_datapoint *points; size_t n;
        datahub_message_series_datapoints(msg, 0, &points, &n);
        /* points[i].timestamp_ms, points[i].value */
    } else {
        handle_json(datahub_message_json(msg));               /* TIMESERIES, EVENT, RESOURCE, … */
    }
    const char *id = datahub_message_id(msg);
    datahub_listener_ack(listener, &id, 1);
    datahub_message_free(msg);
}
done:
datahub_listener_close(listener);
```

The listener is pull-based on purpose: a callback API has to define which thread it runs on, what
it may call and what happens if it blocks, while a `next(timeout)` loop is what every C event loop
already knows how to drive. Call it often enough for the server's 15 s pings to be answered. On a
dropped connection the listener reconnects on its own with backoff and resumes the same
subscriptions; anything not acked is redelivered.

## Console output

The Rust core prints request/response tracing to stdout and stderr by default, which is useful in
a developer's terminal and wrong inside someone else's daemon. This library turns it **off**;
`datahub_set_debug_output(true)` turns it back on for the whole process.

## Version

`datahub_version()` returns the library version; `DATAHUB_VERSION` (and `_MAJOR`/`_MINOR`/`_PATCH`)
in the header say what it was generated from. The two are released together with the Rust crate
and the Python package, under one version number.

## Testing

```bash
../run_c_tests.sh                  # cargo build + cargo test + the C smoke test
../run_c_tests.sh --check-header   # additionally fail if the committed header is stale (CI)
../run_c_tests.sh --smoke-only     # just compile and run tests/c/smoke.c
```

`tests/offline.rs` covers the boundary and the spool paths with no network; `tests/mock_api.rs`
runs against a tiny HTTP mock and asserts what goes on the wire; `tests/live.rs` runs against a
real backend when the repository's `.env` names a `BASE_URL`, and prints `SKIP` otherwise;
`tests/c/smoke.c` is the header and library as a C compiler sees them.

## Licence

Apache-2.0, like the rest of this repository.
