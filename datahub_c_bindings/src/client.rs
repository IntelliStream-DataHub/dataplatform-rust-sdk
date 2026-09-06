// SPDX-License-Identifier: Apache-2.0
//! `datahub_client`: the async `ApiService` plus the Tokio runtime that drives it.

use std::ffi::c_char;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use intellistream_datahub_sdk::generic::ApiServiceProvider;
use intellistream_datahub_sdk::http::set_debug_output;
use intellistream_datahub_sdk::ApiService;
use tokio::runtime::{Builder, Runtime};

use crate::config::{config_value, datahub_config};
use crate::error::{datahub_status, fail, from_config_error, from_response_error, guard};
use crate::util::{mut_arg, opt_str_arg, out_str, ref_arg, str_arg};

/// A connection to one DataHub api. Opaque; may be shared between threads; free with
/// `datahub_client_free` after every listener opened from it has been closed.
pub struct datahub_client {
    pub(crate) api: Arc<ApiService>,
    pub(crate) rt: Arc<Runtime>,
    pub(crate) base_url: String,
}

impl datahub_client {
    /// Drive a core future to completion on this client's runtime, blocking the calling thread.
    pub(crate) fn run<F: Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }
}

/// Whether the host asked for the core's console tracing. Off by default for a library: the
/// core defaults it on, so every client construction re-applies this.
static DEBUG_OUTPUT: AtomicBool = AtomicBool::new(false);

/// The library version, e.g. `"0.3.0"`. Static; never NULL. Compare with `DATAHUB_VERSION`.
#[no_mangle]
pub extern "C" fn datahub_version() -> *const c_char {
    concat!(env!("CARGO_PKG_VERSION"), "\0").as_ptr() as *const c_char
}

/// Turn the SDK's console tracing (response bodies, batch progress, failed-request notices on
/// stdout/stderr) on or off for the whole process. Off by default in this library.
#[no_mangle]
pub extern "C" fn datahub_set_debug_output(enabled: bool) {
    DEBUG_OUTPUT.store(enabled, Ordering::Relaxed);
    set_debug_output(enabled);
}

/// Build a client from a config. The config is copied and may be freed afterwards. On success
/// `*out` is the client; on failure it is NULL and the status says why (`DATAHUB_CONFIG` for an
/// incomplete config, `DATAHUB_IO` if the runtime could not start). No request is made here:
/// tokens are fetched lazily on the first call that needs one.
#[no_mangle]
pub unsafe extern "C" fn datahub_client_new(
    config: *const datahub_config,
    out: *mut *mut datahub_client,
) -> datahub_status {
    guard(|| {
        let out = ffi_try!(mut_arg(out, "out"));
        *out = std::ptr::null_mut();
        let config_ref = ffi_try!(ref_arg(config, "config"));

        // The core's `TokenUrl::new(...).expect(...)` would panic on a malformed token URI; say
        // so as a config error instead.
        if let Some(uri) = ffi_try!(config_value(config, "TOKEN_URI")) {
            if !(uri.starts_with("http://") || uri.starts_with("https://")) {
                return fail(
                    datahub_status::DATAHUB_CONFIG,
                    0,
                    format!("TOKEN_URI must be an http(s) URL, got {uri:?}"),
                );
            }
        }
        let core_config = match config_ref.build() {
            Ok(core_config) => core_config,
            Err(e) => return from_config_error(&e),
        };
        let base_url = config_ref
            .get("BASE_URL")
            .unwrap_or_default()
            .trim_end_matches('/')
            .to_string();

        set_debug_output(DEBUG_OUTPUT.load(Ordering::Relaxed));

        let rt = match Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .thread_name("datahub-sdk")
            .build()
        {
            Ok(rt) => rt,
            Err(e) => {
                return fail(
                    datahub_status::DATAHUB_IO,
                    0,
                    format!("could not start the SDK runtime: {e}"),
                )
            }
        };
        let api = ApiService::new(core_config);
        *out = Box::into_raw(Box::new(datahub_client {
            api,
            rt: Arc::new(rt),
            base_url,
        }));
        datahub_status::DATAHUB_OK
    })
}

/// Release a client. Spooled data stays on disk for the next client that opens the same buffer
/// directory; call `datahub_client_flush` first if it should go out now. NULL is ignored.
#[no_mangle]
pub unsafe extern "C" fn datahub_client_free(client: *mut datahub_client) {
    if !client.is_null() {
        let _ = guard(|| {
            drop(Box::from_raw(client));
            datahub_status::DATAHUB_OK
        });
    }
}

/// Send whatever the datapoint and event spools hold, oldest first, without ingesting anything
/// new. `DATAHUB_OK` when both spools are empty afterwards; `DATAHUB_BUFFERED` when the server is
/// still unreachable and a backlog remains on disk. Always `DATAHUB_OK` when buffering is off.
#[no_mangle]
pub unsafe extern "C" fn datahub_client_flush(client: *const datahub_client) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let datapoints_done = client.run(client.api.time_series.flush_buffer());
        let events_done = client.run(client.api.events.flush_buffer());
        if datapoints_done && events_done {
            datahub_status::DATAHUB_OK
        } else {
            datahub_status::DATAHUB_BUFFERED
        }
    })
}

/// Records (datapoints plus events) currently held in this client's on-disk spools. 0 when
/// buffering is off, or when nothing has been spooled by this client yet.
#[no_mangle]
pub unsafe extern "C" fn datahub_client_buffered_count(client: *const datahub_client) -> u64 {
    match client.as_ref() {
        Some(client) => {
            client.api.time_series.buffered_count() + client.api.events.buffered_count()
        }
        None => 0,
    }
}

/// An authenticated raw request to any api endpoint — the escape hatch for everything this
/// header has no dedicated function for. `method` is `GET` or `POST`; `path` is relative to the
/// base URL (`/events/count`, `/resources/filter`), a query string included; `body` is the JSON
/// request body (NULL sends `{}` for POST, nothing for GET). `*out` receives the raw response
/// body (empty for 204). No buffering applies here; the typed ingest functions have it.
#[no_mangle]
pub unsafe extern "C" fn datahub_request_json(
    client: *const datahub_client,
    method: *const c_char,
    path: *const c_char,
    body: *const c_char,
    out: *mut *mut c_char,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let method = ffi_try!(str_arg(method, "method")).to_ascii_uppercase();
        let path = ffi_try!(str_arg(path, "path"));
        let body = ffi_try!(opt_str_arg(body, "body"));
        let _ = ffi_try!(mut_arg(out, "out"));

        let url = format!(
            "{}{}{}",
            client.base_url,
            if path.starts_with('/') { "" } else { "/" },
            path
        );
        let json: serde_json::Value = match body {
            Some(text) => match serde_json::from_str(text) {
                Ok(value) => value,
                Err(e) => {
                    return fail(
                        datahub_status::DATAHUB_INVALID_ARGUMENT,
                        0,
                        format!("body is not valid JSON: {e}"),
                    )
                }
            },
            None => serde_json::json!({}),
        };
        // Any service carries the request plumbing; the timeseries one is as good as any.
        let service = &client.api.time_series;
        let result = match method.as_str() {
            "GET" => client.run(service.execute_get_request::<String, str>(&url, None)),
            "POST" => {
                client.run(service.execute_post_request::<String, serde_json::Value>(&url, &json))
            }
            other => {
                return fail(
                    datahub_status::DATAHUB_INVALID_ARGUMENT,
                    0,
                    format!("method must be GET or POST, got {other:?}"),
                )
            }
        };
        match result {
            Ok(text) => {
                ffi_try!(out_str(out, text));
                datahub_status::DATAHUB_OK
            }
            Err(e) => from_response_error(&e),
        }
    })
}
