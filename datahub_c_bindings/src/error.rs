// SPDX-License-Identifier: Apache-2.0
//! Status codes, the per-thread last-error slot, and the panic guard every export runs inside.

use std::cell::{Cell, RefCell};
use std::ffi::{c_char, c_int, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};

use intellistream_datahub_sdk::errors::DataHubError;
use intellistream_datahub_sdk::http::ResponseError;
use intellistream_datahub_sdk::ListenError;

use crate::util::to_cstring;

/// Outcome of a call. Everything except `DATAHUB_OK` and `DATAHUB_BUFFERED` leaves a message
/// in `datahub_last_error()`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum datahub_status {
    /// The call succeeded. For an ingest call this means the data reached the server.
    DATAHUB_OK = 0,
    /// The data went into the on-disk spool because the server could not be reached, or refused
    /// the credential. It is sent on a later ingest call or by `datahub_client_flush`. Not an
    /// error: this is the answer an edge device wants when the network is down.
    DATAHUB_BUFFERED = 1,
    /// `datahub_listener_next`: nothing arrived within the timeout.
    DATAHUB_TIMEOUT = 2,
    /// `datahub_listener_next`: the stream has ended.
    DATAHUB_CLOSED = 3,
    /// The api answered with no item where exactly one was asked for.
    DATAHUB_NOT_FOUND = 4,
    /// A NULL where a value was required, invalid UTF-8, an empty id, a non-finite value, or a
    /// JSON body that does not parse as the request the endpoint takes.
    DATAHUB_INVALID_ARGUMENT = 10,
    /// The configuration is incomplete or contradictory: no BASE_URL, no usable credential set,
    /// a malformed URL.
    DATAHUB_CONFIG = 11,
    /// A token could not be obtained, or the api answered 401 or 403. For a 401 the message
    /// includes the SDK's diagnosis of the token's `organization` claim when it has one.
    DATAHUB_AUTH = 12,
    /// The api answered another non-2xx status, or the request got no response at all;
    /// `datahub_last_http_status()` says which. A transport failure (connection refused, DNS,
    /// timeout) is reported as 503, the same way the core treats it: retryable.
    DATAHUB_HTTP = 13,
    /// A local failure: an env file that cannot be read, a runtime that could not start, a
    /// WebSocket that could not be opened or was lost for good.
    DATAHUB_IO = 14,
    /// The listener reported an error for one subscription (unknown id, no read access). The
    /// connection stays open and the other subscriptions keep delivering; call
    /// `datahub_listener_next` again.
    DATAHUB_SUBSCRIPTION = 15,
    /// A Rust panic was caught at the boundary. The message is in `datahub_last_error()`;
    /// please report it, it is a bug in the SDK.
    DATAHUB_PANIC = 99,
}

thread_local! {
    static LAST_ERROR: RefCell<CString> = RefCell::new(CString::default());
    static LAST_HTTP_STATUS: Cell<c_int> = const { Cell::new(0) };
}

/// Record a failure for the calling thread and hand back the status to return.
pub(crate) fn fail(
    status: datahub_status,
    http_status: u16,
    message: impl Into<String>,
) -> datahub_status {
    let message = message.into();
    LAST_ERROR.with(|slot| *slot.borrow_mut() = to_cstring(&message));
    LAST_HTTP_STATUS.with(|slot| slot.set(http_status as c_int));
    status
}

/// Map a core `ResponseError` (an HTTP-level failure, or a token that could not be obtained).
pub(crate) fn from_response_error(error: &ResponseError) -> datahub_status {
    let code = error.get_status().as_u16();
    let status = if code == 401 || code == 403 {
        datahub_status::DATAHUB_AUTH
    } else {
        datahub_status::DATAHUB_HTTP
    };
    fail(status, code, error.to_string())
}

/// Map a core `DataHubError` (configuration and token acquisition).
pub(crate) fn from_config_error(error: &DataHubError) -> datahub_status {
    let status = match error {
        DataHubError::ConfigError(_) | DataHubError::UrlError(_) | DataHubError::JsonError(_) => {
            datahub_status::DATAHUB_CONFIG
        }
        DataHubError::OAuthError(_) => datahub_status::DATAHUB_AUTH,
        DataHubError::HttpError(_) => datahub_status::DATAHUB_IO,
    };
    fail(status, 0, error.to_string())
}

/// Map a listener error.
pub(crate) fn from_listen_error(error: &ListenError) -> datahub_status {
    let status = match error {
        ListenError::Request(message) if message.contains("api token") => {
            datahub_status::DATAHUB_AUTH
        }
        ListenError::Request(_) => datahub_status::DATAHUB_CONFIG,
        ListenError::Handshake(message) if message.contains("401") || message.contains("403") => {
            datahub_status::DATAHUB_AUTH
        }
        ListenError::Handshake(_) | ListenError::WebSocket(_) => datahub_status::DATAHUB_IO,
        ListenError::Deserialize(_) | ListenError::Serialize(_) => datahub_status::DATAHUB_HTTP,
        ListenError::Subscription { .. } => datahub_status::DATAHUB_SUBSCRIPTION,
    };
    fail(status, 0, error.to_string())
}

/// Run an FFI function body with panics converted to `DATAHUB_PANIC`.
///
/// Unwinding across an `extern "C"` boundary is undefined behaviour (and aborts the process on
/// current Rust), so nothing is allowed to escape: the payload's message goes into the last-error
/// slot and the caller gets a status it can log.
pub(crate) fn guard<F: FnOnce() -> datahub_status>(body: F) -> datahub_status {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(status) => status,
        Err(payload) => {
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic with a non-string payload".to_string());
            fail(
                datahub_status::DATAHUB_PANIC,
                0,
                format!("panic inside the DataHub SDK: {message}"),
            )
        }
    }
}

/// The message left by the last failing call on this thread, or `""` when there has been none.
/// Borrowed: valid until the next failing call on the same thread.
#[no_mangle]
pub extern "C" fn datahub_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| slot.borrow().as_ptr())
}

/// The HTTP status of the last `DATAHUB_HTTP` / `DATAHUB_AUTH` / `DATAHUB_NOT_FOUND` failure on
/// this thread, or 0 when the last failure was not an HTTP response.
#[no_mangle]
pub extern "C" fn datahub_last_http_status() -> c_int {
    LAST_HTTP_STATUS.with(|slot| slot.get())
}

/// The name of a status, e.g. `"DATAHUB_BUFFERED"`, for logging. Static; never NULL.
#[no_mangle]
pub extern "C" fn datahub_status_name(status: datahub_status) -> *const c_char {
    let name: &'static str = match status {
        datahub_status::DATAHUB_OK => "DATAHUB_OK\0",
        datahub_status::DATAHUB_BUFFERED => "DATAHUB_BUFFERED\0",
        datahub_status::DATAHUB_TIMEOUT => "DATAHUB_TIMEOUT\0",
        datahub_status::DATAHUB_CLOSED => "DATAHUB_CLOSED\0",
        datahub_status::DATAHUB_NOT_FOUND => "DATAHUB_NOT_FOUND\0",
        datahub_status::DATAHUB_INVALID_ARGUMENT => "DATAHUB_INVALID_ARGUMENT\0",
        datahub_status::DATAHUB_CONFIG => "DATAHUB_CONFIG\0",
        datahub_status::DATAHUB_AUTH => "DATAHUB_AUTH\0",
        datahub_status::DATAHUB_HTTP => "DATAHUB_HTTP\0",
        datahub_status::DATAHUB_IO => "DATAHUB_IO\0",
        datahub_status::DATAHUB_SUBSCRIPTION => "DATAHUB_SUBSCRIPTION\0",
        datahub_status::DATAHUB_PANIC => "DATAHUB_PANIC\0",
    };
    name.as_ptr() as *const c_char
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CStr;

    fn last_error() -> String {
        unsafe { CStr::from_ptr(datahub_last_error()) }
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn a_panic_becomes_a_status_with_its_message() {
        let status = guard(|| panic!("boom {}", 42));
        assert_eq!(status, datahub_status::DATAHUB_PANIC);
        assert_eq!(last_error(), "panic inside the DataHub SDK: boom 42");
        assert_eq!(datahub_last_http_status(), 0);
    }

    #[test]
    fn a_static_str_panic_payload_is_read_too() {
        let status = guard(|| panic!("static"));
        assert_eq!(status, datahub_status::DATAHUB_PANIC);
        assert!(last_error().ends_with("static"));
    }

    #[test]
    fn the_error_slot_is_per_thread() {
        fail(datahub_status::DATAHUB_CONFIG, 0, "on the main thread");
        let seen_elsewhere = std::thread::spawn(last_error).join().unwrap();
        assert_eq!(seen_elsewhere, "", "a fresh thread starts with no error");
        assert_eq!(last_error(), "on the main thread");
    }

    #[test]
    fn a_message_with_an_interior_nul_is_sanitised_not_lost() {
        fail(datahub_status::DATAHUB_IO, 0, "before\0after");
        assert_eq!(last_error(), "before after");
    }

    #[test]
    fn status_names_are_stable_strings() {
        let name = unsafe { CStr::from_ptr(datahub_status_name(datahub_status::DATAHUB_BUFFERED)) };
        assert_eq!(name.to_str().unwrap(), "DATAHUB_BUFFERED");
    }
}
