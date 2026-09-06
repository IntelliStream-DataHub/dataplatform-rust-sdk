// SPDX-License-Identifier: Apache-2.0
//! The JSON convention: a function taking `..._json` accepts exactly the request body the REST
//! endpoint takes and returns exactly the response body it answers with, so the REST API
//! reference doubles as the documentation for it.
//!
//! It is not a raw pass-through. The body is deserialized into the core's typed structs and sent
//! through the same service method Rust and Python callers use — the durable buffer, chunking and
//! auth apply, and a body whose fields have the wrong type is rejected before any request is
//! made — then the typed response is serialized back.

use std::ffi::c_char;

use intellistream_datahub_sdk::generic::DataWrapper;
use intellistream_datahub_sdk::http::ResponseError;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{datahub_status, fail, from_response_error};
use crate::util::out_str;

/// Parse a request body into the type the endpoint takes.
pub(crate) fn parse<T: DeserializeOwned>(body: &str, what: &str) -> Result<T, datahub_status> {
    serde_json::from_str(body).map_err(|e| {
        fail(
            datahub_status::DATAHUB_INVALID_ARGUMENT,
            0,
            format!("body is not a valid {what}: {e}"),
        )
    })
}

/// The wire shape of a response: `items`, plus `nextCursor` when there is another page. Built by
/// hand because the core's `DataWrapper` skips `nextCursor` when serializing — it doubles as a
/// request body, where the field does not exist — and a C caller paging through results needs it.
pub(crate) fn wrapper_to_json<T: Serialize>(wrapper: &DataWrapper<T>) -> String {
    let mut value = serde_json::json!({ "items": wrapper.get_items() });
    if let Some(cursor) = wrapper.next_cursor() {
        value["nextCursor"] = serde_json::Value::String(cursor.to_string());
    }
    value.to_string()
}

/// Turn a service result into the out-parameter and a status. With `may_buffer`, the core's
/// "accepted into the spool" answer (202, no items) becomes `DATAHUB_BUFFERED`.
pub(crate) unsafe fn respond<T: Serialize>(
    out: *mut *mut c_char,
    result: Result<DataWrapper<T>, ResponseError>,
    may_buffer: bool,
) -> datahub_status {
    match result {
        Ok(wrapper) => {
            let buffered = may_buffer
                && wrapper.get_http_status_code() == Some(202)
                && wrapper.get_items().is_empty();
            ffi_try!(out_str(out, wrapper_to_json(&wrapper)));
            if buffered {
                datahub_status::DATAHUB_BUFFERED
            } else {
                datahub_status::DATAHUB_OK
            }
        }
        Err(e) => from_response_error(&e),
    }
}
