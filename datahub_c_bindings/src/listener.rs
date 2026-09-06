// SPDX-License-Identifier: Apache-2.0
//! The subscription listener, pull-based.
//!
//! `datahub_listener_next(timeout)` mirrors the core's `SubscriptionListener::next` rather than
//! delivering through a callback: a callback API has to define which thread it runs on, what it
//! may call and what happens if it blocks; a `next` loop with a timeout defines none of that and
//! is what every C event loop already knows how to drive.
//!
//! A listener keeps the runtime and the api service alive on its own, so it does not matter in
//! which order it and its client are released.

use std::ffi::{c_char, CString};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use intellistream_datahub_sdk::{
    ApiService, ListenError, SubscriptionListener, SubscriptionMessage,
};
use tokio::runtime::Runtime;

use crate::client::datahub_client;
use crate::datapoints::datahub_datapoint;
use crate::error::{datahub_status, fail, from_listen_error, guard};
use crate::util::{mut_arg, ref_arg, str_array_arg, to_cstring};

/// A live WebSocket listener over one or more subscriptions. Opaque; one thread at a time; close
/// (and free) with `datahub_listener_close`.
pub struct datahub_listener {
    inner: SubscriptionListener,
    rt: Arc<Runtime>,
    _api: Arc<ApiService>,
}

struct Series {
    external_id: Option<CString>,
    id: u64,
    points: Vec<datahub_datapoint>,
}

/// One message delivered to a listener. Opaque; free with `datahub_message_free` after acking.
pub struct datahub_message {
    message_id: CString,
    subscription: CString,
    action: CString,
    object: CString,
    json: CString,
    series: Vec<Series>,
}

/// Stream timestamps arrive as strings; both epoch milliseconds and RFC 3339 are understood.
fn parse_stream_timestamp(text: &str) -> i64 {
    if let Ok(ms) = text.parse::<i64>() {
        return ms;
    }
    DateTime::parse_from_rfc3339(text)
        .map(|t| t.timestamp_millis())
        .unwrap_or(i64::MIN)
}

/// The wire name of a serde enum value (`CREATE`, `DATAPOINTS`, …).
fn enum_name<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

impl datahub_message {
    fn wrap(message: SubscriptionMessage) -> Self {
        let series = message
            .payload
            .items
            .iter()
            .map(|collection| Series {
                external_id: collection.external_id.as_deref().map(to_cstring),
                id: collection.id.unwrap_or(0),
                points: collection
                    .datapoints
                    .iter()
                    .map(|dp| datahub_datapoint {
                        timestamp_ms: parse_stream_timestamp(&dp.timestamp),
                        value: dp.value.parse::<f64>().unwrap_or(f64::NAN),
                    })
                    .collect(),
            })
            .collect();
        let json = serde_json::json!({
            "subscriptionExternalId": message.subscription_external_id,
            "messageId": message.message_id,
            "payload": message.payload,
        });
        datahub_message {
            message_id: to_cstring(&message.message_id),
            subscription: to_cstring(&message.subscription_external_id),
            action: to_cstring(&enum_name(&message.payload.event_action)),
            object: to_cstring(&enum_name(&message.payload.event_object)),
            json: to_cstring(&json.to_string()),
            series,
        }
    }
}

/// Open a listener on `count` subscription external ids (0 is allowed; add them later with
/// `datahub_listener_subscribe`). The handshake fetches a token through the client. On a dropped
/// connection the listener reconnects on its own with backoff and resumes the same
/// subscriptions; anything not acked is redelivered.
#[no_mangle]
pub unsafe extern "C" fn datahub_listener_open(
    client: *const datahub_client,
    subscription_external_ids: *const *const c_char,
    count: usize,
    out: *mut *mut datahub_listener,
) -> datahub_status {
    guard(|| {
        let out = ffi_try!(mut_arg(out, "out"));
        *out = std::ptr::null_mut();
        let client = ffi_try!(ref_arg(client, "client"));
        let ids = ffi_try!(str_array_arg(
            subscription_external_ids,
            count,
            "subscription_external_ids"
        ));
        match client.run(client.api.subscriptions.listen(&ids)) {
            Ok(inner) => {
                *out = Box::into_raw(Box::new(datahub_listener {
                    inner,
                    rt: client.rt.clone(),
                    _api: client.api.clone(),
                }));
                datahub_status::DATAHUB_OK
            }
            Err(e) => from_listen_error(&e),
        }
    })
}

/// Wait up to `timeout_ms` for the next message (negative waits indefinitely, 0 only takes what
/// has already arrived). `DATAHUB_OK` with the message in `*out`; `DATAHUB_TIMEOUT` with `*out`
/// NULL when nothing came; `DATAHUB_SUBSCRIPTION` when the server rejected one subscription (the
/// others keep delivering, call again); `DATAHUB_IO` when the connection was lost and could not
/// be re-established after several attempts (calling again retries). Call this often enough for
/// the server's 15 s pings to be answered, or the session is closed as idle and reconnected.
#[no_mangle]
pub unsafe extern "C" fn datahub_listener_next(
    listener: *mut datahub_listener,
    timeout_ms: i64,
    out: *mut *mut datahub_message,
) -> datahub_status {
    guard(|| {
        let out = ffi_try!(mut_arg(out, "out"));
        *out = std::ptr::null_mut();
        let listener = ffi_try!(mut_arg(listener, "listener"));
        let rt = listener.rt.clone();
        let result = if timeout_ms < 0 {
            rt.block_on(listener.inner.next())
        } else {
            let wait = Duration::from_millis(timeout_ms as u64);
            match rt.block_on(tokio::time::timeout(wait, listener.inner.next())) {
                Ok(result) => result,
                Err(_) => return datahub_status::DATAHUB_TIMEOUT,
            }
        };
        match result {
            None => datahub_status::DATAHUB_CLOSED,
            Some(Ok(message)) => {
                *out = Box::into_raw(Box::new(datahub_message::wrap(message)));
                datahub_status::DATAHUB_OK
            }
            Some(Err(e)) => from_listen_error(&e),
        }
    })
}

type IdCall = for<'a> fn(
    &'a mut SubscriptionListener,
    &'a [String],
) -> Pin<Box<dyn Future<Output = Result<(), ListenError>> + 'a>>;

/// The body the four id-list calls share: check the ids, run the call on the listener's runtime.
unsafe fn with_ids(
    listener: *mut datahub_listener,
    ids: *const *const c_char,
    count: usize,
    what: &str,
    call: IdCall,
) -> datahub_status {
    guard(|| {
        let listener = ffi_try!(mut_arg(listener, "listener"));
        let ids = ffi_try!(str_array_arg(ids, count, what));
        if ids.is_empty() {
            return fail(
                datahub_status::DATAHUB_INVALID_ARGUMENT,
                0,
                format!("{what} must name at least one id"),
            );
        }
        let rt = listener.rt.clone();
        match rt.block_on(call(&mut listener.inner, &ids)) {
            Ok(()) => datahub_status::DATAHUB_OK,
            Err(e) => from_listen_error(&e),
        }
    })
}

/// Acknowledge `count` message ids so they are not redelivered.
#[no_mangle]
pub unsafe extern "C" fn datahub_listener_ack(
    listener: *mut datahub_listener,
    message_ids: *const *const c_char,
    count: usize,
) -> datahub_status {
    with_ids(listener, message_ids, count, "message_ids", |l, ids| {
        Box::pin(l.ack(ids))
    })
}

/// Negative-acknowledge `count` message ids so they are redelivered.
#[no_mangle]
pub unsafe extern "C" fn datahub_listener_nack(
    listener: *mut datahub_listener,
    message_ids: *const *const c_char,
    count: usize,
) -> datahub_status {
    with_ids(listener, message_ids, count, "message_ids", |l, ids| {
        Box::pin(l.nack(ids))
    })
}

/// Add `count` subscriptions to the live set without reconnecting.
#[no_mangle]
pub unsafe extern "C" fn datahub_listener_subscribe(
    listener: *mut datahub_listener,
    subscription_external_ids: *const *const c_char,
    count: usize,
) -> datahub_status {
    with_ids(
        listener,
        subscription_external_ids,
        count,
        "subscription_external_ids",
        |l, ids| Box::pin(l.subscribe(ids)),
    )
}

/// Remove `count` subscriptions from the live set.
#[no_mangle]
pub unsafe extern "C" fn datahub_listener_unsubscribe(
    listener: *mut datahub_listener,
    subscription_external_ids: *const *const c_char,
    count: usize,
) -> datahub_status {
    with_ids(
        listener,
        subscription_external_ids,
        count,
        "subscription_external_ids",
        |l, ids| Box::pin(l.unsubscribe(ids)),
    )
}

/// Send a close frame, wait for the server's, and free the listener. Unacked messages are
/// redelivered to the next listener on the same subscriptions. NULL is ignored.
#[no_mangle]
pub unsafe extern "C" fn datahub_listener_close(listener: *mut datahub_listener) -> datahub_status {
    guard(|| {
        if listener.is_null() {
            return datahub_status::DATAHUB_OK;
        }
        let datahub_listener { inner, rt, _api } = *Box::from_raw(listener);
        match rt.block_on(inner.close()) {
            Ok(()) => datahub_status::DATAHUB_OK,
            Err(e) => from_listen_error(&e),
        }
    })
}

unsafe fn message_field(
    message: *const datahub_message,
    pick: fn(&datahub_message) -> &CString,
) -> *const c_char {
    message
        .as_ref()
        .map_or(std::ptr::null(), |m| pick(m).as_ptr())
}

/// The id to pass to `datahub_listener_ack`/`_nack`. Borrowed; valid until the message is freed.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_id(message: *const datahub_message) -> *const c_char {
    message_field(message, |m| &m.message_id)
}

/// The subscription this message was delivered for. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_subscription(
    message: *const datahub_message,
) -> *const c_char {
    message_field(message, |m| &m.subscription)
}

/// What happened: `CREATE`, `UPDATE`, `DELETE` or `RENAME`. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_action(message: *const datahub_message) -> *const c_char {
    message_field(message, |m| &m.action)
}

/// What it happened to: `DATAPOINTS`, `TIMESERIES`, `EVENT`, `RESOURCE`, …. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_object(message: *const datahub_message) -> *const c_char {
    message_field(message, |m| &m.object)
}

/// The whole message as JSON (`subscriptionExternalId`, `messageId`, `payload`). Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_json(message: *const datahub_message) -> *const c_char {
    message_field(message, |m| &m.json)
}

/// How many series this message carries datapoints for (0 unless the object is `DATAPOINTS`).
#[no_mangle]
pub unsafe extern "C" fn datahub_message_series_count(message: *const datahub_message) -> usize {
    message.as_ref().map_or(0, |m| m.series.len())
}

/// External id of the `index`-th series, or NULL when the message named it by id only. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_series_external_id(
    message: *const datahub_message,
    index: usize,
) -> *const c_char {
    message
        .as_ref()
        .and_then(|m| m.series.get(index))
        .and_then(|s| s.external_id.as_ref())
        .map_or(std::ptr::null(), |id| id.as_ptr())
}

/// Numeric id of the `index`-th series, or 0 when the message did not carry one.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_series_id(
    message: *const datahub_message,
    index: usize,
) -> u64 {
    message
        .as_ref()
        .and_then(|m| m.series.get(index))
        .map_or(0, |s| s.id)
}

/// The datapoints of the `index`-th series as a borrowed array (valid until the message is
/// freed). A value that is not numeric reads as NaN, a timestamp that could not be parsed as
/// `INT64_MIN`; the JSON has the originals.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_series_datapoints(
    message: *const datahub_message,
    index: usize,
    out: *mut *const datahub_datapoint,
    out_count: *mut usize,
) -> datahub_status {
    guard(|| {
        let out = ffi_try!(mut_arg(out, "out"));
        let out_count = ffi_try!(mut_arg(out_count, "out_count"));
        *out = std::ptr::null();
        *out_count = 0;
        let message = ffi_try!(ref_arg(message, "message"));
        match message.series.get(index) {
            Some(series) => {
                *out = series.points.as_ptr();
                *out_count = series.points.len();
                datahub_status::DATAHUB_OK
            }
            None => fail(
                datahub_status::DATAHUB_INVALID_ARGUMENT,
                0,
                format!(
                    "series index {index} is out of range (message carries {})",
                    message.series.len()
                ),
            ),
        }
    })
}

/// Release a message. NULL is ignored.
#[no_mangle]
pub unsafe extern "C" fn datahub_message_free(message: *mut datahub_message) {
    if !message.is_null() {
        drop(Box::from_raw(message));
    }
}

#[cfg(test)]
mod tests {
    use super::parse_stream_timestamp;

    #[test]
    fn stream_timestamps_come_as_millis_or_rfc3339() {
        assert_eq!(parse_stream_timestamp("1700000000000"), 1_700_000_000_000);
        assert_eq!(parse_stream_timestamp("1970-01-01T00:00:01Z"), 1000);
        assert_eq!(
            parse_stream_timestamp("1970-01-01T00:00:01.250+00:00"),
            1250
        );
        assert_eq!(parse_stream_timestamp("not a time"), i64::MIN);
    }
}
