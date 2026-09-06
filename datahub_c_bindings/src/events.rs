// SPDX-License-Identifier: Apache-2.0
//! Events, as JSON: create (buffered like datapoints) and filter.

use std::ffi::c_char;

use intellistream_datahub_sdk::filters::EventFilterForm;
use intellistream_datahub_sdk::generic::DataWrapper;
use intellistream_datahub_sdk::Event;

use crate::client::datahub_client;
use crate::error::{datahub_status, guard};
use crate::json::{parse, respond};
use crate::util::{ref_arg, str_arg};

/// `POST /events/create`. `body` is `{"items":[{"externalId":…,"type":…,"eventTime":…}, …]}`.
/// `DATAHUB_OK` with the created events in `*out`; with buffering enabled, `DATAHUB_BUFFERED`
/// (and `{"items":[]}`) when they went to the spool instead. Each event is stamped with a
/// time-ordered UUID before the first attempt, so a retry from the spool is not a duplicate.
#[no_mangle]
pub unsafe extern "C" fn datahub_events_create_json(
    client: *const datahub_client,
    body: *const c_char,
    out: *mut *mut c_char,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let body = ffi_try!(str_arg(body, "body"));
        let request: DataWrapper<Event> = ffi_try!(parse(body, "event create request"));
        let events: Vec<Event> = request.get_items().clone();
        respond(out, client.run(client.api.events.create(&events)), true)
    })
}

/// `POST /events/filter`. `body` is `{"filter":{…},"limit":…,"cursor":…}`; page with the
/// response's `nextCursor`.
#[no_mangle]
pub unsafe extern "C" fn datahub_events_filter_json(
    client: *const datahub_client,
    body: *const c_char,
    out: *mut *mut c_char,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let body = ffi_try!(str_arg(body, "body"));
        let form: EventFilterForm = ffi_try!(parse(body, "event filter request"));
        respond(out, client.run(client.api.events.filter(&form)), false)
    })
}
