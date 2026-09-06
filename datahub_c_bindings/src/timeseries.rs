// SPDX-License-Identifier: Apache-2.0
//! Time series: a typed lookup by external id, and the JSON create/search/filter calls.

use std::ffi::{c_char, CString};

use intellistream_datahub_sdk::generic::{DataWrapper, IdAndExtId, SearchAndFilterForm};
use intellistream_datahub_sdk::{TimeSeries, TimeSeriesFilter, TimeSeriesFilterForm};

use crate::client::datahub_client;
use crate::error::{datahub_status, fail, from_response_error, guard};
use crate::json::{parse, respond};
use crate::util::{mut_arg, nonempty, ref_arg, str_arg, to_cstring};

/// One time series definition. Opaque; read it through the `datahub_timeseries_*` accessors and
/// free it with `datahub_timeseries_free`.
pub struct datahub_timeseries {
    id: u64,
    data_set_id: u64,
    external_id: CString,
    name: CString,
    unit: Option<CString>,
    unit_external_id: Option<CString>,
    value_type: Option<CString>,
    json: CString,
}

impl datahub_timeseries {
    fn wrap(ts: &TimeSeries) -> Self {
        datahub_timeseries {
            id: ts.id.unwrap_or(0),
            data_set_id: ts.data_set_id.unwrap_or(0),
            external_id: to_cstring(&ts.external_id),
            name: to_cstring(&ts.name),
            unit: ts.unit.as_deref().map(to_cstring),
            unit_external_id: ts.unit_external_id.as_deref().map(to_cstring),
            value_type: ts.value_type.as_deref().map(to_cstring),
            json: to_cstring(&serde_json::to_string(ts).unwrap_or_default()),
        }
    }
}

/// Look one time series up by external id. `DATAHUB_NOT_FOUND` when the api knows no such series
/// (or the caller may not read it — the api does not distinguish).
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_get_by_external_id(
    client: *const datahub_client,
    external_id: *const c_char,
    out: *mut *mut datahub_timeseries,
) -> datahub_status {
    guard(|| {
        let out = ffi_try!(mut_arg(out, "out"));
        *out = std::ptr::null_mut();
        let client = ffi_try!(ref_arg(client, "client"));
        let external_id = ffi_try!(nonempty(
            ffi_try!(str_arg(external_id, "external_id")),
            "external_id"
        ));

        let request = DataWrapper::from_vec(vec![IdAndExtId::from_external_id(external_id)]);
        match client.run(client.api.time_series.by_ids(&request)) {
            Ok(response) => match response.get_items().first() {
                Some(ts) => {
                    *out = Box::into_raw(Box::new(datahub_timeseries::wrap(ts)));
                    datahub_status::DATAHUB_OK
                }
                None => fail(
                    datahub_status::DATAHUB_NOT_FOUND,
                    response.get_http_status_code().unwrap_or(0),
                    format!("no time series with external id {external_id:?}"),
                ),
            },
            Err(e) => from_response_error(&e),
        }
    })
}

/// Release a time series handle. NULL is ignored.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_free(ts: *mut datahub_timeseries) {
    if !ts.is_null() {
        drop(Box::from_raw(ts));
    }
}

/// Numeric id; 0 when the api did not send one.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_id(ts: *const datahub_timeseries) -> u64 {
    ts.as_ref().map_or(0, |ts| ts.id)
}

/// Numeric id of the data set the series belongs to; 0 when it has none.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_data_set_id(ts: *const datahub_timeseries) -> u64 {
    ts.as_ref().map_or(0, |ts| ts.data_set_id)
}

unsafe fn required(
    ts: *const datahub_timeseries,
    pick: fn(&datahub_timeseries) -> &CString,
) -> *const c_char {
    ts.as_ref().map_or(std::ptr::null(), |ts| pick(ts).as_ptr())
}

unsafe fn optional(
    ts: *const datahub_timeseries,
    pick: fn(&datahub_timeseries) -> Option<&CString>,
) -> *const c_char {
    ts.as_ref()
        .and_then(pick)
        .map_or(std::ptr::null(), |value| value.as_ptr())
}

/// External id. Borrowed; valid until the handle is freed.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_external_id(
    ts: *const datahub_timeseries,
) -> *const c_char {
    required(ts, |ts| &ts.external_id)
}

/// Display name. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_name(ts: *const datahub_timeseries) -> *const c_char {
    required(ts, |ts| &ts.name)
}

/// Unit symbol, or NULL when the series has none. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_unit(ts: *const datahub_timeseries) -> *const c_char {
    optional(ts, |ts| ts.unit.as_ref())
}

/// Unit catalogue id, or NULL. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_unit_external_id(
    ts: *const datahub_timeseries,
) -> *const c_char {
    optional(ts, |ts| ts.unit_external_id.as_ref())
}

/// Value type (`float`, `bigint`, `text`, …), or NULL when the endpoint did not say. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_value_type(
    ts: *const datahub_timeseries,
) -> *const c_char {
    optional(ts, |ts| ts.value_type.as_ref())
}

/// The whole definition as the api sent it, as JSON. Borrowed.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_json(ts: *const datahub_timeseries) -> *const c_char {
    required(ts, |ts| &ts.json)
}

/// `POST /timeseries/create`. `body` is `{"items":[{"externalId":…,"name":…,"unit":…}, …]}`;
/// `*out` receives the created definitions in the same envelope.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_create_json(
    client: *const datahub_client,
    body: *const c_char,
    out: *mut *mut c_char,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let body = ffi_try!(str_arg(body, "body"));
        let request: DataWrapper<TimeSeries> = ffi_try!(parse(body, "timeseries create request"));
        respond(
            out,
            client.run(client.api.time_series.create(&request)),
            false,
        )
    })
}

/// `POST /timeseries/search`: free-text search with optional narrowing. `body` is
/// `{"search":{"query":…},"filter":{…},"limit":…}`.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_search_json(
    client: *const datahub_client,
    body: *const c_char,
    out: *mut *mut c_char,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let body = ffi_try!(str_arg(body, "body"));
        let form: SearchAndFilterForm<TimeSeriesFilter> =
            ffi_try!(parse(body, "timeseries search request"));
        respond(out, client.run(client.api.time_series.search(&form)), false)
    })
}

/// `POST /timeseries/filter`: structured, AND-combined filtering. `body` is
/// `{"filter":{…},"limit":…,"cursor":…}`; page with the response's `nextCursor`.
#[no_mangle]
pub unsafe extern "C" fn datahub_timeseries_filter_json(
    client: *const datahub_client,
    body: *const c_char,
    out: *mut *mut c_char,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let body = ffi_try!(str_arg(body, "body"));
        let form: TimeSeriesFilterForm = ffi_try!(parse(body, "timeseries filter request"));
        respond(out, client.run(client.api.time_series.filter(&form)), false)
    })
}
