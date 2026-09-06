// SPDX-License-Identifier: Apache-2.0
//! Datapoints: the typed hot path. Arrays of plain C structs in, arrays of plain C structs out,
//! and no JSON library needed on the caller's side.

use std::ffi::c_char;

use chrono::{DateTime, Utc};
use intellistream_datahub_sdk::generic::{
    DataWrapper, Datapoint, DatapointString, DatapointsCollection, IdAndExtId, RetrieveFilter,
};
use intellistream_datahub_sdk::http::ResponseError;

use crate::client::datahub_client;
use crate::error::{datahub_status, fail, from_response_error, guard};
use crate::json::{parse, respond};
use crate::util::{mut_arg, nonempty, ref_arg, str_arg};

/// The `start_ms`/`end_ms` sentinel, exported to C as `DATAHUB_TIME_UNSET` by build.rs.
pub(crate) const TIME_UNSET: i64 = i64::MIN;

/// One numeric datapoint to ingest.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct datahub_datapoint {
    /// Unix epoch milliseconds, UTC.
    pub timestamp_ms: i64,
    /// Must be finite.
    pub value: f64,
}

/// One datapoint as read back. Aggregates the api did not send are NaN, so a plain read never
/// needs a presence flag: a raw read carries `value`, an aggregated read the others.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct datahub_datapoint_agg {
    /// Unix epoch milliseconds, UTC.
    pub timestamp_ms: i64,
    pub value: f64,
    pub min: f64,
    pub max: f64,
    pub average: f64,
    pub sum: f64,
}

impl datahub_datapoint_agg {
    fn from_core(dp: &Datapoint) -> Self {
        datahub_datapoint_agg {
            timestamp_ms: dp.timestamp.timestamp_millis(),
            value: dp.value.unwrap_or(f64::NAN),
            min: dp.min.unwrap_or(f64::NAN),
            max: dp.max.unwrap_or(f64::NAN),
            average: dp.average.unwrap_or(f64::NAN),
            sum: dp.sum.unwrap_or(f64::NAN),
        }
    }
}

/// The core answers a spooled ingest with 202 and no items; the caller must be able to tell.
fn ingest_status(result: Result<DataWrapper<String>, ResponseError>) -> datahub_status {
    match result {
        Ok(wrapper) if wrapper.get_http_status_code() == Some(202) => {
            datahub_status::DATAHUB_BUFFERED
        }
        Ok(_) => datahub_status::DATAHUB_OK,
        Err(e) => from_response_error(&e),
    }
}

unsafe fn send(
    client: &datahub_client,
    external_id: &str,
    datapoints: Vec<DatapointString>,
) -> datahub_status {
    if datapoints.is_empty() {
        return datahub_status::DATAHUB_OK;
    }
    let mut collection = DatapointsCollection::from_external_id(external_id);
    collection.datapoints = datapoints;
    let mut request = DataWrapper::from_vec(vec![collection]);
    ingest_status(client.run(client.api.time_series.insert_datapoints(&mut request)))
}

/// Ingest `count` numeric datapoints into the series with this external id.
///
/// `DATAHUB_OK` means they reached the server. With buffering enabled, `DATAHUB_BUFFERED` means
/// the server could not be reached (or refused the credential) and they are on disk, to be sent
/// on a later call: the on-disk backlog always goes first, so ordering holds. Retries are safe —
/// the server dedups on (series, timestamp). A non-finite value is `DATAHUB_INVALID_ARGUMENT`
/// before anything is sent. `count` of 0 is a no-op.
///
/// The spool's retention window is measured on each datapoint's own timestamp, not on when it
/// was spooled: a backfill older than the window (72 h by default) is reported `DATAHUB_BUFFERED`
/// but does not survive in the spool. Backfill old data with buffering off, or widen the window.
#[no_mangle]
pub unsafe extern "C" fn datahub_datapoints_insert(
    client: *const datahub_client,
    external_id: *const c_char,
    points: *const datahub_datapoint,
    count: usize,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let external_id = ffi_try!(nonempty(
            ffi_try!(str_arg(external_id, "external_id")),
            "external_id"
        ));
        if count == 0 {
            return datahub_status::DATAHUB_OK;
        }
        if points.is_null() {
            return fail(
                datahub_status::DATAHUB_INVALID_ARGUMENT,
                0,
                format!("points must not be NULL when count is {count}"),
            );
        }
        let points = std::slice::from_raw_parts(points, count);
        let mut datapoints = Vec::with_capacity(count);
        for (i, point) in points.iter().enumerate() {
            if !point.value.is_finite() {
                return fail(
                    datahub_status::DATAHUB_INVALID_ARGUMENT,
                    0,
                    format!("points[{i}].value is not finite"),
                );
            }
            datapoints.push(DatapointString::new(
                &point.timestamp_ms.to_string(),
                &point.value.to_string(),
            ));
        }
        send(client, external_id, datapoints)
    })
}

/// Ingest `count` string-valued datapoints (for text-typed series). `timestamps_ms[i]` pairs
/// with `values[i]`. Same status contract as `datahub_datapoints_insert`.
#[no_mangle]
pub unsafe extern "C" fn datahub_datapoints_insert_str(
    client: *const datahub_client,
    external_id: *const c_char,
    timestamps_ms: *const i64,
    values: *const *const c_char,
    count: usize,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let external_id = ffi_try!(nonempty(
            ffi_try!(str_arg(external_id, "external_id")),
            "external_id"
        ));
        if count == 0 {
            return datahub_status::DATAHUB_OK;
        }
        if timestamps_ms.is_null() || values.is_null() {
            return fail(
                datahub_status::DATAHUB_INVALID_ARGUMENT,
                0,
                format!("timestamps_ms and values must not be NULL when count is {count}"),
            );
        }
        let timestamps = std::slice::from_raw_parts(timestamps_ms, count);
        let mut datapoints = Vec::with_capacity(count);
        for (i, timestamp) in timestamps.iter().enumerate() {
            let value = ffi_try!(str_arg(*values.add(i), &format!("values[{i}]")));
            datapoints.push(DatapointString::new(&timestamp.to_string(), value));
        }
        send(client, external_id, datapoints)
    })
}

/// The most recent datapoint of a series, written into `*out`. `DATAHUB_NOT_FOUND` when the
/// series has no datapoints (or does not exist).
#[no_mangle]
pub unsafe extern "C" fn datahub_datapoints_latest(
    client: *const datahub_client,
    external_id: *const c_char,
    out: *mut datahub_datapoint_agg,
) -> datahub_status {
    guard(|| {
        let out = ffi_try!(mut_arg(out, "out"));
        let client = ffi_try!(ref_arg(client, "client"));
        let external_id = ffi_try!(nonempty(
            ffi_try!(str_arg(external_id, "external_id")),
            "external_id"
        ));

        let request = DataWrapper::from_vec(vec![IdAndExtId::from_external_id(external_id)]);
        match client.run(client.api.time_series.retrieve_latest_datapoint(&request)) {
            Ok(response) => {
                let latest = response
                    .get_items()
                    .iter()
                    .flat_map(|collection| collection.datapoints.iter())
                    .max_by_key(|dp| dp.timestamp);
                match latest {
                    Some(dp) => {
                        *out = datahub_datapoint_agg::from_core(dp);
                        datahub_status::DATAHUB_OK
                    }
                    None => fail(
                        datahub_status::DATAHUB_NOT_FOUND,
                        response.get_http_status_code().unwrap_or(0),
                        format!("no datapoints in time series {external_id:?}"),
                    ),
                }
            }
            Err(e) => from_response_error(&e),
        }
    })
}

/// Raw datapoints of a series inside a window. `start_ms` is inclusive, `end_ms` exclusive;
/// pass `DATAHUB_TIME_UNSET` to leave either end open. `limit` of 0 means the server default.
/// `*out` receives an array of `*out_count` points, newest last, released with
/// `datahub_datapoints_free`; both are 0/NULL when the window is empty. For aggregated reads
/// (`aggregates`, `granularity`, paging with `cursor`) use `datahub_datapoints_retrieve_json`.
#[no_mangle]
pub unsafe extern "C" fn datahub_datapoints_retrieve(
    client: *const datahub_client,
    external_id: *const c_char,
    start_ms: i64,
    end_ms: i64,
    limit: u64,
    out: *mut *mut datahub_datapoint_agg,
    out_count: *mut usize,
) -> datahub_status {
    guard(|| {
        let out = ffi_try!(mut_arg(out, "out"));
        let out_count = ffi_try!(mut_arg(out_count, "out_count"));
        *out = std::ptr::null_mut();
        *out_count = 0;
        let client = ffi_try!(ref_arg(client, "client"));
        let external_id = ffi_try!(nonempty(
            ffi_try!(str_arg(external_id, "external_id")),
            "external_id"
        ));

        let bound = |ms: i64, name: &str| -> Result<Option<DateTime<Utc>>, datahub_status> {
            if ms == TIME_UNSET {
                return Ok(None);
            }
            DateTime::<Utc>::from_timestamp_millis(ms)
                .map(Some)
                .ok_or_else(|| {
                    fail(
                        datahub_status::DATAHUB_INVALID_ARGUMENT,
                        0,
                        format!("{name} ({ms}) is not a valid epoch-millisecond timestamp"),
                    )
                })
        };
        let filter = RetrieveFilter {
            start: ffi_try!(bound(start_ms, "start_ms")),
            end: ffi_try!(bound(end_ms, "end_ms")),
            limit: (limit > 0).then_some(limit),
            external_id: Some(external_id.to_string()),
            ..Default::default()
        };
        let request = DataWrapper::from_vec(vec![filter]);
        match client.run(client.api.time_series.retrieve_datapoints(&request)) {
            Ok(response) => {
                let points: Vec<datahub_datapoint_agg> = response
                    .get_items()
                    .iter()
                    .flat_map(|collection| collection.datapoints.iter())
                    .map(datahub_datapoint_agg::from_core)
                    .collect();
                if !points.is_empty() {
                    let boxed = points.into_boxed_slice();
                    *out_count = boxed.len();
                    *out = Box::into_raw(boxed) as *mut datahub_datapoint_agg;
                }
                datahub_status::DATAHUB_OK
            }
            Err(e) => from_response_error(&e),
        }
    })
}

/// Release an array from `datahub_datapoints_retrieve`, with the count it came with. NULL is ignored.
#[no_mangle]
pub unsafe extern "C" fn datahub_datapoints_free(points: *mut datahub_datapoint_agg, count: usize) {
    if !points.is_null() && count > 0 {
        let slice: *mut [datahub_datapoint_agg] = std::ptr::slice_from_raw_parts_mut(points, count);
        drop(Box::from_raw(slice));
    }
}

/// `POST /timeseries/data/list` with the full request: `body` is
/// `{"items":[{"externalId":…,"start":…,"end":…,"limit":…,"aggregates":[…],"granularity":…,"cursor":…}]}`
/// and `*out` receives the response envelope as the api sent it.
#[no_mangle]
pub unsafe extern "C" fn datahub_datapoints_retrieve_json(
    client: *const datahub_client,
    body: *const c_char,
    out: *mut *mut c_char,
) -> datahub_status {
    guard(|| {
        let client = ffi_try!(ref_arg(client, "client"));
        let body = ffi_try!(str_arg(body, "body"));
        let request: DataWrapper<RetrieveFilter> =
            ffi_try!(parse(body, "datapoint retrieve request"));
        respond(
            out,
            client.run(client.api.time_series.retrieve_datapoints(&request)),
            false,
        )
    })
}
