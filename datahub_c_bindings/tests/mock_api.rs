// SPDX-License-Identifier: Apache-2.0
//! End to end against a mock of the api: what goes on the wire, and how answers come back.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common::*;
use intellistream_datahub::*;

const SERIES: &str = r#"{"id":"42","externalId":"pump-1/temperature","name":"Pump 1 temperature","unit":"°C","unitExternalId":"deg_c","valueType":"float","dataSetId":"7","createdTime":"2026-01-01T00:00:00Z","lastUpdatedTime":"2026-01-01T00:00:00Z"}"#;

fn series_by_route(request: &Request) -> (u16, String) {
    match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/timeseries/data") => (204, String::new()),
        ("POST", "/timeseries/byids") => {
            if request.body.contains("pump-1/temperature") {
                (200, format!(r#"{{"items":[{SERIES}]}}"#))
            } else {
                (200, r#"{"items":[]}"#.to_string())
            }
        }
        ("POST", "/timeseries/data/latest") => (
            200,
            r#"{"items":[{"externalId":"pump-1/temperature","datapoints":[{"timestamp":"2026-01-01T00:00:00Z","value":1.5}]}]}"#.to_string(),
        ),
        ("POST", "/timeseries/data/list") => (
            200,
            r#"{"items":[{"externalId":"pump-1/temperature","datapoints":[{"timestamp":"2026-01-01T00:00:00Z","value":1.5},{"timestamp":"2026-01-01T01:00:00Z","min":1.0,"max":2.0,"average":1.5,"sum":3.0}],"nextCursor":"page-2"}]}"#.to_string(),
        ),
        ("POST", "/timeseries/create") | ("POST", "/timeseries/search") | ("POST", "/timeseries/filter") => {
            (200, format!(r#"{{"items":[{SERIES}],"nextCursor":"more"}}"#))
        }
        ("POST", "/events/create") => (200, request.body.clone()),
        ("POST", "/events/filter") => (200, r#"{"items":[]}"#.to_string()),
        ("GET", "/events/count?type=Alarm") => (200, r#"{"count":3}"#.to_string()),
        ("POST", "/anything") => (200, request.body.clone()),
        ("POST", "/nothing") => (204, String::new()),
        _ => (404, format!(r#"{{"error":"no route for {} {}"}}"#, request.method, request.path)),
    }
}

#[test]
fn insert_posts_epoch_millis_strings_with_the_bearer_token() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    let points = [
        point(1_700_000_000_000, 21.5),
        point(1_700_000_001_000, -3.0),
    ];
    assert_eq!(
        client.insert("pump-1/temperature", &points),
        datahub_status::DATAHUB_OK
    );

    let request = server.last_request();
    assert_eq!(
        (request.method.as_str(), request.path.as_str()),
        ("POST", "/timeseries/data")
    );
    assert_eq!(request.header("authorization"), Some("Bearer test-token"));
    let body = request.json();
    assert_eq!(body["items"][0]["externalId"], "pump-1/temperature");
    assert_eq!(
        body["items"][0]["datapoints"][0]["timestamp"],
        "1700000000000"
    );
    assert_eq!(body["items"][0]["datapoints"][0]["value"], "21.5");
    assert_eq!(body["items"][0]["datapoints"][1]["value"], "-3");
    assert!(body["items"][0]["id"].is_null(), "no numeric id was given");
}

#[test]
fn insert_str_carries_text_values_unchanged() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    let id = cstr("pump-1/temperature");
    let timestamps = [1_700_000_000_000i64, 1_700_000_001_000];
    let values = [cstr("OPEN"), cstr("CLOSED")];
    let value_ptrs = [values[0].as_ptr(), values[1].as_ptr()];
    let status = unsafe {
        datahub_datapoints_insert_str(
            client.0,
            id.as_ptr(),
            timestamps.as_ptr(),
            value_ptrs.as_ptr(),
            2,
        )
    };
    assert_eq!(status, datahub_status::DATAHUB_OK);
    let body = server.last_request().json();
    assert_eq!(body["items"][0]["datapoints"][1]["value"], "CLOSED");
    assert_eq!(
        body["items"][0]["datapoints"][1]["timestamp"],
        "1700000001000"
    );
}

#[test]
fn get_by_external_id_exposes_the_definition() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    let id = cstr("pump-1/temperature");
    let mut out = std::ptr::null_mut();
    assert_eq!(
        unsafe { datahub_timeseries_get_by_external_id(client.0, id.as_ptr(), &mut out) },
        datahub_status::DATAHUB_OK
    );
    assert!(!out.is_null());
    unsafe {
        assert_eq!(datahub_timeseries_id(out), 42);
        assert_eq!(datahub_timeseries_data_set_id(out), 7);
        assert_eq!(
            borrow_string(datahub_timeseries_external_id(out)).as_deref(),
            Some("pump-1/temperature")
        );
        assert_eq!(
            borrow_string(datahub_timeseries_name(out)).as_deref(),
            Some("Pump 1 temperature")
        );
        assert_eq!(
            borrow_string(datahub_timeseries_unit(out)).as_deref(),
            Some("°C")
        );
        assert_eq!(
            borrow_string(datahub_timeseries_unit_external_id(out)).as_deref(),
            Some("deg_c")
        );
        assert_eq!(
            borrow_string(datahub_timeseries_value_type(out)).as_deref(),
            Some("float")
        );
        let json: serde_json::Value =
            serde_json::from_str(&borrow_string(datahub_timeseries_json(out)).unwrap()).unwrap();
        assert_eq!(json["externalId"], "pump-1/temperature");
        datahub_timeseries_free(out);
    }
    let body = server.last_request().json();
    assert_eq!(body["items"][0]["externalId"], "pump-1/temperature");

    let unknown = cstr("no-such-series");
    let mut out = std::ptr::null_mut();
    assert_eq!(
        unsafe { datahub_timeseries_get_by_external_id(client.0, unknown.as_ptr(), &mut out) },
        datahub_status::DATAHUB_NOT_FOUND
    );
    assert!(out.is_null());
    assert_eq!(datahub_last_http_status(), 200);
    assert_eq!(
        last_error(),
        "no time series with external id \"no-such-series\""
    );
}

#[test]
fn latest_and_retrieve_map_absent_aggregates_to_nan() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    let id = cstr("pump-1/temperature");

    let mut latest = datahub_datapoint_agg {
        timestamp_ms: 0,
        value: 0.0,
        min: 0.0,
        max: 0.0,
        average: 0.0,
        sum: 0.0,
    };
    assert_eq!(
        unsafe { datahub_datapoints_latest(client.0, id.as_ptr(), &mut latest) },
        datahub_status::DATAHUB_OK
    );
    assert_eq!(latest.timestamp_ms, 1_767_225_600_000);
    assert_eq!(latest.value, 1.5);
    assert!(
        latest.min.is_nan()
            && latest.max.is_nan()
            && latest.average.is_nan()
            && latest.sum.is_nan()
    );

    let mut out = std::ptr::null_mut();
    let mut count = 0usize;
    let status = unsafe {
        datahub_datapoints_retrieve(
            client.0,
            id.as_ptr(),
            1_767_225_600_000,
            DATAHUB_TIME_UNSET,
            500,
            &mut out,
            &mut count,
        )
    };
    assert_eq!(status, datahub_status::DATAHUB_OK);
    assert_eq!(count, 2);
    let points = unsafe { std::slice::from_raw_parts(out, count) };
    assert_eq!(points[0].value, 1.5);
    assert!(points[1].value.is_nan());
    assert_eq!(
        (
            points[1].min,
            points[1].max,
            points[1].average,
            points[1].sum
        ),
        (1.0, 2.0, 1.5, 3.0)
    );
    unsafe { datahub_datapoints_free(out, count) };

    let body = server.last_request().json();
    assert_eq!(body["items"][0]["externalId"], "pump-1/temperature");
    assert_eq!(body["items"][0]["start"], "2026-01-01T00:00:00Z");
    assert!(
        body["items"][0]["end"].is_null(),
        "an unset bound is omitted"
    );
    assert_eq!(body["items"][0]["limit"], 500);

    // An out-of-range bound is caught before any request.
    let requests_before = server.requests().len();
    let status = unsafe {
        datahub_datapoints_retrieve(
            client.0,
            id.as_ptr(),
            i64::MAX,
            DATAHUB_TIME_UNSET,
            0,
            &mut out,
            &mut count,
        )
    };
    assert_eq!(status, datahub_status::DATAHUB_INVALID_ARGUMENT);
    assert_eq!(server.requests().len(), requests_before);
}

/// `DATAHUB_TIME_UNSET` is defined in the header by build.rs; the Rust side uses the same value.
const DATAHUB_TIME_UNSET: i64 = i64::MIN;

#[test]
fn retrieve_json_keeps_the_next_cursor() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    let (status, out) = client.json_call(
        datahub_datapoints_retrieve_json,
        r#"{"items":[{"externalId":"pump-1/temperature","aggregates":["average"],"granularity":"1h"}]}"#,
    );
    assert_eq!(status, datahub_status::DATAHUB_OK);
    let out: serde_json::Value = serde_json::from_str(&out.unwrap()).unwrap();
    assert_eq!(out["items"][0]["datapoints"][1]["average"], 1.5);
    assert_eq!(out["items"][0]["nextCursor"], "page-2");
    let sent = server.last_request().json();
    assert_eq!(sent["items"][0]["aggregates"][0], "average");
    assert_eq!(sent["items"][0]["granularity"], "1h");
}

#[test]
fn events_create_json_stamps_ids_and_returns_the_envelope() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    let (status, out) = client.json_call(
        datahub_events_create_json,
        r#"{"items":[{"externalId":"alarm-1","type":"Alarm","eventTime":"2026-01-01T00:00:00Z","metadata":{"severity":"high"}}]}"#,
    );
    assert_eq!(status, datahub_status::DATAHUB_OK);
    let sent = server.last_request().json();
    assert_eq!(sent["items"][0]["type"], "Alarm");
    assert_eq!(sent["items"][0]["metadata"]["severity"], "high");
    let stamped = sent["items"][0]["id"]
        .as_str()
        .expect("a UUID was stamped before the first send");
    assert_eq!(stamped.len(), 36);
    let out: serde_json::Value = serde_json::from_str(&out.unwrap()).unwrap();
    assert_eq!(out["items"][0]["id"], stamped);
    assert!(out.get("nextCursor").is_none());
}

#[test]
fn events_filter_json_sends_the_form_as_given() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    let (status, out) = client.json_call(
        datahub_events_filter_json,
        r#"{"filter":{"type":["Alarm"]},"limit":10}"#,
    );
    assert_eq!(status, datahub_status::DATAHUB_OK);
    assert_eq!(out.as_deref(), Some(r#"{"items":[]}"#));
    let sent = server.last_request().json();
    assert_eq!(sent["limit"], 10);
    assert_eq!(sent["filter"]["type"][0], "Alarm");
}

#[test]
fn timeseries_json_calls_hit_their_endpoints_and_keep_paging() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();

    let (status, out) = client.json_call(datahub_timeseries_create_json, r#"{"items":[{"externalId":"pump-1/temperature","name":"Pump 1 temperature","unit":"°C"}]}"#);
    assert_eq!(status, datahub_status::DATAHUB_OK);
    assert_eq!(server.last_request().path, "/timeseries/create");
    let out: serde_json::Value = serde_json::from_str(&out.unwrap()).unwrap();
    assert_eq!(out["items"][0]["id"], "42");
    assert_eq!(out["nextCursor"], "more");

    let (status, _) = client.json_call(
        datahub_timeseries_search_json,
        r#"{"search":{"query":"pump"},"limit":5}"#,
    );
    assert_eq!(status, datahub_status::DATAHUB_OK);
    let sent = server.last_request();
    assert_eq!(sent.path, "/timeseries/search");
    assert_eq!(sent.json()["search"]["query"], "pump");

    let (status, _) = client.json_call(
        datahub_timeseries_filter_json,
        r#"{"filter":{"unit":["°C"]},"limit":5}"#,
    );
    assert_eq!(status, datahub_status::DATAHUB_OK);
    let sent = server.last_request();
    assert_eq!(sent.path, "/timeseries/filter");
    assert_eq!(sent.json()["filter"]["unit"][0], "°C");
}

#[test]
fn request_json_is_a_raw_authenticated_call() {
    let server = MockServer::start(series_by_route);
    let client = Config::for_server(&server.base_url, None).build().unwrap();

    assert_eq!(
        client
            .request_json("get", "events/count?type=Alarm", None)
            .unwrap(),
        r#"{"count":3}"#
    );
    let sent = server.last_request();
    assert_eq!(
        (sent.method.as_str(), sent.path.as_str()),
        ("GET", "/events/count?type=Alarm")
    );
    assert_eq!(sent.header("authorization"), Some("Bearer test-token"));

    assert_eq!(
        client
            .request_json("POST", "/anything", Some(r#"{"a":[1,2]}"#))
            .unwrap(),
        r#"{"a":[1,2]}"#
    );
    assert_eq!(
        client.request_json("POST", "/nothing", None).unwrap(),
        "",
        "a 204 is an empty body"
    );
    assert_eq!(
        server.last_request().body,
        "{}",
        "a NULL POST body is sent as {{}}"
    );

    assert_eq!(
        client.request_json("GET", "/missing", None).unwrap_err(),
        datahub_status::DATAHUB_HTTP
    );
    assert_eq!(datahub_last_http_status(), 404);
    assert!(
        last_error().contains("no route for GET /missing"),
        "{}",
        last_error()
    );
}

#[test]
fn a_401_is_an_auth_failure_and_a_500_is_http() {
    let server = MockServer::start(|request: &Request| {
        if request.path.ends_with("/data") {
            (401, String::new())
        } else {
            (500, r#"{"error":"boom"}"#.to_string())
        }
    });
    let client = Config::for_server(&server.base_url, None).build().unwrap();
    assert_eq!(
        client.insert("pump", &[point(1, 1.0)]),
        datahub_status::DATAHUB_AUTH
    );
    assert_eq!(datahub_last_http_status(), 401);
    assert_eq!(
        client
            .request_json("GET", "/events/count", None)
            .unwrap_err(),
        datahub_status::DATAHUB_HTTP
    );
    assert_eq!(datahub_last_http_status(), 500);
    assert!(last_error().contains("boom"), "{}", last_error());
}

#[test]
fn a_spooled_backlog_is_sent_first_once_the_server_is_back() {
    let down = Arc::new(AtomicBool::new(true));
    let server = {
        let down = down.clone();
        MockServer::start(move |request: &Request| {
            if down.load(Ordering::Relaxed) {
                (503, "down".to_string())
            } else {
                series_by_route(request)
            }
        })
    };
    let spool = tempfile::tempdir().unwrap();
    let client = Config::for_server(&server.base_url, Some(spool.path()))
        .build()
        .unwrap();

    // Retention is measured on the data's own timestamp, so spooled records must be recent.
    let now = now_ms();
    assert_eq!(
        client.insert("pump-1/temperature", &[point(now, 1.0)]),
        datahub_status::DATAHUB_BUFFERED
    );
    let (status, _) = client.json_call(
        datahub_events_create_json,
        &format!(
            r#"{{"items":[{{"externalId":"alarm-1","type":"Alarm","eventTime":"{}"}}]}}"#,
            chrono::Utc::now().to_rfc3339()
        ),
    );
    assert_eq!(status, datahub_status::DATAHUB_BUFFERED);
    assert_eq!(client.buffered_count(), 2);
    assert_eq!(client.flush(), datahub_status::DATAHUB_BUFFERED);

    down.store(false, Ordering::Relaxed);
    assert_eq!(client.flush(), datahub_status::DATAHUB_OK);
    assert_eq!(client.buffered_count(), 0);
    let delivered: Vec<String> = server
        .requests()
        .into_iter()
        .filter(|r| r.header("content-length") != Some("0"))
        .filter_map(|r| {
            serde_json::from_str::<serde_json::Value>(&r.body)
                .ok()
                .map(|_| r.path)
        })
        .collect();
    assert!(
        delivered.contains(&"/timeseries/data".to_string()),
        "{delivered:?}"
    );
    assert!(
        delivered.contains(&"/events/create".to_string()),
        "{delivered:?}"
    );

    // And a later insert goes straight through with nothing left behind.
    assert_eq!(
        client.insert("pump-1/temperature", &[point(now + 1000, 2.0)]),
        datahub_status::DATAHUB_OK
    );
    assert_eq!(client.buffered_count(), 0);
}

#[test]
fn spooled_records_older_than_the_retention_window_are_dropped() {
    let server = MockServer::start(always(503, "down"));
    let spool = tempfile::tempdir().unwrap();
    let client = Config::for_server(&server.base_url, Some(spool.path()))
        .build()
        .unwrap();
    // Reported as buffered — the call succeeded — but a 1970 datapoint is outside the 72 h window
    // and never survives in the spool. Backfills older than the window need buffering off.
    assert_eq!(
        client.insert("pump-1/temperature", &[point(1, 1.0)]),
        datahub_status::DATAHUB_BUFFERED
    );
    assert_eq!(client.buffered_count(), 0);
}
