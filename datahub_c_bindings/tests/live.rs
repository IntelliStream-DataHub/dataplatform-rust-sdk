// SPDX-License-Identifier: Apache-2.0
//! Against a live backend, configured by the repository's `.env` (or the process environment).
//! Each test prints `SKIP` and passes when there is no `BASE_URL`, so a checkout without a backend
//! is unaffected — the convention the core's `multi_tenant_integration.rs` follows.

mod common;

use std::ffi::CStr;
use std::time::{SystemTime, UNIX_EPOCH};

use common::*;
use intellistream_datahub::*;

/// The prefix the core's Rust suite uses, so the same cleanup sweeps cover what these leave behind.
const TEST_PREFIX: &str = "rust_sdk_c_";

fn unique(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{TEST_PREFIX}{name}_{}_{nanos}", std::process::id())
}

/// A config from the process environment plus the repository `.env`, or `None` to skip.
fn live_config() -> Option<Config> {
    let config = Config(datahub_config_from_env());
    let env_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(".env");
    if env_file.exists() {
        let path = cstr(env_file.to_str().unwrap());
        assert_eq!(
            unsafe { datahub_config_load_envfile(config.0, path.as_ptr()) },
            datahub_status::DATAHUB_OK,
            "{}",
            last_error()
        );
    }
    if config.get("BASE_URL").is_none() {
        println!("SKIP: no BASE_URL in the environment or ../.env");
        return None;
    }
    Some(config)
}

fn delete_series(client: &Client, external_id: &str) {
    let _ = client.request_json(
        "POST",
        "/timeseries/delete",
        Some(&format!(
            r#"{{"items":[{{"externalId":"{external_id}"}}]}}"#
        )),
    );
}

#[test]
fn datapoints_round_trip_through_a_real_backend() {
    let Some(config) = live_config() else { return };
    let client = config
        .build()
        .unwrap_or_else(|s| panic!("{s:?}: {}", last_error()));
    let external_id = unique("temperature");

    let (status, out) = client.json_call(
        datahub_timeseries_create_json,
        &format!(
            r#"{{"items":[{{"externalId":"{external_id}","name":"C SDK live test","unit":"°C"}}]}}"#
        ),
    );
    assert_eq!(status, datahub_status::DATAHUB_OK, "{}", last_error());
    let created: serde_json::Value = serde_json::from_str(&out.unwrap()).unwrap();
    assert_eq!(created["items"][0]["externalId"], external_id);

    let base = 1_700_000_000_000i64;
    let points = [
        point(base, 20.0),
        point(base + 1000, 21.0),
        point(base + 2000, 22.0),
    ];
    assert_eq!(
        client.insert(&external_id, &points),
        datahub_status::DATAHUB_OK,
        "{}",
        last_error()
    );

    // Ingest is asynchronous server-side; poll briefly for the latest datapoint.
    let id = cstr(&external_id);
    let mut latest = datahub_datapoint_agg {
        timestamp_ms: 0,
        value: 0.0,
        min: 0.0,
        max: 0.0,
        average: 0.0,
        sum: 0.0,
    };
    let mut status = datahub_status::DATAHUB_NOT_FOUND;
    for _ in 0..40 {
        status = unsafe { datahub_datapoints_latest(client.0, id.as_ptr(), &mut latest) };
        if status == datahub_status::DATAHUB_OK {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    assert_eq!(status, datahub_status::DATAHUB_OK, "{}", last_error());
    assert_eq!((latest.timestamp_ms, latest.value), (base + 2000, 22.0));

    let mut out = std::ptr::null_mut();
    let mut count = 0usize;
    let status = unsafe {
        datahub_datapoints_retrieve(
            client.0,
            id.as_ptr(),
            base,
            base + 3000,
            0,
            &mut out,
            &mut count,
        )
    };
    assert_eq!(status, datahub_status::DATAHUB_OK, "{}", last_error());
    assert_eq!(count, 3);
    let read = unsafe { std::slice::from_raw_parts(out, count) };
    assert_eq!(read.iter().map(|p| p.value).sum::<f64>(), 63.0);
    unsafe { datahub_datapoints_free(out, count) };

    let mut handle = std::ptr::null_mut();
    assert_eq!(
        unsafe { datahub_timeseries_get_by_external_id(client.0, id.as_ptr(), &mut handle) },
        datahub_status::DATAHUB_OK
    );
    assert_eq!(
        unsafe { borrow_string(datahub_timeseries_unit(handle)) }.as_deref(),
        Some("°C")
    );
    unsafe { datahub_timeseries_free(handle) };

    delete_series(&client, &external_id);
}

#[test]
fn a_listener_receives_what_is_ingested() {
    let Some(config) = live_config() else { return };
    let client = config
        .build()
        .unwrap_or_else(|s| panic!("{s:?}: {}", last_error()));
    let external_id = unique("stream");
    let subscription = unique("subscription");

    let (status, _) = client.json_call(
        datahub_timeseries_create_json,
        &format!(r#"{{"items":[{{"externalId":"{external_id}","name":"C SDK listener test"}}]}}"#),
    );
    assert_eq!(status, datahub_status::DATAHUB_OK, "{}", last_error());
    let created = client.request_json(
        "POST",
        "/subscriptions/create",
        Some(&format!(
            r#"{{"items":[{{"externalId":"{subscription}","name":"C SDK listener test","timeseries":[{{"externalId":"{external_id}"}}]}}]}}"#
        )),
    );
    assert!(created.is_ok(), "{:?}: {}", created, last_error());

    let ids = [cstr(&subscription)];
    let id_ptrs = [ids[0].as_ptr()];
    let mut listener = std::ptr::null_mut();
    assert_eq!(
        unsafe { datahub_listener_open(client.0, id_ptrs.as_ptr(), 1, &mut listener) },
        datahub_status::DATAHUB_OK,
        "{}",
        last_error()
    );

    let point_ts = 1_700_000_000_000i64;
    assert_eq!(
        client.insert(&external_id, &[point(point_ts, 42.0)]),
        datahub_status::DATAHUB_OK
    );

    let mut message = std::ptr::null_mut();
    let mut received = None;
    for _ in 0..12 {
        match unsafe { datahub_listener_next(listener, 5000, &mut message) } {
            datahub_status::DATAHUB_OK => {
                let object = unsafe { borrow_string(datahub_message_object(message)) }.unwrap();
                let ids = [unsafe { datahub_message_id(message) }];
                assert_eq!(
                    unsafe { datahub_listener_ack(listener, ids.as_ptr(), 1) },
                    datahub_status::DATAHUB_OK
                );
                if object == "DATAPOINTS" {
                    let mut points = std::ptr::null();
                    let mut count = 0usize;
                    assert_eq!(unsafe { datahub_message_series_count(message) }, 1);
                    assert_eq!(
                        unsafe {
                            datahub_message_series_datapoints(message, 0, &mut points, &mut count)
                        },
                        datahub_status::DATAHUB_OK
                    );
                    let points = unsafe { std::slice::from_raw_parts(points, count) };
                    received = Some((points[0].timestamp_ms, points[0].value));
                    unsafe { datahub_message_free(message) };
                    break;
                }
                unsafe { datahub_message_free(message) };
            }
            datahub_status::DATAHUB_TIMEOUT => continue,
            other => panic!(
                "{:?}: {}",
                unsafe { CStr::from_ptr(datahub_status_name(other)) },
                last_error()
            ),
        }
    }
    assert_eq!(
        unsafe { datahub_listener_close(listener) },
        datahub_status::DATAHUB_OK
    );
    assert_eq!(
        received,
        Some((point_ts, 42.0)),
        "the ingested datapoint was delivered"
    );

    let _ = client.request_json(
        "POST",
        "/subscriptions/delete",
        Some(&format!(
            r#"{{"items":[{{"externalId":"{subscription}"}}]}}"#
        )),
    );
    delete_series(&client, &external_id);
}
