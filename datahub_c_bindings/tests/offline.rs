// SPDX-License-Identifier: Apache-2.0
//! The boundary and the offline paths: nothing here needs a network.

mod common;

use std::ffi::CStr;

use common::*;
use intellistream_datahub::*;

/// A port nothing listens on: connection refused, immediately.
const UNREACHABLE: &str = "http://127.0.0.1:9";

#[test]
fn version_is_the_crate_version() {
    let version = unsafe { CStr::from_ptr(datahub_version()) }
        .to_str()
        .unwrap();
    assert_eq!(version, env!("CARGO_PKG_VERSION"));
}

#[test]
fn a_fresh_thread_has_an_empty_error_not_a_null_one() {
    let text = std::thread::spawn(|| {
        let ptr = datahub_last_error();
        assert!(!ptr.is_null());
        last_error()
    })
    .join()
    .unwrap();
    assert_eq!(text, "");
}

#[test]
fn null_handles_are_invalid_arguments_not_crashes() {
    let mut out = std::ptr::null_mut();
    assert_eq!(
        unsafe { datahub_client_new(std::ptr::null(), &mut out) },
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert!(out.is_null());
    assert_eq!(last_error(), "config must not be NULL");

    let config = Config::new();
    assert_eq!(
        unsafe { datahub_client_new(config.0, std::ptr::null_mut()) },
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert_eq!(last_error(), "out must not be NULL");

    assert_eq!(
        unsafe {
            datahub_datapoints_insert(std::ptr::null(), cstr("x").as_ptr(), std::ptr::null(), 0)
        },
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert_eq!(
        unsafe { datahub_client_buffered_count(std::ptr::null()) },
        0
    );
    assert_eq!(unsafe { datahub_timeseries_id(std::ptr::null()) }, 0);
    assert!(unsafe { datahub_timeseries_name(std::ptr::null()) }.is_null());

    // Every free tolerates NULL.
    unsafe {
        datahub_string_free(std::ptr::null_mut());
        datahub_config_free(std::ptr::null_mut());
        datahub_client_free(std::ptr::null_mut());
        datahub_timeseries_free(std::ptr::null_mut());
        datahub_message_free(std::ptr::null_mut());
        datahub_datapoints_free(std::ptr::null_mut(), 0);
        assert_eq!(
            datahub_listener_close(std::ptr::null_mut()),
            datahub_status::DATAHUB_OK
        );
    }
}

#[test]
fn invalid_utf8_and_empty_ids_are_rejected_before_any_request() {
    let config = Config::for_server(UNREACHABLE, None);
    let client = config.build().unwrap();
    let bad = b"pump-\xff\0";
    assert_eq!(
        unsafe {
            datahub_datapoints_insert(client.0, bad.as_ptr() as *const _, std::ptr::null(), 0)
        },
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert!(
        last_error().starts_with("external_id is not valid UTF-8"),
        "{}",
        last_error()
    );

    assert_eq!(
        client.insert("   ", &[point(1, 1.0)]),
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert_eq!(last_error(), "external_id must not be empty");

    assert_eq!(
        client.insert("pump", &[point(1, 1.0), point(2, f64::NAN)]),
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert_eq!(last_error(), "points[1].value is not finite");

    // count > 0 with a NULL array
    let id = cstr("pump");
    assert_eq!(
        unsafe { datahub_datapoints_insert(client.0, id.as_ptr(), std::ptr::null(), 3) },
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    // count == 0 is a no-op, not an error
    assert_eq!(client.insert("pump", &[]), datahub_status::DATAHUB_OK);
}

#[test]
fn a_config_without_base_url_is_a_config_error_with_the_cores_message() {
    let config = Config::new();
    config.set("TOKEN", "t");
    assert_eq!(config.build().unwrap_err(), datahub_status::DATAHUB_CONFIG);
    assert!(
        last_error().contains("BASE_URL is not set"),
        "{}",
        last_error()
    );
}

#[test]
fn a_malformed_token_uri_is_a_config_error_not_a_panic() {
    let config = Config::for_server(UNREACHABLE, None);
    let (id, secret, uri) = (cstr("id"), cstr("secret"), cstr("not a url"));
    assert_eq!(
        unsafe {
            datahub_config_set_client_credentials(
                config.0,
                id.as_ptr(),
                secret.as_ptr(),
                uri.as_ptr(),
            )
        },
        datahub_status::DATAHUB_OK
    );
    assert_eq!(config.build().unwrap_err(), datahub_status::DATAHUB_CONFIG);
    assert!(last_error().contains("TOKEN_URI"), "{}", last_error());
}

#[test]
fn config_set_with_a_null_value_removes_the_key() {
    let config = Config::new();
    config.set("SCOPE", "organization:*");
    assert_eq!(config.get("SCOPE").as_deref(), Some("organization:*"));
    let key = cstr("SCOPE");
    assert_eq!(
        unsafe { datahub_config_set(config.0, key.as_ptr(), std::ptr::null()) },
        datahub_status::DATAHUB_OK
    );
    assert_eq!(config.get("SCOPE"), None);
    let empty = cstr("  ");
    assert_eq!(
        unsafe { datahub_config_set(config.0, empty.as_ptr(), key.as_ptr()) },
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
}

#[test]
fn typed_setters_land_on_the_documented_keys() {
    let config = Config::new();
    let value = cstr("v");
    unsafe {
        assert_eq!(
            datahub_config_set_base_url(config.0, value.as_ptr()),
            datahub_status::DATAHUB_OK
        );
        assert_eq!(
            datahub_config_set_scope(config.0, value.as_ptr()),
            datahub_status::DATAHUB_OK
        );
        assert_eq!(
            datahub_config_set_audience(config.0, value.as_ptr()),
            datahub_status::DATAHUB_OK
        );
        assert_eq!(
            datahub_config_set_assertion_grant(config.0, value.as_ptr()),
            datahub_status::DATAHUB_OK
        );
        assert_eq!(
            datahub_config_set_assertion_credentials(
                config.0,
                value.as_ptr(),
                value.as_ptr(),
                value.as_ptr()
            ),
            datahub_status::DATAHUB_OK
        );
        assert_eq!(
            datahub_config_set_buffer_retention_secs(config.0, 3600),
            datahub_status::DATAHUB_OK
        );
        assert_eq!(
            datahub_config_set_buffer_max_bytes(config.0, 0),
            datahub_status::DATAHUB_INVALID_ARGUMENT
        );
        assert_eq!(
            datahub_config_set_buffer_retention_secs(config.0, -1),
            datahub_status::DATAHUB_INVALID_ARGUMENT
        );
    }
    for key in [
        "BASE_URL",
        "SCOPE",
        "AUDIENCE",
        "ASSERTION_GRANT",
        "ASSERTION_CLIENT_ID",
        "ASSERTION_CLIENT_SECRET",
        "ASSERTION_TOKEN_URI",
    ] {
        assert_eq!(config.get(key).as_deref(), Some("v"), "{key}");
    }
    assert_eq!(config.get("BUFFER_RETENTION_SECS").as_deref(), Some("3600"));
    assert_eq!(config.get("ENABLE_BUFFERING").as_deref(), Some("true"));
}

#[test]
fn an_env_file_is_read_without_touching_the_process_environment() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("gateway.env");
    std::fs::write(
        &file,
        "# gateway\nBASE_URL=https://datahub.example.com\nexport TOKEN='abc def'\nSCOPE=\"organization:*\" # tenant\nBUFFER_DIR=/var/spool/datahub\n\n",
    )
    .unwrap();
    let config = Config::new();
    let path = cstr(file.to_str().unwrap());
    assert_eq!(
        unsafe { datahub_config_load_envfile(config.0, path.as_ptr()) },
        datahub_status::DATAHUB_OK
    );
    assert_eq!(
        config.get("BASE_URL").as_deref(),
        Some("https://datahub.example.com")
    );
    assert_eq!(config.get("TOKEN").as_deref(), Some("abc def"));
    assert_eq!(config.get("SCOPE").as_deref(), Some("organization:*"));
    assert_eq!(
        config.get("BUFFER_DIR").as_deref(),
        Some("/var/spool/datahub")
    );
    assert!(
        std::env::var("BUFFER_DIR").is_err(),
        "the process environment must stay untouched"
    );

    let missing = cstr(dir.path().join("nope.env").to_str().unwrap());
    assert_eq!(
        unsafe { datahub_config_load_envfile(config.0, missing.as_ptr()) },
        datahub_status::DATAHUB_IO
    );
    assert!(
        last_error().starts_with("cannot read env file"),
        "{}",
        last_error()
    );

    std::fs::write(&file, "BASE_URL\n").unwrap();
    assert_eq!(
        unsafe { datahub_config_load_envfile(config.0, path.as_ptr()) },
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert!(last_error().contains("line 1"), "{}", last_error());
}

#[test]
fn from_env_snapshots_the_process_environment() {
    std::env::set_var("DATAHUB_C_TEST_MARKER", "present");
    let config = Config(datahub_config_from_env());
    assert_eq!(
        config.get("DATAHUB_C_TEST_MARKER").as_deref(),
        Some("present")
    );
    std::env::remove_var("DATAHUB_C_TEST_MARKER");
    assert_eq!(
        config.get("DATAHUB_C_TEST_MARKER").as_deref(),
        Some("present"),
        "a snapshot, not a live view"
    );
}

#[test]
fn with_buffering_an_unreachable_server_spools_to_disk() {
    let spool = tempfile::tempdir().unwrap();
    let config = Config::for_server(UNREACHABLE, Some(spool.path()));
    let client = config.build().unwrap();

    let now = now_ms();
    let points = [point(now, 21.5), point(now + 1000, 21.6)];
    assert_eq!(
        client.insert("pump-1/temperature", &points),
        datahub_status::DATAHUB_BUFFERED
    );
    assert_eq!(client.buffered_count(), 2);
    let datapoints_dir = spool.path().join("datapoints");
    let segments: Vec<_> = std::fs::read_dir(&datapoints_dir)
        .expect("the spool directory exists")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        segments.iter().any(|name| name.ends_with(".ndjson")),
        "an active segment is on disk: {segments:?}"
    );

    // Still down: the backlog stays, and so does the count.
    assert_eq!(client.flush(), datahub_status::DATAHUB_BUFFERED);
    assert_eq!(client.buffered_count(), 2);

    // Buffering is per client: another client on the same directory sees the backlog on flush.
    drop(client);
    let again = Config::for_server(UNREACHABLE, Some(spool.path()))
        .build()
        .unwrap();
    assert_eq!(again.flush(), datahub_status::DATAHUB_BUFFERED);
    assert_eq!(
        again.buffered_count(),
        2,
        "the spool was recovered from disk"
    );
}

#[test]
fn without_buffering_an_unreachable_server_is_an_http_503() {
    let config = Config::for_server(UNREACHABLE, None);
    let client = config.build().unwrap();
    assert_eq!(
        client.insert("pump", &[point(1, 1.0)]),
        datahub_status::DATAHUB_HTTP
    );
    assert_eq!(datahub_last_http_status(), 503);
    assert!(last_error().starts_with("503"), "{}", last_error());
    assert_eq!(client.buffered_count(), 0);
    assert_eq!(
        client.flush(),
        datahub_status::DATAHUB_OK,
        "nothing to flush when buffering is off"
    );
}

#[test]
fn a_token_that_cannot_be_minted_is_an_auth_failure_or_a_buffered_ingest() {
    let credentials = |config: &Config| {
        let (id, secret, uri) = (
            cstr("gateway"),
            cstr("secret"),
            cstr("http://127.0.0.1:9/token"),
        );
        assert_eq!(
            unsafe {
                datahub_config_set_client_credentials(
                    config.0,
                    id.as_ptr(),
                    secret.as_ptr(),
                    uri.as_ptr(),
                )
            },
            datahub_status::DATAHUB_OK
        );
    };

    let config = Config::new();
    config.set("BASE_URL", UNREACHABLE);
    credentials(&config);
    let client = config.build().unwrap();
    assert_eq!(
        client.insert("pump", &[point(1, 1.0)]),
        datahub_status::DATAHUB_AUTH
    );
    assert_eq!(datahub_last_http_status(), 401);
    assert!(
        last_error().contains("failed to get api token"),
        "{}",
        last_error()
    );

    // With buffering on, an auth failure is recoverable out of band, so the data is kept.
    let spool = tempfile::tempdir().unwrap();
    let config = Config::new();
    config.set("BASE_URL", UNREACHABLE);
    credentials(&config);
    let dir = cstr(spool.path().to_str().unwrap());
    assert_eq!(
        unsafe { datahub_config_set_buffer_dir(config.0, dir.as_ptr()) },
        datahub_status::DATAHUB_OK
    );
    let client = config.build().unwrap();
    assert_eq!(
        client.insert("pump", &[point(now_ms(), 1.0)]),
        datahub_status::DATAHUB_BUFFERED
    );
    assert_eq!(client.buffered_count(), 1);
}

#[test]
fn request_json_validates_its_arguments_locally() {
    let config = Config::for_server(UNREACHABLE, None);
    let client = config.build().unwrap();
    assert_eq!(
        client.request_json("PUT", "/events", None).unwrap_err(),
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert!(last_error().contains("GET or POST"), "{}", last_error());
    assert_eq!(
        client
            .request_json("POST", "/events/create", Some("{not json"))
            .unwrap_err(),
        datahub_status::DATAHUB_INVALID_ARGUMENT
    );
    assert!(
        last_error().starts_with("body is not valid JSON"),
        "{}",
        last_error()
    );
    assert_eq!(
        client
            .request_json("GET", "/events/count", None)
            .unwrap_err(),
        datahub_status::DATAHUB_HTTP
    );
    assert_eq!(datahub_last_http_status(), 503);
}

#[test]
fn json_bodies_of_the_wrong_shape_never_leave_the_process() {
    let config = Config::for_server(UNREACHABLE, None);
    let client = config.build().unwrap();
    let (status, out) = client.json_call(
        datahub_events_create_json,
        r#"{"items":[{"externalId":"e"}]}"#,
    );
    assert_eq!(
        status,
        datahub_status::DATAHUB_INVALID_ARGUMENT,
        "type and eventTime are required"
    );
    assert!(out.is_none());
    assert!(
        last_error().starts_with("body is not a valid event create request"),
        "{}",
        last_error()
    );

    let (status, _) = client.json_call(
        datahub_timeseries_create_json,
        r#"{"items":[{"externalId":"t","name":"t","unit":7}]}"#,
    );
    assert_eq!(
        status,
        datahub_status::DATAHUB_INVALID_ARGUMENT,
        "a unit that is a number is the wrong type"
    );
}

#[test]
fn a_client_is_usable_from_several_threads_at_once() {
    let spool = tempfile::tempdir().unwrap();
    let config = Config::for_server(UNREACHABLE, Some(spool.path()));
    let client = config.build().unwrap();
    let handle = client.0 as usize;
    let threads: Vec<_> = (0..4)
        .map(|n| {
            std::thread::spawn(move || {
                let client = handle as *mut datahub_client;
                let id = cstr(&format!("series-{n}"));
                let points = [point(now_ms() + n, n as f64)];
                unsafe { datahub_datapoints_insert(client, id.as_ptr(), points.as_ptr(), 1) }
            })
        })
        .collect();
    for thread in threads {
        assert_eq!(thread.join().unwrap(), datahub_status::DATAHUB_BUFFERED);
    }
    assert_eq!(client.buffered_count(), 4);
}
