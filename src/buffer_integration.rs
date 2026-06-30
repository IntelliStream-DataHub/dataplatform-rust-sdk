//! Integration tests for durable buffering and event UUID v7 ids.
//!
//! The offline tests run under a plain `cargo test` (they point the client at an unreachable port,
//! so ingestion buffers to disk). The `#[ignore]` tests need a live backend and a `.env`
//! (`BASE_URL` + `TOKEN` or the OAuth2 set); run them with:
//!   `cargo test --test-threads=1 -- --ignored buffering`

use crate::datahub::DataHubApi;
use crate::events::Event;
use crate::{create_api_service, ApiService, TimeSeries};
use chrono::Utc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let n = COUNTER.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("datahub_rust_it_{}_{}", nanos, n));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A client whose base URL refuses connections, with durable buffering pointed at `dir`.
fn unreachable_buffered_service(dir: &PathBuf) -> Arc<ApiService> {
    let mut config = DataHubApi::from_vars(
        "http://127.0.0.1:9".to_string(),
        Some("dummy-token".to_string()),
        None,
        None,
        None,
        None,
    );
    config
        .set_buffer_dir(dir.clone())
        .set_buffer_retention_secs(3600);
    ApiService::new(config)
}

#[tokio::test]
async fn buffering_spools_to_disk_when_unreachable() {
    let dir = temp_dir();
    let service = unreachable_buffered_service(&dir);

    let r = service
        .time_series
        .insert_datapoint(
            None,
            Some("rust_buffer_test_series".to_string()),
            Utc::now(),
            "1.0".to_string(),
        )
        .await
        .expect("insert_datapoint should buffer, not error");
    assert_eq!(r.get_http_status_code(), Some(202), "datapoint should be buffered");
    assert!(service.time_series.buffered_count() >= 1);

    let mut ev = Event::new("rust_buffer_test_event".to_string());
    ev.set_event_time(Utc::now());
    let er = service.events.create(&ev).await.expect("create should buffer");
    assert_eq!(er.get_http_status_code(), Some(202), "event should be buffered");
    assert!(service.events.buffered_count() >= 1);

    assert!(dir.join("datapoints").is_dir(), "datapoint spool dir exists");
    assert!(dir.join("events").is_dir(), "event spool dir exists");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn buffered_event_is_stamped_with_uuid_v7() {
    let dir = temp_dir();
    let service = unreachable_buffered_service(&dir);

    let ev = Event::new("rust_uuid_v7_event".to_string());
    let _ = service.events.create(&ev).await.expect("create should buffer");

    // The active segment is plain `<ts>\t<json>` NDJSON; find the stamped id and check its version.
    let mut content = String::new();
    for entry in std::fs::read_dir(dir.join("events")).unwrap() {
        let p = entry.unwrap().path();
        if p.extension().map_or(false, |e| e == "ndjson") {
            content = std::fs::read_to_string(&p).unwrap();
        }
    }
    let idx = content.find("\"id\":\"").expect("spooled event carries an id");
    let uuid = &content[idx + 6..idx + 6 + 36];
    assert_eq!(
        uuid.as_bytes()[14],
        b'7',
        "expected a time-ordered UUID v7 id, got {}",
        uuid
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// --- Live tests (require a backend; run with `-- --ignored`) ------------------------------------

#[tokio::test]
#[ignore]
async fn live_datapoint_buffering_roundtrip() {
    let dir = temp_dir();
    let mut config = DataHubApi::from_envfile(None).expect("BASE_URL + auth in .env");
    config
        .set_buffer_dir(dir.clone())
        .set_buffer_retention_secs(3600);
    let service = ApiService::new(config);

    let mut ts = TimeSeries::new("rust_buffer_test_series", "Rust buffer test");
    ts.set_value_type("float");
    let _ = service.time_series.create_one(&ts).await; // ignore "already exists"
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let r = service
        .time_series
        .insert_datapoint(
            None,
            Some("rust_buffer_test_series".to_string()),
            Utc::now(),
            "42.0".to_string(),
        )
        .await
        .expect("insert_datapoint");
    // Backend is reachable, so the live send (and any backlog) should go through, not buffer.
    assert!(matches!(r.get_http_status_code(), Some(204) | Some(200)));
    assert_eq!(service.time_series.buffered_count(), 0, "spool should be empty after a live flush");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[ignore]
async fn live_event_gets_uuid_v7_id() {
    let service = create_api_service();
    let ev = Event::new("rust_uuid_v7_event".to_string());
    let result = service.events.create(&ev).await.expect("create event");
    let created = result.get_items().first().expect("one event returned");
    let id = created.id.expect("event has an id");
    assert_eq!(id.get_version_num(), 7, "expected a v7 uuid, got {}", id);
}
