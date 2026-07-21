//! Integration tests for durable buffering and event UUID v7 ids.
//!
//! The offline tests point the client at an unreachable port, so ingestion buffers to disk without a
//! backend. The live tests need a `.env` (`BASE_URL` + `TOKEN` or the OAuth2 set) and a reachable
//! backend, like the rest of this crate's integration tests.

use crate::datahub::DataHubConfig;
use crate::events::{Event, EventIdCollection};
use crate::generic::{DataWrapper, IdAndExtId};
use crate::tests::cleanup::{cleanup_events, cleanup_timeseries};
use crate::{create_api_service, ApiService, TimeSeries};
use chrono::Utc;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

/// Event ingestion/deletion is eventually consistent, so `by_ids` right after a write can lag.
/// Poll it until it reports `want` matches (or we give up after ~10s) and return the items.
async fn poll_events_by_uuid(service: &ApiService, id: Uuid, want: usize) -> Vec<Event> {
    for _ in 0..20 {
        if let Ok(dw) = service.events.by_ids(&vec![EventIdCollection::from_uuid(id)]).await {
            if dw.get_items().len() == want {
                return dw.get_items().clone();
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Vec::new()
}

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
    let mut config = DataHubConfig::from_vars(
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

    let mut ev = Event::new("rust_uuid_v7_event".to_string());
    ev.set_event_time(Utc::now()); // event_time is required (the SDK won't default it)
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

#[tokio::test]
async fn event_without_event_time_is_rejected() {
    let dir = temp_dir();
    let service = unreachable_buffered_service(&dir);

    // No event_time set: the SDK rejects it (before any send) rather than defaulting to now().
    let ev = Event::new("rust_no_time_event".to_string());
    let err = service
        .events
        .create(&ev)
        .await
        .expect_err("missing event_time should be rejected");
    assert_eq!(err.get_status().as_u16(), 400);
    assert!(err.get_message().contains("event_time"), "{}", err.get_message());
    let _ = std::fs::remove_dir_all(&dir);
}

// --- Live tests (require a backend + .env) ------------------------------------------------------

#[tokio::test]
async fn live_datapoint_buffering_roundtrip() {
    let dir = temp_dir();
    let mut config = DataHubConfig::from_envfile(None).expect("BASE_URL + auth in .env");
    config
        .set_buffer_dir(dir.clone())
        .set_buffer_retention_secs(3600);
    let service = ApiService::new(config);

    let id_collection =
        DataWrapper::from_vec(vec![IdAndExtId::from_external_id("rust_buffer_test_series")]);

    let mut ts = TimeSeries::new("rust_buffer_test_series", "Rust buffer test");
    ts.set_value_type("float");
    ts.set_unit("a.u"); // unit is required by the server (@NotBlank)
    // Start clean (drop any leftover from a prior run), then create the series fresh.
    let _ = service.time_series.delete(&id_collection).await;
    service.time_series.create_one(&ts).await.expect("create series");
    // Arm a Drop-based guard so a panic in the assertions below still deletes the
    // series (otherwise a buffered/failed insert would leave an empty timeseries).
    let mut ts_cleanup = cleanup_timeseries(vec!["rust_buffer_test_series".to_string()]);
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

    // teardown: delete the series (and its datapoints) so re-runs start clean
    let _ = service.time_series.delete(&id_collection).await;
    ts_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn live_event_gets_uuid_v7_id() {
    let service = create_api_service();
    let mut ev = Event::new("rust_uuid_v7_event".to_string());
    ev.set_event_time(Utc::now()); // event_time is required (the SDK won't default it)

    // Events get a fresh uuid per create, so same-external_id inserts pile up
    // instead of overwriting. Arm cleanup before create (so a panic still tears
    // the event down) and delete explicitly on the happy path.
    let mut ev_cleanup = cleanup_events(vec!["rust_uuid_v7_event".to_string()]);

    let result = service.events.create(&ev).await.expect("create event");
    let created = result.get_items().first().expect("one event returned");
    let id = created.id.expect("event has an id");
    assert_eq!(id.get_version_num(), 7, "expected a v7 uuid, got {}", id);

    // teardown: delete by external id, which removes every copy so re-runs don't accumulate.
    let ids = vec![EventIdCollection::from_external_id("rust_uuid_v7_event")];
    let _ = service.events.delete(&ids).await;
    ev_cleanup.disarm(); // explicit delete succeeded; skip the drop teardown
}

#[tokio::test]
async fn live_event_get_by_uuid() {
    let service = create_api_service();
    let mut ev = Event::new("rust_event_get_by_uuid".to_string());
    ev.set_event_time(Utc::now());
    let mut ev_cleanup = cleanup_events(vec!["rust_event_get_by_uuid".to_string()]);

    let created = service.events.create(&ev).await.expect("create event");
    let id = created.get_items().first().expect("one event returned").id.expect("event has an id");

    // Retrieve the event by the UUID the server echoed back (polling for eventual consistency).
    let fetched = poll_events_by_uuid(&service, id, 1).await;
    assert_eq!(fetched.len(), 1, "expected exactly the event we created back");
    assert_eq!(fetched[0].id, Some(id), "returned event should carry the same uuid");

    let _ = service.events.delete(&vec![EventIdCollection::from_uuid(id)]).await;
    ev_cleanup.disarm();
}

#[tokio::test]
async fn live_event_delete_by_uuid() {
    let service = create_api_service();
    let mut ev = Event::new("rust_event_delete_by_uuid".to_string());
    ev.set_event_time(Utc::now());
    let mut ev_cleanup = cleanup_events(vec!["rust_event_delete_by_uuid".to_string()]);

    let created = service.events.create(&ev).await.expect("create event");
    let id = created.get_items().first().expect("one event returned").id.expect("event has an id");

    // Confirm it's queryable by UUID (read-after-write), then delete it by that UUID.
    let present = poll_events_by_uuid(&service, id, 1).await;
    assert_eq!(present.len(), 1, "event should be queryable by its uuid before delete");
    service
        .events
        .delete(&vec![EventIdCollection::from_uuid(id)])
        .await
        .expect("delete by uuid");
    // Poll until the delete has propagated and by_ids on that uuid returns nothing.
    let after = poll_events_by_uuid(&service, id, 0).await;
    assert!(after.is_empty(), "event should be gone after delete-by-uuid");
    ev_cleanup.disarm(); // already deleted
}

// NB: there is deliberately no filter-by-uuid test. The backend types the event filter's `id`
// field as a Long, so filtering events by their UUID id is rejected server-side; `by_ids` (above)
// is the supported way to retrieve an event by its UUID.
