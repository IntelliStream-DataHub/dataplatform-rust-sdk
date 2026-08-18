use crate::datahub::{to_snake_lower_cased_allow_start_with_digits, DataHubConfig};
#[cfg(test)]
use maplit::hashmap;

pub mod ids {
    //! Unique external ids for test entities. The Rust twin of `python_tests/fixtures.unique_id`,
    //! with the same shape: `<prefix><kind>_<12 hex>`.
    //!
    //! Every entity a test creates gets one. A fixed id is a one-way door — the first run that
    //! panics before its teardown strands the entity, and every run afterwards collides with it —
    //! and the collision surfaces as whatever the server says about a duplicate external id, which
    //! for `/resources/create` is a 500 with an empty body.
    //!
    //! The prefix differs from Python's `pytest_` on purpose: both suites run against the same
    //! tenant, and a distinct prefix says which one left a row behind.

    use uuid::Uuid;

    /// Marks every entity this suite creates.
    pub const TEST_PREFIX: &str = "rust_sdk_";

    /// The label to hang on a node when a test just needs *a* label.
    ///
    /// Deliberately fixed and shared. A label is not like the entities above: it is a small
    /// dictionary row that the server creates on first use and refuses to delete while anything
    /// still carries it. Minting a unique one per run therefore grows the label table by a row per
    /// run, and each one is undeletable for exactly as long as the resource wearing it survives —
    /// so one stranded resource strands a label with it. Reusing a single name has neither problem,
    /// and nothing here needs its label to be distinguishable from another run's.
    ///
    /// Mint a unique name only when the test owns the label's *lifecycle* — creating, renaming or
    /// deleting the definition itself, where a shared row would be destroyed underneath another
    /// test. Tests that need to tell two labels apart (set-vs-delta semantics, an AND across a
    /// label list) use their own fixed pair rather than unique ones, for the same reason.
    pub const TEST_LABEL: &str = "TEST";

    /// e.g. `unique_id("ts")` -> `rust_sdk_ts_9f3c1a2b4d5e`.
    ///
    /// Twelve hex characters of a v4 uuid, from the **unhyphenated** form — `Uuid::to_string()`
    /// is hyphenated, so slicing that would bury a `-` inside the id.
    pub fn unique_id(kind: &str) -> String {
        format!(
            "{TEST_PREFIX}{kind}_{}",
            &Uuid::new_v4().simple().to_string()[..12]
        )
    }

    /// The same id with no separators, for the few places that need a bare token — a search
    /// query, or a name the server canonicalises.
    pub fn unique_token(kind: &str) -> String {
        format!(
            "{}{}{}",
            TEST_PREFIX.replace('_', ""),
            kind.replace('_', ""),
            &Uuid::new_v4().simple().to_string()[..12]
        )
    }
}

pub mod polling {
    //! Shared polling helpers for the integration suite. The Rust twin of
    //! `python_tests/polling.py`, with the same contract.
    //!
    //! Backend reads go through eventually-consistent projections (ClickHouse for events, Neo4j
    //! for the graph, search indexes, ...), so a just-written entity is not instantly visible to
    //! every read path. These retry a fetch until it satisfies a predicate or a timeout elapses,
    //! then hand back the last result for the caller to assert on.
    //!
    //! They deliberately never panic on timeout — the caller decides what an unsatisfied predicate
    //! means. Poll for eventual consistency, then assert; don't sleep-once-and-hope, and don't skip
    //! a test that is meant to prove something works.

    use std::future::Future;
    use std::time::{Duration, Instant};

    /// Generous bounds: long enough to ride out normal projection lag, short enough that a
    /// genuinely broken read path fails in reasonable time.
    const TIMEOUT: Duration = Duration::from_secs(30);
    const INTERVAL: Duration = Duration::from_millis(500);

    /// Call `fetch` until `predicate` holds or the timeout elapses; return the last result either
    /// way. `fetch` is invoked at least once.
    pub async fn poll_until<T, F, Fut, P>(fetch: F, predicate: P) -> T
    where
        F: Fn() -> Fut,
        Fut: Future<Output = T>,
        P: Fn(&T) -> bool,
    {
        poll_until_for(TIMEOUT, fetch, predicate).await
    }

    /// [`poll_until`] with an explicit bound, for the reads whose lag is structurally longer than
    /// the default — datapoint ingestion goes through a ClickHouse merge that can take minutes on
    /// a large insert, where every other projection here settles in seconds.
    ///
    /// Prefer [`poll_until`]. Reach for this only when the default has been shown to be too short
    /// for that specific read, and say why at the call site.
    pub async fn poll_until_for<T, F, Fut, P>(timeout: Duration, fetch: F, predicate: P) -> T
    where
        F: Fn() -> Fut,
        Fut: Future<Output = T>,
        P: Fn(&T) -> bool,
    {
        let deadline = Instant::now() + timeout;
        let mut result = fetch().await;
        while !predicate(&result) && Instant::now() < deadline {
            tokio::time::sleep(INTERVAL).await;
            result = fetch().await;
        }
        result
    }
}

pub mod cleanup {
    //! Drop-based teardown for integration tests.
    //!
    //! Backend deletes are `async`, but [`Drop::drop`] is synchronous and runs
    //! during a panic unwind — which is exactly when we want cleanup to fire
    //! (a failed `assert!` is a panic). To bridge the two, the guard runs its
    //! async cleanup on a short-lived dedicated thread with its own
    //! current-thread runtime. Spawning a fresh thread avoids the
    //! "cannot start a runtime from within a runtime" panic, so the guard works
    //! under any `#[tokio::test]` flavor (current-thread or multi_thread).
    //!
    //! Crucially, the teardown builds its **own** [`ApiService`](crate::ApiService)
    //! via [`create_api_service`] *inside* that fresh runtime rather than reusing
    //! the test's service. The test's `reqwest` client has connection-pool
    //! background tasks bound to the test's runtime, which is being torn down
    //! during the unwind; driving an HTTP request through it from another
    //! runtime hangs or fails silently. A runtime-local client sidesteps that.

    use crate::datahub::DataHubConfig;
    use crate::events::EventIdCollection;
    use crate::generic::{DataWrapper, IdAndExtId};
    use crate::{create_api_service, ApiService};
    use std::future::Future;
    use std::pin::Pin;

    type CleanupFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

    /// Runs an async cleanup closure once, when dropped.
    ///
    /// Construct with [`CleanupGuard::new`] for an arbitrary teardown, or with
    /// the [`cleanup_resources`] helper for the common "delete these external
    /// ids" case. Call [`disarm`](Self::disarm) after a successful explicit
    /// cleanup so teardown doesn't run twice.
    ///
    /// ```ignore
    /// #[tokio::test]
    /// async fn test_something() -> Result<(), ResponseError> {
    ///     let api = create_api_service();
    ///     let resources = create_test_resources();
    ///     // Armed *before* create, so a panic between here and the explicit
    ///     // delete still tears the data down.
    ///     let mut guard = cleanup_resources(
    ///         resources.iter().map(|r| r.external_id.clone()).collect(),
    ///     );
    ///
    ///     api.resources.create(resources.clone(), vec![]).await?;
    ///     // ... assertions that may panic ...
    ///
    ///     api.resources.delete(&ids).await?;
    ///     guard.disarm(); // explicit delete succeeded; skip the drop teardown
    ///     Ok(())
    /// }
    /// ```
    pub struct CleanupGuard {
        cleanup: Option<Box<dyn FnOnce() -> CleanupFuture + Send>>,
    }

    impl CleanupGuard {
        /// Build a guard from a closure producing the cleanup future. The
        /// closure and future must be `Send + 'static` because they run on a
        /// dedicated teardown thread.
        pub fn new<F>(cleanup: F) -> Self
        where
            F: FnOnce() -> CleanupFuture + Send + 'static,
        {
            Self {
                cleanup: Some(Box::new(cleanup)),
            }
        }

        /// Cancel the pending teardown. Use after the test has already deleted
        /// its data on the happy path so the drop doesn't issue a second
        /// (failing/noisy) delete.
        pub fn disarm(&mut self) {
            self.cleanup = None;
        }
    }

    impl Drop for CleanupGuard {
        fn drop(&mut self) {
            let Some(cleanup) = self.cleanup.take() else {
                return;
            };
            // Run on a fresh thread + runtime so this works even when dropped
            // from inside the test's own Tokio runtime. `scope` joins the
            // thread, so cleanup completes before drop returns.
            std::thread::scope(|s| {
                s.spawn(|| {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            eprintln!("CleanupGuard: failed to build teardown runtime: {e}");
                            return;
                        }
                    };
                    rt.block_on(cleanup());
                });
            });
        }
    }

    /// Guard that deletes the given resources (by external id) on drop.
    ///
    /// Errors are logged, not panicked on — a teardown failure shouldn't mask
    /// the test's real result, and panicking inside `Drop` during an unwind
    /// would abort the process.
    pub fn cleanup_resources(external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                // Fresh, runtime-local service — see the module docs.
                let api = create_api_service();
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                if let Err(e) = api.resources.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: resource delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Like [`cleanup_resources`], but deletes as the principal described by `config`
    /// instead of the `.env` identity.
    ///
    /// [`cleanup_resources`] builds its service with [`create_api_service`], which reads
    /// `.env` — right for the single-identity suites, wrong for a multi-tenant test where
    /// the data belongs to some other org and the default identity cannot even see it (a
    /// cross-tenant delete is a 404, not an error worth reading). The config is cloned into
    /// the closure and turned into a service on the teardown runtime, so the runtime-local
    /// client property described in the module docs still holds.
    pub fn cleanup_resources_as(config: DataHubConfig, external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = ApiService::new(config);
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                if let Err(e) = api.resources.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: resource delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Like [`cleanup_timeseries`], but deletes as the principal described by `config`.
    /// See [`cleanup_resources_as`] for why the multi-tenant tests need this.
    pub fn cleanup_timeseries_as(config: DataHubConfig, external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = ApiService::new(config);
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                let coll = DataWrapper::from_vec(ids);
                if let Err(e) = api.time_series.delete(&coll).await {
                    eprintln!(
                        "CleanupGuard: timeseries delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Like [`cleanup_datasets`], but deletes as the principal described by `config`.
    /// See [`cleanup_resources_as`] for why the multi-tenant tests need this.
    pub fn cleanup_datasets_as(config: DataHubConfig, external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = ApiService::new(config);
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                if let Err(e) = api.datasets.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: dataset delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given events (by external id) on drop.
    ///
    /// Same teardown semantics as [`cleanup_resources`]; errors are logged
    /// rather than panicked on.
    pub fn cleanup_events(external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                // Fresh, runtime-local service — see the module docs.
                let api = create_api_service();
                let ids: Vec<EventIdCollection> = external_ids
                    .iter()
                    .map(|e| EventIdCollection::from_external_id(e))
                    .collect();
                if let Err(e) = api.events.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: event delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given events **by UUID** on drop.
    ///
    /// Prefer this over [`cleanup_events`] for tests that create events. An external id does not
    /// identify one event: the backend treats events sharing an external id as the lifecycle of
    /// one logical event, and every `create` stamps a fresh UUID v7 (see `EventsService::create`).
    /// So a test that re-runs with fixed external ids adds a row each time, and deleting by
    /// external id does not reclaim the earlier ones — they pile up and eventually break
    /// count-sensitive filter assertions. Take the ids from the `create` response.
    pub fn cleanup_events_by_uuid(ids: Vec<EventIdCollection>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if ids.is_empty() {
                    return;
                }
                // Fresh, runtime-local service — see the module docs.
                let api = create_api_service();
                if let Err(e) = api.events.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: event delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given datasets (by external id) on drop.
    pub fn cleanup_datasets(external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = create_api_service();
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                if let Err(e) = api.datasets.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: dataset delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given time series (by external id) on drop.
    pub fn cleanup_timeseries(external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = create_api_service();
                let idcoll: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                let coll = DataWrapper::from_vec(idcoll);
                if let Err(e) = api.time_series.delete(&coll).await {
                    eprintln!(
                        "CleanupGuard: timeseries delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given files/INodes (by external id) on drop.
    pub fn cleanup_files(external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = create_api_service();
                let idcoll: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                let coll = DataWrapper::from_vec(idcoll);
                if let Err(e) = api.files.delete(&coll).await {
                    eprintln!(
                        "CleanupGuard: file delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given subscriptions (by external id) on drop.
    pub fn cleanup_subscriptions(external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = create_api_service();
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                if let Err(e) = api.subscriptions.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: subscription delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given functions (by external id) on drop.
    pub fn cleanup_functions(external_ids: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if external_ids.is_empty() {
                    return;
                }
                let api = create_api_service();
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
                    .collect();
                if let Err(e) = api.functions.delete(&ids).await {
                    eprintln!(
                        "CleanupGuard: function delete failed during teardown: {}",
                        e.get_message()
                    );
                }
            })
        })
    }

    /// Guard that deletes the given labels (by name) on drop.
    ///
    /// A label is addressed by its name, which the server canonicalises to upper case; the delete
    /// endpoint takes it in the `externalId` slot like every other identifiable. A label still
    /// attached to a resource is refused, so tear the resource down first.
    pub fn cleanup_labels(names: Vec<String>) -> CleanupGuard {
        CleanupGuard::new(move || {
            Box::pin(async move {
                if names.is_empty() {
                    return;
                }
                let api = create_api_service();
                for name in &names {
                    if let Err(e) = api
                        .labels
                        .delete(&IdAndExtId::from_external_id(name))
                        .await
                    {
                        eprintln!(
                            "CleanupGuard: label delete failed during teardown: {}",
                            e.get_message()
                        );
                    }
                }
            })
        })
    }

    #[cfg(test)]
    mod guard_tests {
        use super::CleanupGuard;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        #[tokio::test]
        async fn cleanup_runs_on_panic() {
            let ran = Arc::new(AtomicBool::new(false));
            let ran_for_guard = ran.clone();

            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = CleanupGuard::new(move || {
                    let ran = ran_for_guard.clone();
                    Box::pin(async move {
                        ran.store(true, Ordering::SeqCst);
                    })
                });
                panic!("simulated test failure");
            }));

            assert!(result.is_err(), "the panic should have propagated");
            assert!(
                ran.load(Ordering::SeqCst),
                "cleanup did NOT run during the panic unwind"
            );
        }
    }
}
#[test]
fn test_to_snake_lower_cased_allow_start_with_digits() {
    // tests validation function for externalId
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("Hello World!"),
        "hello_world".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("Another-Test_Case"),
        "another_test_case".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("with_numbers_123"),
        "with_numbers_123".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("  leading and trailing spaces  "),
        "_leading_and_trailing_spaces".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("123_Starts_With_Digits"),
        "123_starts_with_digits".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("Two  spaces"),
        "two_spaces".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits(" Leading space"),
        "_leading_space".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("Trailing space "),
        "trailing_space".to_string()
    );
    assert_eq!(
        to_snake_lower_cased_allow_start_with_digits("!@#$%^&*()"),
        "".to_string()
    );
}
#[tokio::test]
async fn test_create_api_with_token() {
    let map = hashmap! {
        "TOKEN".to_string() => "testtoken".to_string(),
        "BASE_URL".to_string() => "http://localhost:8081".to_string()
    };
    let api = DataHubConfig::from_map(map).unwrap();
    assert_eq!(api.get_api_token().await.unwrap(), "testtoken".to_string());
}
