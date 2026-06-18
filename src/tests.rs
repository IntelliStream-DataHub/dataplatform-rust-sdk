use crate::datahub::{to_snake_lower_cased_allow_start_with_digits, DataHubApi};
#[cfg(test)]
use maplit::hashmap;

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

    use crate::create_api_service;
    use crate::generic::{DataWrapper, IdAndExtId};
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
                let ids: Vec<IdAndExtId> = external_ids
                    .iter()
                    .map(|e| IdAndExtId::from_external_id(e))
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
    let api = DataHubApi::from_map(map).unwrap();
    assert_eq!(api.get_api_token().await.unwrap(), "testtoken".to_string());
}
