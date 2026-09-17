//! What the api actually answers when a request is wrong, asserted against a live backend.
//!
//! The unit tests in [`crate::problem`] prove the SDK can *read* an RFC 9457 document. These prove
//! the api *sends* one, which is the half that keeps drifting: an error shape has no compiler and
//! no caller that notices when it changes, so the only way a refusal stays legible is for something
//! to send a deliberately broken request and read the answer.
//!
//! ```text
//! cargo test problem_                       # this module
//! cargo test problem_ -- --nocapture        # with the bodies printed
//! ```
//!
//! # The unification landed; this suite is now the regression guard
//!
//! The api's `errors/*` series merged, and every refusal below answers with a problem document.
//! Before it, **six** shapes were live at once and a caller could not tell from the outside which
//! one an endpoint would pick:
//!
//! | Shape, before | Example | Now |
//! |---|---|---|
//! | full problem+json with a `type` | unknown field, malformed JSON, bad cursor, bad expression | unchanged |
//! | problem+json with **no** `type` | `GET /resources/{id}` on a missing id | `not-found` |
//! | Spring Boot whitelabel + **stack trace** | `POST /datasets/list`, a `@Valid` search failure | typed problem, no trace |
//! | a **success-shaped** `{"items":[…]}` envelope | `POST /timeseries/create` validation | `validation-failed` with `fields` |
//! | the legacy `{"error":{"code","message",…}}` wrapper | a duplicate `externalId` | `duplicate` with `duplicated` |
//! | plain text | `limit` above 10000 | typed problem with `fields` |
//!
//! Four of those six answered with `Content-Type: application/json`, which is why
//! [`ProblemDetail::parse`] discriminates on structure rather than on the header. That is still the
//! right call: it is what lets these tests state "this is *not* a problem document" as a fact.
//!
//! The two groups are kept apart because which assertions were aspirational is worth remembering:
//!
//! - [`green`] — true before the unification and after it. A failure here means the refactor
//!   changed something it did not mean to.
//! - [`unified`] — arrived *with* the unification. Every one was red on purpose until the api
//!   merged; they are regression guards now, and a failure means a revert.
//!
//! # Why the assertions avoid prose
//!
//! `title` and `detail` are prose and the RFC says so (§3.1.1); `type` is the stable identifier.
//! Every assertion here branches on [`ProblemDetail::slug`], a status, or an extension member —
//! never on wording — so rewording a message cannot fail this suite and changing its meaning can.

#![cfg(test)]

use crate::create_api_service;
use crate::ApiService;
use crate::problem::ProblemDetail;
use std::sync::Arc;

/// Sends deliberately broken requests over the service's own `http_client`.
///
/// Raw rather than through the typed services for two reasons: serde cannot emit invalid JSON, so a
/// malformed body has no other route to the wire; and a typed filter cannot carry a field the api
/// does not have, which is exactly the case worth testing. Auth comes from the same
/// [`DataHubConfig`](crate::datahub::DataHubConfig) as every other call, so there is no second token
/// path to keep working.
struct Probe {
    api: Arc<ApiService>,
}

/// One refusal, as a caller sees it.
struct Refusal {
    status: u16,
    content_type: String,
    body: String,
}

impl Refusal {
    fn problem(&self) -> Option<ProblemDetail> {
        ProblemDetail::parse(&self.body)
    }

    /// The problem's slug, or a description of what came back instead — so a failing assertion
    /// reports the shape the api actually used rather than just `None`.
    fn slug_or_shape(&self) -> String {
        match self.problem() {
            Some(problem) => match problem.slug() {
                Some(slug) => slug.to_string(),
                None => "a problem document with no `type`".to_string(),
            },
            None if self.body.trim().is_empty() => "an empty body".to_string(),
            None if self.body.contains("\"trace\"") => {
                "Spring Boot's whitelabel error body, with a stack trace".to_string()
            }
            None if self.body.trim_start().starts_with("{\"items\"") => {
                "a success-shaped {\"items\":[…]} envelope".to_string()
            }
            None if self.body.trim_start().starts_with("{\"error\"") => {
                "the legacy {\"error\":{…}} wrapper".to_string()
            }
            None if !self.body.trim_start().starts_with('{') => {
                format!("plain text: {:?}", self.body.trim())
            }
            None => format!("JSON that is not a problem: {}", self.preview()),
        }
    }

    fn preview(&self) -> String {
        let body = self.body.trim();
        if body.chars().count() <= 220 {
            return body.to_string();
        }
        format!("{}…", body.chars().take(220).collect::<String>())
    }
}

impl Probe {
    fn new() -> Self {
        Self {
            api: create_api_service(),
        }
    }

    fn url(&self, path: &str) -> String {
        format!(
            "{}/{}",
            self.api.config.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }

    async fn token(&self) -> String {
        self.api
            .config
            .get_api_token()
            .await
            .expect("could not obtain an api token")
    }

    /// POST `body` verbatim — no serialization, so the body may be invalid JSON.
    async fn post(&self, path: &str, body: &str) -> Refusal {
        self.send(reqwest::Method::POST, path, Some(body), true).await
    }

    async fn get(&self, path: &str) -> Refusal {
        self.send(reqwest::Method::GET, path, None, true).await
    }

    /// The same request with no `Authorization` header, for the 401 path.
    async fn get_unauthenticated(&self, path: &str) -> Refusal {
        self.send(reqwest::Method::GET, path, None, false).await
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&str>,
        authenticated: bool,
    ) -> Refusal {
        let mut request = self
            .api
            .http_client
            .request(method, self.url(path))
            // What the SDK's own services send. Worth pinning: Spring negotiates the error
            // representation off this header, and a problem must survive being asked for JSON.
            .header(reqwest::header::ACCEPT, "application/json");
        if authenticated {
            request = request.bearer_auth(self.token().await);
        }
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body.to_string());
        }

        let response = request.send().await.expect("request failed to send");
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = response.text().await.unwrap_or_default();

        println!("[{status} | {content_type}] {path}\n  {body}");
        Refusal {
            status,
            content_type,
            body,
        }
    }
}

/// Refusals that already answer with a problem document, and must keep doing so.
///
/// Nothing here is aspirational: every assertion passes against the api as it stands, so a failure
/// is a regression rather than a not-yet.
mod green {
    use super::Probe;
    use crate::create_api_service;
    use crate::problem::ProblemDetail;

    /// The SDK's own error type is the surface callers actually touch, so the problem has to reach
    /// them through it — not only be present on the wire.
    #[tokio::test]
    async fn a_missing_id_reaches_the_caller_as_a_problem_on_response_error() {
        let api = create_api_service();
        let error = api
            .resources
            .get_by_id(999_999_999)
            .await
            .expect_err("an id that cannot exist should not resolve");

        assert_eq!(error.get_status().as_u16(), 404);
        assert_eq!(
            error.content_type().map(|ct| ct.starts_with("application/problem+json")),
            Some(true),
            "a problem must be labelled as one; got {:?}",
            error.content_type()
        );

        let problem = error
            .problem()
            .expect("ResponseError::problem() should expose the document the api sent");
        assert_eq!(problem.status, Some(404));
        assert!(
            problem.detail.is_some(),
            "a 404 should say what was not found"
        );
        assert_eq!(
            problem.instance.as_deref(),
            Some("/resources/999999999"),
            "`instance` should name the request this problem is about"
        );

        // The raw body stays reachable — existing tests substring-match `get_message()`, and
        // parsing must not have replaced it.
        assert!(
            error.get_message().contains("999999999"),
            "the verbatim body should still be available: {}",
            error.get_message()
        );
    }

    /// Rejecting unknown fields is only an improvement over silently dropping them if the caller is
    /// told *which* field — and, since two unknown fields at different depths accept different
    /// names, which names are valid *there*.
    #[tokio::test]
    async fn an_unknown_field_is_named_along_with_the_names_that_position_accepts() {
        let refusal = Probe::new()
            .post("/timeseries/filter", r#"{"bogusField":["x"],"limit":5}"#)
            .await;

        assert_eq!(refusal.status, 400);
        let problem = refusal
            .problem()
            .unwrap_or_else(|| panic!("expected a problem, got {}", refusal.slug_or_shape()));
        assert_eq!(problem.slug(), Some("unreadable-request-body"));

        let unknown = problem.unknown_fields();
        assert!(
            !unknown.is_empty(),
            "the `errors` extension is the point of this refusal: {}",
            refusal.preview()
        );
        assert!(
            unknown[0]
                .pointer
                .as_deref()
                .is_some_and(|pointer| pointer.contains("bogusField")),
            "the pointer should locate the offending field, got {:?}",
            unknown[0].pointer
        );
        assert!(
            !unknown[0].allowed_fields.is_empty(),
            "the caller cannot fix the request without the names this position accepts"
        );
    }

    /// A body that cannot be parsed at all is a different path from an unrecognised field: parsing
    /// has to finish before anything binds, so the offending *field* is unknowable and a line and
    /// column is all there is to give.
    #[tokio::test]
    async fn a_malformed_body_reports_where_parsing_failed() {
        // Unclosed array. serde cannot produce this, which is why the probe writes bytes directly.
        let refusal = Probe::new().post("/timeseries/filter", r#"{"name":["#).await;

        assert_eq!(refusal.status, 400);
        let problem = refusal
            .problem()
            .unwrap_or_else(|| panic!("expected a problem, got {}", refusal.slug_or_shape()));
        assert_eq!(problem.slug(), Some("unreadable-request-body"));
        assert!(
            problem.location().is_some(),
            "\"could not be read\" on a body of any size is a needle in a haystack without a \
             line and column: {}",
            refusal.preview()
        );
    }

    /// A value of the wrong JSON type takes the same path as a syntax error — it also fails before
    /// binding — so it is worth pinning that it does not fall through to some fifth shape.
    #[tokio::test]
    async fn a_value_of_the_wrong_type_is_refused_as_an_unreadable_body() {
        let refusal = Probe::new()
            .post("/timeseries/filter", r#"{"limit":"not-a-number"}"#)
            .await;

        assert_eq!(refusal.status, 400);
        let problem = refusal
            .problem()
            .unwrap_or_else(|| panic!("expected a problem, got {}", refusal.slug_or_shape()));
        assert_eq!(problem.slug(), Some("unreadable-request-body"));
    }

    /// Cursors are opaque and must be echoed back verbatim. One that is not even base64url cannot
    /// be a cursor this api minted, and is refused rather than silently restarting the walk —
    /// a silent restart would re-serve page one forever and read like a short collection.
    #[tokio::test]
    async fn a_cursor_that_cannot_be_decoded_is_refused_by_name() {
        let refusal = Probe::new()
            .post("/timeseries/filter", r#"{"cursor":"!!!not-a-cursor!!!","limit":5}"#)
            .await;

        assert_eq!(refusal.status, 400);
        let problem = refusal
            .problem()
            .unwrap_or_else(|| panic!("expected a problem, got {}", refusal.slug_or_shape()));
        assert_eq!(problem.slug(), Some("malformed-cursor"));
        assert!(
            problem
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("nextCursor")),
            "the remedy — echo back nextCursor, or omit it — is the useful half: {:?}",
            problem.detail
        );
    }

    /// `advancedFilter` is parsed server-side, so a bad expression can only be reported by the api.
    /// It locates the failure by offset, which is what makes a long expression fixable.
    #[tokio::test]
    async fn a_bad_filter_expression_is_located_by_offset() {
        let refusal = Probe::new()
            .post("/events/filter", r#"{"advancedFilter":"type NOT LIKE (((","limit":5}"#)
            .await;

        assert_eq!(refusal.status, 400);
        let problem = refusal
            .problem()
            .unwrap_or_else(|| panic!("expected a problem, got {}", refusal.slug_or_shape()));
        assert_eq!(problem.slug(), Some("filter-expression"));
        assert!(
            problem.extensions.contains_key("offset"),
            "an expression is fixable only if the api says where it broke: {}",
            refusal.preview()
        );
    }

    /// Every problem the api sends must survive the SDK's own `Accept: application/json`. Spring
    /// negotiates the error representation off that header, and answering it with anything but the
    /// problem document would strand every client that asks for JSON — which is all of them.
    #[tokio::test]
    async fn a_problem_survives_being_asked_for_plain_json() {
        let refusal = Probe::new().get("/timeseries/999999999").await;

        assert_eq!(refusal.status, 404);
        assert!(
            refusal.content_type.starts_with("application/problem+json"),
            "asked for application/json, the api should still answer problem+json, got {:?}",
            refusal.content_type
        );
        assert!(refusal.problem().is_some());
    }

    /// An error this SDK raises before a request exists has no server document behind it, and must
    /// not pretend otherwise — `problem()` is how a caller tells "the api refused and explained" from
    /// "we never got that far".
    #[test]
    fn a_client_side_refusal_carries_no_problem() {
        let error = crate::http::ResponseError::bad_request(
            "a bare Resource labelled `dataset` cannot be sent".to_string(),
        );

        assert_eq!(error.get_status().as_u16(), 400);
        assert_eq!(error.problem(), None);
        assert_eq!(error.content_type(), None, "nothing was received to have a type");
    }

    /// Guards the discriminator against the one body most likely to fool it: the api's own
    /// *success* envelope, which a validation failure is still wrapped in today.
    #[test]
    fn a_success_shaped_validation_envelope_is_not_read_as_a_problem() {
        // Captured verbatim from `POST /timeseries/create` with a blank externalId.
        let body = r#"{"items":[{"externalId":"must not be blank"},{"unit":"timeseries.unit.not.blank"}]}"#;
        assert_eq!(
            ProblemDetail::parse(body),
            None,
            "this is success-shaped; reading it as a problem would invent a title and status"
        );
    }
}

/// The refusals that gained a problem document when the api's `errors/*` series landed.
///
/// Every one of these was red on purpose beforehand, and each still names what it gets instead when
/// it fails — so a revert reports itself as "got plain text" rather than as a bare assertion
/// failure. Do not soften one to match a regression: that is how a wire contract quietly becomes
/// whatever the server happens to do.
mod unified {
    use super::{Probe, Refusal};
    use crate::problem::{ProblemDetail, Retry};

    /// Asserts the one invariant the whole refactor exists to establish.
    fn assert_is_a_typed_problem(refusal: &Refusal, what: &str) -> ProblemDetail {
        assert!(
            refusal.content_type.starts_with("application/problem+json"),
            "{what}: should be labelled application/problem+json, got {:?} — body: {}",
            refusal.content_type,
            refusal.preview()
        );
        let problem = refusal
            .problem()
            .unwrap_or_else(|| panic!("{what}: should answer with a problem document, got {}", refusal.slug_or_shape()));
        assert!(
            problem.slug().is_some(),
            "{what}: should carry a `type` under {} — the member clients branch on. Got {}",
            ProblemDetail::TYPE_BASE,
            refusal.slug_or_shape()
        );
        problem
    }

    /// A limit above the cap is refused as **plain text** today — `Limit cannot be greater than
    /// 10000`, under a `Content-Type: application/json` that is a lie about the body.
    #[tokio::test]
    async fn an_over_cap_limit_is_refused_with_a_problem() {
        let refusal = Probe::new().get("/timeseries?limit=99999").await;

        assert_eq!(refusal.status, 400);
        assert_is_a_typed_problem(&refusal, "a limit above the 10000 cap");
    }

    /// A retired route answers with Spring Boot's whitelabel body today — which carries a **full
    /// stack trace**, naming this api's internal packages, filters and line numbers to any caller
    /// who sends the wrong verb.
    #[tokio::test]
    async fn a_wrong_method_is_refused_with_a_problem_and_no_stack_trace() {
        let refusal = Probe::new().post("/datasets/list", "{}").await;

        assert_eq!(refusal.status, 405);
        assert!(
            !refusal.body.contains("\"trace\""),
            "a refusal must not hand the caller a stack trace of this api's internals: {}",
            refusal.preview()
        );
        assert_is_a_typed_problem(&refusal, "POST to a GET-only path");
    }

    /// A `@Valid` failure escapes to the whitelabel handler today, so asking for a 2-character
    /// search phrase returns a stack trace quoting the controller's Java signature. On the unified
    /// contract it is a `validation-failed` problem whose `fields` name the offending property.
    #[tokio::test]
    async fn a_rejected_field_is_named_in_the_fields_extension() {
        let refusal = Probe::new()
            .post("/timeseries/search", r#"{"search":{"query":"ab"}}"#)
            .await;

        assert_eq!(refusal.status, 400);
        assert!(
            !refusal.body.contains("\"trace\""),
            "a validation failure must not hand back a stack trace: {}",
            refusal.preview()
        );
        let problem = assert_is_a_typed_problem(&refusal, "a search phrase under the 3-character minimum");

        let fields = problem.fields();
        assert!(
            !fields.is_empty(),
            "the caller has to be told which field was rejected: {}",
            refusal.preview()
        );
        assert!(
            fields.iter().any(|field| field
                .field
                .as_deref()
                .is_some_and(|name| name.contains("query"))),
            "the rejected field should be named, got {:?}",
            fields.iter().map(|f| f.field.clone()).collect::<Vec<_>>()
        );
    }

    /// Create-time validation comes back **success-shaped** today: `{"items":[{"externalId":"must
    /// not be blank"}]}`. A client cannot tell that from a successful listing by shape alone, which
    /// is the single worst property an error body can have.
    #[tokio::test]
    async fn a_create_validation_failure_is_not_shaped_like_a_success() {
        let refusal = Probe::new()
            .post(
                "/timeseries/create",
                r#"{"items":[{"externalId":"","name":"probe","valueType":"FLOAT"}]}"#,
            )
            .await;

        assert_eq!(refusal.status, 400);
        assert!(
            !refusal.body.trim_start().starts_with("{\"items\""),
            "an error must not wear the success envelope — a client cannot tell them apart: {}",
            refusal.preview()
        );
        let problem = assert_is_a_typed_problem(&refusal, "a create with a blank externalId");
        assert!(
            !problem.fields().is_empty(),
            "the rejected fields should survive as the `fields` extension: {}",
            refusal.preview()
        );
    }

    /// An unauthenticated call used to answer 401 with `Content-Length: 0`, which is why
    /// [`crate::auth_diagnostics`] reconstructs a reason from the token the SDK just sent. The api
    /// now names the failed check in `detail` — including which organization-claim case a token
    /// hit.
    #[tokio::test]
    async fn an_unauthenticated_call_says_why() {
        let refusal = Probe::new().get_unauthenticated("/timeseries?limit=1").await;

        assert_eq!(refusal.status, 401);
        assert_is_a_typed_problem(&refusal, "a call with no credential");
    }

    /// `retry` and `requestId` are what a caller acts on: whether repeating the request could ever
    /// work, and what to quote to an operator when it cannot. Both are added to every problem by the
    /// api's response advice, so any one refusal is enough to pin them.
    #[tokio::test]
    async fn every_problem_says_what_to_do_next_and_which_request_it_was() {
        let refusal = Probe::new()
            .post("/timeseries/filter", r#"{"bogusField":["x"]}"#)
            .await;
        let problem = assert_is_a_typed_problem(&refusal, "an unknown field");

        assert_eq!(
            problem.retry(),
            Some(Retry::ChangeRequest),
            "only a corrected body can succeed here, and the problem should say so"
        );
        assert!(
            problem.request_id().is_some(),
            "`requestId` is what a caller quotes to an operator: {}",
            refusal.preview()
        );
    }

    /// A delete the api refuses names what is standing in the way, so the caller can go remove it.
    ///
    /// This was a **400** before the unification: "fix your payload", which was never the remedy.
    /// Nothing is wrong with the request — it is well-formed, the id exists, and repeating it
    /// verbatim works once the subscription is gone. That is a conflict with the current state, and
    /// it puts a refused delete alongside the other 409s.
    ///
    /// `blockedBy` carries structured records rather than `field -> message` pairs: the whole point
    /// is that the caller can read the blocking subscription's external id and act on it.
    #[tokio::test]
    async fn a_delete_something_still_references_names_the_blocker() {
        use crate::tests::cleanup::{cleanup_subscriptions, cleanup_timeseries};
        use crate::tests::ids::unique_id;

        let probe = Probe::new();
        let ts_ext = unique_id("problem_referenced_ts");
        let sub_ext = unique_id("problem_referenced_sub");

        // Both guards armed before their creates, so a failing assertion still tears down. The
        // subscription must go first: it is what blocks the series.
        let _ts_cleanup = cleanup_timeseries(vec![ts_ext.clone()]);
        let _sub_cleanup = cleanup_subscriptions(vec![sub_ext.clone()]);

        let created = probe
            .post(
                "/timeseries/create",
                &format!(
                    r#"{{"items":[{{"externalId":"{ts_ext}","name":"referenced probe","unit":"celsius","valueType":"FLOAT"}}]}}"#
                ),
            )
            .await;
        assert!(
            (200..300).contains(&created.status),
            "the series should be created, got {}: {}",
            created.status,
            created.preview()
        );

        let subscribed = probe
            .post(
                "/subscriptions/create",
                &format!(
                    r#"{{"items":[{{"externalId":"{sub_ext}","name":"referenced probe sub","timeseries":[{{"externalId":"{ts_ext}"}}]}}]}}"#
                ),
            )
            .await;
        assert!(
            (200..300).contains(&subscribed.status),
            "the subscription should be created, got {}: {}",
            subscribed.status,
            subscribed.preview()
        );

        let refusal = probe
            .post("/timeseries/delete", &format!(r#"{{"items":[{{"externalId":"{ts_ext}"}}]}}"#))
            .await;

        assert_eq!(
            refusal.status, 409,
            "a delete blocked by live state is a conflict, not a malformed request: {}",
            refusal.preview()
        );
        let problem = assert_is_a_typed_problem(&refusal, "a delete the subscription blocks");
        assert_eq!(problem.slug(), Some("referenced"));

        let blockers = problem.blocked_by();
        assert!(
            !blockers.is_empty(),
            "the caller cannot act without knowing what blocked it: {}",
            refusal.preview()
        );
        assert!(
            blockers.iter().any(|entry| entry
                .values()
                .any(|value| value.as_str() == Some(sub_ext.as_str()))),
            "the blocking subscription should be named by external id, got {blockers:?}"
        );
    }

    /// A taken external id is a conflict, and the caller needs to know *which* id collided — in a
    /// batch it is not otherwise knowable.
    #[tokio::test]
    async fn a_duplicate_external_id_is_a_conflict_naming_the_value() {
        use crate::tests::cleanup::cleanup_timeseries;
        use crate::tests::ids::unique_id;

        let probe = Probe::new();
        let ext_id = unique_id("problem_duplicate");
        let body = format!(
            r#"{{"items":[{{"externalId":"{ext_id}","name":"problem duplicate probe","unit":"celsius","valueType":"FLOAT"}}]}}"#
        );

        // Armed before the create, so a failing assertion still tears the series down.
        let _cleanup = cleanup_timeseries(vec![ext_id.clone()]);
        let created = probe.post("/timeseries/create", &body).await;
        assert!(
            (200..300).contains(&created.status),
            "the first create should succeed, got {}: {}",
            created.status,
            created.preview()
        );

        let refusal = probe.post("/timeseries/create", &body).await;
        assert_eq!(
            refusal.status, 409,
            "a taken external id is a conflict, not a malformed request: {}",
            refusal.preview()
        );
        let problem = assert_is_a_typed_problem(&refusal, "a duplicate external id");
        assert_eq!(problem.slug(), Some("duplicate"));
        assert!(
            problem
                .duplicated()
                .iter()
                .any(|entry| entry.values().any(|value| value.as_str() == Some(ext_id.as_str()))),
            "the colliding value should be named, got {}",
            refusal.preview()
        );
    }

    /// Every `GET /<collection>/{id}` miss answers through one shared advice, and it was the last
    /// refusal to gain a `type` (datahub-platform #117). The untyped version was easy to miss: the
    /// decoration ran on it too, so it carried `requestId` and `retry` and looked finished, while a
    /// caller matching `slug()` on `"not-found"` fell through to its default arm.
    #[tokio::test]
    async fn a_missing_id_is_typed_like_every_other_refusal() {
        let refusal = Probe::new().get("/resources/999999999").await;

        assert_eq!(refusal.status, 404);
        let problem = refusal
            .problem()
            .unwrap_or_else(|| panic!("a 404 should be a problem, got {}", refusal.slug_or_shape()));

        // Pinned so that a revert to a hand-built document shows as a missing `type` rather than
        // being masked by the decoration, which ran on the untyped version too.
        assert!(
            problem.request_id().is_some() && problem.retry().is_some(),
            "the advice decorates every problem: {}",
            refusal.preview()
        );

        assert_eq!(
            problem.slug(),
            Some("not-found"),
            "a by-id miss should carry a `type` under {} like every other refusal — got {:?}. \
             ObjectNotFoundExceptionHandler builds its document by hand instead of calling \
             Problems.notFound().",
            ProblemDetail::TYPE_BASE,
            problem.type_uri
        );
    }
}
