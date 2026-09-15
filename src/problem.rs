//! RFC 9457 `application/problem+json` — the shape the api describes a failure in.
//!
//! The api is converging every refusal on this one document (its `Problems` helper is the single
//! place that builds them). Until that lands a caller meets three kinds of body, so
//! [`ProblemDetail`] is deliberately lenient about what it is handed:
//!
//! - a full problem — `type`, `title`, `status`, `detail`, `instance`, plus extension members;
//! - a bare Spring `ProblemDetail` with **no `type`** (today's `GET /resources/{id}` 404);
//! - not a problem at all — Spring Boot's whitelabel `{"timestamp","status","error","trace"}`, a
//!   success-shaped `{"items":[…]}` envelope, the legacy `{"error":{…}}` wrapper, plain text, or
//!   nothing. `src/problem_integration.rs` has the full catalogue of what answers what.
//!
//! [`ProblemDetail::parse`] answers `None` for the third group, so a caller can ask "did the server
//! explain itself in the documented way" and get a truthful answer rather than a half-filled struct.
//!
//! # `type` is the contract, not `title`
//!
//! RFC 9457 §3.1.1: `title` is prose and may be rewritten at any time; `type` is a stable
//! identifier. Branch on [`ProblemDetail::slug`] — the kebab-case tail under
//! [`ProblemDetail::TYPE_BASE`] — never on the wording of `title` or `detail`.
//!
//! # Unknown members are not an error
//!
//! §3.2 requires consumers to ignore extension members they do not recognise. Everything outside
//! the five standard members is kept verbatim in [`ProblemDetail::extensions`], so an extension the
//! api adds later cannot make this parser fail — and a member the SDK has no typed accessor for is
//! still reachable.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// An RFC 9457 problem document.
///
/// Every standard member is optional, as the RFC has it: a server may send as little as a `status`.
/// Nothing here is defaulted — an absent `type` reads as `None`, never as `about:blank`, because a
/// caller cannot otherwise tell a problem the api typed from one it did not.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProblemDetail {
    /// The stable identifier for *what kind* of failure this is. Absent on the api's bare 404s.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub type_uri: Option<String>,

    /// Human-readable summary. Prose — do not branch on it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,

    /// The status the *server* put in the body. May be absent even when the HTTP status is not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,

    /// Human-readable explanation specific to this occurrence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,

    /// The request this problem is about — the api fills it with the request path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<String>,

    /// Every member outside the five standard ones, verbatim (§3.2).
    #[serde(flatten)]
    pub extensions: Map<String, Value>,
}

/// What the caller can do about a problem — the api's `retry` extension member.
///
/// Advisory, and deliberately *not* wired into [`crate::http::ResponseError::is_bufferable`]: the
/// SDK buffers 401/403 so a rotated credential does not cost the batch, which is a different
/// question from whether repeating the request unchanged could ever work.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Retry {
    /// The same request can succeed later. Honour `Retry-After` when the response carries one.
    SameRequest,
    /// Only a different request can succeed — fix the payload.
    ChangeRequest,
    /// Nothing the caller sends will work until an operator acts. Quote the `requestId`.
    NeedsOperator,
    /// A value this SDK predates.
    Other(String),
}

impl Retry {
    fn from_wire(value: &str) -> Self {
        match value {
            "same-request" => Retry::SameRequest,
            "change-request" => Retry::ChangeRequest,
            "needs-operator" => Retry::NeedsOperator,
            other => Retry::Other(other.to_string()),
        }
    }
}

/// One rejected field, from the `fields` extension.
///
/// `code` is the i18n key behind `message`, so a caller can localise instead of parsing English;
/// `rejected` is an argument such as an offending length — never the submitted value, which the api
/// withholds because it can be a credential.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldProblem {
    pub field: Option<String>,
    pub message: Option<String>,
    pub code: Option<String>,
    pub rejected: Option<Value>,
}

/// One unrecognised property, from the `errors` extension of a `unreadable-request-body` problem.
///
/// `allowed_fields` is scoped to the pointer's own position: two unknown fields at different depths
/// accept different names, which is why the api reports them per-entry rather than as one flat list.
#[derive(Debug, Clone, PartialEq)]
pub struct UnknownField {
    /// RFC 6901 JSON Pointer to the offending property.
    pub pointer: Option<String>,
    pub detail: Option<String>,
    pub allowed_fields: Vec<String>,
}

impl ProblemDetail {
    /// Every problem type the api mints lives under this prefix.
    pub const TYPE_BASE: &'static str = "https://intellistream.ai/errors/";

    /// Reads the leading JSON value and **ignores whatever follows it**.
    ///
    /// Not pedantry about stray bytes: this SDK appends to the body itself. `explain_auth_failure`
    /// adds ` — names 2 organizations (…)` to a 401 that arrived without a reason, so the moment
    /// the api starts sending a problem document on 401 — the case that advice exists for — a
    /// whole-string parse would reject exactly the response it was added to help with.
    fn leading_json(body: &str) -> Option<Value> {
        serde_json::Deserializer::from_str(body)
            .into_iter::<Value>()
            .next()?
            .ok()
    }

    /// Reads a response body as a problem document, or `None` when it is not one.
    ///
    /// Detection is **structural, not by content type**. Two reasons: a problem can reach a client
    /// labelled `application/json` (a handler declaring `produces` that way, or any proxy that
    /// rewrites the header), and — more importantly — a body that *is* labelled `problem+json` is
    /// not automatically a problem document. Requiring one of `type`, `title` or `detail` is what
    /// separates a real problem from Spring Boot's whitelabel `{"timestamp","status","error",
    /// "trace"}`, which carries a `status` and nothing else a caller can use.
    pub fn parse(body: &str) -> Option<Self> {
        let trimmed = body.trim_start();
        if !trimmed.starts_with('{') {
            return None;
        }
        let value = Self::leading_json(trimmed)?;
        let object = value.as_object()?;

        // `status` alone is not enough — the whitelabel error body has one too.
        let described = ["type", "title", "detail"]
            .iter()
            .any(|member| object.get(*member).is_some_and(Value::is_string));
        if !described {
            return None;
        }
        serde_json::from_value(value).ok()
    }

    /// The kebab-case tail of [`type_uri`](Self::type_uri) — `"would-strand"` for
    /// `https://intellistream.ai/errors/would-strand`.
    ///
    /// `None` for an absent type *and* for a type this api did not mint, so a match on the slug
    /// cannot be fooled by another service's URI that happens to end in the same word.
    pub fn slug(&self) -> Option<&str> {
        self.type_uri
            .as_deref()?
            .strip_prefix(Self::TYPE_BASE)
            .filter(|slug| !slug.is_empty())
    }

    /// The `retry` extension: what the caller can do about this problem.
    pub fn retry(&self) -> Option<Retry> {
        self.extension_str("retry").map(Retry::from_wire)
    }

    /// The `requestId` extension — what to quote to an operator.
    pub fn request_id(&self) -> Option<&str> {
        self.extension_str("requestId")
    }

    /// The `docs` extension: the page explaining this problem. Absent when nothing is written yet.
    pub fn docs(&self) -> Option<&str> {
        self.extension_str("docs")
    }

    /// The `fields` extension — one entry per rejected field.
    pub fn fields(&self) -> Vec<FieldProblem> {
        self.extension_array("fields")
            .into_iter()
            .map(|entry| FieldProblem {
                field: string_member(entry, "field"),
                message: string_member(entry, "message"),
                code: string_member(entry, "code"),
                rejected: entry.get("rejected").cloned(),
            })
            .collect()
    }

    /// The `errors` extension — the unrecognised properties of a rejected request body.
    pub fn unknown_fields(&self) -> Vec<UnknownField> {
        self.extension_array("errors")
            .into_iter()
            .map(|entry| UnknownField {
                pointer: string_member(entry, "pointer"),
                detail: string_member(entry, "detail"),
                allowed_fields: entry
                    .get("allowedFields")
                    .and_then(Value::as_array)
                    .map(|names| {
                        names
                            .iter()
                            .filter_map(|name| name.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default(),
            })
            .collect()
    }

    /// The `duplicated` extension of a `duplicate` conflict — the identifiers that collided, as
    /// `field -> value`.
    pub fn duplicated(&self) -> Vec<Map<String, Value>> {
        self.extension_objects("duplicated")
    }

    /// The `blockedBy` extension of a refused delete — what is standing in the way.
    ///
    /// Structured records (a subscription's id and external id, a stranded node's external id),
    /// not `field -> message` pairs: the point of the message is that the caller can read the
    /// blocker's external id and go deal with it.
    pub fn blocked_by(&self) -> Vec<Map<String, Value>> {
        self.extension_objects("blockedBy")
    }

    /// `line`/`column` of a syntax error in the request body, when the api located one.
    pub fn location(&self) -> Option<(u64, u64)> {
        let line = self.extensions.get("line")?.as_u64()?;
        let column = self.extensions.get("column").and_then(Value::as_u64).unwrap_or(0);
        Some((line, column))
    }

    /// The `pointer` extension — the RFC 6901 pointer to a single offending field.
    pub fn pointer(&self) -> Option<&str> {
        self.extension_str("pointer")
    }

    fn extension_str(&self, member: &str) -> Option<&str> {
        self.extensions.get(member)?.as_str()
    }

    fn extension_array(&self, member: &str) -> Vec<&Map<String, Value>> {
        self.extensions
            .get(member)
            .and_then(Value::as_array)
            .map(|entries| entries.iter().filter_map(Value::as_object).collect())
            .unwrap_or_default()
    }

    fn extension_objects(&self, member: &str) -> Vec<Map<String, Value>> {
        self.extension_array(member).into_iter().cloned().collect()
    }
}

fn string_member(object: &Map<String, Value>, member: &str) -> Option<String> {
    object.get(member)?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::{ProblemDetail, Retry};

    /// Captured from the running api: `POST /timeseries/filter` with an unknown property. The
    /// `errors` extension is the whole reason rejecting unknown fields is an improvement over
    /// silently dropping them, so it has to survive the round trip.
    const UNKNOWN_FIELD: &str = r##"{"detail":"Unknown field: bogusField","instance":"/timeseries/filter","status":400,"title":"Bad Request","type":"https://intellistream.ai/errors/unreadable-request-body","errors":[{"detail":"Unknown field","pointer":"#/bogusField/bogusField","allowedFields":["cursor","filter","limit","sort"]}]}"##;

    /// Captured from the running api: `GET /timeseries/999999999`. A real problem document, but
    /// with **no `type`** — the shape that must not be defaulted into `about:blank`.
    const TYPELESS_404: &str = r#"{"detail":"Timeseries with id: 999999999 Not found!","instance":"/timeseries/999999999","status":404,"title":"Not Found"}"#;

    /// Captured from the running api: `POST /datasets/list`. Spring Boot's whitelabel body — a
    /// `status` and a stack trace, none of the members a client can act on.
    const WHITELABEL: &str = r#"{"timestamp":"2026-09-15T14:34:54.280Z","status":405,"error":"Method Not Allowed","trace":"org.springframework.web.HttpRequestMethodNotSupportedException: Request method 'POST' is not supported\n\tat org.springframework.web.servlet...","message":"Method 'POST' is not supported.","path":"/datasets/list"}"#;

    #[test]
    fn a_full_problem_parses_with_its_extension() {
        let problem = ProblemDetail::parse(UNKNOWN_FIELD).expect("a problem document");

        assert_eq!(problem.slug(), Some("unreadable-request-body"));
        assert_eq!(problem.status, Some(400));
        assert_eq!(problem.instance.as_deref(), Some("/timeseries/filter"));

        let unknown = problem.unknown_fields();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].pointer.as_deref(), Some("#/bogusField/bogusField"));
        assert!(
            unknown[0].allowed_fields.contains(&"cursor".to_string()),
            "the names the endpoint does accept should survive: {:?}",
            unknown[0].allowed_fields
        );
    }

    /// An absent `type` stays absent. Defaulting it would report a typed problem where the api sent
    /// none, and `slug()` is what callers branch on.
    #[test]
    fn a_typeless_problem_keeps_a_none_type() {
        let problem = ProblemDetail::parse(TYPELESS_404).expect("a problem document");

        assert_eq!(problem.type_uri, None);
        assert_eq!(problem.slug(), None);
        assert_eq!(problem.title.as_deref(), Some("Not Found"));
        assert_eq!(problem.status, Some(404));
    }

    /// The discriminator is structural. A whitelabel body has a `status`, so keying on that alone
    /// would report a stack trace as an explained failure.
    #[test]
    fn the_whitelabel_error_body_is_not_a_problem() {
        assert_eq!(ProblemDetail::parse(WHITELABEL), None);
    }

    #[test]
    fn a_non_problem_body_is_none_rather_than_an_error() {
        // Every one of these is a real thing this api answers with today.
        for body in [
            "",
            "   ",
            "Limit cannot be greater than 10000",
            "<html><body>502 Bad Gateway</body></html>",
            r#"{"items":[]}"#,
            r#"{"status":503}"#,
            "{not json at all",
            "[]",
        ] {
            assert_eq!(ProblemDetail::parse(body), None, "should not parse: {body:?}");
        }
    }

    /// §3.2: a consumer must ignore extension members it does not recognise. A member added to the
    /// api after this SDK shipped must not turn a parseable problem into an unparseable one.
    #[test]
    fn an_unrecognised_extension_member_does_not_break_parsing() {
        let body = r#"{"type":"https://intellistream.ai/errors/duplicate","title":"Conflict","status":409,"detail":"Already exists.","somethingAddedLater":{"nested":[1,2]},"duplicated":[{"externalId":"pump_1"}]}"#;

        let problem = ProblemDetail::parse(body).expect("a problem document");
        assert_eq!(problem.slug(), Some("duplicate"));
        assert!(
            problem.extensions.contains_key("somethingAddedLater"),
            "an unknown member is kept verbatim, not dropped"
        );
        assert_eq!(problem.duplicated()[0]["externalId"], "pump_1");
    }

    /// A slug match must not be fooled by a foreign URI ending in the same word.
    #[test]
    fn a_foreign_type_uri_has_no_slug() {
        for foreign in [
            r#"{"type":"https://example.invalid/errors/not-found","title":"Not Found"}"#,
            r#"{"type":"about:blank","title":"Not Found"}"#,
            // The base with nothing after it names no problem.
            r#"{"type":"https://intellistream.ai/errors/","title":"Not Found"}"#,
        ] {
            let problem = ProblemDetail::parse(foreign).expect("a problem document");
            assert_eq!(problem.slug(), None, "should not match a slug: {foreign}");
        }
    }

    #[test]
    fn the_validation_fields_extension_carries_code_and_argument() {
        // The api's own golden body (ProblemWireShapeTest): `fields` is top-level, never nested
        // under a `properties` object.
        let body = r#"{"type":"https://intellistream.ai/errors/validation-failed","title":"Validation failed","status":400,"instance":"/boom","fields":[{"field":"source","message":"Source max length is 128 characters.","code":"resource.source.max.length.error","rejected":129}]}"#;

        let problem = ProblemDetail::parse(body).expect("a problem document");
        let fields = problem.fields();

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].field.as_deref(), Some("source"));
        assert_eq!(
            fields[0].code.as_deref(),
            Some("resource.source.max.length.error"),
            "the i18n key is what lets a caller localise instead of parsing prose"
        );
        assert_eq!(fields[0].rejected, Some(serde_json::json!(129)));
        assert!(
            !problem.extensions.contains_key("properties"),
            "`fields` must be a top-level member, not nested"
        );
    }

    #[test]
    fn retry_reads_the_three_documented_values_and_survives_a_fourth() {
        let of = |retry: &str| {
            ProblemDetail::parse(&format!(r#"{{"title":"x","retry":"{retry}"}}"#))
                .unwrap()
                .retry()
        };
        assert_eq!(of("same-request"), Some(Retry::SameRequest));
        assert_eq!(of("change-request"), Some(Retry::ChangeRequest));
        assert_eq!(of("needs-operator"), Some(Retry::NeedsOperator));
        assert_eq!(of("invented-later"), Some(Retry::Other("invented-later".into())));

        // Absent rather than assumed: today's api sends no `retry` at all.
        assert_eq!(ProblemDetail::parse(TYPELESS_404).unwrap().retry(), None);
    }

    /// The SDK appends an organization hint to an unexplained 401 (`explain_auth_failure`). When
    /// the api starts sending a problem on 401, that hint sits *after* the JSON — so a parse that
    /// required the whole body to be one value would fail on precisely the response the hint was
    /// added to improve, and only for a principal in more than one organization.
    #[test]
    fn a_problem_with_the_sdks_own_hint_appended_still_parses() {
        let body = r#"{"type":"https://intellistream.ai/errors/unauthorized","title":"Unauthorized","status":401,"detail":"Authentication is required."} — the token names 2 organizations (acme, beta); set SCOPE=organization:<alias>"#;

        let problem = ProblemDetail::parse(body).expect("the leading document should still parse");
        assert_eq!(problem.slug(), Some("unauthorized"));
        assert_eq!(problem.status, Some(401));
    }

    #[test]
    fn a_syntax_error_reports_where_it_failed() {
        let body = r#"{"type":"https://intellistream.ai/errors/unreadable-request-body","title":"Bad Request","status":400,"detail":"Unexpected end-of-input","line":1,"column":10}"#;
        let problem = ProblemDetail::parse(body).expect("a problem document");
        assert_eq!(problem.location(), Some((1, 10)));
    }

    /// A refused delete names the blockers so the caller can go remove them.
    #[test]
    fn a_blocked_delete_lists_what_is_in_the_way() {
        let body = r#"{"type":"https://intellistream.ai/errors/would-strand","title":"Delete refused","status":409,"detail":"Deleting this edge would strand a resource.","blockedBy":[{"type":"strandedResource","externalId":"rust_sdk_node_b"}]}"#;

        let problem = ProblemDetail::parse(body).expect("a problem document");
        assert_eq!(problem.slug(), Some("would-strand"));

        let blockers = problem.blocked_by();
        assert_eq!(blockers.len(), 1);
        assert_eq!(blockers[0]["externalId"], "rust_sdk_node_b");
    }
}
