//! [`ResponseError`], the error every service method returns.
//!
//! It carries the HTTP status ([`get_status`](ResponseError::get_status)) and the raw response
//! body ([`get_message`](ResponseError::get_message)) and, when the API refused in the documented
//! way, the RFC 9457 problem document behind it: reach that with
//! [`problem`](ResponseError::problem) and branch on its
//! [`slug`](crate::problem::ProblemDetail::slug) rather than on the prose in `title` or `detail`.
//! Failures the SDK raised itself — a connect timeout, a client-side rejection, an unobtainable
//! token — arrive as the same type with no problem document.
//!
//! [`is_transient`](ResponseError::is_transient),
//! [`is_auth_failure`](ResponseError::is_auth_failure) and
//! [`is_bufferable`](ResponseError::is_bufferable) classify a failure for retry; the durable
//! ingest buffer spools on the last of them, so a 401/403 costs a rotated credential rather than
//! the batch.

use crate::generic::DataWrapperDeserialization;
use oauth2::http::StatusCode;
use reqwest::{Error, Response};
use serde::de::DeserializeOwned;
use std::fmt;
use thiserror::Error;

#[derive(Debug, Error, Clone)]
pub struct ResponseError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
    /// The response's `Content-Type`, when the failure came from the server at all. `None` for an
    /// error this SDK raised before or instead of a response (a transport failure, a client-side
    /// rejection), which is what distinguishes "the server said nothing" from "the server answered
    /// in a shape we did not expect".
    pub(crate) content_type: Option<String>,
}

/// `reqwest::Error`'s own `Display` is often a bare category — `"builder error"` for a body that
/// failed to serialize — with the actual reason one level down in `source()`. Walking the chain is
/// what makes a serialization refusal legible to the caller instead of arriving as two words.
fn describe(error: &(dyn std::error::Error + 'static)) -> String {
    let mut out = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        source = cause.source();
    }
    out
}

impl ResponseError {
    pub fn from(message: String) -> Self {
        // 0 is not a valid HTTP status; use 400 so this never panics.
        ResponseError {
            status: StatusCode::BAD_REQUEST,
            message,
            content_type: None,
        }
    }

    /// A client-side validation error (HTTP 400) surfaced before any request is sent.
    pub fn bad_request(message: String) -> Self {
        ResponseError {
            status: StatusCode::BAD_REQUEST,
            message,
            content_type: None,
        }
    }

    pub fn from_err(error: Error) -> Self {
        if let Some(status) = error.status() {
            return ResponseError {
                status,
                message: describe(&error),
                content_type: None,
            };
        }
        // No HTTP status means a transport-level failure (connect/timeout/dropped request). Map those
        // to a retryable 503 so durable buffering retries them rather than treating them as terminal.
        let status = if error.is_connect() || error.is_timeout() || error.is_request() {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::BAD_REQUEST
        };
        ResponseError {
            status,
            message: describe(&error),
            content_type: None,
        }
    }

    pub fn get_message(&self) -> String {
        self.message.clone()
    }

    pub fn get_status(&self) -> StatusCode {
        self.status
    }

    /// The response's `Content-Type`, when this error carries a server response at all.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    /// The RFC 9457 problem document the server explained itself with, or `None` when it did not.
    ///
    /// `None` covers every body that is not a problem — an empty body, a plain-text refusal,
    /// Spring Boot's whitelabel error JSON — and every error raised before a response existed
    /// (a connect failure, a client-side refusal, an unobtainable token). So this
    /// answers "did the api explain this failure in the documented way", which is the question a
    /// caller wants; [`get_message`](Self::get_message) keeps the raw body either way.
    ///
    /// Parsed on each call rather than eagerly: the overwhelming majority of `ResponseError`s are
    /// never asked, and the type stays cheap to clone.
    pub fn problem(&self) -> Option<crate::problem::ProblemDetail> {
        crate::problem::ProblemDetail::parse(&self.message)
    }

    /// The problem type's slug — `"would-strand"`, `"duplicate"` — when the server sent a typed
    /// problem. This is the member to branch on; `title` and `detail` are prose.
    pub fn problem_slug(&self) -> Option<String> {
        self.problem()?.slug().map(str::to_string)
    }

    /// A transient failure worth a quick retry: transport failure (status 0), request timeout (408),
    /// rate limiting (429), or a server error (5xx).
    pub fn is_transient(&self) -> bool {
        let code = self.status.as_u16();
        code == 0 || code == 408 || code == 429 || (500..600).contains(&code)
    }

    /// An authentication/authorization failure: 401 Unauthorized or 403 Forbidden. Recoverable by
    /// fixing the credential out-of-band (e.g. refreshing an expired/rotated token), so ingestion
    /// buffers these rather than dropping the data.
    pub fn is_auth_failure(&self) -> bool {
        let code = self.status.as_u16();
        code == 401 || code == 403
    }

    /// Whether this error is worth buffering and retrying rather than surfacing as terminal. True for
    /// transient failures ([`is_transient`](Self::is_transient)) and auth failures
    /// ([`is_auth_failure`](Self::is_auth_failure)); a genuine terminal 4xx (e.g. 400 Bad Request) is
    /// surfaced so the caller can fix the request. Mirrors the Java SDK's `IngestResult.isBufferable`.
    pub fn is_bufferable(&self) -> bool {
        self.is_transient() || self.is_auth_failure()
    }
}
impl fmt::Display for ResponseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.status, self.message)
    }
}

#[doc(hidden)]
pub async fn process_response<T>(response: Response, path: &str) -> Result<T, ResponseError>
where
    T: DeserializeOwned + DataWrapperDeserialization,
{
    let status = response.status();
    if (200..300).contains(&status.as_u16()) {
        // Read the response body and attempt to deserialize
        let body = response.text().await.map_err(|err| {
            eprintln!("Failed to read response body: {err}",);
            ResponseError {
                status,
                message: err.to_string(),
                content_type: None,
            }
        })?;

        let max_chars = 2000;
        let truncated_body = &body[..body.len().min(max_chars)];
        println!("Response body for path: {}\n{}", path, &truncated_body); // Debug output

        // Conditionally apply custom or default logic
        let result: T = T::deserialize_and_set_status(&body, status.as_u16()).map_err(|err| {
            eprintln!("Failed to deserialize JSON: {err}",);
            ResponseError {
                status,
                message: err.to_string(),
                content_type: None,
            }
        })?;

        Ok(result)
    } else {
        let status = response.status();
        eprintln!("Request failed with status: {status}",);
        // Read the header before the body: `text()` consumes the response.
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        Err(ResponseError {
            status,
            message: response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string()),
            content_type,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ResponseError;
    use oauth2::http::StatusCode;

    fn err(code: u16) -> ResponseError {
        ResponseError {
            status: StatusCode::from_u16(code).unwrap(),
            message: String::new(),
            content_type: None,
        }
    }

    #[test]
    fn auth_failures_are_bufferable_but_not_transient() {
        // 401/403 are recoverable by fixing the credential out-of-band, so they buffer (matching the
        // Java SDK), but they are not "transient" blips.
        for code in [401u16, 403] {
            assert!(err(code).is_auth_failure(), "{code} should be an auth failure");
            assert!(err(code).is_bufferable(), "{code} should buffer");
            assert!(!err(code).is_transient(), "{code} is not transient");
        }
    }

    #[test]
    fn transient_failures_are_bufferable() {
        // (status 0 — the SDK's transport-failure sentinel — can't be built via StatusCode, so it's
        // not exercised here; it's covered by the `code == 0` arm in is_transient.)
        for code in [408u16, 429, 500, 503] {
            assert!(err(code).is_transient(), "{code} should be transient");
            assert!(err(code).is_bufferable(), "{code} should buffer");
            assert!(!err(code).is_auth_failure(), "{code} is not an auth failure");
        }
    }

    /// A `reqwest::Error` wrapping a serialization refusal displays as `"builder error"`; the
    /// reason is one level down. Without walking the chain the caller sees two useless words.
    #[test]
    fn a_wrapped_cause_reaches_the_message() {
        #[derive(Debug)]
        struct Outer(Inner);
        #[derive(Debug)]
        struct Inner;
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "builder error")
            }
        }
        impl std::fmt::Display for Inner {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a bare Resource labelled `dataset` cannot be sent")
            }
        }
        impl std::error::Error for Inner {}
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }

        let described = super::describe(&Outer(Inner));
        assert!(described.starts_with("builder error"));
        assert!(
            described.contains("a bare Resource labelled `dataset` cannot be sent"),
            "the cause should be appended: {described}"
        );
    }

    /// A cause whose text the outer error already carries is not repeated.
    #[test]
    fn a_redundant_cause_is_not_repeated() {
        #[derive(Debug)]
        struct Outer;
        #[derive(Debug)]
        struct Inner;
        impl std::fmt::Display for Outer {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "connection refused by host")
            }
        }
        impl std::fmt::Display for Inner {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "connection refused")
            }
        }
        impl std::error::Error for Inner {}
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&Inner)
            }
        }

        assert_eq!(super::describe(&Outer), "connection refused by host");
    }

    #[test]
    fn terminal_client_errors_are_not_bufferable() {
        // A genuine bad request must surface so the caller fixes it, not spool forever.
        for code in [400u16, 404, 409, 422] {
            assert!(!err(code).is_bufferable(), "{code} should be terminal");
            assert!(!err(code).is_transient());
            assert!(!err(code).is_auth_failure());
        }
    }
}
