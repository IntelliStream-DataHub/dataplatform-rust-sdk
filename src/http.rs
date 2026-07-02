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
}

impl ResponseError {
    pub fn from(message: String) -> Self {
        // 0 is not a valid HTTP status; use 400 so this never panics.
        ResponseError {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    /// A client-side validation error (HTTP 400) surfaced before any request is sent.
    pub fn bad_request(message: String) -> Self {
        ResponseError {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }

    pub fn from_err(error: Error) -> Self {
        if let Some(status) = error.status() {
            return ResponseError {
                status,
                message: error.to_string(),
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
            message: error.to_string(),
        }
    }

    pub fn get_message(&self) -> String {
        self.message.clone()
    }

    pub fn get_status(&self) -> StatusCode {
        self.status
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
            }
        })?;

        Ok(result)
    } else {
        let status = response.status();
        eprintln!("Request failed with status: {status}",);
        Err(ResponseError {
            status,
            message: response
                .text()
                .await
                .unwrap_or_else(|_| "Failed to read response body".to_string()),
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
