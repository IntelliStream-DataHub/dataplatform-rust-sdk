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

    /// Whether this error is worth buffering and retrying (transport failure, timeout, 429 or 5xx)
    /// rather than surfacing (a terminal 4xx).
    pub fn is_transient(&self) -> bool {
        let code = self.status.as_u16();
        code == 0 || code == 408 || code == 429 || (500..600).contains(&code)
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
