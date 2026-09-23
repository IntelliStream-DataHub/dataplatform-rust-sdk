//! [`DataHubError`], the error type for configuration and authentication — what can go wrong
//! *before* a request is made.
//!
//! [`DataHubConfig::from_env`](crate::datahub::DataHubConfig::from_env) and its siblings return it
//! for a missing or unparseable setting, and the token exchange returns it for an OAuth2 or URL
//! failure. Service methods never surface it: the SDK maps a failed token acquisition to a 401
//! [`ResponseError`](crate::http::ResponseError) so every API call has one error type.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataHubError {
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("URL Parse error")]
    UrlError(#[from] oauth2::url::ParseError),
    #[error("OAuth2 Request failed: {0}")]
    OAuthError(String),
    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("JSON serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
}
