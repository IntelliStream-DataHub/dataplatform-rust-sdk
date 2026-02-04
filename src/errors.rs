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