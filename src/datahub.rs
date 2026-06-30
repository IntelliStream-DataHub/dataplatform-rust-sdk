use crate::errors::DataHubError;
use chrono::{DateTime, Duration, Utc};
use dotenv::from_path;
use maplit::hashmap;
use oauth2::basic::{BasicClient, BasicTokenResponse, BasicTokenType};
use oauth2::{
    reqwest, AccessToken, ClientId, ClientSecret, EmptyExtraTokenFields, Scope, TokenResponse,
    TokenUrl,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{default, env};
use tokio::sync::RwLock;

/// Default durable-buffer time window applied when buffering is enabled without an explicit value.
pub const DEFAULT_BUFFER_RETENTION_MS: i64 = 6 * 3600 * 1000; // 6 hours
/// Default durable-buffer size cap (5 GiB) applied when buffering is enabled without an explicit value.
pub const DEFAULT_BUFFER_MAX_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Default directory for the on-disk ingest spools.
pub const DEFAULT_BUFFER_DIR: &str = ".datahub-spool";

#[derive(Default, Deserialize, Debug, Clone)]
pub struct OAuthConfig {
    #[serde(alias = "CLIENT_ID")]
    pub(crate) client_id: Option<String>,

    #[serde(alias = "CLIENT_SECRET")]
    pub(crate) client_secret: Option<String>,

    #[serde(alias = "TOKEN_URI")]
    pub(crate) token_uri: Option<String>,

    #[serde(alias = "PROJECT_NAME")]
    pub(crate) project_name: Option<String>,
}
#[derive(Default, Debug, Clone)]
struct AuthState {
    pub token: Option<oauth2::basic::BasicTokenResponse>,
    pub expire_time: Option<DateTime<Utc>>,
}
#[derive(Debug, Clone)]
pub struct DataHubApi {
    pub(crate) config: Arc<OAuthConfig>,
    pub(crate) auth_state: Arc<RwLock<AuthState>>,
    pub(crate) base_url: String,
    pub(crate) oauth2_client: Option<
        oauth2::Client<
            oauth2::basic::BasicErrorResponse,
            oauth2::basic::BasicTokenResponse,
            oauth2::basic::BasicTokenIntrospectionResponse,
            oauth2::StandardRevocableToken,
            oauth2::basic::BasicRevocationErrorResponse,
            oauth2::EndpointNotSet,
            oauth2::EndpointNotSet,
            oauth2::EndpointNotSet,
            oauth2::EndpointNotSet,
            oauth2::EndpointSet,
        >,
    >,
    pub(crate) http_client: reqwest::Client,
    // Durable ingest buffering (off unless requested). Either bound may be unset; when buffering is
    // on, an unset bound falls back to its default (6h / 5 GiB).
    pub(crate) buffering_requested: bool,
    pub(crate) buffer_retention_ms: Option<i64>,
    pub(crate) buffer_max_bytes: Option<u64>,
    pub(crate) buffer_dir: Option<PathBuf>,
}
impl AuthState {
    pub fn is_expired(&self) -> bool {
        if let Some(expire_time) = self.expire_time {
            expire_time < Utc::now()
        } else {
            false
        }
    }
}

impl DataHubApi {
    pub fn from_envfile(path: Option<&str>) -> Result<Self, DataHubError> {
        if let Some(path) = path {
            // Load a specific .env file
            from_path(Path::new(path)).expect("Failed to load .env from custom path");
        } else {
            // Load the default .env in project root
            dotenv::dotenv().ok();
        }
        Self::from_env()
    }
    pub fn from_env() -> Result<Self, DataHubError> {
        let env_vars = env::vars().collect::<HashMap<String, String>>();
        Self::from_map(env_vars)
    }
    pub fn create_default() -> DataHubApi {
        //let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        DataHubApi::from_env().unwrap()
    }

    pub fn from_vars(
        base_url: String,
        token: Option<String>,
        token_uri: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        project_name: Option<String>,
    ) -> DataHubApi {
        let oauthconfig = OAuthConfig {
            client_id,
            client_secret,
            token_uri,
            project_name,
        };

        // Oauth client will only be configured if all required fields are present
        // Environment passed Token will be used if no oauth config is present
        let client = Self::setup_oauth(&oauthconfig);

        // this handles environment passed token
        let auth_state = if let Some(t) = token {
            let token = BasicTokenResponse::new(
                AccessToken::new(t.to_string()),
                BasicTokenType::Bearer,
                EmptyExtraTokenFields {},
            );

            Arc::new(RwLock::new(AuthState {
                token: Some(token.clone()),
                expire_time: None, // user passed token has no expire time. is_expired() returns true always
            }))
        } else {
            // if token is not passed, token and expire_time will be None
            Arc::new(RwLock::new(AuthState::default()))
        };
        Self {
            config: Arc::new(oauthconfig),
            base_url,
            oauth2_client: client,
            http_client: reqwest::Client::new(),
            auth_state,
            buffering_requested: false,
            buffer_retention_ms: None,
            buffer_max_bytes: None,
            buffer_dir: None,
        }
    }

    pub(crate)  fn from_map(map: HashMap<String, String>) -> Result<Self, DataHubError> {
        let baseurl = map.get("BASE_URL")
            .ok_or_else(|| DataHubError::ConfigError(
                "BASE_URL is not set. Define it in your .env file or export it in the environment (e.g. BASE_URL=http://localhost:8081).".to_string()
            ))?
            .to_string();

        let oauthconfig: OAuthConfig = serde_json::from_value(serde_json::to_value(&map)?)?;

        // Oauth client will only be configured if all required fields are present
        // Environment passed Token will be used if no oauth config is present
        let client = Self::setup_oauth(&oauthconfig);

        // this handles environment passed token
        let auth_state = if let Some(t) = map.get("TOKEN") {
            let token = BasicTokenResponse::new(
                AccessToken::new(t.to_string()),
                BasicTokenType::Bearer,
                EmptyExtraTokenFields {},
            );

            Arc::new(RwLock::new(AuthState {
                token: Some(token.clone()),
                expire_time: None, // user passed token has no expire time. is_expired() returns true always
            }))
        } else {
            // if token is not passed, token and expire_time will be None
            Arc::new(RwLock::new(AuthState::default()))
        };
        // Durable buffering env config (all optional): ENABLE_BUFFERING, BUFFER_RETENTION_SECS,
        // BUFFER_MAX_BYTES, BUFFER_DIR. Setting any retention/size bound also enables buffering.
        let buffering_requested = map
            .get("ENABLE_BUFFERING")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);
        let buffer_retention_ms = map
            .get("BUFFER_RETENTION_SECS")
            .and_then(|v| v.parse::<i64>().ok())
            .map(|secs| secs * 1000);
        let buffer_max_bytes = map.get("BUFFER_MAX_BYTES").and_then(|v| v.parse::<u64>().ok());
        let buffer_dir = map.get("BUFFER_DIR").map(PathBuf::from);

        Ok(Self {
            config: Arc::new(oauthconfig),
            base_url: baseurl.to_string(),
            oauth2_client: client,
            http_client: reqwest::Client::new(),
            auth_state,
            buffering_requested,
            buffer_retention_ms,
            buffer_max_bytes,
            buffer_dir,
        })
    }

    /// Enable durable ingest buffering with default bounds (6h window, 5 GiB cap). Off by default.
    pub fn enable_buffering(&mut self) -> &mut Self {
        self.buffering_requested = true;
        self
    }

    /// Set the buffer time window (seconds); also enables buffering.
    pub fn set_buffer_retention_secs(&mut self, secs: i64) -> &mut Self {
        self.buffer_retention_ms = Some(secs * 1000);
        self.buffering_requested = true;
        self
    }

    /// Set the buffer size cap in bytes (per spool stream); also enables buffering.
    pub fn set_buffer_max_bytes(&mut self, bytes: u64) -> &mut Self {
        self.buffer_max_bytes = Some(bytes);
        self.buffering_requested = true;
        self
    }

    /// Set the directory for the on-disk spools (default `.datahub-spool`).
    pub fn set_buffer_dir<P: Into<PathBuf>>(&mut self, dir: P) -> &mut Self {
        self.buffer_dir = Some(dir.into());
        self
    }

    /// Whether durable ingest buffering is enabled (a bound was set or it was explicitly enabled).
    pub fn buffering_enabled(&self) -> bool {
        self.buffering_requested
            || self.buffer_retention_ms.is_some()
            || self.buffer_max_bytes.is_some()
    }

    /// Effective time window (applies the default when enabled but unset), else `None`.
    pub(crate) fn effective_buffer_retention_ms(&self) -> Option<i64> {
        self.buffering_enabled()
            .then(|| self.buffer_retention_ms.unwrap_or(DEFAULT_BUFFER_RETENTION_MS))
    }

    /// Effective size cap (applies the default when enabled but unset), else `None`.
    pub(crate) fn effective_buffer_max_bytes(&self) -> Option<u64> {
        self.buffering_enabled()
            .then(|| self.buffer_max_bytes.unwrap_or(DEFAULT_BUFFER_MAX_BYTES))
    }

    /// Directory the on-disk spools live in.
    pub(crate) fn buffer_directory(&self) -> PathBuf {
        self.buffer_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_BUFFER_DIR))
    }

    async fn refresh_token(
        &self,
    ) -> Result<Option<oauth2::basic::BasicTokenResponse>, DataHubError> {
        /// will use refresh token if  present otherwise it will make a new client credentials request
        let refresh_token = {
            let authstate = self.auth_state.read().await;
            authstate
                .token
                .as_ref()
                .and_then(|t| t.refresh_token().cloned())
        };

        let token_result = if let Some(refresh_token) = refresh_token {
            let Some(authclient) = self.oauth2_client.as_ref() else {
                return Err(DataHubError::ConfigError(format!(
                    "OAuth2 Client not configured"
                )));
            };
            authclient
                .exchange_refresh_token(&refresh_token)
                .request_async(&self.http_client)
                .await
        } else {
            let Some(authclient) = self.oauth2_client.as_ref() else {
                return Err(DataHubError::ConfigError(format!(
                    "OAuth2 Client not configured"
                )));
            };
            authclient
                .exchange_client_credentials()
                .request_async(&self.http_client)
                .await
        };
        let new_token = token_result
            .map_err(|e| DataHubError::OAuthError(format!("OAuth2 Request failed: {}", e)))?;
        let expire_time = new_token.expires_in().map(|duration| Utc::now() + duration);

        {
            // lock scope
            let mut auth_state = self.auth_state.write().await;
            // double check  the token has not been refreshed while waiting for network
            if let Some(t) = &auth_state.token {
                if !auth_state.is_expired() {
                    return Ok(Some(t.clone()));
                }
            }
            auth_state.token = Some(new_token.clone());
            auth_state.expire_time = expire_time;
            Ok(Some(new_token))
        }
    }

    pub async fn get_api_token(&self) -> Result<String, DataHubError> {
        {
            // lock scope. read and if expired refresh token
            let authstate = self.auth_state.read().await;
            if let Some(t) = &authstate.token {
                if !authstate.is_expired() {
                    return Ok(t.access_token().secret().clone());
                }
            }
        }
        let new_token = self.refresh_token().await?;

        Ok(new_token.unwrap().access_token().secret().clone())
    }
    fn setup_oauth(
        oauth_config: &OAuthConfig,
    ) -> Option<
        oauth2::Client<
            oauth2::basic::BasicErrorResponse,
            oauth2::basic::BasicTokenResponse,
            oauth2::basic::BasicTokenIntrospectionResponse,
            oauth2::StandardRevocableToken,
            oauth2::basic::BasicRevocationErrorResponse,
            oauth2::EndpointNotSet,
            oauth2::EndpointNotSet,
            oauth2::EndpointNotSet,
            oauth2::EndpointNotSet,
            oauth2::EndpointSet,
        >,
    > {
        let (Some(client_id), Some(client_secret), Some(token_uri)) = (
            &oauth_config.client_id,
            &oauth_config.client_secret,
            &oauth_config.token_uri,
        ) else {
            return None;
        };

        Some(
            BasicClient::new(ClientId::new(client_id.clone()))
                .set_client_secret(ClientSecret::new(client_secret.clone()))
                .set_token_uri(TokenUrl::new(token_uri.clone()).expect("Invalid Token URI")),
        )
    }
}

pub fn to_snake_lower_cased_allow_start_with_digits(s: &str) -> String {
    let s = s.to_lowercase();
    let re = Regex::new(r"[\s\W]+").unwrap();
    let replaced = re.replace_all(&s, "_").into_owned();
    // Trim trailing underscores, but preserve leading if the original started with them
    if s.chars()
        .next()
        .map_or(false, |c| c.is_whitespace() || !c.is_alphanumeric())
    {
        replaced.trim_end_matches('_').to_string()
    } else {
        replaced.trim_matches('_').to_string()
    }
}
