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
pub const DEFAULT_BUFFER_RETENTION_MS: i64 = 72 * 3600 * 1000; // 72 hours
/// Default durable-buffer size cap (5 GiB) applied when buffering is enabled without an explicit value.
pub const DEFAULT_BUFFER_MAX_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Default directory for the on-disk ingest spools.
pub const DEFAULT_BUFFER_DIR: &str = ".datahub-spool";
/// RFC 7523 grant type: exchange an externally-issued JWT assertion for a token.
const JWT_BEARER_GRANT: &str = "urn:ietf:params:oauth:grant-type:jwt-bearer";
/// RFC 7523 client-authentication type: authenticate the client itself with a JWT assertion
/// (Keycloak "Signed JWT - Federated") instead of a client secret.
const JWT_BEARER_CLIENT_ASSERTION: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

#[derive(Default, Deserialize, Debug, Clone)]
pub struct OAuthConfig {
    #[serde(alias = "CLIENT_ID")]
    pub(crate) client_id: Option<String>,

    #[serde(alias = "CLIENT_SECRET")]
    pub(crate) client_secret: Option<String>,

    #[serde(alias = "TOKEN_URI")]
    pub(crate) token_uri: Option<String>,

    /// OAuth2 `scope` for the client-credentials request; space-separated for several. Omitted
    /// when unset — Keycloak needs no scope, Entra ID requires `api://<app-id-uri>/.default`.
    #[serde(alias = "SCOPE")]
    pub(crate) scope: Option<String>,

    /// OAuth2 `audience` for the client-credentials request. Omitted when unset; required by
    /// Auth0, unused by Keycloak.
    #[serde(alias = "AUDIENCE")]
    pub(crate) audience: Option<String>,

    /// A ready-made JWT to present as the RFC 7523 `jwt-bearer` assertion. Unset by default; when
    /// set (or when the `assertion_*` credentials are), the token at `token_uri` is obtained with
    /// the `jwt-bearer` grant instead of plain client credentials.
    #[serde(alias = "ASSERTION")]
    pub(crate) assertion: Option<String>,

    #[serde(alias = "ASSERTION_TOKEN_URI")]
    pub(crate) assertion_token_uri: Option<String>,

    #[serde(alias = "ASSERTION_CLIENT_ID")]
    pub(crate) assertion_client_id: Option<String>,

    #[serde(alias = "ASSERTION_CLIENT_SECRET")]
    pub(crate) assertion_client_secret: Option<String>,

    #[serde(alias = "ASSERTION_SCOPE")]
    pub(crate) assertion_scope: Option<String>,

    #[serde(alias = "ASSERTION_AUDIENCE")]
    pub(crate) assertion_audience: Option<String>,

    /// Grant used when the assertion authenticates the client itself (no `CLIENT_SECRET`):
    /// `client_credentials` (default — token issued for the client's service account) or
    /// `jwt-bearer` (identity chaining — token issued for the Keycloak user linked to the
    /// assertion's subject).
    #[serde(alias = "ASSERTION_GRANT")]
    pub(crate) assertion_grant: Option<String>,

    #[serde(alias = "PROJECT_NAME")]
    pub(crate) project_name: Option<String>,
}
#[derive(Default, Debug, Clone)]
struct AuthState {
    pub token: Option<oauth2::basic::BasicTokenResponse>,
    pub expire_time: Option<DateTime<Utc>>,
}
#[derive(Debug, Clone)]
pub struct DataHubConfig {
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
    // on, an unset bound falls back to its default (72h / 5 GiB).
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

impl DataHubConfig {
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
    pub fn create_default() -> DataHubConfig {
        //let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        DataHubConfig::from_env().unwrap()
    }

    pub fn from_vars(
        base_url: String,
        token: Option<String>,
        token_uri: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
        project_name: Option<String>,
    ) -> DataHubConfig {
        let oauthconfig = OAuthConfig {
            client_id,
            client_secret,
            token_uri,
            scope: None,
            audience: None,
            assertion: None,
            assertion_token_uri: None,
            assertion_client_id: None,
            assertion_client_secret: None,
            assertion_scope: None,
            assertion_audience: None,
            assertion_grant: None,
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

    /// Set the OAuth2 `scope` sent with the client-credentials request (space-separated for
    /// several). Unset by default, in which case the parameter is omitted.
    pub fn set_scope<S: Into<String>>(&mut self, scope: S) -> &mut Self {
        Arc::make_mut(&mut self.config).scope = Some(scope.into());
        self
    }

    /// Set the OAuth2 `audience` sent with the client-credentials request. Unset by default, in
    /// which case the parameter is omitted.
    pub fn set_audience<S: Into<String>>(&mut self, audience: S) -> &mut Self {
        Arc::make_mut(&mut self.config).audience = Some(audience.into());
        self
    }

    /// Present a ready-made JWT as the RFC 7523 `jwt-bearer` assertion, exchanging it at
    /// `token_uri`. Prefer [`set_assertion_credentials`](Self::set_assertion_credentials) — a
    /// static assertion is never refreshed and will eventually expire.
    pub fn set_assertion<S: Into<String>>(&mut self, assertion: S) -> &mut Self {
        Arc::make_mut(&mut self.config).assertion = Some(assertion.into());
        self
    }

    /// Fetch the `jwt-bearer` assertion with client credentials from another provider — an Entra ID
    /// app registration, say — then exchange it at `token_uri` for a token this API accepts. Pair
    /// with [`set_assertion_scope`](Self::set_assertion_scope) where the provider demands one.
    pub fn set_assertion_credentials<S: Into<String>>(
        &mut self,
        client_id: S,
        client_secret: S,
        token_uri: S,
    ) -> &mut Self {
        let config = Arc::make_mut(&mut self.config);
        config.assertion_client_id = Some(client_id.into());
        config.assertion_client_secret = Some(client_secret.into());
        config.assertion_token_uri = Some(token_uri.into());
        self
    }

    /// `scope` for the assertion request; Entra ID needs `api://<app-id-uri>/.default`.
    pub fn set_assertion_scope<S: Into<String>>(&mut self, scope: S) -> &mut Self {
        Arc::make_mut(&mut self.config).assertion_scope = Some(scope.into());
        self
    }

    /// `audience` for the assertion request. Omitted when unset.
    pub fn set_assertion_audience<S: Into<String>>(&mut self, audience: S) -> &mut Self {
        Arc::make_mut(&mut self.config).assertion_audience = Some(audience.into());
        self
    }

    /// Grant used with an assertion source when no client secret is configured:
    /// `"client_credentials"` (the default) issues a token for the client's service account;
    /// `"jwt-bearer"` chains the external identity — the token is issued for the Keycloak user
    /// linked to the assertion's subject.
    pub fn set_assertion_grant<S: Into<String>>(&mut self, grant: S) -> &mut Self {
        Arc::make_mut(&mut self.config).assertion_grant = Some(grant.into());
        self
    }

    /// Enable durable ingest buffering with default bounds (72h window, 5 GiB cap). Off by default.
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

        // An assertion source means the token at `token_uri` comes from an assertion exchange. A
        // refresh token, when the provider issued one, still refreshes normally.
        if self.has_assertion_exchange() && refresh_token.is_none() {
            let new_token = self.exchange_assertion().await?;
            let expire_time = new_token.expires_in().map(|duration| Utc::now() + duration);
            let mut auth_state = self.auth_state.write().await;
            if let Some(t) = &auth_state.token {
                if !auth_state.is_expired() {
                    return Ok(Some(t.clone()));
                }
            }
            auth_state.token = Some(new_token.clone());
            auth_state.expire_time = expire_time;
            return Ok(Some(new_token));
        }

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
            let mut request = authclient.exchange_client_credentials();
            if let Some(scope) = self.config.scope.as_deref() {
                for s in scope.split_whitespace() {
                    request = request.add_scope(Scope::new(s.to_string()));
                }
            }
            if let Some(audience) = self.config.audience.as_deref() {
                request = request.add_extra_param("audience", audience.to_string());
            }
            request.request_async(&self.http_client).await
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
    /// True when an assertion source is configured, i.e. the token at `token_uri` is obtained by
    /// exchanging an externally-issued JWT rather than plain client credentials. `CLIENT_SECRET`
    /// is optional here: without one the exchange authenticates with the assertion itself
    /// (federated client authentication) instead of basic auth.
    fn has_assertion_exchange(&self) -> bool {
        let config = &self.config;
        let has_source = config.assertion.is_some()
            || config.assertion_token_uri.is_some()
                && config.assertion_client_id.is_some()
                && config.assertion_client_secret.is_some();
        config.token_uri.is_some() && has_source
    }

    /// The configured static assertion, or one fetched from the assertion provider.
    ///
    /// Deliberately not cached: providers commonly reject a replayed assertion (Keycloak defaults
    /// to one-time use), so each exchange starts from a fresh request.
    async fn fetch_assertion(&self) -> Result<String, DataHubError> {
        let config = &self.config;
        if let Some(assertion) = &config.assertion {
            return Ok(assertion.clone());
        }
        let (Some(uri), Some(client_id), Some(client_secret)) = (
            &config.assertion_token_uri,
            &config.assertion_client_id,
            &config.assertion_client_secret,
        ) else {
            return Err(DataHubError::ConfigError(
                "incomplete jwt-bearer assertion source: provide ASSERTION, or ASSERTION_TOKEN_URI \
                 + ASSERTION_CLIENT_ID + ASSERTION_CLIENT_SECRET"
                    .to_string(),
            ));
        };
        let mut form = vec![("grant_type", "client_credentials".to_string())];
        if let Some(scope) = &config.assertion_scope {
            form.push(("scope", scope.clone()));
        }
        if let Some(audience) = &config.assertion_audience {
            form.push(("audience", audience.clone()));
        }
        let token = self
            .post_token_form(uri, Some((client_id, client_secret)), &form, "assertion request")
            .await?;
        Ok(token.access_token().secret().clone())
    }

    /// Exchange the assertion at `token_uri` for a token from this provider — how an Entra ID
    /// service principal reaches an API that only trusts Keycloak.
    ///
    /// With `CLIENT_ID` + `CLIENT_SECRET` the request is the RFC 7523 `jwt-bearer` grant
    /// authenticated with basic auth. Without a secret the assertion itself authenticates the
    /// client (federated client authentication, Keycloak's "Signed JWT - Federated"), driving
    /// the grant selected by `ASSERTION_GRANT`: `client_credentials` (default — token for the
    /// client's service account) or `jwt-bearer` (identity chaining — token for the user linked
    /// to the assertion's subject).
    async fn exchange_assertion(&self) -> Result<BasicTokenResponse, DataHubError> {
        let config = &self.config;
        let Some(uri) = &config.token_uri else {
            return Err(DataHubError::ConfigError(
                "an assertion exchange needs TOKEN_URI to send the request to".to_string(),
            ));
        };
        let assertion = self.fetch_assertion().await?;
        let mut form: Vec<(&str, String)> = Vec::new();
        if let Some(scope) = &config.scope {
            form.push(("scope", scope.clone()));
        }
        if let Some(audience) = &config.audience {
            form.push(("audience", audience.clone()));
        }
        if let (Some(client_id), Some(client_secret)) = (&config.client_id, &config.client_secret)
        {
            form.push(("grant_type", JWT_BEARER_GRANT.to_string()));
            form.push(("assertion", assertion));
            return self
                .post_token_form(
                    uri,
                    Some((client_id, client_secret)),
                    &form,
                    "jwt-bearer token request",
                )
                .await;
        }
        match config.assertion_grant.as_deref() {
            None | Some("client_credentials") => {
                form.push(("grant_type", "client_credentials".to_string()));
            }
            Some("jwt-bearer") => {
                form.push(("grant_type", JWT_BEARER_GRANT.to_string()));
                form.push(("assertion", assertion.clone()));
            }
            Some(other) => {
                return Err(DataHubError::ConfigError(format!(
                    "unknown ASSERTION_GRANT {other:?}: expected \"client_credentials\" or \"jwt-bearer\""
                )));
            }
        }
        // Deliberately no client_id: Keycloak resolves the client from the assertion's
        // issuer + subject, and a client_id in the form makes the standard `client-jwt`
        // authenticator (which runs earlier in the flow) claim the request and hard-fail.
        form.push(("client_assertion_type", JWT_BEARER_CLIENT_ASSERTION.to_string()));
        form.push(("client_assertion", assertion));
        self.post_token_form(uri, None, &form, "federated token request")
            .await
    }

    async fn post_token_form(
        &self,
        uri: &str,
        basic_credentials: Option<(&str, &str)>,
        form: &[(&str, String)],
        context: &str,
    ) -> Result<BasicTokenResponse, DataHubError> {
        let mut request = self.http_client.post(uri);
        if let Some((client_id, client_secret)) = basic_credentials {
            request = request.basic_auth(client_id, Some(client_secret));
        }
        let response = request.form(form).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(DataHubError::OAuthError(format!(
                "{context} failed ({status}): {body}"
            )));
        }
        serde_json::from_str(&body).map_err(DataHubError::from)
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

#[cfg(test)]
mod jwt_bearer_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// A one-shot HTTP endpoint: replies with `json`, and yields the request it received.
    async fn token_endpoint(json: &'static str) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut data = Vec::new();
            loop {
                let mut chunk = [0u8; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                data.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&data).to_string();
                if let Some(idx) = text.find("\r\n\r\n") {
                    let length = text[..idx]
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    if data.len() >= idx + 4 + length {
                        break;
                    }
                }
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                json.len(),
                json
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.flush().await.unwrap();
            String::from_utf8_lossy(&data).to_string()
        });
        (format!("http://{addr}"), handle)
    }

    fn api_with_client_credentials(token_uri: String) -> DataHubConfig {
        DataHubConfig::from_vars(
            "http://127.0.0.1:1".to_string(),
            None,
            Some(token_uri),
            Some("datahub-jwt-grant".to_string()),
            Some("kc-secret".to_string()),
            None,
        )
    }

    #[tokio::test]
    async fn exchanges_static_assertion_with_jwt_bearer() {
        let (url, server) =
            token_endpoint(r#"{"access_token":"kc-token","token_type":"Bearer","expires_in":3600}"#).await;

        let mut api = api_with_client_credentials(url);
        api.set_assertion("header.payload.signature");
        let token = api.get_api_token().await.unwrap();

        assert_eq!(token, "kc-token");
        let request = server.await.unwrap();
        assert!(
            request.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"),
            "{request}"
        );
        assert!(request.contains("assertion=header.payload.signature"), "{request}");
    }

    #[tokio::test]
    async fn fetches_assertion_then_exchanges_it() {
        let (assertion_url, assertion_server) =
            token_endpoint(r#"{"access_token":"entra.jwt.sig","token_type":"Bearer","expires_in":3600}"#).await;
        let (exchange_url, exchange_server) =
            token_endpoint(r#"{"access_token":"kc-token","token_type":"Bearer","expires_in":3600}"#).await;

        let mut api = api_with_client_credentials(exchange_url);
        api.set_assertion_credentials("entra-app", "entra-secret", &assertion_url);
        api.set_assertion_scope("api://entra-app/.default");
        let token = api.get_api_token().await.unwrap();

        assert_eq!(token, "kc-token");

        // leg 1: client credentials at the assertion provider, carrying its own scope
        let assertion_request = assertion_server.await.unwrap();
        assert!(assertion_request.contains("grant_type=client_credentials"), "{assertion_request}");
        assert!(
            assertion_request.contains("scope=api%3A%2F%2Fentra-app%2F.default"),
            "{assertion_request}"
        );

        // leg 2: the fetched JWT presented as the assertion
        let exchange_request = exchange_server.await.unwrap();
        assert!(exchange_request.contains("assertion=entra.jwt.sig"), "{exchange_request}");
    }

    /// Without a CLIENT_SECRET the exchange authenticates via federated client authentication:
    /// the assertion is sent as the RFC 7523 `client_assertion`, so no Keycloak-issued
    /// credential is involved anywhere in the request. The default grant is
    /// `client_credentials` — the token is issued for the client's service account.
    #[tokio::test]
    async fn federated_exchange_without_client_secret() {
        let (url, server) =
            token_endpoint(r#"{"access_token":"kc-token","token_type":"Bearer","expires_in":3600}"#).await;

        let mut api = DataHubConfig::from_vars(
            "http://127.0.0.1:1".to_string(),
            None,
            Some(url),
            Some("datahub-exchange".to_string()),
            None,
            None,
        );
        api.set_assertion("header.payload.signature");
        let token = api.get_api_token().await.unwrap();

        assert_eq!(token, "kc-token");
        let request = server.await.unwrap();
        assert!(
            !request.to_lowercase().contains("authorization:"),
            "no basic auth expected: {request}"
        );
        assert!(request.contains("grant_type=client_credentials"), "{request}");
        assert!(
            request.contains(
                "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"
            ),
            "{request}"
        );
        assert!(request.contains("client_assertion=header.payload.signature"), "{request}");
        // client_id must NOT be sent: it would make Keycloak's earlier client-jwt
        // authenticator claim the request and reject it.
        assert!(!request.contains("client_id="), "{request}");
    }

    /// `ASSERTION_GRANT=jwt-bearer` selects identity chaining: the assertion is presented
    /// both as the authorization grant and as the client_assertion.
    #[tokio::test]
    async fn federated_jwt_bearer_identity_chaining() {
        let (url, server) =
            token_endpoint(r#"{"access_token":"kc-token","token_type":"Bearer","expires_in":3600}"#).await;

        let mut api = DataHubConfig::from_vars(
            "http://127.0.0.1:1".to_string(),
            None,
            Some(url),
            None,
            None,
            None,
        );
        api.set_assertion("header.payload.signature");
        api.set_assertion_grant("jwt-bearer");
        let token = api.get_api_token().await.unwrap();

        assert_eq!(token, "kc-token");
        let request = server.await.unwrap();
        assert!(
            request.contains("grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"),
            "{request}"
        );
        // present both as the grant assertion and as the client_assertion
        assert_eq!(
            request.matches("assertion=header.payload.signature").count(),
            2,
            "{request}"
        );
    }

    #[tokio::test]
    async fn unknown_assertion_grant_is_rejected() {
        let mut api = DataHubConfig::from_vars(
            "http://127.0.0.1:1".to_string(),
            None,
            Some("http://127.0.0.1:1".to_string()),
            None,
            None,
            None,
        );
        api.set_assertion("header.payload.signature");
        api.set_assertion_grant("password");
        let err = api.get_api_token().await.unwrap_err();
        assert!(err.to_string().contains("ASSERTION_GRANT"), "{err:?}");
    }
}
