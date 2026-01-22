use std::{default, env};
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc, Duration};
use oauth2::{reqwest, RedirectUrl, AuthUrl, ClientId, ClientSecret, EndpointNotSet, EndpointSet, TokenResponse, TokenUrl, Scope, AccessToken, EmptyExtraTokenFields};
use oauth2::basic::{BasicClient, BasicTokenResponse, BasicTokenType};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use crate::errors::DataHubError;

#[derive(Default,Deserialize,Debug,Clone)]
pub struct OAuthConfig {
    #[serde(alias = "CLIENT_ID")]
    pub(crate) client_id: String,

    #[serde(alias = "CLIENT_SECRET")]
    pub(crate) client_secret: String,

    #[serde(alias = "AUTH_URI")]
    pub(crate) auth_uri: String,

    #[serde(alias = "TOKEN_URI")]
    pub(crate) token_uri: String,

    #[serde(alias = "REDIRECT_URI")]
    pub(crate) redirect_uri: String,

    #[serde(alias = "PROJECT_NAME")]
    pub(crate) project_name: Option<String>,
}
#[derive(Default,Debug,Clone)]
struct AuthState {
    pub token: Option<oauth2::basic::BasicTokenResponse>,
    pub expire_time: Option<DateTime<Utc>>,
}
#[derive(Debug,Clone)]
pub struct DataHubApi {
    pub(crate) config: Rc<OAuthConfig>,
    pub(crate) auth_state: Rc<RwLock<AuthState>>,
    pub(crate) base_url: String,
    pub(crate) oauth2_client: oauth2::Client<
        oauth2::basic::BasicErrorResponse,
        oauth2::basic::BasicTokenResponse,
        oauth2::basic::BasicTokenIntrospectionResponse,
        oauth2::StandardRevocableToken,
        oauth2::basic::BasicRevocationErrorResponse,
        oauth2::EndpointSet,
        oauth2::EndpointNotSet,
        oauth2::EndpointNotSet,
        oauth2::EndpointNotSet,
        oauth2::EndpointSet>,
    pub(crate) http_client: reqwest::Client,
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

    pub(crate) fn from_env() -> Result<Self, DataHubError>{
        let env_vars = env::vars().collect::<HashMap<String, String>>();
        Self::from_map(env_vars)
    }
    pub  fn create_default() -> DataHubApi {
        //let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        DataHubApi::from_env().unwrap()
        
    }
    pub(crate)  fn from_map(map: HashMap<String, String>) -> Result<Self, DataHubError> {
        let baseurl = map.get("BASE_URL").unwrap().to_string();

        let oauthconfig: OAuthConfig = serde_json::from_value(serde_json::to_value(&map)?)?;

        let client = BasicClient::new(ClientId::new(oauthconfig.client_id.clone()))
            .set_client_secret(ClientSecret::new(oauthconfig.client_secret.clone()))
            .set_auth_uri(AuthUrl::new(oauthconfig.auth_uri.clone())?)
            .set_token_uri(TokenUrl::new(oauthconfig.token_uri.clone())?)
            .set_redirect_uri(RedirectUrl::new(oauthconfig.redirect_uri.clone())?);

        let auth_http_client = reqwest::ClientBuilder::new()
            // Following redirects opens the client up to SSRF vulnerabilities.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("Client should build");

        //let mut token = client.exchange_client_credentials().add_scope(Scope::new("read".to_string())).request_async(&auth_http_client).await.map_err(|e| DataHubError::ConfigError(format!("OAuth2 Request failed: {}", e)))?;
        //let expire_time = token.expires_in().map(|duration| Utc::now() + duration);

        let auth_state = if let Some(t) = map.get("TOKEN") {
            let token=BasicTokenResponse::new(
                AccessToken::new(t.to_string()),
                BasicTokenType::Bearer,
                EmptyExtraTokenFields{});
            Rc::new(
                RwLock::new(
                    AuthState{
                        token:Some(token.clone()),
                        expire_time:token.expires_in().map(|duration| Utc::now() + duration)
                    }
                )
            )
        } else {
            Rc::new(RwLock::new(AuthState::default()))};
        Ok(Self {
            config:Rc::new(oauthconfig),
            base_url: baseurl.to_string(),
            oauth2_client: client,
            http_client: reqwest::Client::new(),
            auth_state
             })
    }

    async fn refresh_token(&self) -> Result<Option<oauth2::basic::BasicTokenResponse>, DataHubError>{
        // function is called from get_token, if the token is expired.
        let refresh_token = {
            let authstate = self.auth_state.read().await;
            authstate.token.as_ref().and_then(|t| t.refresh_token().cloned())
        };
        let token_result = if let Some(refresh_token) = refresh_token {

            self.oauth2_client
                .exchange_refresh_token(&refresh_token)
                .request_async(&self.http_client)
                .await
        } else {

            self.oauth2_client
                .exchange_client_credentials()
                .request_async(&self.http_client)
                .await
        };
        let new_token=token_result.map_err(|e| DataHubError::OAuthError(format!("OAuth2 Request failed: {}", e)))?;
        let expire_time = new_token.expires_in().map(|duration| Utc::now() + duration);

        {
            let mut auth_state = self.auth_state.write().await;
            if let Some(t) = &auth_state.token {
                if ! auth_state.is_expired(){
                    return Ok(Some(t.clone()))
                }
            }
            auth_state.token = Some(new_token.clone());
            auth_state.expire_time = expire_time;
            Ok(Some(new_token))
        }
    }

    pub async fn get_api_token(& self) -> Result<String,DataHubError> {
        {
            let authstate = self.auth_state.read().await;
            if let Some(t) = &authstate.token {
                if !authstate.is_expired() {
                    return Ok(t.access_token().secret().clone())
                }
            }
        }
        let new_token= self.refresh_token().await?;

        Ok(
            new_token.unwrap().access_token().secret().clone()
        )
    }

    pub fn get_config(& self) -> &OAuthConfig {
        &self.config
    }
}

pub fn to_snake_lower_cased_allow_start_with_digits(s: &str) -> String {
    let s = s.to_lowercase();
    let re = Regex::new(r"[\s\W]+").unwrap();
    let replaced = re.replace_all(&s, "_").into_owned();
    // Trim trailing underscores, but preserve leading if the original started with them
    if s.chars().next().map_or(false, |c| c.is_whitespace() || !c.is_alphanumeric()) {
        replaced.trim_end_matches('_').to_string()
    } else {
        replaced.trim_matches('_').to_string()
    }
}