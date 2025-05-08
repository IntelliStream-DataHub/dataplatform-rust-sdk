use std::env;
use std::time::{SystemTime, UNIX_EPOCH};
use oauth2::{reqwest, AuthUrl, ClientId, ClientSecret, TokenResponse, TokenUrl};
use oauth2::basic::BasicClient;

#[derive(Debug)]
pub struct DataHubConfig {
    pub(crate) client_id: String,
    pub(crate) client_secret: String,
    pub(crate) auth_path: String,
    pub(crate) token_path: String,
    pub(crate) project_name: String
}

pub struct DataHubApi<'a> {
    pub(crate) config: &'a Option<DataHubConfig>,
    pub(crate) token: Option<String>,
    pub(crate) expires: Option<u64>,
    pub(crate) base_url: String
}

impl DataHubApi<'_> {

    pub fn init(config: &Option<DataHubConfig>) -> DataHubApi {
        let base_url = if let Some(config) = config {
            format!("https://api-{}.intellistream.ai/timeseries/data", config.project_name)
        } else {
            "http://localhost:8081".to_string()
        };
        DataHubApi { config, token: None, expires: None, base_url }
    }

    pub fn create_default() -> DataHubApi<'static> {
        let token = env::var("TOKEN").expect("TOKEN environment variable not set");
        let mut api_service = DataHubApi::init(&None);
        api_service.set_token_value(&token);
        api_service
    }

    async fn refresh_token(&mut self) {
        if cfg!(test) {
            println!("Refreshing Api Token...");
        }
        if let Some(config) = self.config {
            let auth_path = config.auth_path.clone();
            let auth_url = AuthUrl::new(auth_path).expect("Invalid authorization endpoint URL");
            let token_path = config.token_path.clone();
            let token_url = TokenUrl::new(token_path).expect("Invalid token endpoint URL");

            let client_id = config.client_id.clone();
            let client_secret = config.client_secret.clone();

            let client =
                BasicClient::new( ClientId::new(client_id))
                      .set_client_secret(ClientSecret::new(client_secret))
                      .set_auth_uri(auth_url)
                      .set_token_uri(token_url);

            let http_client = reqwest::blocking::ClientBuilder::new()
                // Following redirects opens the client up to SSRF vulnerabilities.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("Client should build");

            let token_result =
                client
                    .exchange_client_credentials()
                    .request(&http_client);

            if cfg!(test) {
                println!("Token:{:?}", token_result);
            }

            let access_token = match token_result {
                Ok(t) => t,
                Err(_) => {
                    println!("Error!");
                    std::process::exit(1)
                }
            };

            if cfg!(test) {
                println!("Token: {}", access_token.access_token().secret());
                println!("Expires: {}", access_token.expires_in().unwrap().as_secs());
            }

            self.set_token_value(access_token.access_token().secret());
            let expire_time = access_token.expires_in().unwrap().as_secs();
            self.set_token_expires(self.get_epoch_seconds_now() + (expire_time-60));
        };
    }

    fn get_epoch_seconds_now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn set_token_value(&mut self, value: &str) {
        self.token = Some(String::from(value))
    }

    fn set_token_expires(&mut self, value: u64) {
        self.expires = Some(value)
    }

    fn has_token_expired(&self) -> bool{
        let expires: &u64 = &self.expires.unwrap();
        let now = self.get_epoch_seconds_now();
        if now > *expires {
            return true;
        }
        false
    }

    pub async fn get_api_token(&mut self) -> Option<&String> {
        let token = &self.token;
        match token {
            Some(_t) => {
                if self.has_token_expired(){
                    if cfg!(test) {
                        println!("Token has expired!");
                    }
                    self.refresh_token().await;
                }
            },
            None => {
                if cfg!(test) {
                    println!("Token not found!");
                }
                self.refresh_token().await;
            }
        }
        self.token.as_ref()
    }

    pub fn get_config(&mut self) -> &Option<DataHubConfig> {
        self.config
    }
}