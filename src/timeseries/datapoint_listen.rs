//! Live datapoint tail over `ws(s)://<host>/timeseries/datapoints/listen`.
//!
//! Unlike [`SubscriptionListener`](crate::subscriptions::SubscriptionListener) there is no
//! subscription entity behind it and nothing to ack: each connection reads the firehose from
//! *latest*, non-durably, narrowed server-side to the tenant and the requested timeseries. Points
//! written while the socket is down are not replayed.

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Weak;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::subscriptions::ListenError;
use crate::ApiService;

/// Offered alongside the bearer element so the server has a subprotocol to echo that is not the
/// credential itself.
const NEGOTIATED_SUBPROTOCOL: &str = "datahub.v1";
const BEARER_SUBPROTOCOL_PREFIX: &str = "datahub.bearer.";

const RECONNECT_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const RECONNECT_MAX_BACKOFF: Duration = Duration::from_secs(30);
const RECONNECT_MAX_RETRIES: u32 = 8;

/// One point delivered by [`DatapointListener::next`]. `value` is a string for every value type,
/// as on the other ingest and listen paths.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LiveDatapoint {
    pub external_id: String,
    /// Upper case, e.g. `FLOAT`.
    #[serde(default)]
    pub value_type: Option<String>,
    pub timestamp: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ServerFrame {
    Datapoints {
        datapoints: Vec<LiveDatapoint>,
    },
    Error {
        #[allow(dead_code)]
        error: bool,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        scope: Option<String>,
        #[serde(default)]
        limit: Option<u64>,
        #[serde(default)]
        message: Option<String>,
    },
}

pub(crate) fn decode_frame(text: &str) -> Result<Vec<LiveDatapoint>, ListenError> {
    match serde_json::from_str(text).map_err(|e| ListenError::Deserialize(e.to_string()))? {
        ServerFrame::Datapoints { datapoints } => Ok(datapoints),
        ServerFrame::Error {
            scope: Some(scope),
            limit: Some(limit),
            message,
            reason,
            ..
        } => Err(ListenError::ConnectionLimit {
            scope,
            limit,
            message: message
                .or(reason)
                .unwrap_or_else(|| "connection refused".to_string()),
        }),
        ServerFrame::Error {
            message, reason, ..
        } => Err(ListenError::WebSocket(
            message
                .or(reason)
                .unwrap_or_else(|| "server reported an error".to_string()),
        )),
    }
}

/// Live tail of the datapoints written to a set of timeseries, by external id.
///
/// Drive it by calling [`next`](Self::next) in a loop; change what is streamed with
/// [`subscribe`](Self::subscribe) / [`unsubscribe`](Self::unsubscribe) /
/// [`set_timeseries`](Self::set_timeseries). The server narrows every change to the timeseries
/// whose data set the caller may read, **silently** — an unreadable or unknown external id is
/// dropped, not reported.
///
/// A dropped connection is re-established by `next` with a fresh token and the current interest
/// set; points written in the gap are lost. A refusal — a bad token, a missing role, the connection
/// limit — is returned rather than retried.
pub struct DatapointListener {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    buffered: VecDeque<LiveDatapoint>,
    api_service: Weak<ApiService>,
    ws_url: String,
    interest: Vec<String>,
}

impl DatapointListener {
    pub(crate) async fn connect(
        api_service: Weak<ApiService>,
        timeseries_base_url: &str,
        interest: Vec<String>,
    ) -> Result<Self, ListenError> {
        let ws_url = build_ws_url(timeseries_base_url)?;
        let ws = Self::open(&api_service, &ws_url, &interest).await?;
        Ok(DatapointListener {
            ws,
            buffered: VecDeque::new(),
            api_service,
            ws_url,
            interest,
        })
    }

    async fn open(
        api_service: &Weak<ApiService>,
        ws_url: &str,
        interest: &[String],
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, ListenError> {
        let service = api_service
            .upgrade()
            .ok_or_else(|| ListenError::Request("api service has been dropped".to_string()))?;
        let token = service
            .config
            .get_api_token()
            .await
            .map_err(|e| ListenError::Request(format!("failed to get api token: {}", e)))?;

        let mut url =
            reqwest::Url::parse(ws_url).map_err(|e| ListenError::Request(e.to_string()))?;
        if !interest.is_empty() {
            url.query_pairs_mut()
                .append_pair("externalIds", &interest.join(","));
        }
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|e| ListenError::Request(e.to_string()))?;
        // A browser cannot set Authorization on a handshake, so the api reads the token from the
        // offered subprotocols instead and ignores the header. No space after the comma:
        // tungstenite splits the offer on "," without trimming, and would then reject the echoed
        // `datahub.v1` as one it never offered.
        let protocols: http::HeaderValue =
            format!("{BEARER_SUBPROTOCOL_PREFIX}{token},{NEGOTIATED_SUBPROTOCOL}")
                .parse()
                .map_err(|e: http::header::InvalidHeaderValue| {
                    ListenError::Request(e.to_string())
                })?;
        request
            .headers_mut()
            .insert(http::header::SEC_WEBSOCKET_PROTOCOL, protocols);

        let (ws, _response) = connect_async(request)
            .await
            .map_err(|e| ListenError::Handshake(e.to_string()))?;
        Ok(ws)
    }

    async fn reconnect(&mut self) -> Result<(), ListenError> {
        let mut delay = RECONNECT_INITIAL_BACKOFF;
        let mut last_err = ListenError::WebSocket("connection lost".to_string());
        for _ in 0..RECONNECT_MAX_RETRIES {
            tokio::time::sleep(delay).await;
            match Self::open(&self.api_service, &self.ws_url, &self.interest).await {
                Ok(ws) => {
                    self.ws = ws;
                    return Ok(());
                }
                Err(e) => {
                    last_err = e;
                    delay = (delay * 2).min(RECONNECT_MAX_BACKOFF);
                }
            }
        }
        Err(last_err)
    }

    /// Wait for the next datapoint. Reconnects transparently when the connection drops. Returns
    /// `Some(Err(_))` when a reconnect ultimately fails, a frame cannot be decoded, or the server
    /// refuses the connection.
    pub async fn next(&mut self) -> Option<Result<LiveDatapoint, ListenError>> {
        loop {
            if let Some(point) = self.buffered.pop_front() {
                return Some(Ok(point));
            }
            let frame = match self.ws.next().await {
                Some(Ok(f)) => f,
                None | Some(Err(_)) => match self.reconnect().await {
                    Ok(()) => continue,
                    Err(e) => return Some(Err(e)),
                },
            };
            match frame {
                Message::Text(text) => match decode_frame(&text) {
                    Ok(points) => self.buffered.extend(points),
                    Err(e) => return Some(Err(e)),
                },
                // The api authenticates after the 101, so a bad token, a missing role or a missing
                // tenant arrives as a policy-violation close. Reconnecting would repeat it.
                Message::Close(Some(close)) if close.code == CloseCode::Policy => {
                    return Some(Err(ListenError::Handshake(close.reason.to_string())));
                }
                Message::Close(_) => match self.reconnect().await {
                    Ok(()) => continue,
                    Err(e) => return Some(Err(e)),
                },
                _ => continue,
            }
        }
    }

    /// Add timeseries to the live set.
    pub async fn subscribe<S: AsRef<str>>(&mut self, external_ids: &[S]) -> Result<(), ListenError> {
        for id in external_ids {
            let id = id.as_ref().to_string();
            if !self.interest.contains(&id) {
                self.interest.push(id);
            }
        }
        self.send_interest("subscribe", external_ids).await
    }

    /// Remove timeseries from the live set.
    pub async fn unsubscribe<S: AsRef<str>>(
        &mut self,
        external_ids: &[S],
    ) -> Result<(), ListenError> {
        let removing: Vec<String> = external_ids.iter().map(|s| s.as_ref().to_string()).collect();
        self.interest.retain(|id| !removing.contains(id));
        self.send_interest("unsubscribe", external_ids).await
    }

    /// Replace the whole live set.
    pub async fn set_timeseries<S: AsRef<str>>(
        &mut self,
        external_ids: &[S],
    ) -> Result<(), ListenError> {
        self.interest = external_ids.iter().map(|s| s.as_ref().to_string()).collect();
        self.send_interest("set", external_ids).await
    }

    async fn send_interest<S: AsRef<str>>(
        &mut self,
        action: &str,
        external_ids: &[S],
    ) -> Result<(), ListenError> {
        let ids: Vec<&str> = external_ids.iter().map(|s| s.as_ref()).collect();
        let frame = serde_json::to_string(&serde_json::json!({
            "action": action,
            "externalIds": ids,
        }))?;
        self.ws
            .send(Message::Text(frame.into()))
            .await
            .map_err(|e| ListenError::WebSocket(e.to_string()))
    }

    /// Send a Close frame and drain remaining frames until the peer closes its side.
    pub async fn close(mut self) -> Result<(), ListenError> {
        let _ = self.ws.close(None).await;
        while let Some(frame) = self.ws.next().await {
            if frame.is_err() {
                break;
            }
        }
        Ok(())
    }
}

/// `http(s)://<host>/timeseries` to `ws(s)://<host>/timeseries/datapoints/listen`.
pub(crate) fn build_ws_url(timeseries_base_url: &str) -> Result<String, ListenError> {
    let ws_base = if let Some(rest) = timeseries_base_url.strip_prefix("https://") {
        format!("wss://{}", rest)
    } else if let Some(rest) = timeseries_base_url.strip_prefix("http://") {
        format!("ws://{}", rest)
    } else {
        return Err(ListenError::Request(format!(
            "base_url must start with http:// or https://, got {}",
            timeseries_base_url
        )));
    };
    Ok(format!(
        "{}/datapoints/listen",
        ws_base.trim_end_matches('/')
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::datahub::DataHubConfig;
    use crate::ApiService;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
    use tokio_tungstenite::tungstenite::protocol::CloseFrame;

    /// A server that authenticates the way the api's handler does — the token from the offered
    /// subprotocols, `datahub.v1` echoed — then runs `session` on the socket.
    async fn fake_listen_endpoint<F, Fut>(session: F) -> (std::sync::Arc<ApiService>, tokio::task::JoinHandle<Option<String>>)
    where
        F: FnOnce(WebSocketStream<TcpStream>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut offered = None;
            let mut query = None;
            let ws = tokio_tungstenite::accept_hdr_async(socket, |req: &Request, mut resp: Response| {
                assert_eq!(req.uri().path(), "/timeseries/datapoints/listen");
                query = req.uri().query().map(str::to_string);
                offered = req
                    .headers()
                    .get(http::header::SEC_WEBSOCKET_PROTOCOL)
                    .map(|v| v.to_str().unwrap().to_string());
                resp.headers_mut().insert(
                    http::header::SEC_WEBSOCKET_PROTOCOL,
                    http::HeaderValue::from_static(NEGOTIATED_SUBPROTOCOL),
                );
                Ok(resp)
            })
            .await
            .unwrap();
            let offered = offered.unwrap_or_default();
            let bearer = offered
                .split(',')
                .map(str::trim)
                .find_map(|p| p.strip_prefix(BEARER_SUBPROTOCOL_PREFIX))
                .map(str::to_string);
            assert_eq!(bearer.as_deref(), Some("static-token"), "offered: {offered}");
            session(ws).await;
            query
        });
        let config = DataHubConfig::from_vars(
            format!("http://{addr}"),
            Some("static-token".to_string()),
            None,
            None,
            None,
            None,
        );
        (ApiService::new(config), handle)
    }

    #[tokio::test]
    async fn the_token_rides_in_the_subprotocol_and_points_arrive() {
        let (api, server) = fake_listen_endpoint(|mut ws| async move {
            ws.send(Message::Text(
                r#"{"datapoints":[{"externalId":"a","valueType":"FLOAT","timestamp":"2026-01-01T00:00:00Z","value":"1.5"}]}"#.into(),
            ))
            .await
            .unwrap();
            let Some(Ok(Message::Text(change))) = ws.next().await else {
                panic!("expected the interest change");
            };
            let change: serde_json::Value = serde_json::from_str(&change).unwrap();
            assert_eq!(change, serde_json::json!({"action": "subscribe", "externalIds": ["b"]}));
            let _ = ws.close(None).await;
        })
        .await;

        let mut listener = api.time_series.listen_datapoints(&["a", "x y"]).await.unwrap();
        let point = listener.next().await.unwrap().unwrap();
        assert_eq!(point.external_id, "a");
        listener.subscribe(&["b"]).await.unwrap();
        let query = server.await.unwrap();
        assert_eq!(query.as_deref(), Some("externalIds=a%2Cx+y"));
    }

    /// The api authenticates after the 101 and refuses with a policy-violation close; that is
    /// returned, not retried.
    #[tokio::test]
    async fn a_policy_close_is_returned_rather_than_retried() {
        let (api, server) = fake_listen_endpoint(|mut ws| async move {
            let _ = ws
                .close(Some(CloseFrame {
                    code: CloseCode::Policy,
                    reason: "Invalid access token".into(),
                }))
                .await;
        })
        .await;

        let mut listener = api.time_series.listen_datapoints::<&str>(&[]).await.unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(2), listener.next())
            .await
            .expect("no reconnect loop");
        match outcome {
            Some(Err(ListenError::Handshake(reason))) => assert_eq!(reason, "Invalid access token"),
            other => panic!("expected the refusal, got {other:?}"),
        }
        assert_eq!(server.await.unwrap(), None, "an empty interest set sends no query");
    }

    #[test]
    fn ws_url_swaps_the_scheme_and_appends_the_listen_path() {
        assert_eq!(
            build_ws_url("https://api.example.com/timeseries").unwrap(),
            "wss://api.example.com/timeseries/datapoints/listen"
        );
        assert_eq!(
            build_ws_url("http://localhost:8081/timeseries").unwrap(),
            "ws://localhost:8081/timeseries/datapoints/listen"
        );
        assert!(build_ws_url("ftp://x/timeseries").is_err());
    }

    #[test]
    fn a_datapoints_frame_decodes_every_point() {
        let points = decode_frame(
            r#"{"datapoints":[
                {"externalId":"a","valueType":"FLOAT","timestamp":"2026-01-01T00:00:00Z","value":"1.5"},
                {"externalId":"b","valueType":"TEXT","timestamp":"2026-01-01T00:00:01Z","value":"on"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].external_id, "a");
        assert_eq!(points[1].value, "on");
    }

    #[test]
    fn the_limit_refusal_names_its_scope_and_cap() {
        let err = decode_frame(
            r#"{"error":true,"reason":"websocket-limit-reached","scope":"user","limit":5,"message":"close one"}"#,
        )
        .unwrap_err();
        match err {
            ListenError::ConnectionLimit {
                scope,
                limit,
                message,
            } => {
                assert_eq!(scope, "user");
                assert_eq!(limit, 5);
                assert_eq!(message, "close one");
            }
            other => panic!("expected ConnectionLimit, got {other:?}"),
        }
    }
}
