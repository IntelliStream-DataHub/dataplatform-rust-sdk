use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

#[derive(Debug, Error)]
pub enum ListenError {
    #[error("failed to build request: {0}")]
    Request(String),
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("websocket error: {0}")]
    WebSocket(String),
    #[error("failed to deserialize server frame: {0}")]
    Deserialize(String),
    #[error("failed to serialize client frame: {0}")]
    Serialize(#[from] serde_json::Error),
    #[error("subscription '{external_id}' error: {reason}")]
    Subscription { external_id: String, reason: String },
}

/// One message delivered by the backend. Carries the opaque `message_id` the client must
/// echo back via [`SubscriptionListener::ack`] / [`SubscriptionListener::nack`], and the
/// `subscription_external_id` it was delivered for (set from the frame — useful when one
/// listener multiplexes several subscriptions).
#[derive(Debug, Deserialize, Clone)]
pub struct SubscriptionMessage {
    #[serde(rename = "subscriptionExternalId", default)]
    pub subscription_external_id: String,
    #[serde(rename = "messageId")]
    pub message_id: String,
    pub payload: DataWrapperMessage,
}

/// Envelope the backend's Pulsar consumer wraps around every fan-out event. Mirrors the
/// `DataWrapperMessage` Avro schema on the backend.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataWrapperMessage {
    pub event_action: EventAction,
    pub event_object: EventObject,
    #[serde(default)]
    pub items: Vec<DataCollectionString>,
    pub tenant_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventAction {
    Create,
    Update,
    Delete,
    Rename,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum EventObject {
    Label,
    Relation,
    Resource,
    Timeseries,
    Function,
    Event,
    Datapoints,
    #[serde(rename = "RESOURCE_AND_RELATION")]
    ResourceAndRelation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DataCollectionString {
    #[serde(default)]
    pub datapoints: Vec<WsDatapoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inclusive_begin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclusive_end: Option<String>,
}

/// Datapoint delivered over the listen stream. Values arrive as strings so both numeric
/// and string-typed timeseries share one schema.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WsDatapoint {
    pub timestamp: String,
    pub value: String,
}

/// One message element inside a batch frame. The frame-level `subscriptionExternalId` is applied
/// to each element when decoded into a [`SubscriptionMessage`].
#[derive(Debug, Deserialize)]
struct RawMessage {
    #[serde(rename = "messageId")]
    message_id: String,
    payload: DataWrapperMessage,
}

#[derive(Debug, Deserialize)]
struct WsBatch {
    #[serde(rename = "subscriptionExternalId", default)]
    subscription_external_id: Option<String>,
    messages: Vec<RawMessage>,
}

/// Error frame the server sends when a requested subscription can't be attached (e.g. unknown id).
#[derive(Debug, Deserialize)]
struct WsError {
    #[allow(dead_code)]
    error: bool,
    #[serde(rename = "subscriptionExternalId", default)]
    subscription_external_id: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

/// A server text frame is either a batch of messages or a subscription error. Untagged: a frame
/// with `messages` parses as `Batch`; one with `error` parses as `Error`.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ServerFrame {
    Batch(WsBatch),
    Error(WsError),
}

/// Outcome of decoding one server text frame.
pub(crate) enum DecodedFrame {
    Messages(Vec<SubscriptionMessage>),
    SubscriptionError { external_id: String, reason: String },
}

/// Decode a server text frame into either its messages (with the frame's subscription id stamped
/// onto each) or a subscription error.
pub(crate) fn decode_text_frame(text: &str) -> Result<DecodedFrame, ListenError> {
    let parsed: ServerFrame =
        serde_json::from_str(text).map_err(|e| ListenError::Deserialize(e.to_string()))?;
    Ok(match parsed {
        ServerFrame::Batch(batch) => {
            let sub_id = batch.subscription_external_id.unwrap_or_default();
            DecodedFrame::Messages(
                batch
                    .messages
                    .into_iter()
                    .map(|m| SubscriptionMessage {
                        subscription_external_id: sub_id.clone(),
                        message_id: m.message_id,
                        payload: m.payload,
                    })
                    .collect(),
            )
        }
        ServerFrame::Error(err) => DecodedFrame::SubscriptionError {
            external_id: err.subscription_external_id.unwrap_or_default(),
            reason: err.reason.unwrap_or_else(|| "unknown".to_string()),
        },
    })
}

/// Live WebSocket listener multiplexing one or more subscriptions' fan-out topics.
///
/// Drive it by calling [`SubscriptionListener::next`] in a loop; ack processed messages with
/// [`SubscriptionListener::ack`]; anything left unacked at close is redelivered by Pulsar on
/// the next listener to connect with the same subscription external id. Add or drop subscriptions
/// at runtime with [`SubscriptionListener::subscribe`] / [`SubscriptionListener::unsubscribe`] /
/// [`SubscriptionListener::set_subscriptions`]. Each [`SubscriptionMessage`] carries the
/// `subscription_external_id` it belongs to.
///
/// The server pings every 15s and closes idle sessions after ~45s. `next` transparently
/// handles incoming pings, but only while it is being polled — call it often enough that a
/// pong can be flushed back. Heavy per-message work should run on another task with messages
/// fanned out through a channel.
pub struct SubscriptionListener {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    buffered: VecDeque<SubscriptionMessage>,
}

impl SubscriptionListener {
    pub(crate) async fn connect(ws_url: &str, bearer_token: &str) -> Result<Self, ListenError> {
        let mut request = ws_url
            .into_client_request()
            .map_err(|e| ListenError::Request(e.to_string()))?;
        let header_value: http::HeaderValue = format!("Bearer {}", bearer_token)
            .parse()
            .map_err(|e: http::header::InvalidHeaderValue| ListenError::Request(e.to_string()))?;
        request.headers_mut().insert("Authorization", header_value);

        let (ws, _response) = connect_async(request)
            .await
            .map_err(|e| ListenError::Handshake(e.to_string()))?;

        Ok(SubscriptionListener {
            ws,
            buffered: VecDeque::new(),
        })
    }

    /// Wait for the next message. Returns `None` when the connection has been closed cleanly
    /// by either side; returns `Some(Err(_))` for transport or deserialization errors.
    pub async fn next(&mut self) -> Option<Result<SubscriptionMessage, ListenError>> {
        loop {
            if let Some(msg) = self.buffered.pop_front() {
                return Some(Ok(msg));
            }
            let frame = match self.ws.next().await {
                None => return None,
                Some(Ok(f)) => f,
                Some(Err(e)) => return Some(Err(ListenError::WebSocket(e.to_string()))),
            };
            match frame {
                Message::Text(text) => match decode_text_frame(&text) {
                    Ok(DecodedFrame::Messages(messages)) => {
                        for m in messages {
                            self.buffered.push_back(m);
                        }
                    }
                    // A subscription-level error (e.g. unknown id) is surfaced to the caller but
                    // does NOT close the socket — keep calling `next` to receive from the other
                    // subscriptions on this connection.
                    Ok(DecodedFrame::SubscriptionError { external_id, reason }) => {
                        return Some(Err(ListenError::Subscription { external_id, reason }));
                    }
                    Err(e) => return Some(Err(e)),
                },
                Message::Close(_) => return None,
                // Ping / Pong / Binary / raw Frame — tungstenite queues a pong for pings and
                // flushes it on the next write. Loop and keep reading.
                _ => continue,
            }
        }
    }

    /// Ack the given ids so Pulsar considers them delivered. Unknown ids are ignored server-side.
    pub async fn ack<S: AsRef<str>>(&mut self, message_ids: &[S]) -> Result<(), ListenError> {
        self.send_action("ack", message_ids).await
    }

    /// Nack so Pulsar redelivers on the next receive cycle.
    pub async fn nack<S: AsRef<str>>(&mut self, message_ids: &[S]) -> Result<(), ListenError> {
        self.send_action("nack", message_ids).await
    }

    /// Add subscriptions to the live set without reconnecting.
    pub async fn subscribe<S: AsRef<str>>(&mut self, external_ids: &[S]) -> Result<(), ListenError> {
        self.send_interest("subscribe", external_ids).await
    }

    /// Remove subscriptions from the live set.
    pub async fn unsubscribe<S: AsRef<str>>(
        &mut self,
        external_ids: &[S],
    ) -> Result<(), ListenError> {
        self.send_interest("unsubscribe", external_ids).await
    }

    /// Replace the whole live set of subscriptions.
    pub async fn set_subscriptions<S: AsRef<str>>(
        &mut self,
        external_ids: &[S],
    ) -> Result<(), ListenError> {
        self.send_interest("set", external_ids).await
    }

    async fn send_action<S: AsRef<str>>(
        &mut self,
        action: &str,
        ids: &[S],
    ) -> Result<(), ListenError> {
        let ids: Vec<&str> = ids.iter().map(|s| s.as_ref()).collect();
        let frame = serde_json::to_string(&serde_json::json!({
            "action": action,
            "messageIds": ids,
        }))?;
        self.ws
            .send(Message::Text(frame.into()))
            .await
            .map_err(|e| ListenError::WebSocket(e.to_string()))
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

/// Convert the host base URL (`http(s)://<host>`) to the WebSocket listen URL
/// (`ws(s)://<host>/timeseries/datapoints/subscription/listen[/<id>/<id>...]`). Each external id
/// becomes a path segment that seeds the initial subscription set; pass an empty slice to connect
/// with none and subscribe dynamically.
pub(crate) fn build_ws_url<S: AsRef<str>>(
    host_base_url: &str,
    external_ids: &[S],
) -> Result<String, ListenError> {
    let ws_base = if let Some(rest) = host_base_url.strip_prefix("https://") {
        format!("wss://{}", rest)
    } else if let Some(rest) = host_base_url.strip_prefix("http://") {
        format!("ws://{}", rest)
    } else {
        return Err(ListenError::Request(format!(
            "base_url must start with http:// or https://, got {}",
            host_base_url
        )));
    };
    let mut url = format!(
        "{}/timeseries/datapoints/subscription/listen",
        ws_base.trim_end_matches('/')
    );
    for id in external_ids {
        url.push('/');
        url.push_str(id.as_ref());
    }
    Ok(url)
}
