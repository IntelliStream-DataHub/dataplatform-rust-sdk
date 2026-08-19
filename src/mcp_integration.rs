//! The api's MCP tool surface (`POST /mcp`), driven as a real MCP client would.
//!
//! The api publishes its entity surface as **37 MCP tools** (`timeseries_create`, `event_filter`,
//! …) over Spring AI's *stateless* WebMVC transport, behind the same JWT + `ROLE_DATAHUB_ACCESS`
//! gate as REST — no MCP-specific bypass. Stateless means there is no `initialize` handshake and no
//! session id: every request is a self-contained JSON-RPC call. Tool sources live in the backend's
//! `datahub-api/src/main/java/ai/intellistream/datahub/api/mcp/tools/`.
//!
//! These tests exist because an LLM getting a tool call wrong is a different problem from the tool
//! being wrong. Every write is read back, and cross-checked against the REST services on the same
//! [`ApiService`] wherever they can see the same row — a tool that reports success while writing
//! nothing is indistinguishable from a working one on the MCP side alone.
//!
//! There is no MCP client in the SDK, so [`McpClient`] below is a private test helper: ~100 lines
//! of JSON-RPC over the service's own `http_client`, with the token from
//! [`DataHubConfig::get_api_token`](crate::datahub::DataHubConfig::get_api_token). That is the whole
//! reason these tests are here rather than in `python_tests/`: auth with refresh, and TLS against
//! the OS trust store (`reqwest`'s `rustls-tls-native-roots`, which is why the dev IdP verifies here
//! and not under a venv's certifi), are already wired and must not be reimplemented per language.
//!
//! Two transport details are easy to get wrong and cost a confusing failure each:
//!
//! - **`Accept` must be `application/json, text/event-stream`, byte for byte.** The transport
//!   compares it with `MediaType.equals`, so offering only `application/json` is a bare 400 with
//!   nothing pointing at the header.
//! - **The response must not be double-encoded.** The envelope has regressed to being written as a
//!   `String` which Spring then serializes *as JSON*, so the body is a quoted, escaped document and
//!   the obvious `parse(body)["result"]` yields a string. [`unwrap_envelope`] rejects that rather
//!   than parsing twice: accommodating it would leave the suite green against a wire format no
//!   conformant client can read. While it is present, every test here fails — which is the honest
//!   report, because no tool is reachable.
//!
//! # Every advertised field is tested, and that is enforced rather than asserted
//!
//! [`McpClient::try_call_tool`] records each `(tool, field)` pair it sends, and
//! [`mcp_full_tool_surface`] ends by diffing that against the live `tools/list` schema — 125 fields
//! across the 37 tools today. A parameter added server-side fails the audit until something drives
//! it. That is why the sweep is one sequential test rather than 74 independent ones: `cargo test`
//! runs tests on parallel threads with no ordering hook, so a registry filled by other tests could
//! not be read reliably at the end of any of them.
//!
//! # Tests that are red on purpose
//!
//! Two encode intended behaviour the api does not yet provide, in the same spirit as
//! `test_duplicate_relationship_type_conflicts`: they stay red until the server-side fix lands
//! rather than being softened to match the bug.
//!
//! - [`mcp_event_update_by_uuid_reindexes_the_external_id`] — renaming an event identified by UUID
//!   writes the new `externalId` but never reindexes it.
//! - (resolved) `unitExternalId` as an alternative to `unit` on `timeseries_create`; the test remains
//!   as a regression guard.
//! - [`mcp_response_is_a_json_object`] — whenever the double-encoding regression is present. It names
//!   the fault directly; [`unwrap_envelope`] independently fails every other test for the same cause.

use crate::tests::cleanup::{
    cleanup_datasets, cleanup_events, cleanup_labels, cleanup_resources, cleanup_timeseries,
    CleanupGuard,
};
use crate::tests::ids::unique_id;
use crate::tests::polling::poll_until;
use crate::{create_api_service, ApiService};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

/// The transport requires this exact value; see the module doc.
const ACCEPT: &str = "application/json, text/event-stream";

/// What the MCP spec says a client sends, and what every real client does send.
const SPEC_CONTENT_TYPE: &str = "application/json";

/// A text/* body is read by `StringHttpMessageConverter` instead of the JSON one, and was for a
/// while the only content type that reached a tool at all. The charset is explicit because that
/// converter defaults `text/plain` to ISO-8859-1, which would mangle a non-ASCII argument.
const TEXT_CONTENT_TYPE: &str = "text/plain;charset=UTF-8";

/// A fixed instant well clear of "now", so a datapoint written here cannot collide with one an
/// unrelated test put in the same series and the range queries have deterministic bounds.
const EVENT_TIME: &str = "2026-04-23T13:00:00Z";
const DAY_START: &str = "2026-04-23T00:00:00Z";
const DAY_END: &str = "2026-04-24T00:00:00Z";

/// Every tool the api is expected to publish. Asserted as a subset of what is advertised, so adding
/// a tool does not fail the suite but renaming or dropping one does — a rename is a breaking change
/// for every MCP client and every saved prompt.
const EXPECTED_TOOLS: &[&str] = &[
    "dataset_create", "dataset_delete", "dataset_list", "dataset_search", "dataset_update",
    "edge_create", "edge_create_type", "edge_delete", "edge_get", "edge_list_types",
    "event_create", "event_delete", "event_filter", "event_get", "event_search", "event_update",
    "label_create", "label_list", "label_update",
    "resource_create", "resource_delete", "resource_fetch_nearest", "resource_fetch_related",
    "resource_get", "resource_search", "resource_update",
    "timeseries_create", "timeseries_delete", "timeseries_fetch_datapoints", "timeseries_get",
    "timeseries_get_latest", "timeseries_list", "timeseries_search", "timeseries_send_datapoint",
    "timeseries_update",
    "unit_get", "unit_list",
];

/// The domains `<domain>_<action>` may name.
const TOOL_DOMAINS: &[&str] = &[
    "dataset", "edge", "event", "label", "resource", "timeseries", "unit",
];

// --------------------------------------------------------------------------- //
// The client
// --------------------------------------------------------------------------- //

/// A minimal MCP client over the SDK's own HTTP client and token.
struct McpClient {
    api: Arc<ApiService>,
    url: String,
    next_id: AtomicI64,
    /// Every `(tool, field)` pair sent, for [`assert_every_field_was_exercised`].
    exercised: Mutex<BTreeMap<String, BTreeSet<String>>>,
}

impl McpClient {
    fn new() -> Self {
        let api = create_api_service();
        let url = format!("{}/mcp", api.config.base_url.trim_end_matches('/'));
        Self {
            api,
            url,
            next_id: AtomicI64::new(0),
            exercised: Mutex::new(BTreeMap::new()),
        }
    }

    async fn token(&self) -> String {
        self.api
            .config
            .get_api_token()
            .await
            .expect("could not obtain an api token")
    }

    /// POST `body` verbatim and hand back the status and raw text, for the tests that assert on
    /// transport behaviour rather than on a tool.
    async fn raw(
        &self,
        body: &str,
        content_type: &str,
        accept: &str,
        token: Option<&str>,
    ) -> (reqwest::StatusCode, String) {
        let mut request = self
            .api
            .http_client
            .post(&self.url)
            .header(reqwest::header::ACCEPT, accept)
            .header(reqwest::header::CONTENT_TYPE, content_type)
            .body(body.to_string());
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.expect("POST /mcp failed to send");
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        (status, text)
    }

    /// One JSON-RPC call with the spec content type, asserting a 200 and a matching id.
    async fn rpc(&self, method: &str, params: Option<Value>) -> Value {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let mut body = json!({"jsonrpc": "2.0", "id": id, "method": method});
        if let Some(params) = params {
            body["params"] = params;
        }

        let token = self.token().await;
        let (status, text) = self
            .raw(&body.to_string(), SPEC_CONTENT_TYPE, ACCEPT, Some(&token))
            .await;
        assert_eq!(
            status, 200,
            "{method} -> HTTP {status}: {}",
            truncate(&text)
        );

        let envelope = unwrap_envelope(&text);
        assert_eq!(
            envelope["id"],
            json!(id),
            "{method} answered JSON-RPC id {:?}, expected {id}",
            envelope["id"]
        );
        envelope
    }

    /// Every advertised tool, keyed by name.
    async fn list_tools(&self) -> BTreeMap<String, Value> {
        let envelope = self.rpc("tools/list", None).await;
        assert!(
            envelope["error"].is_null(),
            "tools/list failed: {}",
            envelope["error"]
        );
        envelope["result"]["tools"]
            .as_array()
            .expect("tools/list result has no tools array")
            .iter()
            .map(|tool| {
                (
                    tool["name"]
                        .as_str()
                        .expect("a tool has no name")
                        .to_string(),
                    tool.clone(),
                )
            })
            .collect()
    }

    /// Invoke a tool, panicking with the tool's own message when it reports `isError`.
    async fn call_tool(&self, name: &str, arguments: Value) -> Value {
        match self.try_call_tool(name, arguments).await {
            Ok(payload) => payload,
            Err(message) => panic!("{name} reported isError: {}", truncate(&message)),
        }
    }

    /// Invoke a tool, returning the error text instead of panicking — for the negative tests.
    async fn try_call_tool(&self, name: &str, arguments: Value) -> Result<Value, String> {
        self.record(name, &arguments);

        let envelope = self
            .rpc("tools/call", Some(json!({"name": name, "arguments": arguments})))
            .await;

        // A JSON-RPC error is how the server reports a tool it does not have, or arguments it could
        // not bind at all — a failure the caller may want to inspect rather than a reason to panic.
        if !envelope["error"].is_null() {
            return Err(format!("JSON-RPC error: {}", envelope["error"]));
        }

        let result = &envelope["result"];
        if result["isError"] == json!(true) {
            return Err(tool_text(result));
        }
        Ok(parse_tool_payload(result))
    }

    /// Best-effort teardown call that reports whether it worked.
    ///
    /// Returns `true` only when the tool accepted the delete, so a caller can disarm its
    /// [`CleanupGuard`](crate::tests::cleanup::CleanupGuard) on success and leave it armed
    /// otherwise. A best-effort delete followed by an unconditional `disarm` is exactly how strays
    /// get left behind: the delete fails (a dataset still holding a resource, say), the failure is
    /// swallowed, and the guard that would have caught it has already been switched off.
    async fn quietly(&self, name: &str, arguments: Value) -> bool {
        self.try_call_tool(name, arguments).await.is_ok()
    }

    fn record(&self, name: &str, arguments: &Value) {
        if let Some(fields) = arguments.as_object() {
            let mut exercised = self.exercised.lock().unwrap();
            let entry = exercised.entry(name.to_string()).or_default();
            for key in fields.keys() {
                entry.insert(key.clone());
            }
        }
    }
}

// --------------------------------------------------------------------------- //
// Envelope and payload helpers
// --------------------------------------------------------------------------- //

/// Parse a JSON-RPC response body, and insist it really is one.
///
/// Strict on purpose. A body that is not a single JSON object is not a JSON-RPC message, and a
/// helper that quietly parsed a double-encoded one twice would make the whole suite pass against a
/// wire format no conformant MCP client can read — the tests would be green while every real client
/// was locked out. So the garbling fails here, at the first request that hits it, and takes the rest
/// of the suite with it. That is the accurate signal: when this is broken, nothing works.
fn unwrap_envelope(text: &str) -> Value {
    let parsed: Value = serde_json::from_str(text)
        .unwrap_or_else(|err| panic!("response is not JSON ({err}): {}", truncate(text)));
    match parsed {
        Value::Object(_) => parsed,
        Value::String(_) => panic!(
            "response is double-encoded: the envelope arrived as a JSON string rather than an \
             object, so a client's parse yields a string. Body: {}",
            truncate(text)
        ),
        other => panic!(
            "response is not a JSON-RPC object: {}",
            truncate(&other.to_string())
        ),
    }
}

/// Concatenate the `text` parts of a tool result's content blocks.
fn tool_text(result: &Value) -> String {
    result["content"]
        .as_array()
        .map(|blocks| {
            blocks
                .iter()
                .filter_map(|block| block["text"].as_str())
                .collect::<String>()
        })
        .unwrap_or_default()
}

/// A tool result's text content, parsed as JSON when it is JSON.
///
/// Every tool answers with a JSON document rendered into a single text block, except the deletes,
/// which answer with a bare sentence (`"deleted"`, `"deleted 1 event(s)"`).
fn parse_tool_payload(result: &Value) -> Value {
    let text = tool_text(result);
    serde_json::from_str(&text).unwrap_or(Value::String(text))
}

/// The `items` array of a tool payload, or an empty slice when the key is absent — `event_search`
/// omits `items` entirely on a miss rather than returning an empty list.
fn items(payload: &Value) -> Vec<Value> {
    payload["items"].as_array().cloned().unwrap_or_default()
}

/// The `nodes` array of a graph payload (`resource_create`, `resource_update`, the traversals).
fn nodes(payload: &Value) -> Vec<Value> {
    payload["nodes"].as_array().cloned().unwrap_or_default()
}

/// A required string field, with a message that names the field when it is missing.
fn text_of(value: &Value, key: &str) -> String {
    value[key]
        .as_str()
        .unwrap_or_else(|| panic!("expected a string `{key}` in {}", truncate(&value.to_string())))
        .to_string()
}

/// An id as a number, whether the tool rendered it as a JSON string or a JSON number.
///
/// Most tools answer with string ids; `label_create` and `edge_create_type` answer with numbers.
/// Callers should not have to care.
fn id_of(value: &Value) -> i64 {
    match &value["id"] {
        Value::String(text) => text
            .parse()
            .unwrap_or_else(|_| panic!("id {text:?} is not numeric")),
        Value::Number(number) => number.as_i64().expect("id is not an i64"),
        other => panic!("no usable id in {other}"),
    }
}

fn truncate(text: &str) -> String {
    if text.len() <= 400 {
        text.to_string()
    } else {
        format!("{}…", &text[..400])
    }
}

fn has_external_id(list: &[Value], external_id: &str) -> bool {
    list.iter()
        .any(|entry| entry["externalId"] == json!(external_id))
}

// --------------------------------------------------------------------------- //
// Transport: authentication
// --------------------------------------------------------------------------- //

const TOOLS_LIST_BODY: &str = r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#;

#[tokio::test]
async fn mcp_requires_a_token() {
    let client = McpClient::new();
    let (status, body) = client
        .raw(TOOLS_LIST_BODY, SPEC_CONTENT_TYPE, ACCEPT, None)
        .await;
    assert_eq!(status, 401, "no Authorization header should be a 401: {}", truncate(&body));
}

#[tokio::test]
async fn mcp_rejects_a_garbage_token() {
    let client = McpClient::new();
    let (status, body) = client
        .raw(TOOLS_LIST_BODY, SPEC_CONTENT_TYPE, ACCEPT, Some("not.a.jwt"))
        .await;
    assert_eq!(status, 401, "an unverifiable token should be a 401: {}", truncate(&body));
}

#[tokio::test]
async fn mcp_a_valid_token_gets_past_security() {
    let client = McpClient::new();
    let token = client.token().await;
    let (status, body) = client
        .raw(TOOLS_LIST_BODY, SPEC_CONTENT_TYPE, ACCEPT, Some(&token))
        .await;
    assert!(
        status != 401 && status != 403,
        "the real token was rejected by the security chain: {status} {}",
        truncate(&body)
    );
}

// --------------------------------------------------------------------------- //
// Transport: content negotiation
// --------------------------------------------------------------------------- //

#[tokio::test]
async fn mcp_accept_must_offer_json_and_event_stream() {
    // Documented rather than desired: the transport compares Accept with `MediaType.equals`, so a
    // client offering only `application/json` is refused before any tool runs. Worth a test because
    // the failure is a bare 400 with nothing pointing at the header.
    let client = McpClient::new();
    let token = client.token().await;

    let (status, _) = client
        .raw(TOOLS_LIST_BODY, SPEC_CONTENT_TYPE, "application/json", Some(&token))
        .await;
    assert_eq!(status, 400, "a partial Accept should be refused");

    let (status, _) = client
        .raw(TOOLS_LIST_BODY, SPEC_CONTENT_TYPE, ACCEPT, Some(&token))
        .await;
    assert_eq!(status, 200, "the full Accept should be honoured");
}

#[tokio::test]
async fn mcp_accepts_application_json() {
    // The most consequential thing on the endpoint: every conformant MCP client sets this content
    // type, so if it is refused no off-the-shelf client can call any tool. This has regressed
    // before, as a 500 whose Jackson message points at the body rather than the content type.
    let client = McpClient::new();
    let token = client.token().await;

    let (status, body) = client
        .raw(TOOLS_LIST_BODY, SPEC_CONTENT_TYPE, ACCEPT, Some(&token))
        .await;
    assert_eq!(status, 200, "application/json was refused: {}", truncate(&body));
    assert!(
        !unwrap_envelope(&body)["result"]["tools"].is_null(),
        "no tools in the response"
    );
}

#[tokio::test]
async fn mcp_both_content_types_reach_a_tool() {
    // A text/* body must work too, since the transport reads the body as a String. Not a spec
    // requirement, but it is the fallback that stays available when the JSON converter is the thing
    // that broke, so it tells you *which* of the two is failing.
    let client = McpClient::new();
    let token = client.token().await;

    for content_type in [SPEC_CONTENT_TYPE, TEXT_CONTENT_TYPE] {
        let (status, body) = client
            .raw(TOOLS_LIST_BODY, content_type, ACCEPT, Some(&token))
            .await;
        assert_eq!(status, 200, "{content_type} was refused: {}", truncate(&body));
        assert!(
            !unwrap_envelope(&body)["result"]["tools"].is_null(),
            "{content_type} produced no tools"
        );
    }
}

/// Red whenever the double-encoding regression is present; see the module doc. The rest of the suite
/// goes red with it, by way of [`unwrap_envelope`] — this one is what names the cause.
#[tokio::test]
async fn mcp_response_is_a_json_object() {
    // When the transport writes the envelope as a `String`, Spring serializes that String *as JSON*
    // and the body becomes a quoted, escaped document. A client doing the obvious
    // `parse(body)["result"]` then gets a string instead of an object.
    let client = McpClient::new();
    let token = client.token().await;
    let (status, body) = client
        .raw(TOOLS_LIST_BODY, SPEC_CONTENT_TYPE, ACCEPT, Some(&token))
        .await;

    assert_eq!(status, 200);
    let parsed: Value = serde_json::from_str(&body).expect("response is not JSON at all");
    assert!(
        parsed.is_object(),
        "body is double-encoded: {}",
        truncate(&body)
    );
    assert_eq!(parsed["jsonrpc"], json!("2.0"));
}

// --------------------------------------------------------------------------- //
// Transport: JSON-RPC framing
// --------------------------------------------------------------------------- //

#[tokio::test]
async fn mcp_malformed_json_is_reported_not_swallowed() {
    // The code matters more than the wording: -32700 (parse error) and -32600 (invalid request)
    // both say "your message was unusable", which is what a client acts on.
    let client = McpClient::new();
    let token = client.token().await;
    let (status, body) = client
        .raw("this is not json", SPEC_CONTENT_TYPE, ACCEPT, Some(&token))
        .await;

    assert_ne!(status, 200, "a non-JSON body was accepted");
    let parsed: Value = serde_json::from_str(&body).expect("the error itself is not JSON");
    let code = parsed["jsonRpcError"]["code"].as_i64();
    assert!(
        code == Some(-32700) || code == Some(-32600),
        "expected a parse/invalid-request code, got {code:?}: {}",
        truncate(&body)
    );
}

#[tokio::test]
async fn mcp_an_unknown_method_is_an_error() {
    let client = McpClient::new();
    let token = client.token().await;
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"tools/nonexistent"}"#;
    let (status, text) = client
        .raw(body, SPEC_CONTENT_TYPE, ACCEPT, Some(&token))
        .await;

    if status != 200 {
        return; // a non-200 is an acceptable way to say "no such method"
    }
    assert!(
        !unwrap_envelope(&text)["error"].is_null(),
        "an unknown method returned a result: {}",
        truncate(&text)
    );
}

#[tokio::test]
async fn mcp_an_unknown_tool_is_an_error() {
    let client = McpClient::new();
    let outcome = client.try_call_tool("no_such_tool", json!({})).await;
    assert!(outcome.is_err(), "calling a nonexistent tool succeeded: {outcome:?}");
}

// --------------------------------------------------------------------------- //
// Tool advertisement — the schema is the whole contract an LLM sees
// --------------------------------------------------------------------------- //

#[tokio::test]
async fn mcp_advertises_every_expected_tool() {
    let tools = McpClient::new().list_tools().await;
    let missing: Vec<&str> = EXPECTED_TOOLS
        .iter()
        .copied()
        .filter(|name| !tools.contains_key(*name))
        .collect();
    assert!(missing.is_empty(), "tools/list no longer advertises: {missing:?}");
}

#[tokio::test]
async fn mcp_every_tool_has_a_usable_description_and_schema() {
    let tools = McpClient::new().list_tools().await;
    assert!(!tools.is_empty(), "tools/list advertised nothing");

    for (name, tool) in &tools {
        // The description is the only thing telling an LLM when to use a tool.
        let description = tool["description"].as_str().unwrap_or_default().trim();
        assert!(
            description.len() >= 20,
            "{name} has a missing or stub description: {description:?}"
        );

        let schema = &tool["inputSchema"];
        assert_eq!(schema["type"], json!("object"), "{name}: inputSchema is not an object");
        assert!(
            schema["properties"].is_object(),
            "{name}: inputSchema has no properties"
        );
    }
}

#[tokio::test]
async fn mcp_tool_names_follow_the_domain_action_convention() {
    let tools = McpClient::new().list_tools().await;
    for name in tools.keys() {
        assert_eq!(name, &name.to_lowercase(), "{name} is not lowercase");
        let (domain, action) = name
            .split_once('_')
            .unwrap_or_else(|| panic!("{name} has no <domain>_<action> split"));
        assert!(!action.is_empty(), "{name} has an empty action");
        assert!(
            name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "{name} has unexpected characters"
        );
        assert!(
            TOOL_DOMAINS.contains(&domain),
            "{name} has an unknown domain prefix {domain:?}"
        );
    }
}

#[tokio::test]
async fn mcp_mandatory_parameters_are_marked_required() {
    // A parameter the tool cannot work without must be in `required`. If it isn't, the model is free
    // to omit it and the call fails server-side instead of being caught by schema validation.
    let expectations: &[(&str, &[&str])] = &[
        ("dataset_create", &["externalId", "name"]),
        ("timeseries_create", &["externalId", "name", "dataSetId"]),
        ("timeseries_send_datapoint", &["timestamp", "value"]),
        ("timeseries_fetch_datapoints", &["start", "end"]),
        ("event_create", &["externalId", "type", "dataSetId", "eventTime"]),
        ("resource_create", &["externalId", "name", "labels"]),
        ("resource_fetch_nearest", &["endLabels"]),
        ("edge_create", &["relationshipType"]),
        ("edge_create_type", &["name"]),
        ("edge_get", &["id"]),
        ("edge_delete", &["id"]),
        ("unit_get", &["externalId"]),
    ];

    let tools = McpClient::new().list_tools().await;
    for (tool, required) in expectations {
        let schema = &tools
            .get(*tool)
            .unwrap_or_else(|| panic!("{tool} is not advertised"))["inputSchema"];
        let declared: BTreeSet<&str> = schema["required"]
            .as_array()
            .map(|list| list.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        for field in *required {
            assert!(
                declared.contains(field),
                "{tool}: {field} is not marked required"
            );
            assert!(
                !schema["properties"][field].is_null(),
                "{tool}: {field} is required but absent from properties"
            );
        }
    }
}

#[tokio::test]
async fn mcp_enumerating_tools_are_bounded_by_a_limit() {
    // So a big tenant cannot blow the model's context.
    let tools = McpClient::new().list_tools().await;
    for name in [
        "dataset_list", "timeseries_list", "unit_list", "label_list", "edge_list_types",
    ] {
        let properties = &tools[name]["inputSchema"]["properties"];
        assert_eq!(
            properties["limit"]["type"],
            json!("integer"),
            "{name} has no integer limit parameter"
        );
    }
}

// --------------------------------------------------------------------------- //
// The full field sweep
//
// One sequential test, for the reason given in the module doc: the coverage audit needs the union of
// everything sent, and `cargo test` gives no ordering hook across parallel tests. Each `sweep_*`
// helper owns its entities and deletes them through the MCP delete tools — which is also how those
// tools get exercised.
// --------------------------------------------------------------------------- //

/// A dataset every other entity in the sweep hangs off.
struct SweepDataset {
    id: i64,
    external_id: String,
}

#[tokio::test]
async fn mcp_full_tool_surface() {
    let client = McpClient::new();
    let tools = client.list_tools().await;

    // The label guard is held here, not in `sweep_reference_data`. Dropping it when that helper
    // returned deleted the label mid-sweep, and `resource_create` silently re-created it further
    // down — leaving a label behind on every run, with no guard attached to the new one.
    let (label, relationship_type, _label_guard) = sweep_reference_data(&client).await;
    let (dataset, mut dataset_guard) = sweep_datasets(&client).await;

    sweep_timeseries(&client, &dataset).await;
    sweep_events(&client, &dataset, &label).await;
    sweep_resources_and_edges(&client, &dataset, &label, &relationship_type).await;
    sweep_failure_modes(&client, &dataset, &label).await;

    if client
        .quietly("dataset_delete", json!({"externalId": dataset.external_id}))
        .await
    {
        dataset_guard.disarm();
    }

    assert_every_field_was_exercised(&client, &tools);
}

/// `unit_*`, `label_*`, `edge_*_type*`. Returns a label name, a relationship-type name, and the
/// label's cleanup guard — which the caller must hold for the rest of the sweep, since the later
/// helpers create resources carrying that label.
///
/// Relationship types get no guard: there is no delete for them in the MCP surface or the REST one,
/// so every run leaves one behind. See the note in AGENTS.md.
async fn sweep_reference_data(client: &McpClient) -> (String, String, CleanupGuard) {
    // --- unit_list: limit, and the shared list envelope ---
    let all_units = client.call_tool("unit_list", json!({})).await;
    let unit_items = items(&all_units);
    assert!(!unit_items.is_empty(), "the backend has no units configured");
    assert_eq!(
        all_units["returned"],
        json!(unit_items.len()),
        "unit_list's `returned` disagrees with its items"
    );
    for unit in &unit_items {
        assert!(!text_of(unit, "externalId").is_empty());
        assert!(!text_of(unit, "name").is_empty());
    }

    // `limit` must actually cap, and `truncated` must say when it did — the flag is the only signal
    // an LLM gets that it is looking at a partial catalogue.
    let capped = client.call_tool("unit_list", json!({"limit": 1})).await;
    assert_eq!(items(&capped).len(), 1, "unit_list ignored limit");
    assert_eq!(capped["truncated"], json!(true), "a capped list did not flag truncation");
    let roomy = client
        .call_tool("unit_list", json!({"limit": unit_items.len() + 10}))
        .await;
    assert_eq!(roomy["truncated"], json!(false), "an uncapped list claimed truncation");

    // --- unit_get: externalId ---
    let expected = &unit_items[0];
    let fetched = client
        .call_tool("unit_get", json!({"externalId": text_of(expected, "externalId")}))
        .await;
    let fetched_items = items(&fetched);
    assert_eq!(fetched_items.len(), 1, "unit_get did not return exactly one unit");
    assert_eq!(id_of(&fetched_items[0]), id_of(expected));
    assert_eq!(text_of(&fetched_items[0], "name"), text_of(expected, "name"));

    // A miss is an empty list, not an error — as the tool description promises.
    let missing = client
        .call_tool("unit_get", json!({"externalId": unique_id("mcp_no_unit")}))
        .await;
    assert!(items(&missing).is_empty(), "unit_get invented a unit");

    // --- label_create / label_update: every field ---
    let label_name = unique_id("mcp_label").to_uppercase();
    let created = client
        .call_tool(
            "label_create",
            json!({
                "name": label_name,
                "description": "MCP sweep label",
                "i18nCode": "mcp.sweep.label",
                "color": "#123456",
            }),
        )
        .await;
    let label = items(&created)
        .first()
        .cloned()
        .expect("label_create returned no label");
    let label_guard = cleanup_labels(vec![label_name.clone()]);

    assert_eq!(text_of(&label, "name"), label_name, "label_create changed the name");
    assert_eq!(text_of(&label, "description"), "MCP sweep label");
    assert_eq!(text_of(&label, "i18nCode"), "mcp.sweep.label");
    assert_eq!(text_of(&label, "color"), "#123456");

    let updated = client
        .call_tool(
            "label_update",
            json!({
                "id": id_of(&label),
                "description": "MCP sweep label, revised",
                "i18nCode": "mcp.sweep.label.v2",
                "color": "#654321",
            }),
        )
        .await;
    let updated_label = items(&updated)
        .first()
        .cloned()
        .expect("label_update returned no label");
    assert_eq!(text_of(&updated_label, "description"), "MCP sweep label, revised");
    assert_eq!(text_of(&updated_label, "i18nCode"), "mcp.sweep.label.v2");
    assert_eq!(
        text_of(&updated_label, "color").to_lowercase(),
        "#654321",
        "label_update did not change the colour"
    );
    assert_eq!(
        text_of(&updated_label, "name"),
        label_name,
        "label_update changed the name, which it does not support"
    );

    // --- label_list: limit ---
    let listed = client.call_tool("label_list", json!({"limit": 5000})).await;
    assert!(
        items(&listed)
            .iter()
            .any(|entry| entry["name"] == json!(label_name)),
        "a label created through label_create is not listed"
    );

    // --- edge_create_type: every field; edge_list_types: limit ---
    let relationship_type = unique_id("mcp_rel").to_uppercase();
    let created_type = client
        .call_tool(
            "edge_create_type",
            json!({
                "name": relationship_type,
                "description": "MCP sweep relationship type",
                "i18nCode": "mcp.sweep.rel",
            }),
        )
        .await;
    let rel_type = items(&created_type)
        .first()
        .cloned()
        .expect("edge_create_type returned nothing");
    assert_eq!(text_of(&rel_type, "name"), relationship_type);
    assert_eq!(text_of(&rel_type, "description"), "MCP sweep relationship type");
    assert_eq!(text_of(&rel_type, "i18nCode"), "mcp.sweep.rel");

    let types = client.call_tool("edge_list_types", json!({"limit": 5000})).await;
    let type_names: BTreeSet<String> = items(&types)
        .iter()
        .map(|entry| text_of(entry, "name"))
        .collect();
    // BELONGS_TO is the built-in dataset-hierarchy type; it is always present.
    assert!(type_names.contains("BELONGS_TO"), "the built-in BELONGS_TO type is missing");
    assert!(
        type_names.contains(&relationship_type),
        "a type reported created is not listed"
    );

    (label_name, relationship_type, label_guard)
}

/// `dataset_*`. Returns a live dataset for the rest of the sweep, plus its guard — moved out rather
/// than disarmed and re-armed by the caller, so there is no window where the dataset is unguarded.
async fn sweep_datasets(client: &McpClient) -> (SweepDataset, CleanupGuard) {
    // --- dataset_create: externalId, name, description ---
    let external_id = unique_id("mcp_ds");
    let guard = cleanup_datasets(vec![external_id.clone()]);
    let created = client
        .call_tool(
            "dataset_create",
            json!({
                "externalId": external_id,
                "name": "MCP sweep dataset",
                "description": "created by src/mcp_integration.rs",
            }),
        )
        .await;
    let dataset = items(&created)
        .first()
        .cloned()
        .expect("dataset_create returned nothing");
    assert_eq!(text_of(&dataset, "externalId"), external_id);
    assert_eq!(text_of(&dataset, "name"), "MCP sweep dataset");
    assert_eq!(text_of(&dataset, "description"), "created by src/mcp_integration.rs");
    let dataset_id = id_of(&dataset);

    // The row the MCP tool wrote must be the row REST reads back.
    let through_rest = poll_until(
        || async {
            client
                .api
                .datasets
                .by_ids(&vec![crate::generic::IdAndExtId::from_external_id(&external_id)])
                .await
                .map(|wrapper| wrapper.get_items().clone())
                .unwrap_or_default()
        },
        |found: &Vec<crate::datasets::Dataset>| !found.is_empty(),
    )
    .await;
    assert!(
        !through_rest.is_empty(),
        "dataset {external_id} reported created but REST cannot see it"
    );
    assert_eq!(through_rest[0].name, "MCP sweep dataset");

    // --- dataset_search: query, limit ---
    let found = poll_until(
        || async {
            client
                .call_tool("dataset_search", json!({"query": external_id, "limit": 10}))
                .await
        },
        |payload: &Value| has_external_id(&items(payload), &external_id),
    )
    .await;
    assert!(
        has_external_id(&items(&found), &external_id),
        "dataset_search cannot find a dataset it just created"
    );

    // --- dataset_list: limit ---
    let listed = client.call_tool("dataset_list", json!({"limit": 2})).await;
    assert!(items(&listed).len() <= 2, "dataset_list ignored limit");
    assert_eq!(listed["returned"], json!(items(&listed).len()));

    // --- dataset_update: id, newName, newDescription, newExternalId (on a throwaway) ---
    let throwaway = unique_id("mcp_ds_upd");
    let renamed = format!("{throwaway}_r");
    let mut throwaway_guard = cleanup_datasets(vec![throwaway.clone(), renamed.clone()]);
    let doomed = client
        .call_tool(
            "dataset_create",
            json!({"externalId": throwaway, "name": "MCP dataset to rename"}),
        )
        .await;
    let doomed_id = id_of(&items(&doomed)[0]);

    let updated = client
        .call_tool(
            "dataset_update",
            json!({
                "id": doomed_id,
                "newName": "MCP dataset renamed",
                "newDescription": "MCP dataset redescribed",
                "newExternalId": renamed,
            }),
        )
        .await;
    let updated_dataset = &items(&updated)[0];
    assert_eq!(text_of(updated_dataset, "name"), "MCP dataset renamed");
    assert_eq!(text_of(updated_dataset, "description"), "MCP dataset redescribed");
    assert_eq!(text_of(updated_dataset, "externalId"), renamed);

    // --- dataset_delete: externalId, then id on a second throwaway ---
    client
        .call_tool("dataset_delete", json!({"externalId": renamed}))
        .await;
    let gone = poll_until(
        || async {
            client
                .api
                .datasets
                .by_ids(&vec![crate::generic::IdAndExtId::from_external_id(&renamed)])
                .await
                .map(|wrapper| wrapper.get_items().clone())
                .unwrap_or_default()
        },
        |found: &Vec<crate::datasets::Dataset>| found.is_empty(),
    )
    .await;
    assert!(gone.is_empty(), "dataset_delete by externalId left the dataset in place");
    throwaway_guard.disarm();

    let by_id_target = unique_id("mcp_ds_byid");
    let mut by_id_guard = cleanup_datasets(vec![by_id_target.clone()]);
    let second = client
        .call_tool(
            "dataset_create",
            json!({"externalId": by_id_target, "name": "MCP dataset deleted by id"}),
        )
        .await;
    client
        .call_tool("dataset_delete", json!({"id": id_of(&items(&second)[0])}))
        .await;
    let gone = poll_until(
        || async {
            client
                .api
                .datasets
                .by_ids(&vec![crate::generic::IdAndExtId::from_external_id(&by_id_target)])
                .await
                .map(|wrapper| wrapper.get_items().clone())
                .unwrap_or_default()
        },
        |found: &Vec<crate::datasets::Dataset>| found.is_empty(),
    )
    .await;
    assert!(gone.is_empty(), "dataset_delete by numeric id left the dataset in place");
    by_id_guard.disarm();

    // The sweep's own dataset is torn down by the driver, which takes the guard with it.
    (
        SweepDataset {
            id: dataset_id,
            external_id,
        },
        guard,
    )
}

/// `timeseries_*`, including both datapoint write and all three read shapes.
async fn sweep_timeseries(client: &McpClient, dataset: &SweepDataset) {
    use crate::generic::{DataWrapper, IdAndExtId};

    // A real unit externalId, for `newUnitExternalId` on update and `unitExternalId` on create.
    let unit_external_id = text_of(
        &items(&client.call_tool("unit_list", json!({"limit": 1})).await)[0],
        "externalId",
    );

    let external_id = unique_id("mcp_ts");
    // Both names: `timeseries_update` renames this series partway through, and a guard armed only
    // with the original would look for an id that no longer exists.
    let renamed = format!("{external_id}_r");
    let mut guard = cleanup_timeseries(vec![external_id.clone(), renamed.clone()]);

    // --- timeseries_create: externalId, name, dataSetId, description, valueType, unit ---
    let created = client
        .call_tool(
            "timeseries_create",
            json!({
                "externalId": external_id,
                "name": "MCP sweep series",
                "dataSetId": dataset.id,
                "description": "MCP sweep series description",
                "valueType": "FLOAT",
                "unit": "Celsius",
            }),
        )
        .await;
    let series = items(&created)
        .first()
        .cloned()
        .expect("timeseries_create returned nothing");
    let series_id = id_of(&series);

    assert_eq!(text_of(&series, "externalId"), external_id);
    assert_eq!(text_of(&series, "name"), "MCP sweep series");
    assert_eq!(text_of(&series, "description"), "MCP sweep series description");
    assert_eq!(text_of(&series, "unit"), "Celsius");
    assert_eq!(
        text_of(&series, "valueType").to_uppercase(),
        "FLOAT",
        "timeseries_create did not honour valueType"
    );
    assert_eq!(
        series["dataSetId"].as_str().map(|s| s.parse::<i64>().unwrap()),
        Some(dataset.id),
        "the series did not land in the dataset it was given"
    );

    // Cross-check through REST: same row, same dataset.
    let through_rest = poll_until(
        || async {
            let wrapper: DataWrapper<IdAndExtId> =
                (&vec![IdAndExtId::from_external_id(&external_id)]).into();
            client
                .api
                .time_series
                .by_ids(&wrapper)
                .await
                .map(|found| found.get_items().clone())
                .unwrap_or_default()
        },
        |found: &Vec<crate::TimeSeries>| !found.is_empty(),
    )
    .await;
    assert!(
        !through_rest.is_empty(),
        "a timeseries created through MCP is invisible to REST"
    );
    assert_eq!(through_rest[0].data_set_id, Some(dataset.id as u64));

    // --- timeseries_get: externalId and id must agree ---
    let by_external_id = client
        .call_tool("timeseries_get", json!({"externalId": external_id}))
        .await;
    let by_id = client
        .call_tool("timeseries_get", json!({"id": series_id}))
        .await;
    assert_eq!(items(&by_external_id).len(), 1, "timeseries_get by externalId missed");
    assert_eq!(items(&by_id).len(), 1, "timeseries_get by id missed");
    assert_eq!(id_of(&items(&by_external_id)[0]), series_id);
    assert_eq!(id_of(&items(&by_id)[0]), series_id);

    // --- timeseries_list: limit; timeseries_search: query, limit ---
    let listed = poll_until(
        || async { client.call_tool("timeseries_list", json!({"limit": 1000})).await },
        |payload: &Value| has_external_id(&items(payload), &external_id),
    )
    .await;
    assert!(
        has_external_id(&items(&listed), &external_id),
        "timeseries_list omits a fresh series"
    );

    let searched = poll_until(
        || async {
            client
                .call_tool("timeseries_search", json!({"query": external_id, "limit": 10}))
                .await
        },
        |payload: &Value| has_external_id(&items(payload), &external_id),
    )
    .await;
    assert!(
        has_external_id(&items(&searched), &external_id),
        "timeseries_search cannot find a fresh series"
    );

    // --- timeseries_send_datapoint: externalId/id, timestamp, value ---
    client
        .call_tool(
            "timeseries_send_datapoint",
            json!({"externalId": external_id, "timestamp": EVENT_TIME, "value": "42.5"}),
        )
        .await;
    client
        .call_tool(
            "timeseries_send_datapoint",
            json!({"id": series_id, "timestamp": "2026-04-23T14:00:00Z", "value": "43.5"}),
        )
        .await;

    // --- timeseries_get_latest: externalId and id ---
    let latest = poll_until(
        || async {
            client
                .call_tool("timeseries_get_latest", json!({"externalId": external_id}))
                .await
        },
        |payload: &Value| {
            items(payload)
                .first()
                .map(|entry| !entry["datapoints"].as_array().unwrap_or(&vec![]).is_empty())
                .unwrap_or(false)
        },
    )
    .await;
    let datapoints = items(&latest)[0]["datapoints"].as_array().cloned().unwrap_or_default();
    assert!(!datapoints.is_empty(), "the datapoints never became readable");
    // The most recent of the two is 14:00, not the one written first.
    assert_eq!(
        datapoints[0]["value"].as_f64(),
        Some(43.5),
        "get_latest did not return the most recent datapoint"
    );

    let latest_by_id = client
        .call_tool("timeseries_get_latest", json!({"id": series_id}))
        .await;
    assert!(
        !items(&latest_by_id)[0]["datapoints"]
            .as_array()
            .unwrap_or(&vec![])
            .is_empty(),
        "get_latest by numeric id found nothing"
    );

    // --- timeseries_fetch_datapoints: externalId/id, start, end, limit, aggregates, granularity ---
    let ranged = poll_until(
        || async {
            client
                .call_tool(
                    "timeseries_fetch_datapoints",
                    json!({"externalId": external_id, "start": DAY_START, "end": DAY_END}),
                )
                .await
        },
        |payload: &Value| {
            items(payload)
                .first()
                .map(|entry| entry["datapoints"].as_array().map(|d| d.len()) == Some(2))
                .unwrap_or(false)
        },
    )
    .await;
    assert_eq!(
        items(&ranged)[0]["datapoints"].as_array().map(|d| d.len()),
        Some(2),
        "the range that contains both datapoints did not return both"
    );

    // A range that excludes them must return nothing rather than everything.
    let outside = client
        .call_tool(
            "timeseries_fetch_datapoints",
            json!({
                "externalId": external_id,
                "start": "2020-01-01T00:00:00Z",
                "end": "2020-01-02T00:00:00Z",
            }),
        )
        .await;
    let outside_points = items(&outside)
        .first()
        .and_then(|entry| entry["datapoints"].as_array().cloned())
        .unwrap_or_default();
    assert!(
        outside_points.is_empty(),
        "a range that excludes the datapoints returned them anyway"
    );

    let capped = client
        .call_tool(
            "timeseries_fetch_datapoints",
            json!({"id": series_id, "start": DAY_START, "end": DAY_END, "limit": 1}),
        )
        .await;
    assert_eq!(
        items(&capped)[0]["datapoints"].as_array().map(|d| d.len()),
        Some(1),
        "limit did not cap the datapoints"
    );

    let aggregated = client
        .call_tool(
            "timeseries_fetch_datapoints",
            json!({
                "externalId": external_id,
                "start": DAY_START,
                "end": DAY_END,
                "aggregates": "max,min",
                "granularity": "1d",
            }),
        )
        .await;
    let bucket = items(&aggregated)[0]["datapoints"].as_array().cloned().unwrap_or_default();
    assert!(!bucket.is_empty(), "no aggregate bucket came back");
    assert_eq!(bucket[0]["max"].as_f64(), Some(43.5), "wrong bucket max");
    assert_eq!(bucket[0]["min"].as_f64(), Some(42.5), "wrong bucket min");
    assert!(
        bucket[0]["value"].is_null(),
        "an aggregate response should not carry a raw value"
    );

    // --- timeseries_update: id and every newX field ---
    let updated = client
        .call_tool(
            "timeseries_update",
            json!({
                "id": series_id,
                "newName": "MCP series renamed",
                "newDescription": "MCP series redescribed",
                "newExternalId": renamed,
                "newUnit": "Bar",
                "newUnitExternalId": unit_external_id,
            }),
        )
        .await;
    let updated_series = &items(&updated)[0];
    assert_eq!(text_of(updated_series, "name"), "MCP series renamed");
    assert_eq!(text_of(updated_series, "description"), "MCP series redescribed");
    assert_eq!(text_of(updated_series, "externalId"), renamed);
    assert_eq!(text_of(updated_series, "unit"), "Bar");
    assert_eq!(text_of(updated_series, "unitExternalId"), unit_external_id);
    assert_eq!(
        updated_series["dataSetId"].as_str().map(|s| s.parse::<i64>().unwrap()),
        Some(dataset.id),
        "the rename moved the series out of its dataset"
    );

    // --- timeseries_delete: externalId, then id on a second series ---
    client
        .call_tool("timeseries_delete", json!({"externalId": renamed}))
        .await;
    assert!(
        items(&client.call_tool("timeseries_get", json!({"externalId": renamed})).await).is_empty(),
        "timeseries_delete by externalId left the series in place"
    );
    guard.disarm();

    let second_id = unique_id("mcp_ts_byid");
    let mut second_guard = cleanup_timeseries(vec![second_id.clone()]);
    let second = client
        .call_tool(
            "timeseries_create",
            json!({
                // Both, because `unitExternalId` alone is refused — see
                // `mcp_timeseries_create_accepts_a_unit_external_id`. Passing it here is what keeps
                // the field-coverage audit honest about a parameter the api added.
                "externalId": second_id,
                "name": "MCP series deleted by id",
                "dataSetId": dataset.id,
                "unit": "Celsius",
                "unitExternalId": unit_external_id,
            }),
        )
        .await;
    let second_series_id = id_of(&items(&second)[0]);
    client
        .call_tool("timeseries_delete", json!({"id": second_series_id}))
        .await;
    assert!(
        items(&client.call_tool("timeseries_get", json!({"id": second_series_id})).await).is_empty(),
        "timeseries_delete by numeric id left the series in place"
    );
    second_guard.disarm();
}

/// `event_*`. `event_filter` has the widest parameter surface of any tool, so most of this is it.
async fn sweep_events(client: &McpClient, dataset: &SweepDataset, label: &str) {
    // A resource to hang `relatedResourceExternalIds` off.
    let resource_external_id = unique_id("mcp_ev_res");
    let _resource_guard = cleanup_resources(vec![resource_external_id.clone()]);
    let resource = client
        .call_tool(
            "resource_create",
            json!({
                "externalId": resource_external_id,
                "name": "MCP event-linked resource",
                "labels": label,
                "dataSetId": dataset.id,
            }),
        )
        .await;
    let resource_id = id_of(&nodes(&resource)[0]);

    // --- event_create: every field, including relatedResourceExternalIds ---
    let external_id = unique_id("mcp_ev");
    // Both names, for the same reason as the series above: `event_update` renames this one.
    let renamed = format!("{external_id}_r");
    let mut guard = cleanup_events(vec![external_id.clone(), renamed.clone()]);
    let created = client
        .call_tool(
            "event_create",
            json!({
                "externalId": external_id,
                "type": "McpSweepAlarm",
                "dataSetId": dataset.id,
                "eventTime": EVENT_TIME,
                "subType": "McpSweepSubType",
                "status": "IN_PROGRESS",
                "description": "MCP sweep event",
                "relatedResourceExternalIds": [resource_external_id],
            }),
        )
        .await;
    let event = items(&created)
        .first()
        .cloned()
        .expect("event_create returned nothing");
    let uuid = text_of(&event, "id");

    assert_eq!(text_of(&event, "type"), "McpSweepAlarm");
    assert_eq!(text_of(&event, "subType"), "McpSweepSubType");
    assert_eq!(text_of(&event, "status"), "IN_PROGRESS");
    assert_eq!(text_of(&event, "description"), "MCP sweep event");
    assert!(
        text_of(&event, "eventTime").starts_with("2026-04-23T13:00"),
        "event_create stored the wrong eventTime: {}",
        text_of(&event, "eventTime")
    );
    // The api must resolve the named resource to its numeric id.
    let related = event["relatedResources"].as_array().cloned().unwrap_or_default();
    assert_eq!(related.len(), 1, "the related resource was not linked");
    assert_eq!(text_of(&related[0], "externalId"), resource_external_id);
    assert_eq!(id_of(&related[0]), resource_id);

    // --- event_get: externalIds and ids ---
    let fetched = poll_until(
        || async {
            client
                .call_tool("event_get", json!({"externalIds": [external_id]}))
                .await
        },
        |payload: &Value| !items(payload).is_empty(),
    )
    .await;
    assert!(!items(&fetched).is_empty(), "event_get by externalId found nothing");
    assert_eq!(text_of(&items(&fetched)[0], "type"), "McpSweepAlarm");

    let by_uuid = client.call_tool("event_get", json!({"ids": [uuid]})).await;
    assert_eq!(
        text_of(&items(&by_uuid)[0], "id"),
        uuid,
        "event_get by UUID returned a different event"
    );

    // A batch lookup returns the found subset and silently omits what is missing.
    let batch = client
        .call_tool(
            "event_get",
            json!({"externalIds": [external_id, "mcp_sweep_absent_event"]}),
        )
        .await;
    assert_eq!(
        items(&batch).len(),
        1,
        "a batch event_get did not return exactly the found subset"
    );

    // --- event_search: query, limit ---
    let searched = poll_until(
        || async {
            client
                .call_tool("event_search", json!({"query": external_id, "limit": 10}))
                .await
        },
        |payload: &Value| has_external_id(&items(payload), &external_id),
    )
    .await;
    assert!(
        has_external_id(&items(&searched), &external_id),
        "event_search cannot find a fresh event"
    );

    // `items` is absent rather than empty on a miss, so a client must use a tolerant read.
    let missed = client
        .call_tool("event_search", json!({"query": unique_id("mcp_no_event"), "limit": 5}))
        .await;
    assert_eq!(missed["returned"], json!(0));
    assert!(items(&missed).is_empty());

    // --- event_filter: externalId, and the drill-down envelope ---
    let filtered = poll_until(
        || async {
            client
                .call_tool("event_filter", json!({"externalId": external_id}))
                .await
        },
        |payload: &Value| payload["returned"] == json!(1),
    )
    .await;
    assert_eq!(filtered["returned"], json!(1), "event_filter by externalId missed");
    let listed_events = filtered["events"].as_array().cloned().unwrap_or_default();
    assert_eq!(text_of(&listed_events[0], "externalId"), external_id);

    // --- event_filter: type + the eventTime window (start inclusive, end exclusive) ---
    let inside = client
        .call_tool(
            "event_filter",
            json!({"type": "McpSweepAlarm", "start": DAY_START, "end": DAY_END}),
        )
        .await;
    assert!(
        inside["returned"].as_i64().unwrap_or(0) >= 1,
        "the window containing the event returned nothing"
    );
    let outside = client
        .call_tool(
            "event_filter",
            json!({
                "type": "McpSweepAlarm",
                "start": "2020-01-01T00:00:00Z",
                "end": "2020-01-02T00:00:00Z",
            }),
        )
        .await;
    assert_eq!(
        outside["returned"],
        json!(0),
        "a window that excludes the event still returned it"
    );

    // --- event_filter: subType ---
    let by_sub_type = client
        .call_tool("event_filter", json!({"subType": "McpSweepSubType"}))
        .await;
    assert!(
        has_external_id(
            &by_sub_type["events"].as_array().cloned().unwrap_or_default(),
            &external_id
        ),
        "event_filter by subType missed the event"
    );

    // --- event_filter: source ---
    // `source` is filterable but not settable through event_create, so the only assertion available
    // is that an unmatched source narrows to nothing rather than being ignored — an ignored criterion
    // returns everything the caller can read, which reads exactly like a working query. The missing
    // `source` on event_create is a genuine gap in the tool surface, not a limitation of this test.
    let by_source = client
        .call_tool("event_filter", json!({"source": "mcp_sweep_no_such_source"}))
        .await;
    assert_eq!(
        by_source["returned"],
        json!(0),
        "an unmatched source was ignored instead of narrowing"
    );

    // --- event_filter: dataSetId and dataSetExternalId (both lists) ---
    let by_dataset_id = client
        .call_tool(
            "event_filter",
            json!({"dataSetId": [dataset.id], "type": "McpSweepAlarm"}),
        )
        .await;
    assert!(
        has_external_id(
            &by_dataset_id["events"].as_array().cloned().unwrap_or_default(),
            &external_id
        ),
        "event_filter by dataSetId missed the event"
    );
    let by_dataset_external_id = client
        .call_tool(
            "event_filter",
            json!({"dataSetExternalId": [dataset.external_id], "type": "McpSweepAlarm"}),
        )
        .await;
    assert!(
        has_external_id(
            &by_dataset_external_id["events"].as_array().cloned().unwrap_or_default(),
            &external_id
        ),
        "event_filter by dataSetExternalId missed the event"
    );

    // --- event_filter: relatedResources (numeric) and relatedResourceExternalIds (strings) ---
    let by_related_external_id = client
        .call_tool(
            "event_filter",
            json!({"relatedResourceExternalIds": [resource_external_id]}),
        )
        .await;
    assert!(
        has_external_id(
            &by_related_external_id["events"].as_array().cloned().unwrap_or_default(),
            &external_id
        ),
        "event_filter by relatedResourceExternalIds missed the event"
    );
    let by_related_id = client
        .call_tool("event_filter", json!({"relatedResources": [resource_id]}))
        .await;
    assert!(
        has_external_id(
            &by_related_id["events"].as_array().cloned().unwrap_or_default(),
            &external_id
        ),
        "event_filter by relatedResources missed the event"
    );

    // --- event_filter: groupBy switches to aggregate mode; limit caps the drill-down ---
    let grouped = client
        .call_tool("event_filter", json!({"type": "McpSweepAlarm", "groupBy": "type"}))
        .await;
    assert!(
        grouped["events"].is_null(),
        "grouped mode should aggregate, not list events: {grouped}"
    );

    let capped = client
        .call_tool("event_filter", json!({"type": "McpSweepAlarm", "limit": 1}))
        .await;
    assert_eq!(capped["returned"], json!(1), "event_filter ignored limit");
    assert_eq!(capped["limit"], json!(1));

    // --- event_update: externalId + every newX field ---
    let updated = client
        .call_tool(
            "event_update",
            json!({
                "externalId": external_id,
                "newExternalId": renamed,
                "newDescription": "MCP sweep event, revised",
                "newType": "McpRenamedAlarm",
                "newSubType": "McpRenamedSubType",
                "newStatus": "COMPLETE",
            }),
        )
        .await;
    let updated_event = &items(&updated)[0];
    assert_eq!(text_of(updated_event, "externalId"), renamed);
    assert_eq!(text_of(updated_event, "description"), "MCP sweep event, revised");
    assert_eq!(text_of(updated_event, "type"), "McpRenamedAlarm");
    assert_eq!(text_of(updated_event, "subType"), "McpRenamedSubType");
    assert_eq!(text_of(updated_event, "status"), "COMPLETE");

    // The row can surface under its new externalId before the rest of the update has propagated, so
    // wait on the field under test rather than on mere existence.
    let persisted = poll_until(
        || async {
            client
                .call_tool("event_get", json!({"externalIds": [renamed]}))
                .await
        },
        |payload: &Value| {
            items(payload)
                .first()
                .map(|entry| entry["type"] == json!("McpRenamedAlarm"))
                .unwrap_or(false)
        },
    )
    .await;
    assert_eq!(
        text_of(&items(&persisted)[0], "type"),
        "McpRenamedAlarm",
        "the renamed event's type change never propagated"
    );
    assert_eq!(text_of(&items(&persisted)[0], "status"), "COMPLETE");

    // --- event_update: the `id` (UUID) path, on a field that is not the externalId ---
    // Renaming the externalId by UUID is broken; see
    // `mcp_event_update_by_uuid_reindexes_the_external_id`. A status change by UUID works, and is
    // what covers this parameter.
    let by_uuid_update = client
        .call_tool("event_update", json!({"id": uuid, "newStatus": "FAILED"}))
        .await;
    assert_eq!(
        text_of(&items(&by_uuid_update)[0], "status"),
        "FAILED",
        "event_update by UUID did not apply the status"
    );

    // --- event_delete: externalIds, then ids on a second event ---
    let delete_response = client
        .call_tool("event_delete", json!({"externalIds": [renamed]}))
        .await;
    assert!(
        delete_response.is_string(),
        "event_delete should answer with a sentence, got {delete_response}"
    );
    let gone = poll_until(
        || async {
            client
                .call_tool("event_get", json!({"externalIds": [renamed]}))
                .await
        },
        |payload: &Value| items(payload).is_empty(),
    )
    .await;
    assert!(items(&gone).is_empty(), "event_delete by externalId left the event");
    guard.disarm(); // only now is the event genuinely gone, under either name

    let second_external_id = unique_id("mcp_ev_byid");
    let mut second_guard = cleanup_events(vec![second_external_id.clone()]);
    let second = client
        .call_tool(
            "event_create",
            json!({
                "externalId": second_external_id,
                "type": "McpSweepAlarm",
                "dataSetId": dataset.id,
                "eventTime": EVENT_TIME,
            }),
        )
        .await;
    let second_uuid = text_of(&items(&second)[0], "id");
    poll_until(
        || async { client.call_tool("event_get", json!({"ids": [second_uuid]})).await },
        |payload: &Value| !items(payload).is_empty(),
    )
    .await;

    client
        .call_tool("event_delete", json!({"ids": [second_uuid]}))
        .await;
    let gone = poll_until(
        || async { client.call_tool("event_get", json!({"ids": [second_uuid]})).await },
        |payload: &Value| items(payload).is_empty(),
    )
    .await;
    assert!(items(&gone).is_empty(), "event_delete by UUID left the event");
    second_guard.disarm();

    client
        .quietly("resource_delete", json!({"externalId": resource_external_id}))
        .await;
    // Left armed either way: `_resource_guard` is the backstop if that delete did not land.
}

/// `resource_*` and `edge_*`, including the two graph traversals and their narrowing options.
async fn sweep_resources_and_edges(
    client: &McpClient,
    dataset: &SweepDataset,
    label: &str,
    relationship_type: &str,
) {
    use crate::generic::IdAndExtId;

    // A second dataset, so `newDataSetId` has somewhere to move a resource to.
    let secondary_external_id = unique_id("mcp_ds_alt");
    let mut secondary_guard = cleanup_datasets(vec![secondary_external_id.clone()]);
    let secondary = client
        .call_tool(
            "dataset_create",
            json!({"externalId": secondary_external_id, "name": "MCP alternate dataset"}),
        )
        .await;
    let secondary_id = id_of(&items(&secondary)[0]);

    // --- resource_create: externalId, name, labels, description, dataSetId ---
    let mut created_ids = Vec::new();
    let mut external_ids = Vec::new();
    for which in ["a", "b", "c"] {
        let external_id = unique_id(&format!("mcp_res_{which}"));
        let node = client
            .call_tool(
                "resource_create",
                json!({
                    "externalId": external_id,
                    "name": format!("MCP sweep resource {which}"),
                    "labels": label,
                    "description": format!("MCP sweep resource {which} description"),
                    "dataSetId": dataset.id,
                }),
            )
            .await;
        let node = nodes(&node)
            .first()
            .cloned()
            .expect("resource_create returned no node");

        assert_eq!(text_of(&node, "externalId"), external_id);
        assert_eq!(
            text_of(&node, "description"),
            format!("MCP sweep resource {which} description"),
            "resource_create dropped the description"
        );
        assert!(
            node["labels"]
                .as_array()
                .map(|labels| labels.iter().any(|entry| entry == &json!(label)))
                .unwrap_or(false),
            "resource_create dropped the label"
        );
        assert_eq!(
            node["dataSetId"].as_str().map(|s| s.parse::<i64>().unwrap()),
            Some(dataset.id),
            "the resource did not land in the dataset it was given"
        );

        created_ids.push(id_of(&node));
        external_ids.push(external_id);
    }
    // `resource_update` renames c partway through, so the guard carries that name too.
    let mut guarded_names = external_ids.clone();
    guarded_names.push(format!("{}_r", external_ids[2]));
    let mut resource_guard = cleanup_resources(guarded_names);
    let (a, b, c) = (&external_ids[0], &external_ids[1], &external_ids[2]);
    let (a_id, b_id, c_id) = (created_ids[0], created_ids[1], created_ids[2]);

    // Cross-check through REST.
    let through_rest = poll_until(
        || async {
            client
                .api
                .resources
                .by_ids(&vec![IdAndExtId::from_external_id(a)])
                .await
                .map(|wrapper| wrapper.nodes.clone().unwrap_or_default())
                .unwrap_or_default()
        },
        |found: &Vec<crate::resources::Resource>| !found.is_empty(),
    )
    .await;
    assert!(
        !through_rest.is_empty(),
        "a resource created through MCP is invisible to REST"
    );

    // --- resource_get: externalIds and ids, both comma-separated strings ---
    let by_external_ids = client
        .call_tool("resource_get", json!({"externalIds": format!("{a},{b}")}))
        .await;
    assert_eq!(items(&by_external_ids).len(), 2, "resource_get by externalIds missed one");
    let by_ids = client
        .call_tool("resource_get", json!({"ids": format!("{a_id},{b_id}")}))
        .await;
    let fetched: BTreeSet<i64> = items(&by_ids).iter().map(id_of).collect();
    assert_eq!(
        fetched,
        BTreeSet::from([a_id, b_id]),
        "resource_get by ids returned the wrong resources"
    );

    // --- resource_search: query, limit ---
    let searched = poll_until(
        || async {
            client
                .call_tool("resource_search", json!({"query": a, "limit": 10}))
                .await
        },
        |payload: &Value| has_external_id(&items(payload), a),
    )
    .await;
    assert!(
        has_external_id(&items(&searched), a),
        "resource_search cannot find a fresh resource"
    );

    // --- edge_create: named endpoints, then numeric endpoints with a description ---
    let edge_ab = client
        .call_tool(
            "edge_create",
            json!({
                "fromExternalId": a,
                "toExternalId": b,
                "relationshipType": relationship_type,
            }),
        )
        .await;
    let edge_ab = edge_ab["relations"].as_array().cloned().unwrap_or_default();
    let edge_ab = edge_ab.first().cloned().expect("edge_create returned no relation");
    assert_eq!(text_of(&edge_ab, "start"), a_id.to_string(), "the edge starts at the wrong node");
    assert_eq!(text_of(&edge_ab, "end"), b_id.to_string(), "the edge ends at the wrong node");
    assert_eq!(text_of(&edge_ab, "type"), relationship_type);

    let edge_ac = client
        .call_tool(
            "edge_create",
            json!({
                "fromId": a_id,
                "toId": c_id,
                "relationshipType": relationship_type,
                "description": "wired by numeric id",
            }),
        )
        .await;
    let edge_ac = edge_ac["relations"].as_array().cloned().unwrap_or_default();
    let edge_ac = edge_ac.first().cloned().expect("edge_create by id returned no relation");
    assert_eq!(text_of(&edge_ac, "start"), a_id.to_string());
    assert_eq!(text_of(&edge_ac, "end"), c_id.to_string());
    assert_eq!(
        text_of(&edge_ac, "description"),
        "wired by numeric id",
        "edge_create dropped the description"
    );

    // The third leg makes a triangle, so one edge can be deleted without stranding an endpoint.
    let edge_bc = client
        .call_tool(
            "edge_create",
            json!({
                "fromExternalId": b,
                "toExternalId": c,
                "relationshipType": relationship_type,
            }),
        )
        .await;
    let edge_bc = edge_bc["relations"]
        .as_array()
        .and_then(|relations| relations.first().cloned())
        .expect("edge_create returned no relation for the third leg");

    // --- edge_get: id ---
    let fetched_edge = client
        .call_tool("edge_get", json!({"id": id_of(&edge_ab)}))
        .await;
    assert_eq!(items(&fetched_edge).len(), 1, "edge_get did not return the edge");
    assert_eq!(id_of(&items(&fetched_edge)[0]), id_of(&edge_ab));

    // --- resource_fetch_related: externalId/id, depth ---
    let network = poll_until(
        || async {
            client
                .call_tool("resource_fetch_related", json!({"externalId": a, "depth": 2}))
                .await
        },
        |payload: &Value| nodes(payload).len() >= 3,
    )
    .await;
    let reached: BTreeSet<String> = nodes(&network)
        .iter()
        .map(|node| text_of(node, "externalId"))
        .collect();
    assert!(
        reached.contains(b) && reached.contains(c),
        "fetch_related did not reach both neighbours: {reached:?}"
    );
    assert!(
        network["edges"]
            .as_array()
            .map(|edges| edges.iter().any(|e| e["type"] == json!(relationship_type)))
            .unwrap_or(false),
        "fetch_related returned no edge of the type that was created"
    );

    let by_id_network = client
        .call_tool("resource_fetch_related", json!({"id": a_id, "depth": 1}))
        .await;
    assert!(
        nodes(&by_id_network)
            .iter()
            .any(|node| node["externalId"] == json!(b)),
        "fetch_related by numeric id did not reach the neighbour"
    );

    // --- resource_fetch_nearest: externalId/id, endLabels, limit, relationshipTypes, excludedLabels ---
    let nearest = poll_until(
        || async {
            client
                .call_tool(
                    "resource_fetch_nearest",
                    json!({"externalId": a, "endLabels": label, "limit": 5}),
                )
                .await
        },
        |payload: &Value| nodes(payload).iter().any(|node| node["externalId"] == json!(b)),
    )
    .await;
    assert!(
        nodes(&nearest).iter().any(|node| node["externalId"] == json!(b)),
        "fetch_nearest did not reach the labelled neighbour"
    );

    let nearest_by_id = client
        .call_tool(
            "resource_fetch_nearest",
            json!({"id": a_id, "endLabels": label, "limit": 5}),
        )
        .await;
    assert!(
        nodes(&nearest_by_id).iter().any(|node| node["externalId"] == json!(b)),
        "fetch_nearest by numeric id did not reach the neighbour"
    );

    // Restricting to the edge's own type keeps the neighbour; restricting to an unrelated type
    // must lose it, or the option is being ignored.
    let followed = client
        .call_tool(
            "resource_fetch_nearest",
            json!({
                "externalId": a,
                "endLabels": label,
                "limit": 5,
                "relationshipTypes": relationship_type,
            }),
        )
        .await;
    assert!(
        nodes(&followed).iter().any(|node| node["externalId"] == json!(b)),
        "restricting to the edge's own relationship type lost the neighbour"
    );
    let blocked = client
        .call_tool(
            "resource_fetch_nearest",
            json!({
                "externalId": a,
                "endLabels": label,
                "limit": 5,
                "relationshipTypes": "BELONGS_TO",
            }),
        )
        .await;
    assert!(
        !nodes(&blocked).iter().any(|node| node["externalId"] == json!(b)),
        "relationshipTypes did not restrict the traversal"
    );

    let excluded = client
        .call_tool(
            "resource_fetch_nearest",
            json!({
                "externalId": a,
                "endLabels": label,
                "limit": 5,
                "excludedLabels": label,
            }),
        )
        .await;
    assert!(
        !nodes(&excluded).iter().any(|node| node["externalId"] == json!(b)),
        "excludedLabels did not exclude the labelled node"
    );

    // --- edge_delete: id ---
    // An edge is separately deletable only when both endpoints stay reachable without it, hence the
    // triangle. The check reads the graph projection, which lags the write, so the poll above is what
    // makes this safe — deleting too early gets the wrong answer rather than an error.
    // b -> c is the safe one to drop: b stays reachable through a -> b, and c through a -> c.
    client
        .call_tool("edge_delete", json!({"id": id_of(&edge_bc)}))
        .await;
    assert!(
        items(&client.call_tool("edge_get", json!({"id": id_of(&edge_bc)})).await).is_empty(),
        "edge_delete left the edge in place"
    );
    assert!(
        !items(&client.call_tool("resource_get", json!({"externalIds": c})).await).is_empty(),
        "edge_delete took an endpoint resource with it"
    );

    // --- resource_delete: id ---
    // b now hangs off a single edge from a, so removing it cannot disconnect anything else: a and c
    // stay joined by a -> c.
    client.call_tool("resource_delete", json!({"id": b_id})).await;
    assert!(
        items(&client.call_tool("resource_get", json!({"ids": b_id.to_string()})).await).is_empty(),
        "resource_delete by numeric id left the resource"
    );

    // --- resource_update: id and every newX field ---
    let renamed = format!("{c}_r");
    let updated = client
        .call_tool(
            "resource_update",
            json!({
                "id": c_id,
                "newName": "MCP resource renamed",
                "newDescription": "MCP resource redescribed",
                "newExternalId": renamed,
                "newDataSetId": secondary_id,
            }),
        )
        .await;
    let updated_node = nodes(&updated)
        .first()
        .cloned()
        .expect("resource_update returned no node");
    assert_eq!(text_of(&updated_node, "name"), "MCP resource renamed");
    assert_eq!(text_of(&updated_node, "description"), "MCP resource redescribed");
    assert_eq!(text_of(&updated_node, "externalId"), renamed);
    assert_eq!(
        updated_node["dataSetId"].as_str().map(|s| s.parse::<i64>().unwrap()),
        Some(secondary_id),
        "newDataSetId did not move the resource"
    );

    // --- resource_delete: externalId ---
    client
        .call_tool("resource_delete", json!({"externalId": renamed}))
        .await;
    assert!(
        items(&client.call_tool("resource_get", json!({"externalIds": renamed})).await).is_empty(),
        "resource_delete by externalId left the resource"
    );

    if client
        .quietly("resource_delete", json!({"externalId": a}))
        .await
    {
        resource_guard.disarm();
    }

    // The secondary dataset can only go once the resource that moved into it has — `dataset_delete`
    // does not cascade, so this is the delete most likely to fail and leave a stray.
    if client
        .quietly("dataset_delete", json!({"externalId": secondary_external_id}))
        .await
    {
        secondary_guard.disarm();
    }
}

/// What an LLM hits when it guesses wrong: the tool must say so rather than half-succeed.
async fn sweep_failure_modes(client: &McpClient, dataset: &SweepDataset, label: &str) {
    // A bad dataSetId must fail loudly; a silent success would strand the series.
    let message = client
        .try_call_tool(
            "timeseries_create",
            json!({
                "externalId": unique_id("mcp_ts_orphan"),
                "name": "MCP orphan series",
                "dataSetId": 2_000_000_000i64,
                "unit": "Celsius",
            }),
        )
        .await
        .expect_err("creating a series in a nonexistent dataset succeeded");
    assert!(!message.trim().is_empty(), "the failure carried no explanation");

    // Neither `unit` nor `unitExternalId` must be refused, and the refusal has to name the way out —
    // this one is a good error, and regressing it back to a bare validation key would be a loss.
    let message = client
        .try_call_tool(
            "timeseries_create",
            json!({
                "externalId": unique_id("mcp_ts_nounit"),
                "name": "MCP unitless series",
                "dataSetId": dataset.id,
            }),
        )
        .await
        .expect_err("a series with no unit of any kind was accepted");
    assert!(
        message.contains("unit") && message.contains("unitExternalId"),
        "the missing-unit error does not name both ways to supply one: {message}"
    );

    let message = client
        .try_call_tool(
            "event_create",
            json!({
                "externalId": unique_id("mcp_ev_bad"),
                "type": "McpSweepAlarm",
                "dataSetId": dataset.id,
                "eventTime": "yesterday afternoon",
            }),
        )
        .await
        .expect_err("an unparseable eventTime was accepted");
    assert!(!message.trim().is_empty(), "the failure carried no explanation");

    // At least one label is required — the tool says so, so it must enforce it.
    let message = client
        .try_call_tool(
            "resource_create",
            json!({
                "externalId": unique_id("mcp_res_bad"),
                "name": "MCP unlabelled resource",
                "labels": "",
                "dataSetId": dataset.id,
            }),
        )
        .await
        .expect_err("a resource with no label was accepted");
    assert!(!message.trim().is_empty(), "the failure carried no explanation");

    // A well-formed call with a real label still has to work, so the above are not passing for an
    // unrelated reason.
    let external_id = unique_id("mcp_res_ok");
    let mut guard = cleanup_resources(vec![external_id.clone()]);
    client
        .call_tool(
            "resource_create",
            json!({
                "externalId": external_id,
                "name": "MCP control resource",
                "labels": label,
                "dataSetId": dataset.id,
            }),
        )
        .await;
    client
        .call_tool("resource_delete", json!({"externalId": external_id}))
        .await;
    guard.disarm();
}

/// Diff what the sweep sent against the live `tools/list` schema.
fn assert_every_field_was_exercised(client: &McpClient, tools: &BTreeMap<String, Value>) {
    let exercised = client.exercised.lock().unwrap();
    let mut gaps: Vec<String> = Vec::new();

    for (name, tool) in tools {
        let advertised: BTreeSet<String> = tool["inputSchema"]["properties"]
            .as_object()
            .map(|properties| properties.keys().cloned().collect())
            .unwrap_or_default();
        let sent = exercised.get(name).cloned().unwrap_or_default();
        let untested: Vec<String> = advertised.difference(&sent).cloned().collect();
        if !untested.is_empty() {
            gaps.push(format!("  {name}: {}", untested.join(", ")));
        }
    }

    assert!(
        gaps.is_empty(),
        "advertised tool parameters that the sweep never sent:\n{}",
        gaps.join("\n")
    );
}

// --------------------------------------------------------------------------- //
// Red on purpose — intended behaviour the api does not yet provide
// --------------------------------------------------------------------------- //

/// Renaming an event by UUID must leave it findable under its new `externalId`.
///
/// Identifying the same update by `externalId` reindexes correctly within about half a second; by
/// UUID it does not. `event_update` returns the new value, but the event stays reachable under the
/// *old* externalId and never under the new one, while `event_get` by UUID reports the new one — two
/// identifiers disagreeing about one row. A caller that renames and then looks the event up the
/// obvious way concludes it was deleted.
#[tokio::test]
async fn mcp_event_update_by_uuid_reindexes_the_external_id() {
    let client = McpClient::new();
    let dataset_external_id = unique_id("mcp_ds_rename");
    let mut dataset_guard = cleanup_datasets(vec![dataset_external_id.clone()]);
    let dataset = client
        .call_tool(
            "dataset_create",
            json!({"externalId": dataset_external_id, "name": "MCP rename dataset"}),
        )
        .await;
    let dataset_id = id_of(&items(&dataset)[0]);

    let external_id = unique_id("mcp_ev_rename");
    let renamed = format!("{external_id}_r");
    let mut event_guard = cleanup_events(vec![external_id.clone(), renamed.clone()]);
    let created = client
        .call_tool(
            "event_create",
            json!({
                "externalId": external_id,
                "type": "McpRenameAlarm",
                "dataSetId": dataset_id,
                "eventTime": EVENT_TIME,
            }),
        )
        .await;
    let uuid = text_of(&items(&created)[0], "id");
    poll_until(
        || async { client.call_tool("event_get", json!({"ids": [uuid]})).await },
        |payload: &Value| !items(payload).is_empty(),
    )
    .await;

    let acknowledged = client
        .call_tool("event_update", json!({"id": uuid, "newExternalId": renamed}))
        .await;
    assert_eq!(
        text_of(&items(&acknowledged)[0], "externalId"),
        renamed,
        "the update did not even claim to rename it"
    );

    let found = poll_until(
        || async {
            client
                .call_tool("event_get", json!({"externalIds": [renamed]}))
                .await
        },
        |payload: &Value| !items(payload).is_empty(),
    )
    .await;

    let stale = client
        .call_tool("event_get", json!({"externalIds": [external_id]}))
        .await;

    if client.quietly("event_delete", json!({"ids": [uuid]})).await {
        event_guard.disarm();
    }
    if client
        .quietly("dataset_delete", json!({"externalId": dataset_external_id}))
        .await
    {
        dataset_guard.disarm();
    }

    assert!(
        !items(&found).is_empty(),
        "renamed by UUID, but the new externalId resolves to nothing"
    );
    assert!(
        items(&stale).is_empty(),
        "the old externalId still resolves after the rename"
    );
}

/// `timeseries_create` must accept `unitExternalId` in place of `unit`, as it documents.
///
/// The `unit` parameter is described as "Required unless unitExternalId is given", and the tool's
/// missing-unit error points at both. This asserts the alternative actually resolves: passing only a
/// catalogue `unitExternalId` creates the series and fills `unit` in from the catalogue.
///
/// This was broken — `unitExternalId` alone was refused with the raw `unit: timeseries.unit.not.blank`
/// while the tool documented it as a way out — and has since been fixed. Kept as the regression guard,
/// with the unit taken from `unit_list` rather than hardcoded, because an externalId that is merely
/// absent from this tenant fails for an entirely different reason ("Unknown unit externalId").
#[tokio::test]
async fn mcp_timeseries_create_accepts_a_unit_external_id() {
    let client = McpClient::new();
    let tools = client.list_tools().await;
    let unit_parameter = &tools["timeseries_create"]["inputSchema"]["properties"]["unitExternalId"];
    assert!(
        !unit_parameter.is_null(),
        "timeseries_create no longer advertises unitExternalId; this test should be revisited"
    );

    let catalogue = items(&client.call_tool("unit_list", json!({"limit": 1})).await);
    let unit_external_id = text_of(
        catalogue.first().expect("the backend has no units configured"),
        "externalId",
    );

    let dataset_external_id = unique_id("mcp_ds_unit");
    let mut dataset_guard = cleanup_datasets(vec![dataset_external_id.clone()]);
    let dataset = client
        .call_tool(
            "dataset_create",
            json!({"externalId": dataset_external_id, "name": "MCP unit-by-reference dataset"}),
        )
        .await;
    let dataset_id = id_of(&items(&dataset)[0]);

    let external_id = unique_id("mcp_ts_unitref");
    let mut series_guard = cleanup_timeseries(vec![external_id.clone()]);
    let outcome = client
        .try_call_tool(
            "timeseries_create",
            json!({
                "externalId": external_id,
                "name": "MCP unit-by-reference series",
                "dataSetId": dataset_id,
                "unitExternalId": unit_external_id,
            }),
        )
        .await;

    if client
        .quietly("timeseries_delete", json!({"externalId": external_id}))
        .await
    {
        series_guard.disarm();
    }
    if client
        .quietly("dataset_delete", json!({"externalId": dataset_external_id}))
        .await
    {
        dataset_guard.disarm();
    }

    let created = outcome.unwrap_or_else(|message| {
        panic!(
            "unitExternalId is documented as an alternative to unit, but supplying it alone failed: \
             {message}"
        )
    });
    assert_eq!(text_of(&items(&created)[0], "externalId"), external_id);
}
