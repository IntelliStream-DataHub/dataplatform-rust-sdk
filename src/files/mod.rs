mod test;

use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
use crate::events::Event;
use crate::generic::{ApiServiceProvider, DataWrapper, INode, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use chrono::{DateTime, Utc};
use reqwest::Body;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Weak;
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};

pub struct FileService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl FileService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let base_url = format!("{}/files", base_url);
        FileService {
            api_service,
            base_url,
        }
    }

    pub async fn upload_file(
        &self,
        file_upload: FileUpload,
    ) -> Result<DataWrapper<INode>, ResponseError> {
        // The backend takes the file content as the raw PUT body; all metadata travels in
        // `X-Datahub-*` headers (see `FileController.upload`).
        let body = file_upload.get_body().await;
        let headers = file_upload.upload_headers();
        self.execute_file_upload_request(self.base_url.as_str(), body, headers)
            .await
    }

    pub async fn list_root_directory(&self) -> Result<DataWrapper<INode>, ResponseError> {
        // Create and send an HTTP GET request
        let full_path = format!("{}/list", self.base_url.as_str());
        self.execute_get_request(full_path.as_str(), None::<&str>)
            .await
    }

    pub async fn list_directory_by_path(
        &self,
        path: &str,
    ) -> Result<DataWrapper<INode>, ResponseError> {
        let full_path = format!("{}/list{}", self.base_url.as_str(), path);
        self.execute_get_request(full_path.as_str(), None::<&str>)
            .await
    }

    pub async fn delete(
        &self,
        id_collection: &DataWrapper<IdAndExtId>,
    ) -> Result<DataWrapper<Event>, ResponseError> {
        let full_path = format!("{}/delete", self.base_url.as_str());
        self.execute_post_request(full_path.as_str(), id_collection)
            .await
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileUpload {
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub file_path: String,
    pub destination_path: Option<String>,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    pub source: Option<String>,
    #[serde(rename = "dataSetId")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "mimeType")]
    pub mime_type: Option<String>,
    pub related_resources: Option<Vec<u64>>,
    #[serde(rename = "sourceDateCreated")]
    pub source_date_created: Option<DateTime<Utc>>,
    #[serde(rename = "sourceLastUpdated")]
    pub source_last_updated: Option<DateTime<Utc>>,
}

impl FileUpload {
    pub fn new_with_destination_path(file_path: &str, destination_path: &str) -> Self {
        let mut f = Self::new(file_path);
        f.set_destination_path(destination_path.to_string());
        f
    }

    pub fn new(file_path: &str) -> Self {
        let metadata = fs::metadata(file_path).unwrap_or_else(|e| {
            panic!("Failed to get metadata for file '{}': {}", file_path, e);
        });

        if !metadata.is_file() {
            panic!("Path '{}' is not a regular file.", file_path);
        }

        let mut source_date_created: Option<DateTime<Utc>> = None;
        if let Ok(created) = metadata.created() {
            let datetime_utc: DateTime<Utc> = created.into();
            source_date_created = Some(datetime_utc);
        }
        let mut source_last_updated: Option<DateTime<Utc>> = None;
        if let Ok(modified) = metadata.modified() {
            let datetime_utc: DateTime<Utc> = modified.into();
            source_last_updated = Some(datetime_utc);
        }

        let path_obj = Path::new(file_path);

        let file_name = std::path::Path::new(path_obj)
            .file_name()
            .and_then(|name| name.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| panic!("Could not get file name from path: {:?}", file_path));

        let kind: Option<String> = match infer::get_from_path(file_path) {
            Ok(Some(file_type)) => Some(file_type.mime_type().to_string()),
            Ok(None) => {
                println!("Could not determine file type for: {}", file_path);
                Some("application/octet-stream".to_string())
            }
            Err(e) => {
                eprintln!("Error detecting file type for {}: {}", file_path, e);
                None
            }
        };

        Self {
            external_id: to_snake_lower_cased_allow_start_with_digits(file_name.as_str()),
            file_path: file_path.to_string(),
            destination_path: None,
            name: file_name,
            metadata: None,
            description: None,
            source: None,
            data_set_id: None,
            mime_type: kind,
            related_resources: None,
            source_date_created,
            source_last_updated,
        }
    }

    /// Opens the file and returns its contents as a streaming request body. The file content is
    /// the raw PUT body of the new `/files` upload endpoint.
    pub async fn get_body(&self) -> Body {
        let file = File::open(&self.file_path).await.unwrap_or_else(|e| {
            panic!("Failed to open file '{}': {}", self.file_path, e);
        });
        let stream = FramedRead::new(file, BytesCodec::new());
        Body::wrap_stream(stream)
    }

    /// Builds the `X-Datahub-*` and `Content-Type` headers the upload endpoint reads before it
    /// touches the body. Every value is percent-encoded the way the server decodes it (the path
    /// segment-by-segment, everything else with `URLDecoder.decode` — including the external id,
    /// which the server then slug-sanitizes). `metadata` and `relatedResources` go as
    /// percent-encoded JSON, and the two source dates as percent-encoded ISO-8601 (RFC 3339). An
    /// omitted/octet-stream content type makes the server auto-detect the MIME type.
    pub fn upload_headers(&self) -> Vec<(&'static str, String)> {
        let mut headers = vec![
            ("X-Datahub-Path", self.encoded_full_path()),
            (
                "X-Datahub-External-Id",
                encode_uri_component(&self.external_id),
            ),
        ];
        if let Some(description) = &self.description {
            headers.push(("X-Datahub-Description", encode_uri_component(description)));
        }
        if let Some(data_set_id) = &self.data_set_id {
            headers.push(("X-Datahub-Dataset-Id", data_set_id.to_string()));
        }
        if let Some(source) = &self.source {
            headers.push(("X-Datahub-Source", encode_uri_component(source)));
        }
        if let Some(created) = &self.source_date_created {
            headers.push((
                "X-Datahub-Source-Date-Created",
                encode_uri_component(&created.to_rfc3339()),
            ));
        }
        if let Some(updated) = &self.source_last_updated {
            headers.push((
                "X-Datahub-Source-Last-Updated",
                encode_uri_component(&updated.to_rfc3339()),
            ));
        }
        if let Some(metadata) = &self.metadata {
            // The server expects a JSON object; percent-encode it so the braces/quotes survive
            // the header and its URLDecoder.decode round-trips back to valid JSON.
            let json = serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_string());
            headers.push(("X-Datahub-Metadata", encode_uri_component(&json)));
        }
        if let Some(related_resources) = &self.related_resources {
            // The server expects a JSON array of ids.
            let json = serde_json::to_string(related_resources).unwrap_or_else(|_| "[]".to_string());
            headers.push(("X-Datahub-Related-Resources", encode_uri_component(&json)));
        }
        let content_type = self
            .mime_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string());
        headers.push(("Content-Type", content_type));
        headers
    }

    /// The full destination path (folder + filename) the server splits on its last `/`. Defaults
    /// the folder to the root when no destination path was set.
    fn full_path(&self) -> String {
        let folder = self
            .destination_path
            .as_deref()
            .unwrap_or("/")
            .trim_end_matches('/');
        if folder.is_empty() {
            format!("/{}", self.name)
        } else if folder.starts_with('/') {
            format!("{}/{}", folder, self.name)
        } else {
            format!("/{}/{}", folder, self.name)
        }
    }

    /// Percent-encodes each `/`-separated segment of [`full_path`](Self::full_path) so non-ASCII
    /// characters and spaces survive the header while the path separators stay literal — matching
    /// the server's per-segment `URLDecoder.decode`.
    fn encoded_full_path(&self) -> String {
        self.full_path()
            .split('/')
            .map(encode_uri_component)
            .collect::<Vec<_>>()
            .join("/")
    }

    pub fn set_external_id(&mut self, external_id: String) {
        self.external_id = external_id;
    }

    pub fn set_file_name(&mut self, file_name: String) {
        self.name = file_name;
    }

    pub fn set_destination_path(&mut self, destination_path: String) {
        self.destination_path = Some(destination_path);
    }

    pub fn set_metadata(&mut self, metadata: HashMap<String, String>) {
        self.metadata = Some(metadata);
    }

    pub fn set_description(&mut self, description: String) {
        self.description = Some(description);
    }

    pub fn set_source(&mut self, source: String) {
        self.source = Some(source);
    }

    pub fn set_data_set_id(&mut self, data_set_id: u64) {
        self.data_set_id = Some(data_set_id);
    }

    pub fn set_mime_type(&mut self, mime_type: String) {
        self.mime_type = Some(mime_type);
    }
}

/// Percent-encodes a string the way JavaScript's `encodeURIComponent` does for the unreserved
/// set, emitting `%XX` (uppercase, UTF-8 bytes) for everything outside `[A-Za-z0-9-_.~]`. The
/// server decodes these header values with `URLDecoder.decode`, which round-trips this encoding.
fn encode_uri_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &byte in s.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => out.push_str(&format!("%{:02X}", byte)),
        }
    }
    out
}
