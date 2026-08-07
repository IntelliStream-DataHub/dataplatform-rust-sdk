mod test;

use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
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
use tokio::io::AsyncWriteExt;
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
    ) -> Result<DataWrapper<INode>, ResponseError> {
        let full_path = format!("{}/delete", self.base_url.as_str());
        self.execute_post_request(full_path.as_str(), id_collection)
            .await
    }

    /// `GET /files?id=` — metadata for a single file or folder.
    ///
    /// A node the caller cannot read answers 404 rather than 403, so a hidden file's existence
    /// isn't leaked; both arrive here as a [`ResponseError`] with that status.
    pub async fn get_by_id(&self, id: u64) -> Result<DataWrapper<INode>, ResponseError> {
        self.execute_get_request(self.base_url.as_str(), Some(&[("id", id.to_string())]))
            .await
    }

    /// `GET /files?externalId=` — metadata for a single file or folder.
    ///
    /// See [`get_by_id`](Self::get_by_id) for how an unreadable node is reported.
    pub async fn get_by_external_id(
        &self,
        external_id: &str,
    ) -> Result<DataWrapper<INode>, ResponseError> {
        self.execute_get_request(
            self.base_url.as_str(),
            Some(&[("externalId", external_id.to_string())]),
        )
        .await
    }

    /// `GET /files/search?q=` — case-insensitive full-text search over file and folder names and
    /// descriptions, across the whole tree, narrowed to the caller's readable datasets.
    ///
    /// A blank query is answered with an empty item list rather than an error.
    pub async fn search(&self, query: &str) -> Result<DataWrapper<INode>, ResponseError> {
        let full_path = format!("{}/search", self.base_url.as_str());
        self.execute_get_request(full_path.as_str(), Some(&[("q", query.to_string())]))
            .await
    }

    /// `GET /files/trash` — the soft-deleted files the caller can read.
    ///
    /// Folders are never listed: only files are soft-deleted. `name` and `path` are the
    /// pre-deletion values, while the `external_id` has been rewritten to
    /// `DELETED_<checksum>_<id>_<epochMillis>`. Use the `id` to [`restore`](Self::restore) —
    /// see there for why the rewritten external id does not round-trip.
    pub async fn list_trash(&self) -> Result<DataWrapper<INode>, ResponseError> {
        let full_path = format!("{}/trash", self.base_url.as_str());
        self.execute_get_request(full_path.as_str(), None::<&str>)
            .await
    }

    /// `POST /files/restore` — move soft-deleted files out of the trash back to their original
    /// location.
    ///
    /// **Identify each file by numeric id.** The external-id route does not currently work for
    /// trashed files: the server hashes the supplied id through `ExternalIds.hash`, which
    /// lowercases, while the stored hash for a `DELETED_<checksum>_<id>_<epochMillis>` id was not
    /// lowercased — so the lookup misses and the call answers 404. That is a server-side bug; the
    /// id route sidesteps the hash entirely.
    ///
    /// The call never overwrites: if a file's original path or external id is taken, or its
    /// original folder is gone, the whole request is refused with 409 and nothing is restored.
    pub async fn restore(
        &self,
        id_collection: &DataWrapper<IdAndExtId>,
    ) -> Result<DataWrapper<INode>, ResponseError> {
        let full_path = format!("{}/restore", self.base_url.as_str());
        self.execute_post_request(full_path.as_str(), id_collection)
            .await
    }

    /// `POST /files/update` — partial update of one file or folder.
    ///
    /// Only the fields set on the [`FileUpdate`] are applied; everything left unset is untouched.
    /// See [`FileUpdate`] for what each field does.
    pub async fn update(&self, update: &FileUpdate) -> Result<DataWrapper<INode>, ResponseError> {
        let full_path = format!("{}/update", self.base_url.as_str());
        self.execute_post_request(full_path.as_str(), update).await
    }

    /// `GET /files/download/{id}` — the file's content, in memory.
    ///
    /// Suitable for files that comfortably fit in RAM; for anything larger use
    /// [`download_to_path`](Self::download_to_path), which streams straight to disk.
    pub async fn download(&self, id: u64) -> Result<FileDownload, ResponseError> {
        let full_path = format!("{}/download/{}", self.base_url.as_str(), id);
        let response = self.execute_get_stream_request(full_path.as_str()).await?;

        let file_name = filename_from_content_disposition(&response);
        let mime_type = header_value(&response, reqwest::header::CONTENT_TYPE);
        let status = response.status();
        let bytes = response.bytes().await.map_err(|err| {
            eprintln!("Failed to read download body: {}", err);
            ResponseError {
                status,
                message: err.to_string(),
            }
        })?;

        Ok(FileDownload {
            file_name,
            mime_type,
            bytes: bytes.to_vec(),
        })
    }

    /// `GET /files/download/{id}`, streamed to `destination` without buffering the whole file in
    /// memory. Returns the number of bytes written.
    ///
    /// The destination is created if missing and truncated if it already exists.
    pub async fn download_to_path(
        &self,
        id: u64,
        destination: impl AsRef<Path>,
    ) -> Result<u64, ResponseError> {
        let full_path = format!("{}/download/{}", self.base_url.as_str(), id);
        let mut response = self.execute_get_stream_request(full_path.as_str()).await?;
        let status = response.status();

        let io_error = |err: std::io::Error| ResponseError {
            status,
            message: err.to_string(),
        };

        let mut file = File::create(destination.as_ref()).await.map_err(io_error)?;
        let mut written: u64 = 0;
        while let Some(chunk) = response.chunk().await.map_err(|err| {
            eprintln!("Download stream failed: {}", err);
            ResponseError {
                status,
                message: err.to_string(),
            }
        })? {
            file.write_all(&chunk).await.map_err(io_error)?;
            written += chunk.len() as u64;
        }
        file.flush().await.map_err(io_error)?;
        Ok(written)
    }
}

/// The response of [`FileService::download`]: the file content plus what the server said it is.
#[derive(Debug, Clone)]
pub struct FileDownload {
    /// The name from the `Content-Disposition` header, when the server sent a parseable one.
    pub file_name: Option<String>,
    /// The `Content-Type` header. The server falls back to `application/octet-stream` whenever the
    /// stored MIME type is missing or malformed, so this is rarely absent.
    pub mime_type: Option<String>,
    pub bytes: Vec<u8>,
}

/// The `filename="..."` of a `Content-Disposition` header, if there is one.
///
/// The server writes `attachment; filename="<name>"` with the name unencoded, so this only has to
/// handle the quoted form it actually produces.
fn filename_from_content_disposition(response: &reqwest::Response) -> Option<String> {
    let value = header_value(response, reqwest::header::CONTENT_DISPOSITION)?;
    filename_from_content_disposition_value(&value)
}

pub(crate) fn filename_from_content_disposition_value(value: &str) -> Option<String> {
    let start = value.find("filename=")? + "filename=".len();
    let rest = value[start..].trim();
    let name = match rest.strip_prefix('"') {
        Some(quoted) => quoted.split('"').next()?,
        None => rest.split(';').next()?.trim(),
    };
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn header_value(response: &reqwest::Response, name: reqwest::header::HeaderName) -> Option<String> {
    response
        .headers()
        .get(name)?
        .to_str()
        .ok()
        .map(|s| s.to_string())
}

/// A partial update for one file or folder (`POST /files/update`).
///
/// The node is identified by external id or numeric id — build with [`FileUpdate::by_external_id`]
/// or [`FileUpdate::by_id`]. Every other field is optional and only applied when set; an unset
/// field means "leave unchanged", so there is no way to clear a value back to null through this
/// endpoint.
///
/// The mutations are applied in a fixed order — dataset, then metadata-ish fields, then the
/// rename/move — so a failed move is the only step that can leave disk and database diverged.
#[derive(Debug, Default, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FileUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default, with = "crate::serde_helper::opt_string_id")]
    pub data_set_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_resources: Option<Vec<u64>>,
}

impl FileUpdate {
    /// Target the node with this external id.
    pub fn by_external_id(external_id: &str) -> Self {
        Self {
            external_id: Some(external_id.to_string()),
            ..Default::default()
        }
    }

    /// Target the node with this numeric id.
    pub fn by_id(id: u64) -> Self {
        Self {
            id: Some(id),
            ..Default::default()
        }
    }

    /// Rename the node, keeping it in its current folder.
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Move the node into this folder, creating the folder hierarchy if it doesn't exist. `"/"`
    /// moves it to the root.
    pub fn with_path(mut self, path: &str) -> Self {
        self.path = Some(path.to_string());
        self
    }

    /// Assign a dataset. On a folder this also fills the dataset in on every descendant that
    /// currently has none; already-governed subtrees are left alone.
    pub fn with_data_set_id(mut self, data_set_id: u64) -> Self {
        self.data_set_id = Some(data_set_id);
        self
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }

    pub fn with_source(mut self, source: &str) -> Self {
        self.source = Some(source.to_string());
        self
    }

    /// Replace the metadata map wholesale — this is not a merge.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Replace the set of related resource ids wholesale.
    pub fn with_related_resources(mut self, related_resources: Vec<u64>) -> Self {
        self.related_resources = Some(related_resources);
        self
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
