mod test;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Weak;
use chrono::{DateTime, Utc};
use reqwest::{Body};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use tokio_util::codec::{BytesCodec, FramedRead};
use crate::ApiService;
use crate::generic::{ApiServiceProvider, DataWrapper};
use crate::http::{ResponseError};
use crate::datahub::to_snake_lower_cased_allow_start_with_digits;

pub struct FileService<'a>{
    pub(crate) api_service: Weak<ApiService<'a>>,
    base_url: String
}

impl<'a> FileService<'a> {
    pub fn new(api_service: Weak<ApiService<'a>>, base_url: &String) -> Self {
        let base_url = format!("{}/files", base_url);
        FileService { api_service, base_url }
    }
    
    pub async fn upload_file(&self, file_upload: FileUpload) -> Result<DataWrapper<FileUpload>, ResponseError> {
        let multipart_form = file_upload.get_form().await;
        // Create and send an HTTP PUT request
        self.execute_file_upload_request(self.base_url.as_str(), multipart_form).await
    }

    pub async fn list_directory(&self, path: &str) -> Result<DataWrapper<String>, ResponseError> {
        // Create and send an HTTP GET request
        let full_path = format!("{}/list{}", self.base_url.as_str(), path);
        self.execute_get_request(full_path.as_str()).await
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileUpload {
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub file_path: String,
    pub path: Option<String>,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    pub source: Option<String>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "mimeType")]
    pub mime_type: Option<String>,
    pub related_resources: Option<Vec<u64>>,
    #[serde(rename = "sourceDateCreated")]   
    pub source_date_created: Option<DateTime<Utc>>,
    #[serde(rename = "sourceLastUpdated")]
    pub source_last_updated: Option<DateTime<Utc>>
}

impl FileUpload {

    pub fn new_with_destination_path(file_path: &str, destination_path: &str) -> Self {
        let mut f = Self::new(file_path);
        f.set_destination_path(destination_path.to_string());
        f
    }
    
    pub fn new(file_path: &str) -> Self {
        let metadata = fs::metadata(file_path)
            .unwrap_or_else(|e| {
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
            },
            Err(e) => {
                eprintln!("Error detecting file type for {}: {}", file_path, e);
                None
            }
        };

        Self {
            external_id: to_snake_lower_cased_allow_start_with_digits(file_name.as_str()),
            file_path: file_path.to_string(),
            path: None,
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

    pub async fn get_form(&self) -> Form {
        let full_path = PathBuf::from(&self.file_path);
        let file = File::open(&full_path).await.unwrap_or_else(|e| {
            panic!("Failed to open file '{}': {}", full_path.display(), e);
        });

        let stream = FramedRead::new(file, BytesCodec::new());
        let file_part = Part::stream(Body::wrap_stream(stream))
            .file_name(self.name.clone())
            .mime_str("application/octet-stream")
            .unwrap_or_else(|e| { // Handle error for mime_str as well
                panic!("Failed to set MIME type or filename for file part: {}", e);
            });

        let mut form = Form::new();
        form = form.text("externalId", self.external_id.clone());
        if let Some(source) = &self.source {
            form = form.text("source", source.clone());
        }
        if let Some(path) = &self.path {
            form = form.text("path", path.clone());
        }
        if let Some(description) = &self.description {
            form = form.text("description", description.clone());
        }
        if let Some(data_set_id) = &self.data_set_id {
            form = form.text("dataSetId", data_set_id.to_string());
        }
        if let Some(mime_type) = &self.mime_type {
            form = form.text("mimeType", mime_type.clone());
        }

        if let Some(source_date_created) = &self.source_date_created {
            form = form.text("sourceDateCreated", source_date_created.to_rfc3339());
        }
        if let Some(source_last_updated) = &self.source_last_updated {
            form = form.text("sourceLastUpdated", source_last_updated.to_rfc3339());
        }
        
        if let Some(metadata) = &self.metadata {
            // Serialize metadata to JSON string
            form = form.text("metadata", serde_json::to_string(metadata).unwrap_or_default());
        }
        if let Some(related_resources) = &self.related_resources {
            let resources_str: Vec<String> = related_resources.iter().map(|&id| id.to_string()).collect();
            form = form.text("relatedResources", resources_str.join(","));
        }

        // Add the file part last
        form = form.part("file", file_part);
        
        form
    }
    
    pub fn set_external_id(&mut self, external_id: String) {
        self.external_id = external_id;   
    }

    pub fn set_file_name(&mut self, file_name: String) {
        self.name = file_name;
    }

    pub fn set_destination_path(&mut self, destination_path: String) {
        self.path = Some(destination_path);
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