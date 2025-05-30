mod test;

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Weak;
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
        self.execute_file_upload_request("", multipart_form).await
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FileUpload {
    #[serde(rename = "externalId")]
    pub external_id: String,
    pub path: String,
    pub name: String,
    pub metadata: Option<HashMap<String, String>>,
    pub description: Option<String>,
    pub source: Option<String>,
    #[serde(rename = "dataSetId")]
    pub data_set_id: Option<u64>,
    #[serde(rename = "mimeType")]
    pub mime_type: Option<String>,
    pub related_resources: Option<Vec<u64>>,
}

impl FileUpload {
    pub fn new(file_path: &str) -> Self {
        let metadata = fs::metadata(file_path)
            .unwrap_or_else(|e| {
                panic!("Failed to get metadata for file '{}': {}", file_path, e);
            });

        if !metadata.is_file() {
            panic!("Path '{}' is not a regular file.", file_path);
        }

        let path_obj = Path::new(file_path);

        let file_name = std::path::Path::new(path_obj)
            .file_name()
            .and_then(|name| name.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| panic!("Could not get file name from path: {:?}", file_path));

        let folder_path = path_obj
            .parent() // Get the parent directory (returns an Option<&Path>)
            .and_then(|p| p.to_str()) // Convert the Path to a &str (returns Option<&str>)
            .map(|s| s.to_string()) // Convert &str to String
            .unwrap_or_else(|| {
                // This case handles paths like "file.txt" (no parent directory)
                // or if the path is invalid for parent()
                panic!("Could not extract folder path from: {:?}", file_path);
            });

        let kind: Option<String> = match infer::get_from_path(file_path) {
            Ok(Some(file_type)) => Some(file_type.mime_type().to_string()),
            Ok(None) => {
                println!("Could not determine file type for: {}", file_path);
                None
            },
            Err(e) => {
                eprintln!("Error detecting file type for {}: {}", file_path, e);
                None
            }
        };

        Self {
            external_id: to_snake_lower_cased_allow_start_with_digits(file_name.as_str()),
            path: folder_path,
            name: file_name,
            metadata: None,
            description: None,
            source: None,
            data_set_id: None,
            mime_type: kind,
            related_resources: None,
        }
    }

    pub async fn get_form(&self) -> Form {
        let full_path = PathBuf::from(&self.path).join(&self.name);

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

        let mut form = Form::new().part("file", file_part);
        form = form.text("path", "/foo/bar");
        if let Some(source) = &self.source {
            form = form.text("source", source.clone());
        }
        if let Some(description) = &self.description {
            form = form.text("description", description.clone());
        }
        form
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