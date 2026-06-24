#[cfg(test)]
mod tests {
    use crate::files::FileUpload;
    use crate::generic::{DataWrapper, INode, IdAndExtId};
    use crate::tests::cleanup::cleanup_files;

    use crate::{create_api_service, ApiService};

    #[tokio::test]
    async fn test_file_upload() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        // Delete uploaded files
        delete(&api_service).await;

        let mut upload_forms = vec![];

        let file_path = "resources/test/random_values.csv";
        let file_upload_form = FileUpload::new_with_destination_path(file_path, "/foo/bar");
        upload_forms.push(file_upload_form);

        let file_path = "resources/test/image.jpg";
        let mut file_upload_form = FileUpload::new_with_destination_path(file_path, "/images/");
        file_upload_form.set_file_name("sola.jpg".to_string());
        file_upload_form.set_external_id("image_sola_jpg".to_string());
        upload_forms.push(file_upload_form);

        let file_path = "resources/test/image2.jpg";
        let mut file_upload_form =
            FileUpload::new_with_destination_path(file_path, "/images/insects");
        file_upload_form.set_file_name("fly.jpg".to_string());
        file_upload_form.set_external_id("image_fly_jpg".to_string());
        upload_forms.push(file_upload_form);

        let file_path = "resources/test/image3.jpg";
        let mut file_upload_form =
            FileUpload::new_with_destination_path(file_path, "/images/norway/");
        file_upload_form.set_file_name("teigland.jpg".to_string());
        file_upload_form.set_external_id("image_teigland_bomlo_jpg".to_string());
        upload_forms.push(file_upload_form);

        for f in upload_forms {
            do_file_upload(&api_service, f).await;
        }

        // Ensure uploaded files are cleaned up even if a later assertion panics.
        let mut file_cleanup = cleanup_files(vec![
            "image_sola_jpg".to_string(),
            "image_fly_jpg".to_string(),
            "image_teigland_bomlo_jpg".to_string(),
        ]);

        // Now test uploaded files
        let _ = api_service
            .files
            .list_directory_by_path("/images/")
            .await
            .is_ok_and(|res| test_uploaded_content(res));
        let _ = api_service
            .files
            .list_directory_by_path("/images")
            .await
            .is_ok_and(|res| test_uploaded_content(res));

        // Delete uploaded files
        delete(&api_service).await;
        file_cleanup.disarm();

        Ok(())
    }

    fn test_uploaded_content(res: DataWrapper<INode>) -> bool {
        assert_eq!(res.get_http_status_code().unwrap(), 200);
        for inode in res.get_items() {
            let node_type = inode.r#type.clone().unwrap().clone();
            let name = inode.name.clone();
            if node_type == "FILE" {
                assert_eq!(name, "sola.jpg");
            }
            if node_type == "FOLDER" && name == "insects" {
                let path = inode.path.clone();
                assert_eq!(path, "/images/insects");
            }
            if node_type == "FOLDER" && name == "norway" {
                let path = inode.path.clone();
                assert_eq!(path, "/images/norway");
            }
        }
        true
    }

    async fn do_file_upload(api_service: &ApiService, upload_form: FileUpload) {
        let result = api_service.files.upload_file(upload_form).await;
        let status = match result {
            Ok(res) => res.get_http_status_code().unwrap(),
            Err(err) => err.get_status().as_u16(),
        };
        assert_eq!(
            status, 200,
            "Unexpected status code: {}. Expected 200 (OK)",
            status
        );
    }

    #[tokio::test]
    async fn list_folders() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        let result = api_service.files.list_root_directory().await;
        match result {
            Ok(response) => {
                assert_eq!(response.get_http_status_code().unwrap(), 200);
                println!("{:?}", response);
            } // Added comma
            Err(e) => {
                eprintln!("{:?}", e.message);
                panic!("List directory request failed.");
            }
        }
        let _ = api_service
            .files
            .list_directory_by_path("/")
            .await
            .is_ok_and(|res| {
                assert_eq!(res.get_http_status_code().unwrap(), 200);
                true
            });
        Ok(())
    }

    // Pure (no-backend) checks of how a FileUpload maps to the raw-PUT upload headers: the
    // destination path + filename become a percent-encoded X-Datahub-Path, and optional metadata
    // becomes the matching X-Datahub-* / Content-Type headers.
    #[test]
    fn upload_headers_encode_path_and_metadata() {
        use crate::files::FileUpload;
        use std::collections::HashMap;

        fn header<'a>(headers: &'a [(&str, String)], name: &str) -> Option<&'a str> {
            headers
                .iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| v.as_str())
        }

        // Build the struct directly so the test doesn't need a real file on disk.
        let mut upload = FileUpload {
            external_id: "image_sola_jpg".to_string(),
            file_path: "resources/test/image.jpg".to_string(),
            destination_path: Some("/images/".to_string()),
            name: "sola.jpg".to_string(),
            metadata: None,
            description: None,
            source: None,
            data_set_id: None,
            mime_type: Some("image/jpeg".to_string()),
            related_resources: None,
            source_date_created: None,
            source_last_updated: None,
        };

        let headers = upload.upload_headers();
        // Trailing slash on the folder collapses; path is one clean segment chain.
        assert_eq!(header(&headers, "X-Datahub-Path"), Some("/images/sola.jpg"));
        assert_eq!(
            header(&headers, "X-Datahub-External-Id"),
            Some("image_sola_jpg")
        );
        assert_eq!(header(&headers, "Content-Type"), Some("image/jpeg"));
        // Optional headers are absent until set.
        assert_eq!(header(&headers, "X-Datahub-Description"), None);
        assert_eq!(header(&headers, "X-Datahub-Dataset-Id"), None);

        // A folder without a leading slash, a filename and description with characters that must
        // be percent-encoded, plus a dataset id, source, source dates and related resources.
        upload.destination_path = Some("my docs/rapport".to_string());
        upload.name = "årsrapport (2024).pdf".to_string();
        upload.description = Some("Q4 / final".to_string());
        upload.data_set_id = Some(77);
        upload.source = Some("scada system".to_string());
        upload.metadata = Some(HashMap::from([("k".to_string(), "v".to_string())]));
        upload.related_resources = Some(vec![1, 2, 3]);
        // 2024-01-01T00:00:00Z and 2024-01-02T00:00:00Z.
        upload.source_date_created =
            Some(chrono::DateTime::from_timestamp_millis(1_704_067_200_000).unwrap());
        upload.source_last_updated =
            Some(chrono::DateTime::from_timestamp_millis(1_704_153_600_000).unwrap());

        let headers = upload.upload_headers();
        assert_eq!(
            header(&headers, "X-Datahub-Path"),
            // "/" prefix added, each segment encoded, real "/" separators kept literal.
            Some("/my%20docs/rapport/%C3%A5rsrapport%20%282024%29.pdf")
        );
        assert_eq!(header(&headers, "X-Datahub-Description"), Some("Q4%20%2F%20final"));
        assert_eq!(header(&headers, "X-Datahub-Dataset-Id"), Some("77"));
        assert_eq!(header(&headers, "X-Datahub-Source"), Some("scada%20system"));
        // metadata is percent-encoded JSON: {"k":"v"}.
        assert_eq!(
            header(&headers, "X-Datahub-Metadata"),
            Some("%7B%22k%22%3A%22v%22%7D")
        );
        // relatedResources is a percent-encoded JSON array: [1,2,3].
        assert_eq!(
            header(&headers, "X-Datahub-Related-Resources"),
            Some("%5B1%2C2%2C3%5D")
        );
        // Source dates are percent-encoded ISO-8601 (RFC 3339): 2024-01-01T00:00:00+00:00.
        assert_eq!(
            header(&headers, "X-Datahub-Source-Date-Created"),
            Some("2024-01-01T00%3A00%3A00%2B00%3A00")
        );
        assert_eq!(
            header(&headers, "X-Datahub-Source-Last-Updated"),
            Some("2024-01-02T00%3A00%3A00%2B00%3A00")
        );
    }

    // Covers the epoch-millis form of the source-date headers. The SDK emits ISO-8601 (asserted in
    // `upload_headers_encode_path_and_metadata`, and exercised end-to-end by `test_file_upload`,
    // whose FileUploads carry the files' filesystem timestamps), but the backend also accepts epoch
    // milliseconds for `X-Datahub-Source-Date-Created` / `X-Datahub-Source-Last-Updated`. Here we
    // drive an upload with those headers as epoch millis directly through the HTTP layer and confirm
    // the dates round-trip on the returned INode.
    #[tokio::test]
    async fn test_file_upload_source_dates_epoch_millis() -> Result<(), Box<dyn std::error::Error>> {
        use crate::files::FileUpload;
        use crate::generic::ApiServiceProvider;
        use chrono::{DateTime, Utc};

        let api_service = create_api_service();
        let base_url = std::env::var("BASE_URL").expect("BASE_URL must be set in .env");
        let url = format!("{}/files", base_url);

        let ext_id = "dates_epoch_jpg";
        let cleanup = DataWrapper::from_vec(vec![
            IdAndExtId::from_external_id(ext_id),
            IdAndExtId::from_external_id("datahub_folder_dates"),
        ]);
        let _ = api_service.files.delete(&cleanup).await;

        // 2024-01-01T00:00:00Z and 2024-01-02T00:00:00Z, expressed as epoch milliseconds.
        let created_millis: i64 = 1_704_067_200_000;
        let updated_millis: i64 = 1_704_153_600_000;

        let upload = FileUpload::new_with_destination_path("resources/test/image.jpg", "/dates");
        let body = upload.get_body().await;
        let headers: Vec<(&str, String)> = vec![
            ("X-Datahub-Path", "/dates/epoch.jpg".to_string()),
            ("X-Datahub-External-Id", ext_id.to_string()),
            ("X-Datahub-Source-Date-Created", created_millis.to_string()),
            ("X-Datahub-Source-Last-Updated", updated_millis.to_string()),
            ("Content-Type", "image/jpeg".to_string()),
        ];

        let result: DataWrapper<INode> = api_service
            .files
            .execute_file_upload_request(url.as_str(), body, headers)
            .await?;

        assert_eq!(
            result.get_http_status_code().unwrap(),
            200,
            "epoch-millis upload should succeed"
        );
        let items = result.get_items();
        let node = &items[0];
        assert_eq!(
            node.source_date_created,
            DateTime::<Utc>::from_timestamp_millis(created_millis)
        );
        assert_eq!(
            node.source_last_updated,
            DateTime::<Utc>::from_timestamp_millis(updated_millis)
        );

        let _ = api_service.files.delete(&cleanup).await;
        Ok(())
    }

    async fn delete(api_service: &ApiService) {
        let id_collection = DataWrapper::from_vec(vec![
            IdAndExtId::from_external_id("datahub_folder_foo"),
            IdAndExtId::from_external_id("datahub_folder_bar"),
            IdAndExtId::from_external_id("random_values_csv"),
            IdAndExtId::from_external_id("datahub_folder_images"),
            IdAndExtId::from_external_id("image_sola_jpg"),
            IdAndExtId::from_external_id("datahub_folder_insects"),
            IdAndExtId::from_external_id("image_fly_jpg"),
            IdAndExtId::from_external_id("datahub_folder_norway"),
            IdAndExtId::from_external_id("image_teigland_bomlo_jpg"),
        ]);
        println!("{:?}", id_collection);
        let result = api_service.files.delete(&id_collection).await;
        match result {
            Ok(response) => {
                assert_eq!(response.get_http_status_code().unwrap(), 204);
                println!("{:?}", response);
            } // Added comma
            Err(e) => {
                eprintln!("{:?}", e.message);
                panic!("Delete request failed.");
            }
        }
    }
}
