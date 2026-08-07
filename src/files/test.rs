#[cfg(test)]
mod tests {
    use crate::files::{FileUpdate, FileUpload};
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

    // Pure (no-backend) check of the `/files/update` body: camelCase names, string-encoded ids,
    // and — the point of the type — unset fields omitted entirely, since the server reads a
    // missing field as "leave unchanged" and an explicit null the same way.
    #[test]
    fn file_update_serializes_only_the_fields_that_were_set() {
        use crate::files::FileUpdate;
        use std::collections::HashMap;

        let minimal = FileUpdate::by_external_id("image_sola_jpg");
        let json = serde_json::to_value(&minimal).expect("FileUpdate should serialize");
        assert_eq!(
            json,
            serde_json::json!({ "externalId": "image_sola_jpg" }),
            "an untouched FileUpdate should carry nothing but the selector"
        );

        let full = FileUpdate::by_id(5677892)
            .with_name("renamed.jpg")
            .with_path("/images/norway")
            .with_data_set_id(12)
            .with_description("A description")
            .with_source("SAP")
            .with_metadata(HashMap::from([("vendor".to_string(), "acme".to_string())]))
            .with_related_resources(vec![34, 166]);
        let json = serde_json::to_value(&full).expect("FileUpdate should serialize");
        assert_eq!(
            json,
            serde_json::json!({
                "id": "5677892",
                "name": "renamed.jpg",
                "path": "/images/norway",
                "dataSetId": "12",
                "description": "A description",
                "source": "SAP",
                "metadata": { "vendor": "acme" },
                "relatedResources": [34, 166],
            })
        );
    }

    // Pure check of the Content-Disposition parsing behind `FileDownload::file_name`. The server
    // writes the quoted form with the name unencoded; the unquoted and absent cases are covered so
    // a header shape change degrades to `None` rather than a wrong name.
    #[test]
    fn download_filename_is_read_from_content_disposition() {
        use crate::files::filename_from_content_disposition_value;

        assert_eq!(
            filename_from_content_disposition_value("attachment; filename=\"sola.jpg\""),
            Some("sola.jpg".to_string())
        );
        assert_eq!(
            filename_from_content_disposition_value("attachment; filename=sola.jpg"),
            Some("sola.jpg".to_string())
        );
        assert_eq!(
            filename_from_content_disposition_value("attachment; filename=\"a b (1).jpg\""),
            Some("a b (1).jpg".to_string())
        );
        assert_eq!(
            filename_from_content_disposition_value("attachment; filename=\"\""),
            None
        );
        assert_eq!(filename_from_content_disposition_value("attachment"), None);
    }

    /// Upload one file, then exercise every read/mutate endpoint against it: fetch by id and by
    /// external id, search, update (rename + move + description), download both ways, then delete,
    /// find it in the trash and restore it.
    #[tokio::test]
    async fn file_lifecycle_get_search_update_download_trash_restore(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();
        let ext_id = "lifecycle_sola_jpg";

        // Start from a clean slate; the file may be left over from a failed run.
        let _ = api_service
            .files
            .delete(&DataWrapper::from_vec(vec![IdAndExtId::from_external_id(
                ext_id,
            )]))
            .await;

        let mut upload =
            FileUpload::new_with_destination_path("resources/test/image.jpg", "/lifecycle");
        upload.set_file_name("sola.jpg".to_string());
        upload.set_external_id(ext_id.to_string());
        let source_bytes = std::fs::read("resources/test/image.jpg")?;

        let uploaded = api_service.files.upload_file(upload).await?;
        assert_eq!(uploaded.get_http_status_code().unwrap(), 200);
        let mut file_cleanup = cleanup_files(vec![ext_id.to_string()]);
        let id = uploaded.get_items()[0]
            .id
            .expect("upload should return the new node's id");

        // --- get by id / external id ---
        let by_id = api_service.files.get_by_id(id).await?;
        assert_eq!(by_id.get_http_status_code().unwrap(), 200);
        assert_eq!(by_id.get_items()[0].external_id, ext_id);

        let by_ext = api_service.files.get_by_external_id(ext_id).await?;
        assert_eq!(by_ext.get_items()[0].id, Some(id));

        // --- search ---
        let found = api_service.files.search("sola").await?;
        assert_eq!(found.get_http_status_code().unwrap(), 200);
        assert!(
            found.get_items().iter().any(|n| n.external_id == ext_id),
            "search for 'sola' should surface the uploaded file"
        );

        // A blank query is answered with an empty list, not an error.
        let empty = api_service.files.search("").await?;
        assert_eq!(empty.get_http_status_code().unwrap(), 200);

        // --- download, in memory and streamed to disk ---
        let downloaded = api_service.files.download(id).await?;
        assert_eq!(
            downloaded.bytes, source_bytes,
            "downloaded content should be byte-identical to what was uploaded"
        );
        assert_eq!(downloaded.file_name.as_deref(), Some("sola.jpg"));

        let dest = std::env::temp_dir().join("datahub_sdk_lifecycle_download.jpg");
        let written = api_service.files.download_to_path(id, &dest).await?;
        assert_eq!(written as usize, source_bytes.len());
        assert_eq!(std::fs::read(&dest)?, source_bytes);
        let _ = std::fs::remove_file(&dest);

        // --- update: rename, move and set a description in one call ---
        let update = FileUpdate::by_external_id(ext_id)
            .with_name("renamed.jpg")
            .with_path("/lifecycle/moved")
            .with_description("Updated by the SDK test suite");
        let updated = api_service.files.update(&update).await?;
        assert_eq!(updated.get_http_status_code().unwrap(), 200);
        let node = &updated.get_items()[0];
        assert_eq!(node.name, "renamed.jpg");
        // Only the folder prefix is asserted, not the whole path: the server's IndexNode
        // transformer appends the name to a FILE's path, which already ends in that name, so every
        // file comes back with a doubled tail (`/lifecycle/moved/renamed.jpg/renamed.jpg`). See
        // FileTransformer.transform in the backend. Asserting the prefix survives the fix.
        assert!(
            node.path.starts_with("/lifecycle/moved/"),
            "expected the file to have moved under /lifecycle/moved/, got {}",
            node.path
        );
        assert_eq!(
            node.description.as_deref(),
            Some("Updated by the SDK test suite")
        );

        // --- delete, then find it in the trash and restore it ---
        let deleted = api_service
            .files
            .delete(&DataWrapper::from_vec(vec![IdAndExtId::from_external_id(
                ext_id,
            )]))
            .await?;
        assert_eq!(deleted.get_http_status_code().unwrap(), 204);

        let trash = api_service.files.list_trash().await?;
        assert_eq!(trash.get_http_status_code().unwrap(), 200);
        let trashed = trash
            .get_items()
            .into_iter()
            .find(|n| n.id == Some(id))
            .expect("the deleted file should be in the trash");
        // The trashed external id is rewritten to DELETED_<checksum>_<id>_<epochMillis>.
        assert!(
            trashed.external_id.starts_with("DELETED_"),
            "expected a trashed externalId, got {}",
            trashed.external_id
        );

        // Restore by numeric id. The external-id route does not currently work for trashed files:
        // the server hashes the supplied id through ExternalIds.hash, which lowercases, while the
        // stored hash for a `DELETED_...` id was not lowercased — so the lookup misses and the
        // call 404s. Numeric id sidesteps the hash entirely.
        let restored = api_service
            .files
            .restore(&DataWrapper::from_vec(vec![IdAndExtId::from_id(id)]))
            .await?;
        assert_eq!(restored.get_http_status_code().unwrap(), 200);
        assert_eq!(restored.get_items()[0].id, Some(id));

        // Restored under its original external id, so the guard can clean it up.
        let after = api_service.files.get_by_id(id).await?;
        assert_eq!(after.get_items()[0].external_id, ext_id);

        let _ = api_service
            .files
            .delete(&DataWrapper::from_vec(vec![
                IdAndExtId::from_external_id(ext_id),
                IdAndExtId::from_external_id("datahub_folder_moved"),
                IdAndExtId::from_external_id("datahub_folder_lifecycle"),
            ]))
            .await;
        file_cleanup.disarm();

        Ok(())
    }

}
