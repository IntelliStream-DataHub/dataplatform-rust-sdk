#[cfg(test)]
mod tests {
    use crate::{create_api_service, ApiService};
    use crate::files::FileUpload;
    use crate::generic::{DataWrapper, INode, IdAndExtIdCollection};

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
        let mut file_upload_form = FileUpload::new_with_destination_path(file_path, "/images/insects");
        file_upload_form.set_file_name("fly.jpg".to_string());
        file_upload_form.set_external_id("image_fly_jpg".to_string());
        upload_forms.push(file_upload_form);

        let file_path = "resources/test/image3.jpg";
        let mut file_upload_form = FileUpload::new_with_destination_path(file_path, "/images/norway/");
        file_upload_form.set_file_name("teigland.jpg".to_string());
        file_upload_form.set_external_id("image_teigland_bomlo_jpg".to_string());
        upload_forms.push(file_upload_form);
        
        for f in upload_forms {
            do_file_upload( &api_service, f ).await;
        }

        // Now test uploaded files
        let _ = api_service.files.list_directory_by_path("/images/").await.is_ok_and(| res| {
            test_uploaded_content(res)
        });
        let _ = api_service.files.list_directory_by_path("/images").await.is_ok_and(| res| {
            test_uploaded_content(res)
        });
        
        // Delete uploaded files
        delete(&api_service).await;

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
        assert_eq!(status, 200, "Unexpected status code: {}. Expected 200 (OK)", status);
    }

    #[tokio::test]
    async fn list_folders() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();
        
        let result = api_service.files.list_root_directory().await;
        match result {
            Ok(response) => {
                assert_eq!(response.get_http_status_code().unwrap(), 200);
                println!("{:?}", response);
            }, // Added comma
            Err(e) => {
                eprintln!("{:?}", e.message);
                panic!("List directory request failed.");
            }
        }
        let _ = api_service.files.list_directory_by_path("/").await.is_ok_and(|res| {
            assert_eq!(res.get_http_status_code().unwrap(), 200);
            true
        });
        Ok(())
    }

    async fn delete(api_service: &ApiService) {

        let id_collection = IdAndExtIdCollection::from_external_id_vec(
            vec![
                "datahub_folder_foo",
                "datahub_folder_bar",
                "random_values_csv",
                "datahub_folder_images",
                "image_sola_jpg",
                "datahub_folder_insects",
                "image_fly_jpg",
                "datahub_folder_norway",
                "image_teigland_bomlo_jpg",
            ]
        );
        println!("{:?}", id_collection);
        let result = api_service.files.delete(&id_collection).await;
        match result {
            Ok(response) => {
                assert_eq!(response.get_http_status_code().unwrap(), 204);
                println!("{:?}", response);
            }, // Added comma
            Err(e) => {
                eprintln!("{:?}", e.message);
                panic!("Delete request failed.");
            }
        }
    }
    
}