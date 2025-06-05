#[cfg(test)]
mod tests {
    use crate::{create_api_service, ApiService};
    use crate::files::FileUpload;

    #[tokio::test]
    async fn test_file_upload() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();
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
        Ok(())
    }
    
    async fn do_file_upload(api_service: &ApiService<'_>, upload_form: FileUpload) {
        let _ = api_service.files.upload_file(upload_form).await.is_ok_and(|res| {
            assert_eq!(res.get_http_status_code().unwrap(), 200);
            true
        });
    }

    #[tokio::test]
    async fn list_folders() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        // First test root path
        let empty_root_path = "";
        let root_path_with_slash = "/";
        
        let file_path = "resources/test/random_values.csv";

        let result = api_service.files.list_directory(empty_root_path).await;
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
        let _ = api_service.files.list_directory(root_path_with_slash).await.is_ok_and(|res| {
            assert_eq!(res.get_http_status_code().unwrap(), 200);
            true
        });
        Ok(())
    }
    
}