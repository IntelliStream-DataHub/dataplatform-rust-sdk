#[cfg(test)]
mod tests {
    use crate::{create_api_service};
    use crate::files::FileUpload;

    #[tokio::test]
    async fn test_file_upload() -> Result<(), Box<dyn std::error::Error>> {
        let api_service = create_api_service();

        let file_path = "resources/test/random_values.csv";
        let file_upload_form = FileUpload::new(file_path);

        let result = api_service.files.upload_file(file_upload_form).await;
        match result {
            Ok(response) => { 
                assert_eq!(response.get_http_status_code().unwrap(), 200);
                println!("{:?}", response);
            }, // Added comma
            Err(e) => {
                eprintln!("{:?}", e.message);
                panic!("File upload request failed.");
            }
        }
        Ok(())
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