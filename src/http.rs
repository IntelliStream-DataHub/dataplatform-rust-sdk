use oauth2::http;
use oauth2::http::StatusCode;
use reqwest::{Error, Response};
use serde::de::DeserializeOwned;

pub struct ResponseError {
    status: StatusCode,
    message: String,
}

impl ResponseError {

    pub fn from(message: String) -> Self {
        ResponseError{status: StatusCode::from_u16(0).unwrap(), message}
    }

    pub fn from_err(error: Error) -> Self {
        ResponseError{status: error.status().unwrap(), message: error.to_string()}
    }

    pub fn get_message(&self) -> String {
        self.message.clone()
    }

    pub fn get_status(&self) -> StatusCode {
        self.status
    }
}

pub async fn process_response<T: DeserializeOwned>(response: Response) -> Result<T, ResponseError> {
    let status = response.status();
    if status == StatusCode::OK {
        // Read the response body and attempt to deserialize
        let body = response.text().await.map_err(|err| {
            eprintln!("Failed to read response body: {}", err);
            ResponseError {status, message: err.to_string()}
        })?;

        println!("Response body: {}", body); // Debug output

        serde_json::from_str::<T>(&body).map_err(|err| {
            eprintln!("Failed to deserialize JSON: {}", err);
            ResponseError {status, message: err.to_string()}
        })
    } else {
        let status = response.status();
        eprintln!("Request failed with status: {}", status);
        Err(ResponseError{
            status,
            message: response.text().await.unwrap_or_else(|_|
                "Failed to read response body".to_string()
            )
        })
    }
}