use oauth2::http::StatusCode;
use reqwest::{Error, Response};
use serde::de::DeserializeOwned;
use crate::generic::{DataWrapperDeserialization};

pub struct ResponseError {
    pub(crate) status: StatusCode,
    pub(crate) message: String,
}

impl ResponseError {

    pub fn from(message: String) -> Self {
        ResponseError{status: StatusCode::from_u16(0).unwrap(), message}
    }

    pub fn from_err(error: Error) -> Self {
        if error.status().is_some() {
            return ResponseError{status: error.status().unwrap(), message: error.to_string()}
        }
        ResponseError{status: StatusCode::BAD_REQUEST, message: error.to_string()}
    }

    pub fn get_message(&self) -> String {
        self.message.clone()
    }

    pub fn get_status(&self) -> StatusCode {
        self.status
    }
}

pub async fn process_response<T>(response: Response, path: &str) -> Result<T, ResponseError>
where
    T: DeserializeOwned + DataWrapperDeserialization,
{
    let status = response.status();
    if (200..300).contains(&status.as_u16()) {
        // Read the response body and attempt to deserialize
        let body = response.text().await.map_err(|err| {
            eprintln!("Failed to read response body: {}", err);
            ResponseError {status, message: err.to_string()}
        })?;

        let max_chars = 2000;
        let truncated_body = &body[..body.len().min(max_chars)];
        println!("Response body for path: {}\n{}", path, &truncated_body); // Debug output

        // Conditionally apply custom or default logic
        let result: T = T::deserialize_and_set_status(&body, status.as_u16()).map_err(|err| {
            eprintln!("Failed to deserialize JSON: {}", err);
            ResponseError {
                status,
                message: err.to_string(),
            }
        })?;

        Ok(result)

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