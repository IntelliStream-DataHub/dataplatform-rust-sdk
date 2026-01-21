use std::rc::{Rc, Weak};
use reqwest::{ClientBuilder};
use reqwest::Client;
use dotenv::dotenv;

use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};

use crate::datahub::DataHubApi;
use crate::events::EventsService;
use crate::files::FileService;
use crate::generic::{IdAndExtIdCollection};
use crate::timeseries::{TimeSeriesService};
use crate::unit::{UnitsService};

mod unit;
mod generic;
mod timeseries;
mod datahub;
mod fields;
mod events;
mod http;
mod files;
mod filters;
mod serde_helper;


struct ApiService{
    config: Box<DataHubApi>,
    pub time_series: TimeSeriesService,
    pub units: UnitsService,
    pub events: EventsService,
    pub files: FileService,
    http_client: Client,
}

fn create_api_service() -> Rc<ApiService> {
    dotenv().ok(); // Reads the .env file
    let dataplatform_api = DataHubApi::create_default();

    let t = "Bearer ".to_owned() + dataplatform_api.token.as_ref().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(t.as_str()).unwrap());
    headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers.insert(ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let http_client = ClientBuilder::new().default_headers(headers).build().unwrap();
    let boxed_config = Box::new(dataplatform_api.clone());
    // Clone the base_url before moving boxed_config into ApiService
    let base_url_clone = boxed_config.base_url.clone();

    let api_service = Rc::new_cyclic(|weak_self| {
        ApiService {
            config: boxed_config,
            time_series: TimeSeriesService::new(Weak::clone(weak_self), &base_url_clone), // Initialize any other services here
            units: UnitsService::new ( Weak::clone(weak_self), &base_url_clone ), // Pass the Weak reference
            events: EventsService::new ( Weak::clone(weak_self), &base_url_clone ),
            files: FileService::new ( Weak::clone(weak_self), &base_url_clone ),
            http_client,
        }
    });

    api_service
    
}

#[cfg(test)]
mod tests {
    use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
    use super::*;

    #[test]
    fn test_to_snake_lower_cased_allow_start_with_digits() {
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("Hello World!"), "hello_world".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("Another-Test_Case"), "another_test_case".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("with_numbers_123"), "with_numbers_123".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("  leading and trailing spaces  "), "_leading_and_trailing_spaces".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("123_Starts_With_Digits"), "123_starts_with_digits".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("Two  spaces"), "two_spaces".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits(" Leading space"), "_leading_space".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("Trailing space "), "trailing_space".to_string());
        assert_eq!(to_snake_lower_cased_allow_start_with_digits("!@#$%^&*()"), "".to_string());
    }

    #[tokio::test]
    async fn test_unit_requests() -> Result<(), Box<dyn std::error::Error>> {

        println!("test_unit_requests");

        let api_service = create_api_service();

        let result = api_service.units.list().await;
        match result {
            Ok(unit_response) => {
                // Directly access the `items` field from the response.
                let units = unit_response.get_items();

                // Verify that the number of units matches the expected count.
                assert_eq!(units.len(), 23);
            }
            Err(error) => {
                // Log the error that occurred during the fetch operation.
                panic!("Error occurred while fetching units: {:?}", error.get_message());
            }
        }

        let id_collection = IdAndExtIdCollection::from_id_vec(vec![9, 23]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 2);
                let items = unit_response.get_items();
                let first_unit = items.get(0).unwrap();
                let second_unit = items.get(1).unwrap();
                assert_eq!(first_unit.external_id, "area_m2");
                assert_eq!(second_unit.external_id, "volume_barrel_pet_us");
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // Test empty id collection
        let id_collection = IdAndExtIdCollection::from_id_vec(vec![]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 0);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec!["energy_kw_hr", "concentration_ppm"]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 2);
                let items = unit_response.get_items();
                let first_unit = items.get(0).unwrap();
                let second_unit = items.get(1).unwrap();
                assert_eq!(first_unit.id, 2);
                assert_eq!(second_unit.id, 5);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // try unit that doesnt exist:
        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec!["australia", "london"]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 0);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        // test empty external id
        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec![]);
        let result = api_service.units.by_ids(&id_collection).await;
        match result {
            Ok(unit_response) => {
                assert_eq!(unit_response.length(), 0);
            },
            Err(e) => {
                panic!("{:?}", e.get_message());
            }
        }

        let result = api_service.units.by_external_id("volume_barrel_pet_us").await;
        match result {
            Ok(units) => {
                assert_eq!(units.length(), 1);
                let items = units.get_items();
                let first_unit = items.get(0).unwrap();
                assert_eq!(first_unit.id, 23);
                assert_eq!(first_unit.name, "Barrel US petroleum");
                assert_eq!(first_unit.long_name, "Barrel (US)");
                assert_eq!(first_unit.symbol, "bbl{US petroleum}");
                assert_eq!(first_unit.description, "Unit of the volume for crude oil according to the Anglo-American system of units.");
                assert_eq!(first_unit.alias_names, vec!["bbl_us", "bbl", "bbl-us"]);
            },
            Err(e) => {
                println!("{:?}", e.get_message());
            }
        }

        Ok(())
    }
}
