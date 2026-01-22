
#[cfg(test)]
mod tests {
    use crate::create_api_service;
    use crate::datahub::to_snake_lower_cased_allow_start_with_digits;
    use crate::generic::IdAndExtIdCollection;
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
                assert!(items.iter().map(|item| item.id).eq([9, 23].iter().copied()));
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
                assert_eq!(items.first().unwrap().external_id,"volume_barrel_pet_us".to_string())
            },
            Err(e) => {
                println!("{:?}", e.get_message());
            }
        }

        Ok(())
    }
}