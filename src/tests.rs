#[cfg(test)]
use maplit::hashmap;
use crate::datahub::{DataHubApi, to_snake_lower_cased_allow_start_with_digits};
#[test]
fn test_to_snake_lower_cased_allow_start_with_digits() {
    // tests validation function for externalId
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
async fn test_create_api_with_token() {

    let map = hashmap!{
        "TOKEN".to_string() => "testtoken".to_string(),
        "BASE_URL".to_string() => "http://localhost:8081".to_string()
    };
    let api= DataHubApi::from_map(map).unwrap();
    assert_eq!(api.get_api_token().await.unwrap(), "testtoken".to_string());
}
