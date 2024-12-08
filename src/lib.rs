use pretend::{pretend, Json, Pretend, Response};
use pretend_reqwest::Client;
use pretend_reqwest::reqwest::Url;
use pretend_reqwest::reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use pretend_reqwest::reqwest::Client as RClient;
use crate::datahub::DataHubApi;
use crate::generic::{DataWrapper, IdAndExtIdCollection};
use crate::timeseries::{LimitParam, TimeSeriesCollection, TimeSeries, TimeSeriesUpdateCollection};
use crate::unit::{UnitResponse};

mod unit;
mod generic;
mod timeseries;
mod datahub;
mod fields;

pub(crate) type UnitResult = Response<Json<UnitResponse>>;
pub(crate) type TimeSeriesResult = Response<Json<DataWrapper<TimeSeries>>>;

struct ApiConfig{
    base_url: String,
    token: String,
    client_id: Option<String>,
    client_secret: Option<String>,
    token_url: Option<String>,
}

impl ApiConfig {

    fn new_with_token(base_url: String, token: String) -> ApiConfig {
        ApiConfig{base_url, token, client_id: None, client_secret: None, token_url: None}
    }

}

#[pretend]
trait ApiService {

    // Units
    #[request(method = "GET", path = "/units")]
    async fn get_all_units(&self) -> pretend::Result<UnitResult>;

    #[request(method = "GET", path = "/units/{value}")]
    async fn get_unit_by_external_id(&self, value: &str) -> pretend::Result<UnitResult>;

    #[request(method = "POST", path = "/units/byids")]
    async fn get_units_by_ids(&self, json: &IdAndExtIdCollection) -> pretend::Result<UnitResult>;

    // Time Series

    #[request(method = "GET", path = "/timeseries")]
    async fn get_all_time_series(&self, query: &LimitParam) -> pretend::Result<TimeSeriesResult>;

    #[request(method = "POST", path = "/timeseries/create")]
    async fn create_time_series(&self, json: &TimeSeriesCollection) -> pretend::Result<TimeSeriesResult>;

    #[request(method = "POST", path = "/timeseries/delete")]
    async fn delete_time_series(&self, json: &IdAndExtIdCollection) -> pretend::Result<Response<()>>;

    #[request(method = "POST", path = "/timeseries/update")]
    async fn update_time_series(&self, json: &TimeSeriesUpdateCollection) -> pretend::Result<TimeSeriesResult>;

    // Events
    // Resources
}

fn create_api_service(dataplatform_api: &DataHubApi) -> impl ApiService {
    let url = Url::parse(&*dataplatform_api.base_url).unwrap();

    let t = "Bearer ".to_owned() + dataplatform_api.token.as_ref().unwrap();
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(t.as_str()).unwrap());
    headers.insert(CONTENT_TYPE, HeaderValue::from_str("application/json").unwrap());
    headers.insert(ACCEPT, HeaderValue::from_str("application/json").unwrap());

    let rw_http_client = RClient::builder().default_headers(headers).build().unwrap();
    let http_client = Client::new(rw_http_client);
    Pretend::for_client(http_client).with_url(url)
}

#[cfg(test)]
mod tests {
    use pretend::StatusCode;
    use super::*;
    use crate::timeseries::{TimeSeries, TimeSeriesCollection};

    #[tokio::test]
    async fn test_unit_requests() -> Result<(), Box<dyn std::error::Error>> {

        let api_service = create_api_service(&DataHubApi::create_default());

        let result = api_service.get_all_units().await;
        match result {
            Ok(response) => {
                let units = response.into_body().value().clone();
                assert_eq!(units.length(), 22);
            },
            Err(e) => {
                println!("{:?}", e);
            }
        }

        let id_collection = IdAndExtIdCollection::from_id_vec(vec![9, 23]);
        let result = api_service.get_units_by_ids(&id_collection).await;
        match result {
            Ok(response) => {
                let units = response.into_body().value().clone();
                assert_eq!(units.length(), 2);
                let items = units.get_items();
                let first_unit = items.get(0).unwrap();
                let second_unit = items.get(1).unwrap();
                assert_eq!(first_unit.external_id, "area_m2");
                assert_eq!(second_unit.external_id, "volume_barrel_pet_us");
            },
            Err(e) => {
                println!("{:?}", e);
            }
        }

        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec!["energy_kw_hr", "concentration_ppm"]);
        println!("{:?}", id_collection);
        let result = api_service.get_units_by_ids(&id_collection).await;
        match result {
            Ok(response) => {
                let units = response.into_body().value().clone();
                assert_eq!(units.length(), 2);
                let items = units.get_items();
                let first_unit = items.get(0).unwrap();
                let second_unit = items.get(1).unwrap();
                assert_eq!(first_unit.id, 2);
                assert_eq!(second_unit.id, 5);
            },
            Err(e) => {
                println!("{:?}", e);
            }
        }

        let result = api_service.get_unit_by_external_id("volume_barrel_pet_us").await;
        match result {
            Ok(response) => {
                let units = response.into_body().value().clone();
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
                println!("{:?}", e);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_timeseries_requests() -> Result<(), Box<dyn std::error::Error>> {

        let api_service = create_api_service(&DataHubApi::create_default());

        let mut params = LimitParam::new();
        params.set_limit(5);

        let result = api_service.get_all_time_series(&params).await;
        match result {
            Ok(response) => {
                let timeseries = response.into_body().value().clone();
                assert_eq!(timeseries.length(), params.get_limit());

                let items = timeseries.get_items();
                println!("{:?}", items);
                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "basement_airthings_voc_1001") {
                    assert_eq!(item.id, 239);
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "basement_airthings_battery_1001") {
                    assert_eq!(item.id, 240);
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

            },
            Err(e) => {
                println!("{:?}", e);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_create_and_delete_timeseries() -> Result<(), Box<dyn std::error::Error>> {

        let api_service = create_api_service(&DataHubApi::create_default());

        // Delete timeseries first, in case a test failed and the time series exists
        delete_timeseries(&api_service).await;

        let mut ts_collection = TimeSeriesCollection::new();
        let ts1 = TimeSeries::builder()
            .set_external_id("rust_sdk_test_1_ts")
            .set_name("Rust SDK Test 1 TimeSeries")
            .set_description("This is test timeseries generated by rust sdk test code.")
            .set_unit("celsius")
            .set_value_type("float").clone();
        ts_collection.add_item(ts1);
        let ts2 = TimeSeries::builder()
            .set_external_id("rust_sdk_test_2_ts")
            .set_name("Rust SDK Test 2 TimeSeries")
            .set_unit("watt")
            .set_value_type("bigint").clone();
        ts_collection.add_item(ts2);

        let result = api_service.create_time_series(&ts_collection).await;
        match result {
            Ok(response) => {
                let timeseries = response.into_body().value().clone();
                assert_eq!(timeseries.length(), 2);

                let mut items = timeseries.get_items();

                println!("{:?}", items);
                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_1_ts") {
                    assert_eq!(item.external_id, "rust_sdk_test_1_ts");
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

                if let Some(item) = items.iter().find(|&&ref item| item.external_id == "rust_sdk_test_2_ts") {
                    assert_eq!(item.external_id, "rust_sdk_test_2_ts");
                } else {
                    assert_eq!(StatusCode::OK, StatusCode::NO_CONTENT);
                }

            },
            Err(e) => {
                println!("{:?}", e);
            }
        }

        // Delete timeseries
        delete_timeseries(&api_service).await;

        Ok(())
    }

    async fn delete_timeseries(api_service: &dyn ApiService) {
        let id_collection = IdAndExtIdCollection::from_external_id_vec(vec!["rust_sdk_test_1_ts", "rust_sdk_test_2_ts"]);
        let result = api_service.delete_time_series(&id_collection).await;
        match result {
            Ok(response) => {
                assert_eq!(response.status().as_u16(), StatusCode::NO_CONTENT);

            },
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }
}
