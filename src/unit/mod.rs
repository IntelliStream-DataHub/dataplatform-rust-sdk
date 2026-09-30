//! The unit catalogue — the engineering units a
//! [`TimeSeries`](crate::timeseries::TimeSeries) can be expressed in.
//!
//! [`UnitsService`] is reached as `api.units` and is read-only: the SDK exposes no create, update
//! or delete here, because the catalogue is maintained server-side. Fetch all of it with
//! [`list`](UnitsService::list) — it is a small, slow-changing set — or resolve individual entries
//! with [`by_external_id`](UnitsService::by_external_id) and [`by_ids`](UnitsService::by_ids).
//!
//! A [`Unit`] carries its display `symbol`, the physical `quantity` it measures, its `alias_names`
//! and a `conversion` table. Attach one to a series through `TimeSeries::unit_external_id`.

mod test;

use crate::generic::{ApiServiceProvider, DataWrapper, IdAndExtId};
use crate::http::ResponseError;
use crate::ApiService;
use serde::{Deserialize, Serialize};
use std::clone::Clone;
use std::collections::HashMap;
use std::sync::Weak;

/// Read-only access to the unit catalogue. Reached as `api.units`; see the [module docs](self).
pub struct UnitsService {
    pub(crate) api_service: Weak<ApiService>,
    base_url: String,
}

impl UnitsService {
    pub fn new(api_service: Weak<ApiService>, base_url: &String) -> Self {
        let unit_base_url = format!("{}/units", base_url);
        UnitsService {
            api_service,
            base_url: unit_base_url,
        }
    }

    /// Every unit in the catalogue. It is a small, slow-changing set, so fetching it whole is cheap.
    pub async fn list(&self) -> Result<DataWrapper<Unit>, ResponseError> {
        self.execute_get_request(&self.base_url, None::<&str>).await
    }

    /// One unit by its external id, e.g. `temperature_deg_c`.
    pub async fn by_external_id(&self, value: &str) -> Result<DataWrapper<Unit>, ResponseError> {
        let path = &format!("{}/{value}", self.base_url, value = value);
        self.execute_get_request(path, None::<&str>).await
    }

    /// Units by id or external id, answering the subset it found.
    pub async fn by_ids(
        &self,
        json: &DataWrapper<IdAndExtId>,
    ) -> Result<DataWrapper<Unit>, ResponseError> {
        let path = &format!("{}/byids", &self.base_url);
        self.execute_post_request(path, json).await
    }
}

/// An engineering unit from the platform's catalogue, referenced by a series through
/// `unit_external_id`.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Unit {
    #[serde(default, with = "crate::serde_helper::string_id")]
    pub id: u64,
    pub external_id: String,
    pub name: String,
    pub long_name: String,
    pub symbol: String,
    pub description: String,
    pub alias_names: Vec<String>,
    pub quantity: String,
    pub conversion: HashMap<String, f64>,
    pub source: String,
    pub source_reference: String,
}
