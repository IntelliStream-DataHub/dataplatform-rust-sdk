use crate::PyIdCollection;
use crate::subscriptions::{PySubscription, SubscriptionTimeseriesId};
use chrono::{DateTime, Utc};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::subscriptions::Subscription;
use pyo3::pymethods;

#[pymethods]
impl PySubscription {
    #[new]
    #[pyo3(signature=(external_id, name, timeseries))]
    pub fn __init__(
        external_id: String,
        name: String,
        timeseries: Vec<SubscriptionTimeseriesId>,
    ) -> Self {
        let ts: Vec<IdAndExtId> = timeseries.into_iter().map(IdAndExtId::from).collect();
        Self {
            inner: Subscription::new(external_id, name, ts),
        }
    }

    #[getter]
    pub fn id(&self) -> Option<u64> {
        self.inner.id
    }

    #[getter]
    pub fn external_id(&self) -> &str {
        &self.inner.external_id
    }

    #[getter]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    #[getter]
    pub fn timeseries(&self) -> Vec<PyIdCollection> {
        self.inner
            .timeseries
            .iter()
            .cloned()
            .map(PyIdCollection::from)
            .collect()
    }

    #[getter]
    pub fn date_created(&self) -> Option<DateTime<Utc>> {
        self.inner.date_created
    }

    #[getter]
    pub fn last_updated(&self) -> Option<DateTime<Utc>> {
        self.inner.last_updated
    }
}
