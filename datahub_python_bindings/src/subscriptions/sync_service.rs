use crate::subscriptions::listener::{PySubscriptionListener, shared_listener};
use crate::subscriptions::{
    PyDataSort, PySubscription, PySubscriptionRetriever, SubscriptionIdentifyable,
    SubscriptionTimeseriesId,
};
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::subscriptions::{
    Subscription, SubscriptionFilter, SubscriptionRetriever,
};
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(module = "datahub_python_sdk", name = "SubscriptionsServiceSync")]
pub struct PySubscriptionsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PySubscriptionsServiceSync {
    fn create(&self, py: Python<'_>, input: Vec<PySubscription>) -> PyResult<Vec<PySubscription>> {
        let subs: Vec<Subscription> = input.into_iter().map(Subscription::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.subscriptions.create(&subs))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PySubscription::from)
                .collect())
        })
    }

    #[pyo3(signature=(retriever=None, *, timeseries=None, limit=None, sort=None))]
    fn list(
        &self,
        py: Python<'_>,
        retriever: Option<PySubscriptionRetriever>,
        timeseries: Option<Vec<SubscriptionTimeseriesId>>,
        limit: Option<u32>,
        sort: Option<PyDataSort>,
    ) -> PyResult<Vec<PySubscription>> {
        let retriever = build_retriever(retriever, timeseries, limit, sort)?;
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.subscriptions.list(&retriever))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PySubscription::from)
                .collect())
        })
    }

    fn delete(&self, py: Python<'_>, input: Vec<SubscriptionIdentifyable>) -> PyResult<()> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.subscriptions.delete(&ids))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(())
        })
    }

    /// Open a WebSocket listener for the named subscription. Returns a SubscriptionListener
    /// you can iterate or call .next_message() / .ack() / .close() on.
    fn listen(
        &self,
        py: Python<'_>,
        subscription_external_id: String,
    ) -> PyResult<PySubscriptionListener> {
        let service = self.api_service.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            let listener = self
                .runtime
                .block_on(service.subscriptions.listen(&subscription_external_id))
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(PySubscriptionListener {
                listener: shared_listener(listener),
                runtime,
            })
        })
    }
}

pub(crate) fn build_retriever(
    retriever: Option<PySubscriptionRetriever>,
    timeseries: Option<Vec<SubscriptionTimeseriesId>>,
    limit: Option<u32>,
    sort: Option<PyDataSort>,
) -> PyResult<SubscriptionRetriever> {
    let kwargs_used = timeseries.is_some() || limit.is_some() || sort.is_some();
    if retriever.is_some() && kwargs_used {
        return Err(PyValueError::new_err(
            "pass either a SubscriptionRetriever or kwargs, not both",
        ));
    }
    if let Some(r) = retriever {
        return Ok(r.into());
    }
    let mut r = SubscriptionRetriever::default();
    if let Some(ts) = timeseries {
        r.filter = SubscriptionFilter {
            timeseries: ts.into_iter().map(IdAndExtId::from).collect(),
        };
    }
    if let Some(l) = limit {
        r.limit = l;
    }
    if let Some(s) = sort {
        r.sort = s.into();
    }
    Ok(r)
}

