use crate::subscriptions::listener::{PySubscriptionListener, shared_listener};
use crate::subscriptions::{
    PySubscription, PySubscriptionFilter, SubscriptionIdentifyable, SubscriptionTimeseriesId,
    subscription_filter_form,
};
use intellistream_datahub_sdk::ApiService;
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::subscriptions::Subscription;
use pyo3::prelude::*;
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "SubscriptionsServiceSync")]
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PySubscription::from)
                .collect())
        })
    }

    /// Subscriptions in the tenant, newest first. `limit` defaults to the server's 1000 and may
    /// not exceed 10000; there is no paging, so a bigger tenant is truncated rather than paged —
    /// use `filter` to narrow instead.
    #[pyo3(signature = (limit = None))]
    fn list(&self, py: Python<'_>, limit: Option<u64>) -> PyResult<Vec<PySubscription>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.subscriptions.list(limit))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PySubscription::from)
                .collect())
        })
    }

    /// Subscriptions matching every criterion on the filter, newest first.
    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, timeseries=None,
                        created_time=None, last_updated_time=None, limit=None, sort_by=None,
                        sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter(
        &self,
        py: Python<'_>,
        filter: Option<PySubscriptionFilter>,
        id: Option<Vec<u64>>,
        external_id: Option<crate::StringOrList>,
        name: Option<crate::StringOrList>,
        timeseries: Option<Vec<SubscriptionTimeseriesId>>,
        created_time: Option<crate::events::PyTimeFilter>,
        last_updated_time: Option<crate::events::PyTimeFilter>,
        limit: Option<u64>,
        sort_by: Option<crate::StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<crate::PyPage> {
        let form = subscription_filter_form(
            filter, id, external_id, name, timeseries, created_time, last_updated_time, limit,
            sort_by, sort_order, cursor,
        )?;
        let service = self.api_service.clone();
        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.subscriptions.filter(&form))
                .map_err(crate::datahub_err)?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PySubscription> =
                result.get_items().iter().cloned().map(PySubscription::from).collect();
            Ok::<_, PyErr>((items, next_cursor))
        })?;
        crate::PyPage::new(py, items, next_cursor)
    }

    fn delete(&self, py: Python<'_>, input: Vec<SubscriptionIdentifyable>) -> PyResult<()> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        py.detach(|| {
            self.runtime
                .block_on(service.subscriptions.delete(&ids))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }

    /// Open a WebSocket listener multiplexing the named subscriptions. The ids seed the initial
    /// set (may be empty — add more with .subscribe()). Returns a SubscriptionListener you can
    /// iterate or call .next_message() / .ack() / .subscribe() / .close() on.
    fn listen(
        &self,
        py: Python<'_>,
        subscription_external_ids: Vec<String>,
    ) -> PyResult<PySubscriptionListener> {
        let service = self.api_service.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            let listener = self
                .runtime
                .block_on(service.subscriptions.listen(&subscription_external_ids))
                .map_err(crate::listen_err)?;
            Ok(PySubscriptionListener {
                listener: shared_listener(listener),
                runtime,
            })
        })
    }
}
