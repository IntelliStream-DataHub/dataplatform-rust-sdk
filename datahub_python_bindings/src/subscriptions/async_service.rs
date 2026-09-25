use crate::subscriptions::listener::{PySubscriptionListenerAsync, shared_listener};
use crate::subscriptions::{
    PySubscription, PySubscriptionFilter, SubscriptionIdentifyable, SubscriptionTimeseriesId,
    subscription_filter_form,
};
use intellistream_datahub_sdk::ApiService;
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::subscriptions::Subscription;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "SubscriptionsServiceAsync")]
pub struct PySubscriptionsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PySubscriptionsServiceAsync {
    fn create<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PySubscription>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let subs: Vec<Subscription> = input.into_iter().map(Subscription::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .subscriptions
                .create(&subs)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PySubscription::from)
                .collect::<Vec<_>>())
        })
    }

    /// Subscriptions in the tenant, newest first. `limit` defaults to the server's 1000 and may
    /// not exceed 10000; there is no paging, so a bigger tenant is truncated rather than paged —
    /// use `filter` to narrow instead.
    #[pyo3(signature = (limit = None))]
    fn list<'py>(&self, py: Python<'py>, limit: Option<u64>) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .subscriptions
                .list(limit)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PySubscription::from)
                .collect::<Vec<_>>())
        })
    }

    /// Subscriptions matching every criterion on the filter, newest first.
    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, timeseries=None,
                        created_time=None, last_updated_time=None, limit=None, sort_by=None,
                        sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter<'py>(
        &self,
        py: Python<'py>,
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
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = subscription_filter_form(
            filter, id, external_id, name, timeseries, created_time, last_updated_time, limit,
            sort_by, sort_order, cursor,
        )?;
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .subscriptions
                .filter(&form)
                .await
                .map_err(crate::datahub_err)?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PySubscription> =
                result.get_items().iter().cloned().map(PySubscription::from).collect();
            Python::attach(|py| crate::PyPage::new(py, items, next_cursor))
        })
    }

    fn delete<'py>(
        &self,
        py: Python<'py>,
        input: Vec<SubscriptionIdentifyable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let ids: Vec<IdAndExtId> = input.into_iter().map(IdAndExtId::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            service
                .subscriptions
                .delete(&ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn listen<'py>(
        &self,
        py: Python<'py>,
        subscription_external_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let listener = service
                .subscriptions
                .listen(&subscription_external_ids)
                .await
                .map_err(crate::listen_err)?;
            Ok(PySubscriptionListenerAsync {
                listener: shared_listener(listener),
            })
        })
    }
}
