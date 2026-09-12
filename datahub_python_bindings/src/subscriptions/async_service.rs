use crate::subscriptions::listener::{PySubscriptionListenerAsync, shared_listener};
use crate::subscriptions::sync_service::build_filter_form;
use crate::subscriptions::{
    PyDataSort, PySubscription, PySubscriptionFilterForm, SubscriptionIdentifyable,
    SubscriptionTimeseriesId,
};
use intellistream_datahub_sdk::ApiService;
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::subscriptions::Subscription;
use pyo3::exceptions::PyException;
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

    /// Subscriptions matching every criterion on the filter.
    #[pyo3(signature=(form=None, *, timeseries=None, limit=None, sort=None))]
    fn filter<'py>(
        &self,
        py: Python<'py>,
        form: Option<PySubscriptionFilterForm>,
        timeseries: Option<Vec<SubscriptionTimeseriesId>>,
        limit: Option<u32>,
        sort: Option<PyDataSort>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let form = build_filter_form(form, timeseries, limit, sort)?;
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .subscriptions
                .filter(&form)
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
            Ok(())
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
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(PySubscriptionListenerAsync {
                listener: shared_listener(listener),
            })
        })
    }
}
