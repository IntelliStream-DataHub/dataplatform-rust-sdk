use crate::subscriptions::listener::{PySubscriptionListenerAsync, shared_listener};
use crate::subscriptions::sync_service::build_retriever;
use crate::subscriptions::{
    PyDataSort, PySubscription, PySubscriptionRetriever, SubscriptionIdentifyable,
    SubscriptionTimeseriesId,
};
use dataplatform_rust_sdk::ApiService;
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::subscriptions::Subscription;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_python_sdk", name = "SubscriptionsServiceAsync")]
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
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result
                .get_items()
                .iter()
                .cloned()
                .map(PySubscription::from)
                .collect::<Vec<_>>())
        })
    }

    #[pyo3(signature=(retriever=None, *, timeseries=None, limit=None, sort=None))]
    fn list<'py>(
        &self,
        py: Python<'py>,
        retriever: Option<PySubscriptionRetriever>,
        timeseries: Option<Vec<SubscriptionTimeseriesId>>,
        limit: Option<u32>,
        sort: Option<PyDataSort>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let retriever = build_retriever(retriever, timeseries, limit, sort)?;
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .subscriptions
                .list(&retriever)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;
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
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(())
        })
    }

    fn listen<'py>(
        &self,
        py: Python<'py>,
        subscription_external_id: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let listener = service
                .subscriptions
                .listen(&subscription_external_id)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))?;
            Ok(PySubscriptionListenerAsync {
                listener: shared_listener(listener),
            })
        })
    }
}
