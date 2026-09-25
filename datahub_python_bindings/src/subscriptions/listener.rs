use crate::subscriptions::PySubscriptionMessage;
use intellistream_datahub_sdk::subscriptions::SubscriptionListener;
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

type SharedListener = Arc<Mutex<Option<SubscriptionListener>>>;

/// Synchronous Python wrapper around the Rust `SubscriptionListener`. Iterating drives the
/// underlying WebSocket: `for msg in listener:` blocks until the next message or returns when
/// the connection closes cleanly.
#[pyclass(module = "intellistream_datahub_sdk", name = "SubscriptionListener")]
pub struct PySubscriptionListener {
    pub(crate) listener: SharedListener,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PySubscriptionListener {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<PySubscriptionMessage> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                match l.next().await {
                    Some(Ok(msg)) => Ok(PySubscriptionMessage::from(msg)),
                    Some(Err(e)) => Err(crate::listen_err(e)),
                    None => Err(PyStopIteration::new_err(())),
                }
            })
        })
    }

    /// Wait for the next message. Returns None when the connection has been closed cleanly,
    /// raises on transport / deserialization errors. Equivalent to driving the iterator one
    /// step but without using StopIteration as the close signal.
    fn next_message(&self, py: Python<'_>) -> PyResult<Option<PySubscriptionMessage>> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                match l.next().await {
                    Some(Ok(msg)) => Ok(Some(PySubscriptionMessage::from(msg))),
                    Some(Err(e)) => Err(crate::listen_err(e)),
                    None => Ok(None),
                }
            })
        })
    }

    fn ack(&self, py: Python<'_>, message_ids: Vec<String>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                l.ack(&message_ids)
                    .await
                    .map_err(crate::listen_err)
            })
        })
    }

    fn nack(&self, py: Python<'_>, message_ids: Vec<String>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                l.nack(&message_ids)
                    .await
                    .map_err(crate::listen_err)
            })
        })
    }

    fn subscribe(&self, py: Python<'_>, external_ids: Vec<String>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                l.subscribe(&external_ids)
                    .await
                    .map_err(crate::listen_err)
            })
        })
    }

    fn unsubscribe(&self, py: Python<'_>, external_ids: Vec<String>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                l.unsubscribe(&external_ids)
                    .await
                    .map_err(crate::listen_err)
            })
        })
    }

    fn set_subscriptions(&self, py: Python<'_>, external_ids: Vec<String>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                l.set_subscriptions(&external_ids)
                    .await
                    .map_err(crate::listen_err)
            })
        })
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                if let Some(l) = guard.take() {
                    l.close()
                        .await
                        .map_err(crate::listen_err)?;
                }
                Ok(())
            })
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature=(_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<()> {
        self.close(py)
    }
}

/// Asynchronous Python wrapper. Use `async for msg in listener:` on the asyncio side.
#[pyclass(module = "intellistream_datahub_sdk", name = "SubscriptionListenerAsync")]
pub struct PySubscriptionListenerAsync {
    pub(crate) listener: SharedListener,
}

#[pymethods]
impl PySubscriptionListenerAsync {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            match l.next().await {
                Some(Ok(msg)) => Ok(PySubscriptionMessage::from(msg)),
                Some(Err(e)) => Err(crate::listen_err(e)),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }

    fn next_message<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            match l.next().await {
                Some(Ok(msg)) => Ok(Some(PySubscriptionMessage::from(msg))),
                Some(Err(e)) => Err(crate::listen_err(e)),
                None => Ok(None),
            }
        })
    }

    fn ack<'py>(&self, py: Python<'py>, message_ids: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            l.ack(&message_ids)
                .await
                .map_err(crate::listen_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn nack<'py>(&self, py: Python<'py>, message_ids: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            l.nack(&message_ids)
                .await
                .map_err(crate::listen_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn subscribe<'py>(
        &self,
        py: Python<'py>,
        external_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            l.subscribe(&external_ids)
                .await
                .map_err(crate::listen_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn unsubscribe<'py>(
        &self,
        py: Python<'py>,
        external_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            l.unsubscribe(&external_ids)
                .await
                .map_err(crate::listen_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn set_subscriptions<'py>(
        &self,
        py: Python<'py>,
        external_ids: Vec<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            l.set_subscriptions(&external_ids)
                .await
                .map_err(crate::listen_err)?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            if let Some(l) = guard.take() {
                l.close()
                    .await
                    .map_err(crate::listen_err)?;
            }
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature=(_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.close(py)
    }
}

pub(crate) fn shared_listener(l: SubscriptionListener) -> SharedListener {
    Arc::new(Mutex::new(Some(l)))
}
