use crate::subscriptions::PySubscriptionMessage;
use dataplatform_rust_sdk::subscriptions::SubscriptionListener;
use pyo3::exceptions::{PyException, PyStopAsyncIteration, PyStopIteration};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

type SharedListener = Arc<Mutex<Option<SubscriptionListener>>>;

/// Synchronous Python wrapper around the Rust `SubscriptionListener`. Iterating drives the
/// underlying WebSocket: `for msg in listener:` blocks until the next message or returns when
/// the connection closes cleanly.
#[pyclass(module = "datahub_python_sdk", name = "SubscriptionListener")]
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
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                match l.next().await {
                    Some(Ok(msg)) => Ok(PySubscriptionMessage::from(msg)),
                    Some(Err(e)) => Err(PyException::new_err(e.to_string())),
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
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                match l.next().await {
                    Some(Ok(msg)) => Ok(Some(PySubscriptionMessage::from(msg))),
                    Some(Err(e)) => Err(PyException::new_err(e.to_string())),
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
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                l.ack(&message_ids)
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))
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
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                l.nack(&message_ids)
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))
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
                        .map_err(|e| PyException::new_err(e.to_string()))?;
                }
                Ok(())
            })
        })
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature=(_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<()> {
        self.close(py)
    }
}

/// Asynchronous Python wrapper. Use `async for msg in listener:` on the asyncio side.
#[pyclass(module = "datahub_python_sdk", name = "SubscriptionListenerAsync")]
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
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            match l.next().await {
                Some(Ok(msg)) => Ok(PySubscriptionMessage::from(msg)),
                Some(Err(e)) => Err(PyException::new_err(e.to_string())),
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
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            match l.next().await {
                Some(Ok(msg)) => Ok(Some(PySubscriptionMessage::from(msg))),
                Some(Err(e)) => Err(PyException::new_err(e.to_string())),
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
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            l.ack(&message_ids)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))
        })
    }

    fn nack<'py>(&self, py: Python<'py>, message_ids: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            l.nack(&message_ids)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            if let Some(l) = guard.take() {
                l.close()
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn __aenter__<'py>(slf: Py<Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_py(py, async move { Ok(slf) })
    }

    #[pyo3(signature=(_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<PyObject>,
        _exc_value: Option<PyObject>,
        _traceback: Option<PyObject>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.close(py)
    }
}

pub(crate) fn shared_listener(l: SubscriptionListener) -> SharedListener {
    Arc::new(Mutex::new(Some(l)))
}
