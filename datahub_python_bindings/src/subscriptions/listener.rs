use crate::subscriptions::PySubscriptionMessage;
use intellistream_datahub_sdk::subscriptions::SubscriptionListener;
use pyo3::exceptions::{PyException, PyStopAsyncIteration, PyStopIteration, PyTimeoutError, PyValueError};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

type SharedListener = Arc<Mutex<Option<SubscriptionListener>>>;

/// How long a read waits before raising `TimeoutError`, when the caller names no timeout.
///
/// There is a default at all because the alternative is a wait nothing ends: the listener answers
/// pings without returning and reconnects when the server closes an idle session, so a stream that
/// has gone quiet blocks forever and takes its Pulsar consumer with it — which in turn makes the
/// subscription undeletable. 30s is well clear of the server's 15s ping and of observed fan-out
/// latency, so it reads as "this is not coming" rather than "not yet".
///
/// A genuinely long-lived consumer with quiet periods passes `timeout=None` to wait indefinitely.
const DEFAULT_TIMEOUT_SECS: f64 = 30.0;

/// Seconds from Python into a `Duration`, or `None` for "wait indefinitely".
fn read_timeout(timeout: Option<f64>) -> PyResult<Option<std::time::Duration>> {
    match timeout {
        None => Ok(None),
        Some(secs) if secs.is_finite() && secs > 0.0 => {
            Ok(Some(std::time::Duration::from_secs_f64(secs)))
        }
        Some(secs) => Err(PyValueError::new_err(format!(
            "timeout must be a positive number of seconds, or None to wait indefinitely; got {secs}"
        ))),
    }
}

/// One read, bounded or not. Keeps the two listener classes from disagreeing about the default.
async fn read_next(
    l: &mut SubscriptionListener,
    timeout: Option<std::time::Duration>,
) -> Option<Result<intellistream_datahub_sdk::subscriptions::SubscriptionMessage, intellistream_datahub_sdk::subscriptions::ListenError>> {
    match timeout {
        Some(d) => l.next_timeout(d).await,
        None => l.next().await,
    }
}

/// A timed-out read is its own Python exception, never a `None`/`StopIteration` that would read as
/// "the stream ended" — the caller cannot tell a quiet stream from a finished one otherwise.
fn listen_err(e: intellistream_datahub_sdk::subscriptions::ListenError) -> PyErr {
    match e {
        intellistream_datahub_sdk::subscriptions::ListenError::Timeout { .. } => {
            PyTimeoutError::new_err(e.to_string())
        }
        other => PyException::new_err(other.to_string()),
    }
}

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

    /// Iteration uses the default timeout, so `for msg in listener:` raises `TimeoutError`
    /// on a stream that has gone quiet instead of blocking forever. Call `next_message` when
    /// another bound is wanted.
    fn __next__(&self, py: Python<'_>) -> PyResult<PySubscriptionMessage> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        let timeout = read_timeout(Some(DEFAULT_TIMEOUT_SECS))?;
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                match read_next(l, timeout).await {
                    Some(Ok(msg)) => Ok(PySubscriptionMessage::from(msg)),
                    Some(Err(e)) => Err(listen_err(e)),
                    None => Err(PyStopIteration::new_err(())),
                }
            })
        })
    }

    /// Wait for the next message. Returns None when the connection has been closed cleanly,
    /// raises on transport / deserialization errors. Equivalent to driving the iterator one
    /// step but without using StopIteration as the close signal.
    ///
    /// `timeout` is in seconds and defaults to 30; `TimeoutError` is raised when it expires, and
    /// the listener stays usable, so a polling loop can simply go round again. Pass `timeout=None`
    /// to wait indefinitely — only sensible for a consumer that is genuinely long-lived, since
    /// nothing else ends that wait.
    #[pyo3(signature = (timeout = Some(DEFAULT_TIMEOUT_SECS)))]
    fn next_message(
        &self,
        py: Python<'_>,
        timeout: Option<f64>,
    ) -> PyResult<Option<PySubscriptionMessage>> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        let timeout = read_timeout(timeout)?;
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                match read_next(l, timeout).await {
                    Some(Ok(msg)) => Ok(Some(PySubscriptionMessage::from(msg))),
                    Some(Err(e)) => Err(listen_err(e)),
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

    fn subscribe(&self, py: Python<'_>, external_ids: Vec<String>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                l.subscribe(&external_ids)
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))
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
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                l.unsubscribe(&external_ids)
                    .await
                    .map_err(|e| PyException::new_err(e.to_string()))
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
                    .ok_or_else(|| PyException::new_err("listener is closed"))?;
                l.set_subscriptions(&external_ids)
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

    /// As with the sync listener, iteration carries the default timeout.
    fn __anext__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        let timeout = read_timeout(Some(DEFAULT_TIMEOUT_SECS))?;
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            match read_next(l, timeout).await {
                Some(Ok(msg)) => Ok(PySubscriptionMessage::from(msg)),
                Some(Err(e)) => Err(listen_err(e)),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }

    /// See the sync listener's `next_message`: seconds, default 30, `None` waits indefinitely.
    #[pyo3(signature = (timeout = Some(DEFAULT_TIMEOUT_SECS)))]
    fn next_message<'py>(&self, py: Python<'py>, timeout: Option<f64>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        let timeout = read_timeout(timeout)?;
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            match read_next(l, timeout).await {
                Some(Ok(msg)) => Ok(Some(PySubscriptionMessage::from(msg))),
                Some(Err(e)) => Err(listen_err(e)),
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
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            l.subscribe(&external_ids)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))
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
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            l.unsubscribe(&external_ids)
                .await
                .map_err(|e| PyException::new_err(e.to_string()))
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
                .ok_or_else(|| PyException::new_err("listener is closed"))?;
            l.set_subscriptions(&external_ids)
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
