use intellistream_datahub_sdk::timeseries::{DatapointListener, LiveDatapoint, ValueTypeRecommendation};
use pyo3::exceptions::{PyStopAsyncIteration, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;
use tokio::sync::Mutex;

type SharedListener = Arc<Mutex<Option<DatapointListener>>>;

pub(crate) fn shared_listener(l: DatapointListener) -> SharedListener {
    Arc::new(Mutex::new(Some(l)))
}

/// One point from `timeseries.listen_datapoints()`. `value` is a string for every value type.
#[pyclass(module = "intellistream_datahub_sdk", name = "LiveDatapoint", get_all, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyLiveDatapoint {
    pub external_id: String,
    pub value_type: Option<String>,
    pub timestamp: String,
    pub value: String,
}

impl From<LiveDatapoint> for PyLiveDatapoint {
    fn from(p: LiveDatapoint) -> Self {
        Self {
            external_id: p.external_id,
            value_type: p.value_type,
            timestamp: p.timestamp,
            value: p.value,
        }
    }
}

#[pymethods]
impl PyLiveDatapoint {
    fn __repr__(&self) -> String {
        format!(
            "LiveDatapoint(external_id='{}', timestamp='{}', value='{}')",
            self.external_id, self.timestamp, self.value
        )
    }
}

/// Answer of `timeseries.recommend_value_type()`.
#[pyclass(module = "intellistream_datahub_sdk", name = "ValueTypeRecommendation", get_all, frozen, skip_from_py_object)]
#[derive(Clone)]
pub struct PyValueTypeRecommendation {
    pub unit_external_id: String,
    pub recommended_value_type: String,
    pub reason: String,
    pub recognized: bool,
}

impl From<ValueTypeRecommendation> for PyValueTypeRecommendation {
    fn from(r: ValueTypeRecommendation) -> Self {
        Self {
            unit_external_id: r.unit_external_id,
            recommended_value_type: r.recommended_value_type,
            reason: r.reason,
            recognized: r.recognized,
        }
    }
}

#[pymethods]
impl PyValueTypeRecommendation {
    fn __repr__(&self) -> String {
        format!(
            "ValueTypeRecommendation(unit_external_id='{}', recommended_value_type='{}', recognized={})",
            self.unit_external_id,
            self.recommended_value_type,
            if self.recognized { "True" } else { "False" }
        )
    }
}

/// Synchronous live tail. `for point in listener:` blocks until the next datapoint.
#[pyclass(module = "intellistream_datahub_sdk", name = "DatapointListener")]
pub struct PyDatapointListener {
    pub(crate) listener: SharedListener,
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyDatapointListener {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<PyLiveDatapoint> {
        match self.next_datapoint(py)? {
            Some(point) => Ok(point),
            None => Err(PyStopIteration::new_err(())),
        }
    }

    /// Wait for the next datapoint; `None` once the listener is done.
    fn next_datapoint(&self, py: Python<'_>) -> PyResult<Option<PyLiveDatapoint>> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                let mut guard = listener.lock().await;
                let l = guard
                    .as_mut()
                    .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
                match l.next().await {
                    Some(Ok(point)) => Ok(Some(PyLiveDatapoint::from(point))),
                    Some(Err(e)) => Err(crate::listen_err(e)),
                    None => Ok(None),
                }
            })
        })
    }

    /// Add timeseries to the live set.
    fn subscribe(&self, py: Python<'_>, external_ids: Vec<String>) -> PyResult<()> {
        self.change(py, "subscribe", external_ids)
    }

    /// Remove timeseries from the live set.
    fn unsubscribe(&self, py: Python<'_>, external_ids: Vec<String>) -> PyResult<()> {
        self.change(py, "unsubscribe", external_ids)
    }

    /// Replace the whole live set.
    fn set_timeseries(&self, py: Python<'_>, external_ids: Vec<String>) -> PyResult<()> {
        self.change(py, "set", external_ids)
    }

    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| {
            runtime.block_on(async move {
                if let Some(l) = listener.lock().await.take() {
                    l.close().await.map_err(crate::listen_err)?;
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

impl PyDatapointListener {
    fn change(&self, py: Python<'_>, action: &'static str, ids: Vec<String>) -> PyResult<()> {
        let listener = self.listener.clone();
        let runtime = self.runtime.clone();
        py.detach(|| runtime.block_on(change_interest(listener, action, ids)))
    }
}

async fn change_interest(
    listener: SharedListener,
    action: &'static str,
    ids: Vec<String>,
) -> PyResult<()> {
    let mut guard = listener.lock().await;
    let l = guard
        .as_mut()
        .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
    match action {
        "subscribe" => l.subscribe(&ids).await,
        "unsubscribe" => l.unsubscribe(&ids).await,
        _ => l.set_timeseries(&ids).await,
    }
    .map_err(crate::listen_err)
}

/// Asynchronous live tail. `async for point in listener:`.
#[pyclass(module = "intellistream_datahub_sdk", name = "DatapointListenerAsync")]
pub struct PyDatapointListenerAsync {
    pub(crate) listener: SharedListener,
}

#[pymethods]
impl PyDatapointListenerAsync {
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
                Some(Ok(point)) => Ok(PyLiveDatapoint::from(point)),
                Some(Err(e)) => Err(crate::listen_err(e)),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
    }

    /// Wait for the next datapoint; `None` once the listener is done.
    fn next_datapoint<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            let mut guard = listener.lock().await;
            let l = guard
                .as_mut()
                .ok_or_else(|| PyValueError::new_err("listener is closed"))?;
            match l.next().await {
                Some(Ok(point)) => Ok(Some(PyLiveDatapoint::from(point))),
                Some(Err(e)) => Err(crate::listen_err(e)),
                None => Ok(None),
            }
        })
    }

    fn subscribe<'py>(&self, py: Python<'py>, external_ids: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            change_interest(listener, "subscribe", external_ids).await?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn unsubscribe<'py>(&self, py: Python<'py>, external_ids: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            change_interest(listener, "unsubscribe", external_ids).await?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn set_timeseries<'py>(&self, py: Python<'py>, external_ids: Vec<String>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            change_interest(listener, "set", external_ids).await?;
            Ok(Python::attach(|py| py.None()))
        })
    }

    fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let listener = self.listener.clone();
        future_into_py(py, async move {
            if let Some(l) = listener.lock().await.take() {
                l.close().await.map_err(crate::listen_err)?;
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
