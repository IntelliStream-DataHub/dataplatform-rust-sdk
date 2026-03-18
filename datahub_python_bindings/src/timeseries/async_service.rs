use std::sync::Arc;
use pyo3::{pyclass, pymethods, Bound, PyAny, PyResult, Python};
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::tokio::future_into_py;
use dataplatform_rust_sdk::{ApiService, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection};
use dataplatform_rust_sdk::generic::{DataWrapper, DatapointString, DatapointsCollection, DeleteFilter, IdAndExtId, IdAndExtIdCollection, RetrieveFilter, SearchAndFilterForm};
use crate::{PyIdCollection, PyRetrieveFilter, PySearchAndFilterForm};
use crate::timeseries::{PyDatapointsCollectionDatapoints, PyDatapointsCollectionString, PyDeleteFilter, PyTimeSeries, PyTimeSeriesUpdate};
use crate::unit::PyUnit;

#[pyclass]
pub struct PyTimeSeriesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyTimeSeriesServiceAsync {
    fn list<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.time_series.list().await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }

    fn create<'p>(&self, py: Python<'p>,input: Vec<PyTimeSeries>) -> PyResult<Bound<'p, PyAny>> {
        let timeseries = input.iter().cloned().map(TimeSeries::from).collect();
        let payload = DataWrapper::from_vec(timeseries);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service.time_series.create(&payload).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'p>(&self, py: Python<'p>, input: Vec<PyIdCollection>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        future_into_py(py, async move {
            let result = service
                .time_series
                .by_ids(&wrapper)
                .await
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|u| PyTimeSeries { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'p>(&self, py: Python<'p>,input: Vec<PyIdCollection>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        future_into_py(py, async move {
            let result = service.time_series.delete(&wrapper).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }
    fn update<'p>(&self, py: Python<'p>,input: Vec<PyTimeSeriesUpdate>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input = input.iter().cloned().map(TimeSeriesUpdate::from).collect();
        let wrapper =TimeSeriesUpdateCollection::from_vec(input);

        future_into_py(py, async move {
            let result = service.time_series.update(&wrapper).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }
    fn search<'p>(&self, py: Python<'p>, input: PySearchAndFilterForm) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service.time_series.search(&input.into()).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }
    
    fn insert_datapoints<'py>(&self, py: Python<'py>, input: Vec<PyDatapointsCollectionString> )-> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let vec: Vec<DatapointsCollection<DatapointString>> = input.into_iter().map(|item| item.into()).collect();
        let mut wrapper = DataWrapper::<DatapointsCollection<DatapointString>>::from_vec(vec);
        future_into_py(py, async move {
            let result = service.time_series.insert_datapoints(&mut wrapper).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;
            let val = result.get_items().clone();
            Ok(result.get_items().clone())
        })
    }
    fn retrieve_datapoints<'py>(&self,  py: Python<'py>, input: PyRetrieveFilter )-> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let wrapper = DataWrapper::<RetrieveFilter>::from_vec(vec![input.into()]);
        future_into_py(py, async move {
            let result = service.time_series.retrieve_datapoints(&wrapper).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;
            let result: Vec<PyDatapointsCollectionDatapoints> = result.get_items().into_iter().map(|ts| PyDatapointsCollectionDatapoints { inner: ts.clone() }).collect();
            Ok(result)
        })
    }
    fn delete_datapoints<'py>(&self,  py: Python<'py>, input: Vec<PyDeleteFilter> )-> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let wrapper = DataWrapper::<DeleteFilter>::from_vec(input.into_iter().map(|f|f.into()).collect());
        future_into_py(py, async move {
            let result = service.time_series.delete_datapoints(&wrapper).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;
            Ok(())
        })
    }
    fn retrive_latest_datapoints<'py>(&self,  py: Python<'py>, input: Vec<PyIdCollection> )-> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);
        future_into_py(py, async move {
            let result = service.time_series.retrieve_latest_datapoint(&wrapper).await.map_err(|e| {
                PyException::new_err(e.get_message())
            })?;
            Ok(())
        })
    }

}