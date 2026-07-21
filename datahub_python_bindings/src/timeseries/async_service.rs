use crate::timeseries::datapoints::{
    PyDatapoint, PyDatapointsCollectionDatapoints, PyDatapointsCollectionString,
};
use crate::timeseries::{
    PyDeleteFilter, PyTimeSeries, PyTimeSeriesUpdate, PyTimeseriesIdentifiable,
};
use crate::{
    DatahubIdentity, Identifiable, PyIdCollection, PyRetrieveFilter, PySearchAndFilterForm,
};
use crate::datetime::py_datetime_to_utc;
use dataplatform_rust_sdk::generic::{
    DataWrapper, DatapointString, DatapointsCollection, DeleteFilter, IdAndExtId, RetrieveFilter,
};
use dataplatform_rust_sdk::{ApiService, TimeSeries, TimeSeriesUpdate, TimeSeriesUpdateCollection};
use pyo3::prelude::*;
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "TimeSeriesServiceAsync")]
pub struct PyTimeSeriesServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyTimeSeriesServiceAsync {
    #[pyo3(signature = (limit=None))]
    fn list<'p>(&self, py: Python<'p>, limit: Option<u64>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = match limit {
                Some(l) => service.time_series.list_with_limit(Some(l)).await,
                None => service.time_series.list().await,
            }
            .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }

    fn create<'p>(&self, py: Python<'p>, input: Vec<PyTimeSeries>) -> PyResult<Bound<'p, PyAny>> {
        let timeseries = input.iter().cloned().map(TimeSeries::from).collect();
        let payload = DataWrapper::from_vec(timeseries);
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .time_series
                .create(&payload)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'p>(
        &self,
        py: Python<'p>,
        input: Vec<PyTimeseriesIdentifiable>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(Into::into)
            .collect::<Vec<IdAndExtId>>();
        let wrapper = DataWrapper::from_vec(input_ids);

        future_into_py(py, async move {
            let result = service
                .time_series
                .by_ids(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|u| PyTimeSeries { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'p>(
        &self,
        py: Python<'p>,
        input: Vec<PyTimeseriesIdentifiable>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(Into::into)
            .collect::<Vec<IdAndExtId>>();
        let wrapper = DataWrapper::from_vec(input_ids);

        future_into_py(py, async move {
            let result = service
                .time_series
                .delete(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }
    fn update<'p>(
        &self,
        py: Python<'p>,
        input: Vec<PyTimeSeriesUpdate>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input = input.iter().cloned().map(TimeSeriesUpdate::from).collect();
        let wrapper = TimeSeriesUpdateCollection::from_vec(input);

        future_into_py(py, async move {
            let result = service
                .time_series
                .update(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }
    fn search<'p>(
        &self,
        py: Python<'p>,
        input: PySearchAndFilterForm,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();

        future_into_py(py, async move {
            let result = service
                .time_series
                .search(&input.into())
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries { inner: ts.clone() })
                .collect();
            Ok(py_ts)
        })
    }

    fn insert_datapoints<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyDatapointsCollectionString>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let vec: Vec<DatapointsCollection<DatapointString>> =
            input.into_iter().map(|item| item.into()).collect();
        let mut wrapper = DataWrapper::<DatapointsCollection<DatapointString>>::from_vec(vec);
        future_into_py(py, async move {
            let result = service
                .time_series
                .insert_datapoints(&mut wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            let val = result.get_items().clone();
            Ok(result.get_items().clone())
        })
    }
    fn insert_from_lists<'py>(
        &self,
        py: Python<'py>,
        timestamps: Vec<Bound<'py, PyAny>>,
        values: Vec<f64>,
        ts: Identifiable,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let datapoints: Vec<DatapointString> = timestamps
            .into_iter()
            .zip(values.into_iter())
            .map(|(timestamp, value)| {
                Ok(DatapointString {
                    timestamp: py_datetime_to_utc(&timestamp)?.timestamp_millis().to_string(),
                    value: value.to_string(),
                })
            })
            .collect::<PyResult<Vec<_>>>()?;
        let inner: DatapointsCollection<DatapointString> = DatapointsCollection {
            datapoints,
            next_cursor: None,
            id: ts.id_collection().id,
            external_id: ts.id_collection().external_id,
            unit: None,
            unit_external_id: None,
        };
        let mut wrapper =
            DataWrapper::<DatapointsCollection<DatapointString>>::from_vec(vec![inner]);
        future_into_py(py, async move {
            let result = service
                .time_series
                .insert_datapoints(&mut wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            let val = result.get_items().clone();
            Ok(result.get_items().clone())
        })
    }

    fn retrieve_datapoints<'py>(
        &self,
        py: Python<'py>,
        input: PyRetrieveFilter,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let wrapper = DataWrapper::<RetrieveFilter>::from_vec(vec![input.into()]);
        future_into_py(py, async move {
            let result = service
                .time_series
                .retrieve_datapoints(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            let result: Vec<PyDatapointsCollectionDatapoints> = result
                .get_items()
                .into_iter()
                .map(|ts| PyDatapointsCollectionDatapoints { inner: ts.clone() })
                .collect();
            Ok(result)
        })
    }
    fn delete_datapoints<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyDeleteFilter>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let wrapper =
            DataWrapper::<DeleteFilter>::from_vec(input.into_iter().map(|f| f.into()).collect());
        future_into_py(py, async move {
            let result = service
                .time_series
                .delete_datapoints(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            Ok(())
        })
    }
    /// Retrieve latest datapoints for a collection of Timeseries
    ///
    /// Errors: type error if input is not a collection of Identifyable
    /// Value error if input is empty
    /// Value error if input does not contain a external id or a id
    ///
    fn retrieve_latest_datapoints<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyTimeseriesIdentifiable>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(Into::into)
            .collect::<Vec<IdAndExtId>>();
        let wrapper = DataWrapper::from_vec(input_ids);
        future_into_py(py, async move {
            let result = service
                .time_series
                .retrieve_latest_datapoint(&wrapper)
                .await
                .map_err(|e| crate::datahub_err(e))?;
            let res: Vec<PyDatapointsCollectionDatapoints> = result
                .get_items()
                .iter()
                .map(|ts| PyDatapointsCollectionDatapoints::from(ts.clone()))
                .collect();
            Ok(res)
        })
    }
    /*
    fn unpack_series(series: &PyAny) -> PyResult<(Vec<f64>, Vec<i64>)> {
        // 1. Extract values as a NumPy array
        let values_attr = series.getattr("values")?;
        let val_array: &PyArray1<f64> = values_attr.downcast::<PyArray1<f64>>()?;

        // 2. Extract index as a NumPy array (assuming integer index)
        let index_attr = series.getattr("index")?.call_method0("to_numpy")?;
        let idx_array: &PyArray1<i64> = index_attr.downcast::<PyArray1<i64>>()?;

        // 3. Convert to Rust Vectors
        // Note: .to_vec() copies the data. For large datasets,
        // you might prefer working with the arrays directly.
        let values_vec = val_array.to_vec()?;
        let index_vec = idx_array.to_vec()?;

        Ok((values_vec, index_vec))
    }
    */
}
