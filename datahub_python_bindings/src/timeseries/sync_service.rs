use super::*;
use crate::timeseries::datapoints::{
    PyDatapointsCollectionDatapoints, PyDatapointsCollectionString,
};
use crate::{DatahubIdentity, Identifiable};
use crate::{PyIdCollection, PyRetrieveFilter, PySearchAndFilterForm};
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId, IdAndExtIdCollection};
use dataplatform_rust_sdk::{ApiService, TimeSeriesUpdateCollection};
use pyo3::exceptions::PyException;
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "datahub_python_sdk", name = "TimeSeriesServiceSync")]
pub struct PyTimeSeriesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyTimeSeriesServiceSync {
    fn list(&self, py: Python<'_>) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.list())
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .cloned()
                .map(PyTimeSeries::from)
                .collect();

            Ok(py_units)
        })
    }

    fn create<'p>(&self, py: Python<'p>, input: Vec<PyTimeSeries>) -> PyResult<Vec<PyTimeSeries>> {
        let timeseries = input.iter().cloned().map(TimeSeries::from).collect();
        let payload = DataWrapper::from_vec(timeseries);
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.create(&payload))
                .map_err(|e| PyException::new_err(e.get_message()))?;

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
    ) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(Into::into)
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.by_ids(&wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|u| PyTimeSeries { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'p>(&self, py: Python<'p>, input: Vec<PyTimeseriesIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(Into::into)
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.delete(&wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            Ok(())
        })
    }
    fn update<'p>(
        &self,
        py: Python<'p>,
        input: Vec<PyTimeSeriesUpdate>,
    ) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();
        let input = input.iter().cloned().map(TimeSeriesUpdate::from).collect();
        let wrapper = TimeSeriesUpdateCollection::from_vec(input);

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.update(&wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;

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
    ) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.search(&input.into()))
                .map_err(|e| PyException::new_err(e.get_message()))?;
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
    ) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        let vec: Vec<DatapointsCollection<DatapointString>> =
            input.into_iter().map(|item| item.into()).collect();
        let mut wrapper = DataWrapper::<DatapointsCollection<DatapointString>>::from_vec(vec);
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.insert_datapoints(&mut wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result.get_items().clone())
        })
    }
    fn insert_from_lists<'py>(
        &self,
        py: Python<'py>,
        timestamps: Vec<DateTime<chrono::Utc>>,
        values: Vec<f64>,
        ts: Identifiable,
    ) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        let datapoints: Vec<DatapointString> = timestamps
            .into_iter()
            .zip(values.into_iter())
            .map(|(timestamp, value)| DatapointString {
                timestamp: timestamp.timestamp_millis().to_string(),
                value: value.to_string(),
            })
            .collect();
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
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.insert_datapoints(&mut wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            Ok(result.get_items().clone())
        })
    }

    fn retrieve_datapoints<'py>(
        &self,
        py: Python<'py>,
        input: PyRetrieveFilter,
    ) -> PyResult<Vec<PyDatapointsCollectionDatapoints>> {
        let service = self.api_service.clone();
        let wrapper = DataWrapper::<RetrieveFilter>::from_vec(vec![input.into()]);
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.retrieve_datapoints(&wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;
            let result: Vec<PyDatapointsCollectionDatapoints> = result
                .get_items()
                .into_iter()
                .map(|ts| PyDatapointsCollectionDatapoints { inner: ts.clone() })
                .collect();
            Ok(result)
        })
    }
    fn delete_datapoints<'py>(&self, py: Python<'py>, input: Vec<PyDeleteFilter>) -> PyResult<()> {
        let service = self.api_service.clone();
        let wrapper =
            DataWrapper::<DeleteFilter>::from_vec(input.into_iter().map(|f| f.into()).collect());
        let result = py.detach(|| {
            self.runtime
                .block_on(service.time_series.delete_datapoints(&wrapper))
        });

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        Ok(())
    }
    fn retrieve_latest_datapoints<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyTimeseriesIdentifiable>,
    ) -> PyResult<Vec<PyDatapointsCollectionDatapoints>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(Into::into)
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);
        let result = py.detach(|| {
            self.runtime
                .block_on(service.time_series.retrieve_latest_datapoint(&wrapper))
        });

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        let res: Vec<PyDatapointsCollectionDatapoints> = result
            .get_items()
            .into_iter()
            .map(|ts| PyDatapointsCollectionDatapoints::from(ts.clone()))
            .collect();
        Ok(res)
    }
}
