use super::*;
use pyo3::exceptions::PyException;
use std::sync::Arc;
use pyo3_async_runtimes::tokio::future_into_py;
use dataplatform_rust_sdk::{ApiService, TimeSeriesUpdateCollection};
use dataplatform_rust_sdk::generic::{DataWrapper, IdAndExtId, IdAndExtIdCollection};
use crate::{PyIdCollection, PyRetrieveFilter, PySearchAndFilterForm};

#[pyclass]
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
    

    fn create<'p>(&self, py: Python<'p>,input: Vec<PyTimeSeries>) -> PyResult<Vec<PyTimeSeries>> {
        let timeseries = input.iter().cloned().map(TimeSeries::from).collect();
        let payload = DataWrapper::from_vec(timeseries);
        let service = self.api_service.clone();
        py.detach(||{ 
 let result = self.runtime.block_on(service.time_series.create(&payload)).map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'p>(&self, py: Python<'p>, input: Vec<PyIdCollection>) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        py.detach(||{
 let result = self.runtime.block_on(service.time_series.by_ids(&wrapper))
                .map_err(|e| PyException::new_err(e.get_message()))?;

            let py_units: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|u| PyTimeSeries { inner: u.clone() })
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'p>(&self, py: Python<'p>,input: Vec<PyIdCollection>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);

        py.detach(||{ 
 let result = self.runtime.block_on(service.time_series.delete(&wrapper)).map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            Ok(())
        })
    }
    fn update<'p>(&self, py: Python<'p>,input: Vec<PyTimeSeriesUpdate>) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();
        let input = input.iter().cloned().map(TimeSeriesUpdate::from).collect();
        let wrapper =TimeSeriesUpdateCollection::from_vec(input);

        py.detach(||{ 
 let result = self.runtime.block_on(service.time_series.update(&wrapper)).map_err(|e| {
                PyException::new_err(e.get_message())
            })?;

            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }
    fn search<'p>(&self, py: Python<'p>, input: PySearchAndFilterForm) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();

        py.detach(||{ 
 let result = self.runtime.block_on(service.time_series.search(&input.into())).map_err(|e| {
                PyException::new_err(e.get_message())
            })?;
            let py_ts: Vec<PyTimeSeries> = result.get_items().iter().map(|ts| PyTimeSeries { inner: ts.clone() }).collect();
            Ok(py_ts)
        })
    }

    fn insert_datapoints<'py>(&self, py: Python<'py>, input: Vec<PyDatapointsCollectionString> )-> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        let vec: Vec<DatapointsCollection<DatapointString>> = input.into_iter().map(|item| item.into()).collect();
        let mut wrapper = DataWrapper::<DatapointsCollection<DatapointString>>::from_vec(vec);
        py.detach(||{ 
 let result = self.runtime.block_on(service.time_series.insert_datapoints(&mut wrapper)).map_err(|e| {
                PyException::new_err(e.get_message())
            })?;
            Ok(result.get_items().clone())
        })
    }
    fn retrieve_datapoints<'py>(&self,  py: Python<'py>, input: PyRetrieveFilter )-> PyResult<Vec<PyDatapointsCollectionDatapoints>> {
        let service = self.api_service.clone();
        let wrapper = DataWrapper::<RetrieveFilter>::from_vec(vec![input.into()]);
        py.detach(||{ 
 let result = self.runtime.block_on(service.time_series.retrieve_datapoints(&wrapper)).map_err(|e| {
                PyException::new_err(e.get_message())
            })?;
            let result: Vec<PyDatapointsCollectionDatapoints> = result.get_items().into_iter().map(|ts| PyDatapointsCollectionDatapoints { inner: ts.clone() }).collect();
            Ok(result)
        })
    }
    fn delete_datapoints<'py>(&self,  py: Python<'py>, input: Vec<PyDeleteFilter> )-> PyResult<()> {
        let service = self.api_service.clone();
        let wrapper = DataWrapper::<DeleteFilter>::from_vec(input.into_iter().map(|f|f.into()).collect());
        let result = py.detach(|| {
            self.runtime
                .block_on(service.time_series.delete_datapoints(&wrapper))
        });

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        Ok(())
        
    }
    fn retrieve_latest_datapoints<'py>(&self, py: Python<'py>, input: Vec<PyIdCollection> ) -> PyResult<PyDatapointsCollectionDatapoints> {
        let service = self.api_service.clone();
        let input_ids = input
            .iter()
            .map(|u| u.inner.clone())
            .collect::<Vec<IdAndExtId>>();
        let wrapper = IdAndExtIdCollection::from_id_and_ext_id_vec(input_ids);
        let result = py.detach(|| {
            self.runtime
                .block_on(service.time_series.retrieve_latest_datapoint(&wrapper))
        });

        let result = result.map_err(|e| PyException::new_err(e.get_message()))?;

        let datapoint = result
            .get_items()
            .into_iter()
            .map(|ts| PyDatapointsCollectionDatapoints { inner: ts.clone() })
            .next()
            .ok_or_else(|| PyException::new_err("No datapoints returned"))?;
        Ok(datapoint)
    }
}