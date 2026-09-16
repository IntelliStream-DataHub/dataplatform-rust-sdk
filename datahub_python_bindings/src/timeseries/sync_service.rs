use super::*;
use crate::datetime::py_datetime_to_utc;
use crate::timeseries::datapoints::{
    PyDatapointsCollectionDatapoints, PyDatapointsCollectionString,
};
use crate::{DatahubIdentity, Identifiable};
use crate::{PyIdCollection, PyRetrieveFilter};
use intellistream_datahub_sdk::generic::{DataWrapper, IdAndExtId};
use intellistream_datahub_sdk::{ApiService, TimeSeriesUpdateCollection};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "TimeSeriesServiceSync")]
pub struct PyTimeSeriesServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyTimeSeriesServiceSync {
    /// The first `limit` series in the tenant, newest created first. `limit` defaults to the
    /// server's 1000 and may not exceed 10000; there is no paging, so a bigger tenant is truncated
    /// rather than paged — use `filter` to narrow instead.
    #[pyo3(signature = (limit=None))]
    fn list(&self, py: Python<'_>, limit: Option<u64>) -> PyResult<Vec<PyTimeSeries>> {
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.list(limit))
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .cloned()
                .map(|ts| PyTimeSeries::with_client(ts, service.clone()))
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
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries::with_client(ts.clone(), service.clone()))
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
        let wrapper = DataWrapper::from_vec(input_ids);

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.by_ids(&wrapper))
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|u| PyTimeSeries::with_client(u.clone(), service.clone()))
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
        let wrapper = DataWrapper::from_vec(input_ids);

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.delete(&wrapper))
                .map_err(|e| crate::datahub_err(e))?;

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
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }
    #[pyo3(signature = (query, filter = None, limit = None))]
    fn search<'p>(
        &self,
        py: Python<'p>,
        query: String,
        filter: Option<crate::PyTimeSeriesFilter>,
        limit: Option<u64>,
    ) -> PyResult<Vec<PyTimeSeries>> {
        let form = crate::search_form(query, filter.map(|f| f.inner), limit);
        let service = self.api_service.clone();

        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.search(&form))
                .map_err(|e| crate::datahub_err(e))?;
            let py_ts: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, source=None,
                        labels=None, metadata=None, created_time=None, last_updated_time=None,
                        data_set_id=None, unit=None, unit_external_id=None, value_type=None,
                        limit=None, sort_by=None, sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter<'p>(
        &self,
        py: Python<'p>,
        filter: Option<crate::PyTimeSeriesFilter>,
        id: Option<Vec<u64>>,
        external_id: Option<crate::StringOrList>,
        name: Option<crate::StringOrList>,
        source: Option<crate::StringOrList>,
        labels: Option<crate::StringOrList>,
        metadata: Option<std::collections::HashMap<String, Option<String>>>,
        created_time: Option<crate::events::PyTimeFilter>,
        last_updated_time: Option<crate::events::PyTimeFilter>,
        data_set_id: Option<Vec<crate::DataSetRef>>,
        unit: Option<crate::StringOrList>,
        unit_external_id: Option<crate::StringOrList>,
        value_type: Option<crate::StringOrList>,
        limit: Option<u64>,
        sort_by: Option<crate::StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<crate::PyPage> {
        let form = crate::timeseries_filter_form(
            filter, id, external_id, name, source, labels, metadata, created_time,
            last_updated_time, data_set_id, unit, unit_external_id, value_type, limit, sort_by,
            sort_order, cursor,
        )?;
        let service = self.api_service.clone();

        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.filter(&form))
                .map_err(|e| crate::datahub_err(e))?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyTimeSeries> = result
                .get_items()
                .iter()
                .map(|ts| PyTimeSeries::with_client(ts.clone(), service.clone()))
                .collect();
            Ok::<_, pyo3::PyErr>((items, next_cursor))
        })?;
        crate::PyPage::new(py, items, next_cursor)
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
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result.get_items().clone())
        })
    }
    /// `POST /timeseries/data/binary`: the same collections as `insert_datapoints`, sent as
    /// zstd-compressed Arrow frames. `zstd_level` is 1, 3 or 9 and defaults to 9.
    #[pyo3(signature = (input, zstd_level=None))]
    fn insert_datapoints_binary<'py>(
        &self,
        py: Python<'py>,
        input: Vec<PyDatapointsCollectionString>,
        zstd_level: Option<i32>,
    ) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        let vec: Vec<DatapointsCollection<DatapointString>> =
            input.into_iter().map(|item| item.into()).collect();
        let wrapper = DataWrapper::<DatapointsCollection<DatapointString>>::from_vec(vec);
        let options = binary_options(zstd_level);
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.insert_datapoints_binary(&wrapper, &options))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result.get_items().clone())
        })
    }

    /// The binary twin of `insert_from_lists`: parallel timestamp and value sequences for one
    /// series, which is the shape a DataFrame column pair arrives in.
    #[pyo3(signature = (timestamps, values, ts, zstd_level=None))]
    fn insert_from_lists_binary<'py>(
        &self,
        py: Python<'py>,
        timestamps: Vec<Bound<'py, PyAny>>,
        values: Vec<f64>,
        ts: Identifiable,
        zstd_level: Option<i32>,
    ) -> PyResult<Vec<String>> {
        let service = self.api_service.clone();
        let collection = lists_to_collection(timestamps, values, ts)?;
        let wrapper = DataWrapper::<DatapointsCollection<DatapointString>>::from_vec(vec![collection]);
        let options = binary_options(zstd_level);
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.insert_datapoints_binary(&wrapper, &options))
                .map_err(|e| crate::datahub_err(e))?;
            Ok(result.get_items().clone())
        })
    }

    fn insert_from_lists<'py>(
        &self,
        py: Python<'py>,
        timestamps: Vec<Bound<'py, PyAny>>,
        values: Vec<f64>,
        ts: Identifiable,
    ) -> PyResult<Vec<String>> {
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
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.time_series.insert_datapoints(&mut wrapper))
                .map_err(|e| crate::datahub_err(e))?;
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
                .map_err(|e| crate::datahub_err(e))?;
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

        let result = result.map_err(|e| crate::datahub_err(e))?;

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
        let wrapper = DataWrapper::from_vec(input_ids);
        let result = py.detach(|| {
            self.runtime
                .block_on(service.time_series.retrieve_latest_datapoint(&wrapper))
        });

        let result = result.map_err(|e| crate::datahub_err(e))?;

        let res: Vec<PyDatapointsCollectionDatapoints> = result
            .get_items()
            .into_iter()
            .map(|ts| PyDatapointsCollectionDatapoints::from(ts.clone()))
            .collect();
        Ok(res)
    }
}
