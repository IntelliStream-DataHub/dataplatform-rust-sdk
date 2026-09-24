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

/// The blocking `/timeseries` surface: the series definitions, and the datapoints behind them.
///
/// Reached as `client.timeseries`. Two halves worth keeping apart — `create` / `by_ids` /
/// `list` / `filter` / `search` / `update` / `delete` operate on the *definitions*, while
/// `insert_datapoints` / `insert_from_lists` / `retrieve_datapoints` /
/// `retrieve_latest_datapoints` / `delete_datapoints` operate on the values.
///
/// Datapoints land in ClickHouse and settle after the call returns, so a read straight after a
/// write can come back short. Poll rather than assert once.
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

    /// Create series definitions, returning the server's echo of them.
    ///
    /// The echoed objects carry a client, so `neighbors()` and the other navigation methods work
    /// on them — locally built ones raise instead. A duplicate `external_id` is a **409**.
    ///
    /// This creates the definition only; the datapoints go in separately with
    /// `insert_from_lists` or `insert_datapoints`.
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

    /// Series by id or external id — a bare `int` is an id, a bare `str` an external id, and a
    /// `TimeSeries` or `IdCollection` may carry both.
    ///
    /// Batch lookups answer with the subset that was found, so a shorter list back is the normal
    /// way an unknown id is reported. Match on `external_id` rather than on position.
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
    /// Delete series definitions **and their datapoints**. Returns `None`.
    ///
    /// The definition is gone when the call returns; the datapoints are purged afterwards.
    /// Nothing can read them in between, because every read resolves the series first.
    ///
    /// Remove any subscription or edge pointing at the series first — the api refuses to strand
    /// one. To empty a series but keep its definition, edges and subscriptions, use
    /// `delete_datapoints` with both bounds left as `None`.
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
    /// Apply partial updates, returning the series as they stand afterwards.
    ///
    /// Each `TimeSeriesUpdate` names one series and carries only the fields to change; anything
    /// it leaves out is untouched.
    ///
    /// **There is no `value_type` on the update form.** A series' storage type is fixed at
    /// creation — re-typing it would invalidate the datapoints already stored. Create a new
    /// series and re-ingest instead.
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
    /// Free-text search over series names and descriptions, best match first.
    ///
    /// `filter` takes the same criteria as `filter()` and only ever *removes* hits from the
    /// phrase's — it cannot widen them, so omitting it returns them as found. `query` is
    /// required, 3–140 characters.
    ///
    /// `limit` defaults to **100** and caps at **1000**; the `filter` endpoints use 1000/10000,
    /// which is easy to conflate.
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
    /// Series matching every criterion, newest created first.
    ///
    /// Pass either a prepared `filter=` object or the individual criteria keywords — passing
    /// both is a `TypeError`. Paging (`limit`, `sort_by`, `sort_order`, `cursor`) always lives
    /// on the call rather than on the filter, so one filter can be reused across `filter()` and
    /// `search()` without carrying a stale cursor.
    ///
    /// Returns a `Page`: list-like, plus `.next_cursor`, which is `None` on the last page. A
    /// *full* page may still be the last, so a walk ends with one empty request.
    ///
    /// `limit` defaults to 1000 and caps at 10000 — above that is a 400, not a clamp.
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

    /// Write datapoints, one `DatapointsCollectionString` per series.
    ///
    /// **Returns an empty list, always** — the api answers a successful write with 204 and no
    /// body, and a buffered write with 202 and no body. So the return value tells you nothing;
    /// what tells you the write failed is the exception.
    ///
    /// Large batches are cut into chunks of at most 100 000 datapoints and sent concurrently. A
    /// chunk that is refused raises, and the chunks after it are not sent — so a failed call can
    /// leave part of the batch written. Re-sending is safe: datapoints are keyed by
    /// `(series, timestamp)` and a repeat replaces rather than duplicates.
    ///
    /// **With buffering enabled** (see `DataHubClient`), a write that cannot get through spools
    /// to disk and returns normally. Nothing in the return value distinguishes that from a
    /// confirmed write, and 401/403 are buffered too, so an expired credential also looks like
    /// success here.
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

    /// Write datapoints for a single series from parallel `timestamps` and `values` lists — the
    /// shape a DataFrame column pair arrives in.
    ///
    /// `ts` names the series by external id, numeric id, or a `TimeSeries`. Timestamps must be
    /// timezone-aware; a naive one raises `TypeError`.
    ///
    /// **The two lists are zipped, and a length mismatch is not an error** — the tail of the
    /// longer one is silently dropped. Check `len(timestamps) == len(values)` yourself. (The
    /// binary twin, `insert_from_lists_binary`, does raise.)
    ///
    /// Otherwise identical to `insert_datapoints`, including the empty return and the buffering
    /// behaviour.
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

    /// Read datapoints for **one** series.
    ///
    /// Takes a single `RetrieveFilter`, not a list, and answers with a list holding at most one
    /// collection — so the idiom is `client.timeseries.retrieve_datapoints(rf)[0]`, and an
    /// unmatched read can give you an empty list.
    ///
    /// **The window is half-open: `start` is included, `end` is excluded.** This is the opposite
    /// of `TimeFilter`, which backs `created_time` / `last_updated_time` / `event_time` and is
    /// inclusive at both ends. Two different idioms in one SDK, deliberately, so a window written
    /// for one is wrong for the other.
    ///
    /// Asking for `aggregates` changes what comes back: the datapoints then carry `min`, `max`
    /// and `average`, and `value` is `None`. `as_dict()` reads only `value`, so use
    /// `get_datapoints()` for an aggregate read.
    ///
    /// Page with the collection's `next_cursor`, fed back as the next `RetrieveFilter`'s
    /// `cursor`; it is `None` when the result fit in one page.
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
    /// Remove datapoints from one or more series, a window at a time. Returns `None`.
    ///
    /// Takes a **list** of `DeleteFilter` — note the asymmetry with `retrieve_datapoints`, which
    /// takes one. Each filter names a series and a half-open window; leaving both bounds `None`
    /// clears every datapoint of that series while keeping its definition, edges and
    /// subscriptions, which is how a bad backfill is undone.
    ///
    /// An item naming a series that does not exist fails the **whole** request with a 400.
    ///
    /// **Accepted is not done, and none of it can be undone.** The call returns once the request
    /// is accepted; the purge runs afterwards, so a read straight after can still see the rows.
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
    /// The most recent datapoint of each named series, one collection per series.
    ///
    /// Series are named by external id, numeric id, `TimeSeries` or `IdCollection`. This becomes
    /// readable sooner after a write than a range read does, which makes it the usual way to
    /// check whether an ingest landed at all.
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
