use crate::datasets::{
    DatasetIdentifiable, PyDataset, PyDatasetFilter, PyDatasetUpdate,
};
use crate::resources::PyResource;
use crate::{DatahubIdentity, Identifiable, PyIdCollection, PySearchAndFilterForm};
use intellistream_datahub_sdk::ApiService;
use intellistream_datahub_sdk::datasets::{Dataset, DatasetUpdate};
use intellistream_datahub_sdk::generic::{DataWrapper, IdAndExtId};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use pyo3_async_runtimes::tokio::future_into_py;
use std::sync::Arc;

#[pyclass(module = "intellistream_datahub_sdk", name = "DatasetsServiceAsync")]
pub struct PyDatasetsServiceAsync {
    pub api_service: Arc<ApiService>,
}

#[pymethods]
impl PyDatasetsServiceAsync {
    #[pyo3(signature = (limit = None))]
    fn list<'p>(&self, py: Python<'p>, limit: Option<u64>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .datasets
                .list(limit)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|ts| PyDataset::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    fn create<'p>(&self, py: Python<'p>, input: Vec<PyDataset>) -> PyResult<Bound<'p, PyAny>> {
        let datasets: Vec<Dataset> = input.iter().cloned().map(Dataset::from).collect();
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .datasets
                .create(&datasets)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|ts| PyDataset::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    fn by_ids<'p>(
        &self,
        py: Python<'p>,
        input: Vec<DatasetIdentifiable>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .datasets
                .by_ids(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_units: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|u| PyDataset::with_client(u.clone(), service.clone()))
                .collect();
            Ok(py_units)
        })
    }
    fn delete<'p>(
        &self,
        py: Python<'p>,
        input: Vec<DatasetIdentifiable>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        future_into_py(py, async move {
            let result = service
                .datasets
                .delete(&input_ids)
                .await
                .map_err(|e| crate::datahub_err(e))?;

            let py_ts: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|ts| PyDataset::with_client(ts.clone(), service.clone()))
                .collect();
            Ok(py_ts)
        })
    }

    /// Datasets matching every criterion on the filter, newest first.
    fn filter<'p>(&self, py: Python<'p>, input: PyDatasetFilter) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .datasets
                .filter(&input.into())
                .await
                .map_err(crate::datahub_err)?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|d| PyDataset::with_client(d.clone(), service.clone()))
                .collect();
            Python::attach(|py| crate::PyPage::new(py, items, next_cursor))
        })
    }

    /// Full-text search over a dataset's name, external id and description at once. The last term
    /// is a prefix match, so this works from a search box mid-word.
    ///
    /// Results are **not** ranked — do not read the first item as the best match. `limit` caps the
    /// result at 1000, and no match is an empty list rather than an error.
    ///
    /// `query` must be 3–140 characters *and* Latin letters, spaces or digits only
    /// (`^[\p{IsLatin}\p{Zs}\p{Nd}]+`). An underscore is rejected with a 400, so an external id
    /// is usually not a legal query even though the index covers it — search on words, and use
    /// `filter`'s `external_id` (a trailing `*` is a prefix search) to look up by id. The
    /// `filter` argument is declared by the endpoint and **ignored server-side** today.
    #[pyo3(signature = (query, limit = None, filter = None))]
    fn search<'p>(
        &self,
        py: Python<'p>,
        query: &str,
        limit: Option<u64>,
        filter: Option<crate::datasets::PyBasicDatasetFilter>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let form = crate::datasets::dataset_search_form(query, limit, filter);
        future_into_py(py, async move {
            let result = service
                .datasets
                .search(&form)
                .await
                .map_err(crate::datahub_err)?;
            let py_ds: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|d| PyDataset::with_client(d.clone(), service.clone()))
                .collect();
            Ok(py_ds)
        })
    }

    /// Apply partial updates, returning the datasets as they stand afterwards.
    ///
    /// A dataset is the unit access is granted on, so the server treats editing one as an operator
    /// action: this needs an all-datasets write grant and raises 403 without one, even for a
    /// caller who can write the dataset's contents.
    ///
    /// **Do not combine a `metadata` change with `write_protected` / `deactivated` in one update.**
    /// The server stores those flags as node metadata, so setting either in the same call as a
    /// metadata delta silently drops the delta — 200, no error, half the change lost. Send two
    /// updates. Their keys (`property:is_write_protected`, `property:is_deactivated`) are also
    /// visible in `Dataset.metadata`.
    fn update<'p>(
        &self,
        py: Python<'p>,
        input: Vec<PyDatasetUpdate>,
    ) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        let updates: Vec<DatasetUpdate> = input.into_iter().map(DatasetUpdate::from).collect();
        future_into_py(py, async move {
            let result = service
                .datasets
                .update(&updates)
                .await
                .map_err(crate::datahub_err)?;
            let py_ds: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|d| PyDataset::with_client(d.clone(), service.clone()))
                .collect();
            Ok(py_ds)
        })
    }

    /// The access policies a dataset can be associated with, as `Resource`s.
    ///
    /// **Known to come back empty even when policies exist** — the server answers 200 with no body
    /// at all. That is a server-side bug, not something these bindings can work around, so treat
    /// an empty result as "unknown" rather than "none".
    fn policies<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
        let service = self.api_service.clone();
        future_into_py(py, async move {
            let result = service
                .datasets
                .policies()
                .await
                .map_err(crate::datahub_err)?;
            let py_res: Vec<PyResource> = result
                .get_items()
                .iter()
                .map(|r| PyResource::with_client(r.clone(), service.clone()))
                .collect();
            Ok(py_res)
        })
    }
}
