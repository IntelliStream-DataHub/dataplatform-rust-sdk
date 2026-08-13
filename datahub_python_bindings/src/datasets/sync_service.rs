use crate::datasets::{
    DatasetIdentifiable, PyDataset, PyDatasetFilter, PyDatasetUpdate,
};
use crate::resources::PyResource;
use crate::{PyIdCollection, PySearchAndFilterForm};
use dataplatform_rust_sdk::datasets::{Dataset, DatasetUpdate};
use dataplatform_rust_sdk::generic::IdAndExtId;
use dataplatform_rust_sdk::{ApiService, Resource};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

#[pyclass(module = "datahub_sdk", name = "DatasetsServiceSync")]
pub struct PyDatasetsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyDatasetsServiceSync {
    fn create<'py>(&self, py: Python<'py>, input: Vec<PyDataset>) -> PyResult<Vec<PyDataset>> {
        let datasets: Vec<Dataset> = input.iter().cloned().map(Dataset::from).collect();
        let service = self.api_service.clone();
        let result = py.detach(|| self.runtime.block_on(service.datasets.create(&datasets)));

        let result = result.map_err(|e| crate::datahub_err(e))?;

        let py_res: Vec<PyDataset> = result
            .get_items()
            .iter()
            .map(|ts| PyDataset::with_client(ts.clone(), service.clone()))
            .collect();
        Ok(py_res)
    }

    fn by_ids<'py>(
        &self,
        py: Python<'py>,
        input: Vec<DatasetIdentifiable>,
    ) -> PyResult<Vec<PyDataset>> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        let result = py.detach(|| self.runtime.block_on(service.datasets.by_ids(&input_ids)));

        let result = result.map_err(|e| crate::datahub_err(e))?;

        let py_res: Vec<PyDataset> = result
            .get_items()
            .iter()
            .map(|ts| PyDataset::with_client(ts.clone(), service.clone()))
            .collect();
        Ok(py_res)
    }
    fn delete<'py>(&self, py: Python<'py>, input: Vec<DatasetIdentifiable>) -> PyResult<()> {
        let service = self.api_service.clone();
        let input_ids = input
            .into_iter()
            .map(IdAndExtId::from)
            .collect::<Vec<IdAndExtId>>();

        py.detach(|| {
            self.runtime
                .block_on(service.datasets.delete(&input_ids))
                .map_err(|e| crate::datahub_err(e))
        })?;
        Ok(())
    }

    /// Datasets in the tenant, newest first. `limit` defaults to the server's 100 and may not
    /// exceed 10000; there is no paging, so a bigger tenant is truncated rather than paged —
    /// use `filter` to narrow instead.
    #[pyo3(signature = (limit = None))]
    fn list(&self, py: Python<'_>, limit: Option<u64>) -> PyResult<Vec<PyDataset>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.datasets.list(limit))
                .map_err(crate::datahub_err)?;
            let py_ds: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|d| PyDataset::with_client(d.clone(), service.clone()))
                .collect();
            Ok(py_ds)
        })
    }

    /// Datasets matching every criterion on the filter, newest first.
    fn filter(&self, py: Python<'_>, input: PyDatasetFilter) -> PyResult<crate::PyPage> {
        let service = self.api_service.clone();
        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.datasets.filter(&input.into()))
                .map_err(crate::datahub_err)?;
            let next_cursor = result.next_cursor().map(str::to_string);
            let items: Vec<PyDataset> = result
                .get_items()
                .iter()
                .map(|d| PyDataset::with_client(d.clone(), service.clone()))
                .collect();
            Ok::<_, pyo3::PyErr>((items, next_cursor))
        })?;
        crate::PyPage::new(py, items, next_cursor)
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
    /// `filter`'s `external_id` (a trailing `*` is a prefix search) to look up by id.
    ///
    /// The `filter` argument is declared by the endpoint and **ignored server-side** today; use
    /// `filter()` for criteria.
    #[pyo3(signature = (query, limit = None, filter = None))]
    fn search(
        &self,
        py: Python<'_>,
        query: &str,
        limit: Option<u64>,
        filter: Option<crate::datasets::PyBasicDatasetFilter>,
    ) -> PyResult<Vec<PyDataset>> {
        let service = self.api_service.clone();
        let form = crate::datasets::dataset_search_form(query, limit, filter);
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.datasets.search(&form))
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
    fn update(&self, py: Python<'_>, input: Vec<PyDatasetUpdate>) -> PyResult<Vec<PyDataset>> {
        let service = self.api_service.clone();
        let updates: Vec<DatasetUpdate> = input.into_iter().map(DatasetUpdate::from).collect();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.datasets.update(&updates))
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
    fn policies(&self, py: Python<'_>) -> PyResult<Vec<PyResource>> {
        let service = self.api_service.clone();
        py.detach(|| {
            let result = self
                .runtime
                .block_on(service.datasets.policies())
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
