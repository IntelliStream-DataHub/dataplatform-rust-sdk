use crate::datasets::{
    DatasetIdentifiable, PyDataset, PyDatasetUpdate,
};
use crate::resources::PyResource;
use crate::{PyIdCollection};
use intellistream_datahub_sdk::datasets::{Dataset, DatasetUpdate};
use intellistream_datahub_sdk::generic::IdAndExtId;
use intellistream_datahub_sdk::{ApiService, Resource};
use pyo3::{Bound, PyAny, PyResult, Python, pyclass, pymethods};
use std::sync::Arc;

/// The blocking `/datasets` surface.
///
/// Reached as `client.datasets`.
#[pyclass(module = "intellistream_datahub_sdk", name = "DatasetsServiceSync")]
pub struct PyDatasetsServiceSync {
    pub api_service: Arc<ApiService>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

#[pymethods]
impl PyDatasetsServiceSync {
    /// Create data sets, returning the echo with the server-assigned `id`s.
    ///
    /// A `data_set_id` on the input is silently dropped. Build a hierarchy with an explicit
    /// `BELONGS_TO` edge; `connected_data_sets` does not create one.
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

    /// Data sets by id or external id — a bare `int` is an id, a bare `str` an external id, and
    /// a `Dataset` or `IdCollection` may carry both.
    ///
    /// Silently omits what it cannot find rather than raising.
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
    /// Delete data sets. Returns `None`.
    ///
    /// **Does not cascade**, and is refused while anything still belongs to the data set. Delete
    /// or re-home the contents first.
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

    /// Datasets in the tenant, newest first. `limit` defaults to 1000 and may not exceed 10000. No
    /// paging; narrow with `filter`.
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
    #[pyo3(signature = (filter=None, id=None, external_id=None, name=None, source=None,
                        labels=None, metadata=None, created_time=None, last_updated_time=None,
                        limit=None, sort_by=None, sort_order=None, cursor=None))]
    #[allow(clippy::too_many_arguments)]
    fn filter(
        &self,
        py: Python<'_>,
        filter: Option<crate::datasets::PyDatasetFilter>,
        id: Option<Vec<u64>>,
        external_id: Option<crate::StringOrList>,
        name: Option<crate::StringOrList>,
        source: Option<crate::StringOrList>,
        labels: Option<crate::StringOrList>,
        metadata: Option<std::collections::HashMap<String, Option<String>>>,
        created_time: Option<crate::events::PyTimeFilter>,
        last_updated_time: Option<crate::events::PyTimeFilter>,
        limit: Option<u64>,
        sort_by: Option<crate::StringOrList>,
        sort_order: Option<String>,
        cursor: Option<String>,
    ) -> PyResult<crate::PyPage> {
        let form = crate::datasets::dataset_filter_form(
            filter, id, external_id, name, source, labels, metadata, created_time,
            last_updated_time, limit, sort_by, sort_order, cursor,
        )?;
        let service = self.api_service.clone();
        let (items, next_cursor) = py.detach(|| {
            let result = self
                .runtime
                .block_on(service.datasets.filter(&form))
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
    /// Results are ranked and tie-broken by id, so the first item is the best match and the order
    /// is stable. `limit` caps the result at 1000, and no match is an empty list rather than an
    /// error.
    ///
    /// `query` must be 3–140 characters; the Latin-letters-spaces-digits pattern that used to sit
    /// alongside that is gone, so an external id is a legal query. `filter` narrows the phrase's
    /// hits and never widens them.
    #[pyo3(signature = (query, filter = None, limit = None))]
    fn search(
        &self,
        py: Python<'_>,
        query: &str,
        filter: Option<crate::datasets::PyDatasetFilter>,
        limit: Option<u64>,
    ) -> PyResult<Vec<PyDataset>> {
        let service = self.api_service.clone();
        let form = crate::search_form(query.to_string(), filter.map(Into::into), limit);
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
    /// Needs an all-datasets write grant (**403** without), even for a caller who can write the
    /// dataset's contents.
    ///
    /// Settable: `external_id`, `name`, `description`, `metadata` and `labels`. Changing
    /// `external_id` to one already taken is a **409**.
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
    /// **Can come back empty even when policies exist** (a server bug), so an empty result means
    /// "unknown".
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
