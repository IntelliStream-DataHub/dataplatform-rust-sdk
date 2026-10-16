use std::sync::Arc;

use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use std::sync::Mutex;

use arrow_array::{
    ArrayRef, Decimal128Array, Float32Array, Float64Array, Int64Array,
    RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::StreamExt;
use intellistream_datahub_sdk::timeseries::{
    BinaryDatapointStream, BinaryDatapoints, BinaryValues, DatapointValueType,
};
use tokio::runtime::Runtime;
use pyo3::exceptions::{PyImportError, PyRuntimeError, PyStopIteration, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyList};

/// One series' rows from `retrieve_datapoints_binary`, as an Arrow table: `timestamp` and the
/// value columns of its type. Read it with anything that takes the Arrow PyCapsule protocol.
#[pyclass(module = "intellistream_datahub_sdk", name = "BinaryDatapoints", frozen)]
pub struct PyBinaryDatapoints {
    id: u64,
    external_id: String,
    value_type: DatapointValueType,
    aggregates: bool,
    batch: RecordBatch,
}

/// A series' `timestamp` and value columns, as Arrow arrays that take over its vectors.
fn columns(
    value_type: DatapointValueType,
    timestamps: Vec<i64>,
    values: BinaryValues,
) -> Result<(Vec<Field>, Vec<ArrayRef>), ArrowError> {
    let timestamp = Field::new(
        "timestamp",
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        false,
    );
    let mut fields = vec![timestamp];
    let mut columns: Vec<ArrayRef> =
        vec![Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC"))];
    let mut push = |name: &str, nullable: bool, array: ArrayRef| {
        fields.push(Field::new(name, array.data_type().clone(), nullable));
        columns.push(array);
    };
    match values {
        BinaryValues::Bigint(v) => push("value", false, Arc::new(Int64Array::from(v))),
        BinaryValues::Float(v) => push("value", false, Arc::new(Float64Array::from(v))),
        BinaryValues::Float32(v) => push("value", false, Arc::new(Float32Array::from(v))),
        BinaryValues::Decimal { unscaled, scale } => {
            let precision = if value_type == DatapointValueType::Decimal32 { 9 } else { 18 };
            let array = Decimal128Array::from(unscaled).with_precision_and_scale(precision, scale)?;
            push("value", false, Arc::new(array))
        }
        BinaryValues::Text(v) => push("value", false, Arc::new(StringArray::from(v))),
        BinaryValues::Mixed { numeric, text } => {
            push("value_numeric", true, Arc::new(Float64Array::from(numeric)));
            push("value_text", true, Arc::new(StringArray::from(text)));
        }
        BinaryValues::Aggregates { avg, min, max, sum } => {
            push("avg", false, Arc::new(Float64Array::from(avg)));
            push("min", false, Arc::new(Float64Array::from(min)));
            push("max", false, Arc::new(Float64Array::from(max)));
            push("sum", false, Arc::new(Float64Array::from(sum)));
        }
        other => {
            return Err(ArrowError::NotYetImplemented(format!(
                "binary values {other:?} have no Arrow mapping"
            )))
        }
    }
    Ok((fields, columns))
}

impl TryFrom<BinaryDatapoints> for PyBinaryDatapoints {
    type Error = ArrowError;

    fn try_from(series: BinaryDatapoints) -> Result<Self, ArrowError> {
        let aggregates = series.is_aggregates();
        let (fields, columns) = columns(series.value_type, series.timestamps, series.values)?;
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        Ok(Self {
            id: series.id,
            external_id: series.external_id,
            value_type: series.value_type,
            aggregates,
            batch,
        })
    }
}

/// One frame's rows as a batch of the long table: `timeseries_id` ahead of the series' own
/// columns. No external id column: as a dictionary column polars spent 0.8 s per 100M rows making
/// it categorical, more than the rest of the conversion; `external_ids` maps the ids instead.
fn long_batch(series: BinaryDatapoints) -> Result<RecordBatch, ArrowError> {
    let id = Arc::new(Int64Array::from(vec![series.id as i64; series.len()])) as ArrayRef;
    let (fields, columns) = columns(series.value_type, series.timestamps, series.values)?;
    let mut all_fields = vec![Field::new("timeseries_id", DataType::Int64, false)];
    all_fields.extend(fields);
    let mut all_columns = vec![id];
    all_columns.extend(columns);
    RecordBatch::try_new(Arc::new(Schema::new(all_fields)), all_columns)
}

#[pymethods]
impl PyBinaryDatapoints {
    #[getter]
    fn id(&self) -> u64 {
        self.id
    }

    #[getter]
    fn external_id(&self) -> &str {
        &self.external_id
    }

    /// The series' value type, as a timeseries read reports it: `float`, `bigint`, `text`, ...
    #[getter]
    fn value_type(&self) -> String {
        format!("{:?}", self.value_type).to_lowercase()
    }

    /// Whether the rows are buckets with `avg`, `min`, `max` and `sum` rather than values.
    #[getter]
    fn is_aggregates(&self) -> bool {
        self.aggregates
    }

    /// The column names, in order.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.batch.schema().fields().iter().map(|f| f.name().clone()).collect()
    }

    fn __len__(&self) -> usize {
        self.batch.num_rows()
    }

    fn __repr__(&self) -> String {
        format!(
            "BinaryDatapoints(external_id={:?}, value_type={:?}, rows={}{})",
            self.external_id,
            self.value_type(),
            self.batch.num_rows(),
            if self.aggregates { ", aggregates" } else { "" }
        )
    }

    /// The Arrow PyCapsule stream protocol. Each call is a new stream over the same buffers, so
    /// the object can be converted more than once. `requested_schema` is ignored, as the protocol
    /// allows: the columns come in their own types.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let reader = RecordBatchIterator::new(vec![Ok(self.batch.clone())], self.batch.schema());
        let stream = FFI_ArrowArrayStream::new(Box::new(reader));
        PyCapsule::new_with_value(py, stream, c"arrow_array_stream")
    }

    /// A `polars.DataFrame`, without copying. Needs polars.
    fn to_polars<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let polars = import(slf.py(), "polars", "to_polars")?;
        polars.call_method1("DataFrame", (slf,))
    }

    /// A `pandas.DataFrame`. Needs pandas and pyarrow, which pandas converts Arrow through.
    fn to_pandas<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let pyarrow = import(py, "pyarrow", "to_pandas")?;
        pyarrow.call_method1("table", (slf,))?.call_method0("to_pandas")
    }
}

/// A binary read as it arrives, from `retrieve_datapoints_binary_stream`. Single use: read it
/// either by iterating, one `BinaryDatapoints` per frame, or as an Arrow stream of the long table.
#[pyclass(module = "intellistream_datahub_sdk", name = "BinaryDatapointReader", frozen)]
pub struct PyBinaryDatapointReader {
    runtime: Arc<Runtime>,
    state: Mutex<ReaderState>,
    seen: Seen,
}

/// The series read so far, by id, in the order they first came.
type Seen = Arc<Mutex<Vec<(u64, String)>>>;

fn note(seen: &Seen, series: &BinaryDatapoints) {
    let mut seen = seen.lock().unwrap();
    if !seen.iter().any(|(id, _)| *id == series.id) {
        seen.push((series.id, series.external_id.clone()));
    }
}

enum ReaderState {
    Unread(BinaryDatapointStream),
    Iterating(BinaryDatapointStream),
    Done,
}

impl PyBinaryDatapointReader {
    pub(crate) fn new(runtime: Arc<Runtime>, stream: BinaryDatapointStream) -> Self {
        Self {
            runtime,
            state: Mutex::new(ReaderState::Unread(stream)),
            seen: Arc::default(),
        }
    }
}

#[pymethods]
impl PyBinaryDatapointReader {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// The next frame's rows: a chunk of one series, which may continue in the next item.
    fn __next__(&self, py: Python<'_>) -> PyResult<PyBinaryDatapoints> {
        let mut state = self.state.lock().unwrap();
        let mut stream = match std::mem::replace(&mut *state, ReaderState::Done) {
            ReaderState::Unread(stream) | ReaderState::Iterating(stream) => stream,
            ReaderState::Done => return Err(PyStopIteration::new_err(())),
        };
        let item = py.detach(|| self.runtime.block_on(stream.next()));
        match item {
            Some(Ok(part)) => {
                *state = ReaderState::Iterating(stream);
                note(&self.seen, &part);
                PyBinaryDatapoints::try_from(part).map_err(|e| PyValueError::new_err(e.to_string()))
            }
            Some(Err(error)) => Err(crate::datahub_err(error)),
            None => Err(PyStopIteration::new_err(())),
        }
    }

    /// `{timeseries_id: external_id}` for the series read so far, in the order they came: the key
    /// for the long table's `timeseries_id`. Complete once the reader is exhausted.
    #[getter]
    fn external_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let ids = PyDict::new(py);
        for (id, external_id) in self.seen.lock().unwrap().iter() {
            ids.set_item(id, external_id)?;
        }
        Ok(ids)
    }

    /// The Arrow PyCapsule stream protocol, as one long table: `timeseries_id`, `timestamp` and
    /// the value columns, a batch per frame. Every series in the read has to
    /// share those columns; one that does not is an error at its batch. A read with no rows has
    /// no schema to declare and raises.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let mut stream = {
            let mut state = self.state.lock().unwrap();
            match std::mem::replace(&mut *state, ReaderState::Done) {
                ReaderState::Unread(stream) => stream,
                other => {
                    *state = other;
                    return Err(PyRuntimeError::new_err(
                        "this reader has already been read; a binary read streams once",
                    ));
                }
            }
        };
        let first = match py.detach(|| self.runtime.block_on(stream.next())) {
            Some(Ok(part)) => {
                note(&self.seen, &part);
                long_batch(part).map_err(|e| PyValueError::new_err(e.to_string()))?
            }
            Some(Err(error)) => return Err(crate::datahub_err(error)),
            None => {
                return Err(PyValueError::new_err(
                    "the read returned no rows, so there is no schema for an Arrow stream",
                ))
            }
        };
        let reader = LongTable {
            schema: first.schema(),
            first: Some(first),
            stream,
            runtime: self.runtime.clone(),
            seen: self.seen.clone(),
        };
        let ffi = FFI_ArrowArrayStream::new(Box::new(reader));
        PyCapsule::new_with_value(py, ffi, c"arrow_array_stream")
    }

    /// A `polars.DataFrame`: the long table, or with `wide` one column per series, matched on
    /// timestamp. Needs polars. Reads the reader, like any other use of it.
    #[pyo3(signature = (wide=false))]
    fn to_polars<'py>(slf: &Bound<'py, Self>, wide: bool) -> PyResult<Bound<'py, PyAny>> {
        tables_to_polars(slf.as_any(), wide)
    }
}

fn tables_to_polars<'py>(read: &Bound<'py, PyAny>, wide: bool) -> PyResult<Bound<'py, PyAny>> {
    read.py()
        .import("intellistream_datahub_sdk._tables")?
        .call_method1("to_polars", (read, wide))
}

/// A buffered binary read, from `retrieve_datapoints_binary`: the series it returned, one
/// `BinaryDatapoints` each, which it behaves as a list of, and the same tables the streamed
/// reader gives.
#[pyclass(module = "intellistream_datahub_sdk", name = "BinaryReadResult", frozen, sequence)]
pub struct PyBinaryReadResult {
    series: Vec<Py<PyBinaryDatapoints>>,
}

impl PyBinaryReadResult {
    pub(crate) fn new(py: Python<'_>, series: Vec<BinaryDatapoints>) -> PyResult<Self> {
        let series = series
            .into_iter()
            .map(|s| {
                let s = PyBinaryDatapoints::try_from(s).map_err(|e| PyValueError::new_err(e.to_string()))?;
                Py::new(py, s)
            })
            .collect::<PyResult<_>>()?;
        Ok(Self { series })
    }
}

#[pymethods]
impl PyBinaryReadResult {
    fn __len__(&self) -> usize {
        self.series.len()
    }

    fn __getitem__(&self, py: Python<'_>, index: isize) -> PyResult<Py<PyBinaryDatapoints>> {
        let len = self.series.len() as isize;
        let at = if index < 0 { index + len } else { index };
        if at < 0 || at >= len {
            return Err(pyo3::exceptions::PyIndexError::new_err("series index out of range"));
        }
        Ok(self.series[at as usize].clone_ref(py))
    }

    fn __iter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        Ok(PyList::new(py, &self.series)?.into_any().try_iter()?.into_any())
    }

    fn __repr__(&self) -> String {
        let rows: usize = self.series.iter().map(|s| s.get().batch.num_rows()).sum();
        format!("BinaryReadResult(series={}, rows={rows})", self.series.len())
    }

    /// `{timeseries_id: external_id}` for the series in the read, in order: the key to the long
    /// table's `timeseries_id`.
    #[getter]
    fn external_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let ids = PyDict::new(py);
        for s in &self.series {
            ids.set_item(s.get().id, &s.get().external_id)?;
        }
        Ok(ids)
    }

    /// The Arrow PyCapsule stream protocol, as the long table: `timeseries_id`, `timestamp` and
    /// the value columns, a batch per series, which all have to share those columns. A read with
    /// no rows has no schema and raises.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let mut batches = Vec::with_capacity(self.series.len());
        for s in &self.series {
            let s = s.get();
            let id = Arc::new(Int64Array::from(vec![s.id as i64; s.batch.num_rows()])) as ArrayRef;
            let mut fields = vec![Arc::new(Field::new("timeseries_id", DataType::Int64, false))];
            fields.extend(s.batch.schema().fields().iter().cloned());
            let mut columns = vec![id];
            columns.extend(s.batch.columns().iter().cloned());
            let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            if let Some(first) = batches.first() {
                let first: &RecordBatch = first;
                if first.schema().fields() != batch.schema().fields() {
                    return Err(PyValueError::new_err(format!(
                        "series {} has columns {:?}, the table's are {:?}; series of different value \
                         types do not share one long table, but to_polars(wide=True) holds them",
                        s.external_id,
                        batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                        first.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                    )));
                }
            }
            batches.push(batch);
        }
        let Some(first) = batches.first() else {
            return Err(PyValueError::new_err(
                "the read returned no rows, so there is no schema for an Arrow stream",
            ));
        };
        let schema = first.schema();
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        PyCapsule::new_with_value(py, FFI_ArrowArrayStream::new(Box::new(reader)), c"arrow_array_stream")
    }

    /// A `polars.DataFrame`: the long table, or with `wide` one column per series, matched on
    /// timestamp. Needs polars.
    #[pyo3(signature = (wide=false))]
    fn to_polars<'py>(slf: &Bound<'py, Self>, wide: bool) -> PyResult<Bound<'py, PyAny>> {
        tables_to_polars(slf.as_any(), wide)
    }
}

/// The long table behind `__arrow_c_stream__`: the frame peeked for its schema, then the rest as
/// the consumer asks. Waits for the network with the GIL held, as the C stream gives no way to
/// release it; the reader belongs to the sync client.
struct LongTable {
    schema: SchemaRef,
    first: Option<RecordBatch>,
    stream: BinaryDatapointStream,
    runtime: Arc<Runtime>,
    seen: Seen,
}

impl Iterator for LongTable {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }
        let part = match self.runtime.block_on(self.stream.next())? {
            Ok(part) => part,
            Err(error) => {
                let message = format!("{}: {}", error.get_status(), error.get_message());
                return Some(Err(ArrowError::ExternalError(message.into())));
            }
        };
        note(&self.seen, &part);
        let external_id = part.external_id.clone();
        let batch = match long_batch(part) {
            Ok(batch) => batch,
            Err(error) => return Some(Err(error)),
        };
        if batch.schema().fields() != self.schema.fields() {
            return Some(Err(ArrowError::SchemaError(format!(
                "series {external_id} has columns {:?}, the stream's are {:?}; read series of \
                 different value types with to_polars(wide=True), or by iterating",
                batch.schema().fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
                self.schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>(),
            ))));
        }
        Some(Ok(batch))
    }
}

impl RecordBatchReader for LongTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

fn import<'py>(py: Python<'py>, module: &str, method: &str) -> PyResult<Bound<'py, PyModule>> {
    py.import(module).map_err(|_| {
        PyImportError::new_err(format!(
            "BinaryDatapoints.{method}() needs {module}; install it with `pip install {module}`"
        ))
    })
}



/// The table behind any object with `__arrow_c_stream__`, imported without copying: its batches,
/// and the names pandas records as its index columns.
pub(crate) fn import_table(table: &Bound<'_, PyAny>) -> PyResult<(Vec<RecordBatch>, Vec<String>)> {
    if !table.hasattr("__arrow_c_stream__")? {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "expected a table with __arrow_c_stream__: a polars or pandas DataFrame (pandas 2.2 or \
             later, with pyarrow), or a pyarrow Table or RecordBatchReader",
        ));
    }
    let capsule = table.call_method0("__arrow_c_stream__")?;
    let capsule = capsule.cast::<PyCapsule>()?;
    // Checks the capsule's name is arrow_array_stream before handing out the pointer.
    let pointer = capsule.pointer_checked(Some(c"arrow_array_stream"))?.cast::<FFI_ArrowArrayStream>();
    // Moves the stream out of the capsule and leaves it released, as the protocol requires.
    let reader = unsafe { ArrowArrayStreamReader::from_raw(pointer.as_ptr()) }
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let schema = reader.schema();
    let batches = reader
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let index = schema
        .metadata()
        .get("pandas")
        .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
        .and_then(|m| m.get("index_columns").cloned())
        .and_then(|c| c.as_array().cloned())
        .map(|c| c.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default();
    Ok((batches, index))
}

/// The timestamp column: as named, else the pandas index if it is a timestamp, else `timestamp`,
/// else the one timestamp column there is.
pub(crate) fn timestamp_column(schema: &Schema, named: Option<String>, index: &[String]) -> PyResult<String> {
    if let Some(name) = named {
        return Ok(name);
    }
    let is_time = |name: &str| {
        schema
            .field_with_name(name)
            .is_ok_and(|f| matches!(f.data_type(), DataType::Timestamp(_, _)))
    };
    if let [only] = index {
        if is_time(only) {
            return Ok(only.clone());
        }
    }
    if schema.field_with_name("timestamp").is_ok() {
        return Ok("timestamp".to_string());
    }
    let times: Vec<&str> = schema
        .fields()
        .iter()
        .filter(|f| matches!(f.data_type(), DataType::Timestamp(_, _)))
        .map(|f| f.name().as_str())
        .collect();
    match times[..] {
        [only] => Ok(only.to_string()),
        [] => Err(PyValueError::new_err(
            "the table has no timestamp column; name one with timestamp=, or give it a \
             timestamp index",
        )),
        _ => Err(PyValueError::new_err(format!(
            "the table has several timestamp columns ({}); name one with timestamp=",
            times.join(", ")
        ))),
    }
}

/// The batches without the pandas index columns that are not the timestamp: an index is not a
/// series.
pub(crate) fn without_index(batches: Vec<RecordBatch>, index: &[String], timestamp: &str) -> PyResult<Vec<RecordBatch>> {
    let Some(first) = batches.first() else { return Ok(batches) };
    let schema = first.schema();
    let keep: Vec<usize> = (0..schema.fields().len())
        .filter(|&i| {
            let name = schema.field(i).name();
            name == timestamp || !index.iter().any(|x| x == name)
        })
        .collect();
    if keep.len() == schema.fields().len() {
        return Ok(batches);
    }
    batches
        .iter()
        .map(|b| b.project(&keep).map_err(|e| PyValueError::new_err(e.to_string())))
        .collect()
}
