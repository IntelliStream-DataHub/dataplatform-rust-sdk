use std::sync::Arc;

use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{
    ArrayRef, Decimal128Array, Float32Array, Float64Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use intellistream_datahub_sdk::timeseries::{BinaryDatapoints, BinaryValues, DatapointValueType};
use pyo3::exceptions::{PyImportError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyCapsule;

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

impl TryFrom<BinaryDatapoints> for PyBinaryDatapoints {
    type Error = ArrowError;

    fn try_from(series: BinaryDatapoints) -> Result<Self, ArrowError> {
        let aggregates = series.is_aggregates();
        let timestamp = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        );
        let mut fields = vec![timestamp];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(TimestampMillisecondArray::from(series.timestamps).with_timezone("UTC"))];
        let mut push = |name: &str, nullable: bool, array: ArrayRef| {
            fields.push(Field::new(name, array.data_type().clone(), nullable));
            columns.push(array);
        };
        match series.values {
            BinaryValues::Bigint(v) => push("value", false, Arc::new(Int64Array::from(v))),
            BinaryValues::Float(v) => push("value", false, Arc::new(Float64Array::from(v))),
            BinaryValues::Float32(v) => push("value", false, Arc::new(Float32Array::from(v))),
            BinaryValues::Decimal { unscaled, scale } => {
                let precision = if series.value_type == DatapointValueType::Decimal32 { 9 } else { 18 };
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

fn import<'py>(py: Python<'py>, module: &str, method: &str) -> PyResult<Bound<'py, PyModule>> {
    py.import(module).map_err(|_| {
        PyImportError::new_err(format!(
            "BinaryDatapoints.{method}() needs {module}; install it with `pip install {module}`"
        ))
    })
}

pub(crate) fn to_py(series: Vec<BinaryDatapoints>) -> PyResult<Vec<PyBinaryDatapoints>> {
    series
        .into_iter()
        .map(|s| PyBinaryDatapoints::try_from(s).map_err(|e| PyValueError::new_err(e.to_string())))
        .collect()
}
