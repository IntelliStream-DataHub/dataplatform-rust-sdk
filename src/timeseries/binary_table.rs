//! Binary ingest straight from Arrow: a wide table, one timestamp column and one column per
//! series named by its external id, as polars, pyarrow and pandas all hand it over.
//!
//! The columns are used as they are wherever they already have the canon's type (a `Float64`
//! column for a `float` series is sliced, never copied); timestamps get one pass to epoch
//! milliseconds, the per-row `timeseries_id` column is materialised per frame, and the IPC
//! encoding and zstd are the serialisation. What this path does not do is make an object, or a
//! string, per value, which is what the string path costs.
//!
//! Values reach the series as the string path would store them: decimals round half-up at the
//! series' scale and `decimal32` clamps; text is held to the same limits. Two differences are
//! deliberate. A null is "no point at this timestamp", which is what makes a ragged wide table
//! work. And a `mixed` series takes a typed column as it is typed: a numeric column is
//! `value_numeric`, a string column `value_text`, where the string path classifies each string.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int64Array,
    RecordBatch, StringArray, TimestampMillisecondArray, UInt32Array,
};
use arrow_schema::{DataType, TimeUnit};
use oauth2::http::StatusCode;

use crate::http::ResponseError;
use crate::timeseries::binary::{
    assemble_frame, checked_text, ipc_stream, is_stale_series_rejection, parse_scaled,
    unprocessable, BinaryIngestOptions, DatapointValueType, Frame, ResolvedSeries,
    DECIMAL32_UNSCALED_MAX, MAX_SERIES_PER_FRAME, NUMERIC_UNSCALED_MAX,
};
use crate::timeseries::TimeSeriesService;

/// What [`TimeSeriesService::insert_table_binary`] sent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableIngest {
    /// Points sent: non-null cells, after a repeated timestamp in one series kept its last value.
    pub points: usize,
    pub series: usize,
    pub frames: usize,
    /// `decimal32` values clamped to the type's range.
    pub clamped: usize,
}

/// One series' points from one batch, in the canon: timestamps ascending and unique, values in the
/// column type(s) the series' frames carry.
struct Points {
    series: usize,
    timestamps: ArrayRef,
    values: Vec<ArrayRef>,
}

impl Points {
    fn len(&self) -> usize {
        self.timestamps.len()
    }
}

/// Epoch milliseconds from a timestamp column. Copy-free when it is already in milliseconds.
fn timestamps_ms(name: &str, column: &ArrayRef) -> Result<ArrayRef, String> {
    if column.null_count() > 0 {
        return Err(format!("timestamp column {name} has nulls"));
    }
    let exact = |values: &[i64], per_ms: i64| -> Result<ArrayRef, String> {
        let mut out = Vec::with_capacity(values.len());
        for &v in values {
            if v % per_ms != 0 {
                return Err(format!(
                    "timestamp column {name} has a value finer than a millisecond; \
                     the series store milliseconds, so round it first"
                ));
            }
            out.push(v / per_ms);
        }
        Ok(Arc::new(TimestampMillisecondArray::from(out).with_timezone("UTC")))
    };
    match column.data_type() {
        DataType::Timestamp(_, None) => Err(format!(
            "timestamp column {name} has no time zone; give it one (e.g. UTC) rather than leave \
             the instant to be guessed"
        )),
        DataType::Timestamp(TimeUnit::Millisecond, Some(_)) => Ok(Arc::new(
            TimestampMillisecondArray::new(column.as_primitive::<TimestampMillisecondType>().values().clone(), None)
                .with_timezone("UTC"),
        )),
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => {
            exact(column.as_primitive::<TimestampMicrosecondType>().values(), 1_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, Some(_)) => {
            exact(column.as_primitive::<TimestampNanosecondType>().values(), 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Second, Some(_)) => Ok(Arc::new(
            TimestampMillisecondArray::from_iter_values(
                column.as_primitive::<TimestampSecondType>().values().iter().map(|s| s * 1_000),
            )
            .with_timezone("UTC"),
        )),
        DataType::Int64 => Ok(Arc::new(
            TimestampMillisecondArray::new(column.as_primitive::<Int64Type>().values().clone(), None)
                .with_timezone("UTC"),
        )),
        other => Err(format!(
            "timestamp column {name} is {other}; it has to be a timestamp with a time zone, or \
             Int64 epoch milliseconds"
        )),
    }
}

/// A numeric column as `f64`, for `float` and `mixed`. `None` when the column is not numeric.
fn as_f64(column: &ArrayRef) -> Option<Float64Array> {
    macro_rules! widen {
        ($t:ty) => {
            Float64Array::from_iter_values(column.as_primitive::<$t>().values().iter().map(|&v| v as f64))
        };
    }
    Some(match column.data_type() {
        DataType::Float64 => column.as_primitive::<Float64Type>().clone(),
        DataType::Float32 => widen!(Float32Type),
        DataType::Int8 => widen!(Int8Type),
        DataType::Int16 => widen!(Int16Type),
        DataType::Int32 => widen!(Int32Type),
        DataType::Int64 => widen!(Int64Type),
        DataType::UInt8 => widen!(UInt8Type),
        DataType::UInt16 => widen!(UInt16Type),
        DataType::UInt32 => widen!(UInt32Type),
        DataType::UInt64 => widen!(UInt64Type),
        DataType::Decimal128(_, scale) => {
            let divisor = 10f64.powi(*scale as i32);
            Float64Array::from_iter_values(
                column.as_primitive::<Decimal128Type>().values().iter().map(|&v| v as f64 / divisor),
            )
        }
        _ => return None,
    })
}

/// A string column of any of the three string layouts, each value held to the text limits.
fn as_text(name: &str, column: &ArrayRef) -> Option<Result<StringArray, String>> {
    let checked = |values: &mut dyn Iterator<Item = Option<&str>>| -> Result<StringArray, String> {
        values
            .map(|v| checked_text(v.unwrap_or_default()).map_err(|e| format!("column {name}: {e}")))
            .collect::<Result<Vec<String>, String>>()
            .map(StringArray::from)
    };
    Some(match column.data_type() {
        DataType::Utf8 => checked(&mut column.as_string::<i32>().iter()),
        DataType::LargeUtf8 => checked(&mut column.as_string::<i64>().iter()),
        DataType::Utf8View => checked(&mut column.as_string_view().iter()),
        _ => return None,
    })
}

/// Unscaled values at `scale` decimals, rounded half-up like the string path's `parse_scaled`.
fn as_unscaled(name: &str, column: &ArrayRef, scale: i8) -> Result<Vec<i128>, String> {
    let rescale = |v: i128, from: i8| -> i128 {
        if from <= scale {
            v * 10i128.pow((scale - from) as u32)
        } else {
            let divisor = 10i128.pow((from - scale) as u32);
            let (quotient, remainder) = (v / divisor, v % divisor);
            if remainder.abs() * 2 >= divisor {
                quotient + v.signum()
            } else {
                quotient
            }
        }
    };
    macro_rules! ints {
        ($t:ty) => {
            column.as_primitive::<$t>().values().iter().map(|&v| rescale(v as i128, 0)).collect()
        };
    }
    let floats = |values: &mut dyn Iterator<Item = f64>| -> Result<Vec<i128>, String> {
        values
            .map(|v| {
                parse_scaled(&v.to_string(), scale as u32)
                    .ok_or_else(|| format!("column {name}: {v} is not a decimal number"))
            })
            .collect()
    };
    Ok(match column.data_type() {
        DataType::Decimal128(_, from) => {
            let from = *from;
            column.as_primitive::<Decimal128Type>().values().iter().map(|&v| rescale(v, from)).collect()
        }
        DataType::Int8 => ints!(Int8Type),
        DataType::Int16 => ints!(Int16Type),
        DataType::Int32 => ints!(Int32Type),
        DataType::Int64 => ints!(Int64Type),
        DataType::UInt8 => ints!(UInt8Type),
        DataType::UInt16 => ints!(UInt16Type),
        DataType::UInt32 => ints!(UInt32Type),
        DataType::UInt64 => ints!(UInt64Type),
        DataType::Float64 => floats(&mut column.as_primitive::<Float64Type>().values().iter().copied())?,
        DataType::Float32 => floats(&mut column.as_primitive::<Float32Type>().values().iter().map(|&v| v as f64))?,
        other => return Err(format!("column {name} is {other}, which is not a number")),
    })
}

/// A column with no nulls, in the canon's value type(s) for `value_type`.
fn to_canon(
    name: &str,
    value_type: DatapointValueType,
    column: &ArrayRef,
    clamped: &mut usize,
) -> Result<Vec<ArrayRef>, String> {
    let wrong = || format!("column {name} is {}, which a {value_type:?} series cannot hold", column.data_type());
    Ok(match value_type {
        DatapointValueType::Float => vec![Arc::new(as_f64(column).ok_or_else(wrong)?) as ArrayRef],
        DatapointValueType::Float32 => {
            let values: Float32Array = match column.data_type() {
                DataType::Float32 => column.as_primitive::<Float32Type>().clone(),
                _ => {
                    let wide = as_f64(column).ok_or_else(wrong)?;
                    Float32Array::from_iter_values(wide.values().iter().map(|&v| v as f32))
                }
            };
            vec![Arc::new(values)]
        }
        DatapointValueType::Bigint => {
            macro_rules! widen {
                ($t:ty) => {
                    Int64Array::from_iter_values(column.as_primitive::<$t>().values().iter().map(|&v| v as i64))
                };
            }
            let values = match column.data_type() {
                DataType::Int64 => column.as_primitive::<Int64Type>().clone(),
                DataType::Int8 => widen!(Int8Type),
                DataType::Int16 => widen!(Int16Type),
                DataType::Int32 => widen!(Int32Type),
                DataType::UInt8 => widen!(UInt8Type),
                DataType::UInt16 => widen!(UInt16Type),
                DataType::UInt32 => widen!(UInt32Type),
                DataType::UInt64 => {
                    let values = column.as_primitive::<UInt64Type>().values();
                    if values.iter().any(|&v| v > i64::MAX as u64) {
                        return Err(format!("column {name} has a value past the bigint range"));
                    }
                    widen!(UInt64Type)
                }
                _ => return Err(wrong()),
            };
            vec![Arc::new(values)]
        }
        DatapointValueType::Numeric | DatapointValueType::Decimal32 => {
            let (precision, scale) = if value_type == DatapointValueType::Numeric { (18, 6) } else { (9, 4) };
            let mut unscaled = as_unscaled(name, column, scale)?;
            if value_type == DatapointValueType::Numeric {
                if let Some(v) = unscaled.iter().find(|v| v.abs() > NUMERIC_UNSCALED_MAX) {
                    return Err(format!(
                        "column {name}: {} does not fit Decimal(18, 6)",
                        *v as f64 / 1e6
                    ));
                }
            } else {
                for v in unscaled.iter_mut() {
                    let bounded = (*v).clamp(-DECIMAL32_UNSCALED_MAX, DECIMAL32_UNSCALED_MAX);
                    if bounded != *v {
                        *clamped += 1;
                        *v = bounded;
                    }
                }
            }
            let array = Decimal128Array::from(unscaled)
                .with_precision_and_scale(precision, scale)
                .map_err(|e| e.to_string())?;
            vec![Arc::new(array)]
        }
        DatapointValueType::Text => vec![Arc::new(as_text(name, column).ok_or_else(wrong)??)],
        DatapointValueType::Mixed => {
            let rows = column.len();
            if let Some(numeric) = as_f64(column) {
                vec![Arc::new(numeric), Arc::new(StringArray::new_null(rows))]
            } else if let Some(text) = as_text(name, column) {
                vec![Arc::new(Float64Array::new_null(rows)), Arc::new(text?)]
            } else {
                return Err(wrong());
            }
        }
    })
}

/// One series' column of a batch as its points: nulls dropped, values in the canon, timestamps
/// ascending with a repeated one keeping its last value, as the string path keeps it.
fn points(
    index: usize,
    series: &ResolvedSeries,
    name: &str,
    timestamps: &ArrayRef,
    column: &ArrayRef,
    clamped: &mut usize,
) -> Result<Points, String> {
    let (timestamps, column) = if column.null_count() == 0 {
        (timestamps.clone(), column.clone())
    } else {
        let present = BooleanArray::from_iter((0..column.len()).map(|i| Some(column.is_valid(i))));
        let filter = |a: &ArrayRef| arrow_select::filter::filter(a.as_ref(), &present).map_err(|e| e.to_string());
        (filter(timestamps)?, filter(column)?)
    };
    let mut values = to_canon(name, series.value_type, &column, clamped)?;
    let mut timestamps = timestamps;

    let ts = timestamps.as_primitive::<TimestampMillisecondType>().values();
    if !ts.windows(2).all(|w| w[0] < w[1]) {
        let mut order: Vec<u32> = (0..ts.len() as u32).collect();
        order.sort_by_key(|&i| ts[i as usize]);
        // Stable, so equal timestamps stay in input order and the last of each run wins.
        let kept: Vec<u32> = order
            .iter()
            .enumerate()
            .filter(|(at, &i)| order.get(at + 1).is_none_or(|&next| ts[next as usize] != ts[i as usize]))
            .map(|(_, &i)| i)
            .collect();
        let indices = UInt32Array::from(kept);
        let take = |a: &ArrayRef| arrow_select::take::take(a.as_ref(), &indices, None).map_err(|e| e.to_string());
        timestamps = take(&timestamps)?;
        values = values.iter().map(take).collect::<Result<_, _>>()?;
    }
    Ok(Points { series: index, timestamps, values })
}

/// One frame's worth of points: slices of series of one value type, in ascending id order.
struct FrameSpec {
    value_type: DatapointValueType,
    pieces: Vec<(usize, usize, usize)>,
}

/// Cuts the points into frames: each value type on its own, series in ascending id order, a frame
/// filled to its row cap, and a series longer than the room left continued in the next frame.
fn plan_frames(points: &[Points], series: &[ResolvedSeries]) -> Vec<FrameSpec> {
    let mut order: Vec<usize> = (0..points.len()).filter(|&p| points[p].len() > 0).collect();
    order.sort_by_key(|&p| (series[points[p].series].value_type.id(), series[points[p].series].id));
    let mut specs: Vec<FrameSpec> = Vec::new();
    for p in order {
        let value_type = series[points[p].series].value_type;
        let max_rows = value_type.max_rows();
        let mut start = 0;
        while start < points[p].len() {
            let open = specs.last().is_some_and(|f| {
                f.value_type == value_type
                    && f.pieces.iter().map(|(_, _, n)| n).sum::<usize>() < max_rows
                    && f.pieces.len() < MAX_SERIES_PER_FRAME
            });
            if !open {
                specs.push(FrameSpec { value_type, pieces: Vec::new() });
            }
            let frame = specs.last_mut().unwrap();
            let room = max_rows - frame.pieces.iter().map(|(_, _, n)| n).sum::<usize>();
            let take = room.min(points[p].len() - start);
            frame.pieces.push((p, start, take));
            start += take;
        }
    }
    specs
}

fn build_frame(
    spec: &FrameSpec,
    points: &[Points],
    series: &[ResolvedSeries],
    zstd_level: i32,
) -> Result<Frame, String> {
    let rows: usize = spec.pieces.iter().map(|(_, _, n)| n).sum();
    let mut ids: Vec<i64> = Vec::with_capacity(rows);
    let mut directory: Vec<(i64, &str)> = Vec::with_capacity(spec.pieces.len());
    for &(p, _, n) in &spec.pieces {
        let s = &series[points[p].series];
        ids.extend(std::iter::repeat_n(s.id as i64, n));
        directory.push((s.id as i64, s.external_id.as_str()));
    }
    // A frame of one slice is the series' own arrays, sliced; several are concatenated.
    let column = |pick: &dyn Fn(&Points) -> &ArrayRef| -> Result<ArrayRef, String> {
        if let [(p, start, n)] = spec.pieces[..] {
            return Ok(pick(&points[p]).slice(start, n));
        }
        let slices: Vec<ArrayRef> = spec.pieces.iter().map(|&(p, start, n)| pick(&points[p]).slice(start, n)).collect();
        let refs: Vec<&dyn Array> = slices.iter().map(|a| a.as_ref()).collect();
        arrow_select::concat::concat(&refs).map_err(|e| e.to_string())
    };
    let mut columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids)), column(&|p| &p.timestamps)?];
    let width = points[spec.pieces[0].0].values.len();
    for v in 0..width {
        columns.push(column(&|p| &p.values[v])?);
    }
    let batch = RecordBatch::try_new(spec.value_type.schema(), columns).map_err(|e| e.to_string())?;
    let raw = ipc_stream(&batch, rows * 32 + 2048)?;
    assemble_frame(spec.value_type, rows, &directory, &raw, zstd_level)
}

/// A batch's frames, compressed on as many threads as there are cores and frames.
fn table_frames(
    batch: &RecordBatch,
    timestamp: &str,
    series: &[ResolvedSeries],
    zstd_level: i32,
) -> Result<(Vec<Frame>, TableIngest), String> {
    let schema = batch.schema();
    let ts_index = schema.index_of(timestamp).map_err(|_| format!("no column named {timestamp}"))?;
    let timestamps = timestamps_ms(timestamp, batch.column(ts_index))?;
    let mut clamped = 0;
    let mut all = Vec::with_capacity(series.len());
    let mut s = 0;
    for (c, field) in schema.fields().iter().enumerate() {
        if c == ts_index {
            continue;
        }
        all.push(points(s, &series[s], field.name(), &timestamps, batch.column(c), &mut clamped)?);
        s += 1;
    }
    let specs = plan_frames(&all, series);
    let next = AtomicUsize::new(0);
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get()).min(specs.len()).max(1);
    let mut built: Vec<(usize, Result<Frame, String>)> = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut done = Vec::new();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        let Some(spec) = specs.get(i) else { return done };
                        done.push((i, build_frame(spec, &all, series, zstd_level)));
                    }
                })
            })
            .collect();
        workers.into_iter().flat_map(|w| w.join().expect("a frame builder panicked")).collect()
    });
    built.sort_unstable_by_key(|(i, _)| *i);
    let frames = built.into_iter().map(|(_, f)| f).collect::<Result<Vec<_>, _>>()?;
    let summary = TableIngest {
        points: all.iter().map(Points::len).sum(),
        series: all.iter().filter(|p| p.len() > 0).count(),
        frames: frames.len(),
        clamped,
    };
    Ok((frames, summary))
}

impl TimeSeriesService {
    /// Inserts a wide Arrow table through `POST /timeseries/data/binary`: the column named
    /// `timestamp` holds the timestamps, and every other column is a series, named by its
    /// external id. The batches are taken as they are, one after another; polars, pyarrow and
    /// pandas hand a table over this way through the Arrow C stream.
    ///
    /// The timestamp column is a timestamp with a time zone (any unit, whole milliseconds), or
    /// `Int64` epoch milliseconds; a naive one, or one with nulls, is refused. In a series column
    /// a null is "no point at this timestamp", so a ragged table works; NaN is a value. A column
    /// is converted to its series' value type as the string path would store it (see the module
    /// documentation); one that cannot be, such as a float column for a `bigint` series, is a
    /// 422 before anything is sent. Rows need not be sorted, and a timestamp repeated within a
    /// series keeps its last value.
    ///
    /// Series are resolved by external id as on the string path: a name that is not a series,
    /// or one you cannot read, is a 404 naming it before anything is sent. Each batch is posted
    /// before the next is converted, so memory holds one batch's frames. A table larger than one
    /// request is not atomic: a refusal can leave the earlier requests stored.
    pub async fn insert_table_binary(
        &self,
        batches: &[RecordBatch],
        timestamp: &str,
        options: &BinaryIngestOptions,
    ) -> Result<TableIngest, ResponseError> {
        options.validate()?;
        let Some(first) = batches.first() else {
            return Ok(TableIngest::default());
        };
        let names: Vec<String> = first
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .filter(|n| n != timestamp)
            .collect();
        if first.schema().index_of(timestamp).is_err() {
            return Err(ResponseError::bad_request(format!(
                "no column named {timestamp}; the columns are {names:?}"
            )));
        }
        let mut total = TableIngest::default();
        for batch in batches {
            if batch.schema().fields() != first.schema().fields() {
                return Err(ResponseError::bad_request(
                    "the batches do not share one schema".to_string(),
                ));
            }
            let sent = match self.insert_table_batch(batch, timestamp, &names, options).await {
                Err(error) if is_stale_series_rejection(&error) => {
                    self.evict_binary_series_cache();
                    self.insert_table_batch(batch, timestamp, &names, options).await
                }
                other => other,
            }?;
            total.points += sent.points;
            total.series = total.series.max(sent.series);
            total.frames += sent.frames;
            total.clamped += sent.clamped;
        }
        Ok(total)
    }

    async fn insert_table_batch(
        &self,
        batch: &RecordBatch,
        timestamp: &str,
        names: &[String],
        options: &BinaryIngestOptions,
    ) -> Result<TableIngest, ResponseError> {
        let wanted: Vec<(Option<u64>, Option<&str>)> = names.iter().map(|n| (None, Some(n.as_str()))).collect();
        let series = self.resolve_series(&wanted).await?;
        let (batch, timestamp, level) = (batch.clone(), timestamp.to_string(), options.zstd_level);
        let (frames, summary) =
            tokio::task::spawn_blocking(move || table_frames(&batch, &timestamp, &series, level))
                .await
                .map_err(|e| ResponseError {
                    status: StatusCode::INTERNAL_SERVER_ERROR,
                    message: format!("building frames failed: {e}"),
                    content_type: None,
                })?
                .map_err(unprocessable)?;
        if !frames.is_empty() {
            self.post_frame_requests(frames, options).await?;
        }
        Ok(summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseries::binary::FrameWriter;
    use crate::timeseries::binary_read::{parse_read_response, BinaryDatapoints, BinaryValues, END_OF_RESPONSE};
    use arrow_array::{LargeStringArray, StringViewArray, TimestampMicrosecondArray};
    use arrow_schema::{Field, Schema};

    fn series(id: u64, external_id: &str, value_type: DatapointValueType) -> ResolvedSeries {
        ResolvedSeries { id, external_id: external_id.to_string(), value_type }
    }

    fn table(timestamps: ArrayRef, columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
        let mut fields = vec![Field::new("timestamp", timestamps.data_type().clone(), false)];
        let mut arrays = vec![timestamps];
        for (name, array) in columns {
            fields.push(Field::new(name, array.data_type().clone(), true));
            arrays.push(array);
        }
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    fn ms(values: &[i64]) -> ArrayRef {
        Arc::new(TimestampMillisecondArray::from(values.to_vec()).with_timezone("UTC"))
    }

    /// What a body of these frames reads back as.
    fn decode(frames: &[Frame]) -> Vec<BinaryDatapoints> {
        let mut body: Vec<u8> = frames.iter().flat_map(|f| f.bytes.clone()).collect();
        body.extend_from_slice(END_OF_RESPONSE);
        parse_read_response(&body).unwrap()
    }

    /// The same points through the string path's `FrameWriter`.
    fn through_strings(s: &ResolvedSeries, points: &[(i64, &str)]) -> Vec<BinaryDatapoints> {
        let mut writer = FrameWriter::new(s.value_type);
        writer.series(s.id, &s.external_id).unwrap();
        for (t, v) in points {
            writer.add(s.id, *t, v).unwrap();
        }
        decode(&[writer.build(1).unwrap()])
    }

    fn through_table(batch: &RecordBatch, resolved: &[ResolvedSeries]) -> Vec<BinaryDatapoints> {
        decode(&table_frames(batch, "timestamp", resolved, 1).unwrap().0)
    }

    #[test]
    fn a_float_column_stores_what_the_string_path_stores() {
        let s = [series(7, "pump", DatapointValueType::Float)];
        // Unsorted, a repeated timestamp (the last value wins), a null (no point), microseconds.
        let ts = Arc::new(
            TimestampMicrosecondArray::from(vec![3_000_000, 1_000_000, 2_000_000, 1_000_000, 4_000_000])
                .with_timezone("UTC"),
        );
        let values = Arc::new(Float64Array::from(vec![Some(3.5), Some(1.0), None, Some(1.25), Some(f64::NAN)]));
        let got = through_table(&table(ts, vec![("pump", values)]), &s);
        let want = through_strings(&s[0], &[(3_000, "3.5"), (1_000, "1.0"), (1_000, "1.25"), (4_000, "NaN")]);
        assert_eq!(got[0].timestamps, want[0].timestamps);
        assert_eq!(got[0].timestamps, [1_000, 3_000, 4_000]);
        assert_eq!(format!("{:?}", got[0].values), format!("{:?}", want[0].values));
    }

    #[test]
    fn decimals_round_and_clamp_as_the_string_path_does() {
        let s = [
            series(1, "numeric_from_float", DatapointValueType::Numeric),
            series(2, "decimal32_from_decimal", DatapointValueType::Decimal32),
            series(3, "numeric_from_int", DatapointValueType::Numeric),
        ];
        let floats = Arc::new(Float64Array::from(vec![0.0000005, -1.2345675, 123.456789]));
        let decimals = Arc::new(
            Decimal128Array::from(vec![123_456_789_i128, -12_345_678_950, 99_999_999_995])
                .with_precision_and_scale(20, 6)
                .unwrap(),
        );
        let ints = Arc::new(arrow_array::Int32Array::from(vec![7, -3, 0]));
        let batch = table(ms(&[1, 2, 3]), vec![("numeric_from_float", floats), ("decimal32_from_decimal", decimals), ("numeric_from_int", ints)]);
        let (frames, summary) = table_frames(&batch, "timestamp", &s, 1).unwrap();
        let got = decode(&frames);

        let strings: [&[&str]; 3] = [
            &["0.0000005", "-1.2345675", "123.456789"],
            &["123.456789", "-12345.67895", "99999.999995"],
            &["7", "-3", "0"],
        ];
        for (i, values) in strings.iter().enumerate() {
            let points: Vec<(i64, &str)> = values.iter().enumerate().map(|(t, v)| (t as i64 + 1, *v)).collect();
            let want = through_strings(&s[i], &points);
            let got = got.iter().find(|g| g.id == s[i].id).unwrap();
            assert_eq!(got.values, want[0].values, "{}", s[i].external_id);
        }
        // 99999.999995 rounds past decimal32's range and clamps; the string path counts the same.
        assert_eq!(summary.clamped, 1);
    }

    #[test]
    fn text_comes_from_every_string_layout_and_mixed_from_typed_columns() {
        let s = [
            series(1, "a", DatapointValueType::Text),
            series(2, "b", DatapointValueType::Text),
            series(3, "c", DatapointValueType::Mixed),
            series(4, "d", DatapointValueType::Mixed),
        ];
        let batch = table(
            ms(&[1, 2]),
            vec![
                ("a", Arc::new(StringViewArray::from(vec!["open", "blåbær"]))),
                ("b", Arc::new(LargeStringArray::from(vec![Some("x"), None]))),
                ("c", Arc::new(Float64Array::from(vec![1.5, 2.5]))),
                ("d", Arc::new(StringArray::from(vec!["3.5", "fault"]))),
            ],
        );
        let got = through_table(&batch, &s);
        assert_eq!(got[0].values, BinaryValues::Text(vec!["open".into(), "blåbær".into()]));
        assert_eq!(got[1].values, BinaryValues::Text(vec!["x".into()]));
        assert_eq!(
            got[2].values,
            BinaryValues::Mixed { numeric: vec![Some(1.5), Some(2.5)], text: vec![None, None] }
        );
        // A string column of a mixed series is text, "3.5" included: the column is typed.
        assert_eq!(
            got[3].values,
            BinaryValues::Mixed { numeric: vec![None, None], text: vec![Some("3.5".into()), Some("fault".into())] }
        );
    }

    #[test]
    fn long_series_are_cut_at_the_row_cap_and_short_ones_share_a_frame() {
        let s = [
            series(9, "long", DatapointValueType::Float),
            series(2, "short_b", DatapointValueType::Float),
            series(1, "short_a", DatapointValueType::Float),
        ];
        let n = 250_000;
        let ts: Vec<i64> = (0..n as i64).collect();
        let long = Arc::new(Float64Array::from_iter_values((0..n).map(|i| i as f64)));
        let sparse = |every: usize| -> ArrayRef {
            Arc::new(Float64Array::from_iter((0..n).map(|i| (i % every == 0).then_some(1.0))))
        };
        let batch = table(ms(&ts), vec![("long", long), ("short_b", sparse(100_000)), ("short_a", sparse(50_000))]);
        let (frames, summary) = table_frames(&batch, "timestamp", &s, 1).unwrap();

        assert_eq!(summary.points, n + 3 + 5);
        // Ascending ids: the two short series (5 + 3 rows) open the first frame, and the long one
        // fills the rest of it and runs on over two more.
        let rows: Vec<usize> = frames.iter().map(|f| f.rows).collect();
        assert_eq!(rows, [100_000, 100_000, 50_008]);
        assert_eq!(frames[0].series, 3, "short_a, short_b and the start of long");
        let read = decode(&frames);
        assert_eq!(read.iter().map(|s| (s.id, s.len())).collect::<Vec<_>>(), [(1, 5), (2, 3), (9, n)]);
    }

    #[test]
    fn what_the_table_cannot_say_is_refused() {
        let float = [series(1, "v", DatapointValueType::Float)];
        let naive = Arc::new(TimestampMillisecondArray::from(vec![1]));
        assert!(table_frames(&table(naive, vec![("v", Arc::new(Float64Array::from(vec![1.0])))]), "timestamp", &float, 1)
            .unwrap_err()
            .contains("no time zone"));

        let sub_ms = Arc::new(TimestampMicrosecondArray::from(vec![1_500]).with_timezone("UTC"));
        assert!(table_frames(&table(sub_ms, vec![("v", Arc::new(Float64Array::from(vec![1.0])))]), "timestamp", &float, 1)
            .unwrap_err()
            .contains("finer than a millisecond"));

        let bigint = [series(1, "v", DatapointValueType::Bigint)];
        assert!(table_frames(&table(ms(&[1]), vec![("v", Arc::new(Float64Array::from(vec![1.0])))]), "timestamp", &bigint, 1)
            .unwrap_err()
            .contains("cannot hold"));

        let text = [series(1, "v", DatapointValueType::Text)];
        assert!(table_frames(&table(ms(&[1]), vec![("v", Arc::new(StringArray::from(vec!["x".repeat(65)])))]), "timestamp", &text, 1)
            .unwrap_err()
            .contains("characters"));
    }
}
