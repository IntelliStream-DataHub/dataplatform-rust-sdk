//! The binary datapoint contract behind `POST /timeseries/data/binary`.
//!
//! A request body is one or more *frames*. Each frame carries the datapoints of one value type as
//! an Arrow IPC stream in the schema the server's ClickHouse table wants, compressed with zstd by
//! the client and forwarded compressed all the way to the consumer, behind a 28-byte envelope and
//! a directory naming the series inside. The layout is the platform's `binary_datapoints_format.md`.
//!
//! [`FrameWriter`] builds one frame. [`TimeSeriesService::insert_datapoints_binary`] does the rest:
//! resolves series to their ids and value types (cached), cuts frames at the caps, compresses them
//! in parallel, packs them into requests and posts them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{
    ArrayRef, Decimal128Array, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use futures::future::join_all;
use oauth2::http::StatusCode;

use crate::generic::{
    ApiServiceProvider, DataWrapper, DatapointString, DatapointsCollection, IdAndExtId,
};
use crate::http::ResponseError;
use crate::timeseries::TimeSeriesService;

/// The request media type. Anything else is a 415.
pub const MEDIA_TYPE: &str = "application/vnd.intellistream.datapoint-block";
pub const MAGIC: &[u8; 4] = b"DHDP";
pub const VERSION: u8 = 1;
pub const CODEC_ARROW_IPC: u8 = 1;
pub const COMPRESSION_ZSTD: u8 = 1;
/// Bytes before the series directory.
pub const HEADER_BYTES: usize = 28;

/// Decompressed bytes per frame; a frame is one Pulsar message on the server.
pub const MAX_FRAME_RAW_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_FRAMES_PER_REQUEST: usize = 32;
/// Decompressed bytes per request, summed over its frames.
pub const MAX_REQUEST_RAW_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SERIES_PER_FRAME: usize = 10_000;
pub const MAX_EXTERNAL_ID_BYTES: usize = 1024;
pub const MAX_VALUE_CHARS: usize = 64;
pub const MAX_VALUE_BYTES: usize = 256;
/// Rows per frame for the numeric value types, the JSON path's per-collection cap.
pub const NUMERIC_ROWS_PER_FRAME: usize = 100_000;
/// Rows per frame for `text` and `mixed`.
pub const TEXT_ROWS_PER_FRAME: usize = 10_000;

const NUMERIC_UNSCALED_MAX: i128 = 999_999_999_999_999_999;
const DECIMAL32_UNSCALED_MAX: i128 = 999_999_999;
/// Headroom under the raw cap: arrow-rs pads every buffer to 64 bytes, which the estimate ignores.
const RAW_BYTES_MARGIN: usize = 64 * 1024;

/// A timeseries value type, numbered as the frame envelope carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatapointValueType {
    Bigint = 1,
    Float = 2,
    Numeric = 3,
    Text = 4,
    Decimal32 = 5,
    Mixed = 6,
    Float32 = 7,
}

impl DatapointValueType {
    /// The name a timeseries read reports in `valueType`, matched case-insensitively.
    pub fn from_name(name: &str) -> Option<Self> {
        Some(match name.to_ascii_lowercase().as_str() {
            "bigint" => Self::Bigint,
            "float" => Self::Float,
            "numeric" => Self::Numeric,
            "text" => Self::Text,
            "decimal32" => Self::Decimal32,
            "mixed" => Self::Mixed,
            "float32" => Self::Float32,
            _ => return None,
        })
    }

    pub fn id(self) -> u8 {
        self as u8
    }

    pub fn carries_text(self) -> bool {
        matches!(self, Self::Text | Self::Mixed)
    }

    /// Rows one frame of this type may carry.
    pub fn max_rows(self) -> usize {
        if self.carries_text() {
            TEXT_ROWS_PER_FRAME
        } else {
            NUMERIC_ROWS_PER_FRAME
        }
    }

    /// The one Arrow schema a frame of this type is accepted in.
    pub fn schema(self) -> SchemaRef {
        let id = Field::new("timeseries_id", DataType::Int64, false);
        let timestamp = Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        );
        let fields = match self {
            Self::Bigint => vec![id, timestamp, Field::new("value", DataType::Int64, false)],
            Self::Float => vec![id, timestamp, Field::new("value", DataType::Float64, false)],
            Self::Float32 => vec![id, timestamp, Field::new("value", DataType::Float32, false)],
            Self::Numeric => vec![id, timestamp, Field::new("value", DataType::Decimal128(18, 6), false)],
            Self::Decimal32 => vec![id, timestamp, Field::new("value", DataType::Decimal128(9, 4), false)],
            Self::Text => vec![id, timestamp, Field::new("value", DataType::Utf8, false)],
            Self::Mixed => vec![
                id,
                timestamp,
                Field::new("value_numeric", DataType::Float64, true),
                Field::new("value_text", DataType::Utf8, true),
            ],
        };
        Arc::new(Schema::new(fields))
    }

    fn bytes_per_row(self) -> usize {
        match self {
            Self::Bigint | Self::Float => 24,
            Self::Float32 => 20,
            Self::Numeric | Self::Decimal32 => 32,
            Self::Text => 20,
            Self::Mixed => 28,
        }
    }
}

/// How [`TimeSeriesService::insert_datapoints_binary`] compresses and retries.
#[derive(Debug, Clone)]
pub struct BinaryIngestOptions {
    /// zstd level per frame: 1, 3 or 9. Compression is mandatory, so there is no way to turn it off.
    pub zstd_level: i32,
    /// Retries of one request after a 429 or a 5xx, one second apart per attempt.
    pub max_retries: u32,
    /// Requests in flight at once when one call packs into more than one, which happens above
    /// 3.2 million points. Below that a call is a single request and this has no effect.
    pub request_concurrency: usize,
}

impl Default for BinaryIngestOptions {
    fn default() -> Self {
        BinaryIngestOptions {
            zstd_level: 9,
            max_retries: 3,
            request_concurrency: 4,
        }
    }
}

impl BinaryIngestOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn zstd_level(mut self, level: i32) -> Self {
        self.zstd_level = level;
        self
    }

    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    pub fn request_concurrency(mut self, requests: usize) -> Self {
        self.request_concurrency = requests;
        self
    }

    fn validate(&self) -> Result<(), ResponseError> {
        if ![1, 3, 9].contains(&self.zstd_level) {
            return Err(ResponseError::bad_request(format!(
                "zstd level {} is not one of 1, 3 or 9",
                self.zstd_level
            )));
        }
        Ok(())
    }
}

/// A series as the binary path needs it: its id, the external id the frame directory names it by,
/// and the value type that picks the frame it goes into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedSeries {
    pub id: u64,
    pub external_id: String,
    pub value_type: DatapointValueType,
}

/// One finished frame, ready to post as is or packed with others.
#[derive(Debug, Clone)]
pub struct Frame {
    pub bytes: Vec<u8>,
    pub rows: usize,
    pub series: usize,
    /// Payload bytes before compression; what the request cap counts.
    pub raw_len: usize,
    /// Decimal32 values that were clamped to the type's range.
    pub clamped: usize,
}

#[derive(Debug, Clone)]
enum Cell {
    I64(i64),
    F64(f64),
    F32(f32),
    Scaled(i128),
    Text(String),
    MixedNumeric(f64),
    MixedText(String),
}

#[derive(Debug, Clone)]
struct Row {
    id: i64,
    timestamp: i64,
    cell: Cell,
}

/// Builds one frame for one value type: rows in any order in, a sorted, de-duplicated, compressed
/// frame out. Values arrive in the JSON contract's string form and are checked the way the JSON
/// path checks them on the server, so both paths store the same bytes for the same input.
#[derive(Debug, Clone)]
pub struct FrameWriter {
    value_type: DatapointValueType,
    series: HashMap<i64, String>,
    rows: Vec<Row>,
    text_bytes: usize,
    clamped: usize,
}

impl FrameWriter {
    pub fn new(value_type: DatapointValueType) -> Self {
        FrameWriter {
            value_type,
            series: HashMap::new(),
            rows: Vec::new(),
            text_bytes: 0,
            clamped: 0,
        }
    }

    pub fn value_type(&self) -> DatapointValueType {
        self.value_type
    }

    /// Names a series the frame will carry; every id added must be named before [`build`](Self::build).
    pub fn series(&mut self, id: u64, external_id: &str) -> Result<(), String> {
        if self.series.contains_key(&(id as i64)) {
            return Ok(());
        }
        if external_id.is_empty() {
            return Err(format!("external id for series {id} is empty"));
        }
        if external_id.len() > MAX_EXTERNAL_ID_BYTES {
            return Err(format!(
                "external id for series {id} exceeds {MAX_EXTERNAL_ID_BYTES} bytes"
            ));
        }
        self.series.insert(id as i64, external_id.to_string());
        Ok(())
    }

    pub fn has_series(&self, id: u64) -> bool {
        self.series.contains_key(&(id as i64))
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    /// Decimal32 values clamped so far, for the caller to warn about.
    pub fn clamped_count(&self) -> usize {
        self.clamped
    }

    /// Payload bytes the frame would need before compression, for cutting frames before building.
    pub fn estimated_raw_bytes(&self) -> usize {
        2048 + self.value_type.bytes_per_row() * self.rows.len() + self.text_bytes
    }

    /// Whether another row for `id` would push the frame past a cap.
    pub fn is_full_for(&self, id: u64) -> bool {
        self.rows.len() >= self.value_type.max_rows()
            || self.estimated_raw_bytes() + MAX_VALUE_BYTES + RAW_BYTES_MARGIN > MAX_FRAME_RAW_BYTES
            || (!self.has_series(id) && self.series.len() >= MAX_SERIES_PER_FRAME)
    }

    /// Adds a value in the JSON contract's string form, parsed by the frame's value type.
    pub fn add(&mut self, id: u64, timestamp_ms: i64, value: &str) -> Result<(), String> {
        let cell = match self.value_type {
            DatapointValueType::Bigint => Cell::I64(
                value
                    .parse::<i64>()
                    .map_err(|_| format!("{value:?} is not a bigint"))?,
            ),
            DatapointValueType::Float => Cell::F64(
                value
                    .parse::<f64>()
                    .map_err(|_| format!("{value:?} is not a float"))?,
            ),
            DatapointValueType::Float32 => Cell::F32(
                value
                    .parse::<f32>()
                    .map_err(|_| format!("{value:?} is not a float32"))?,
            ),
            DatapointValueType::Numeric => {
                let unscaled = parse_scaled(value, 6)
                    .ok_or_else(|| format!("{value:?} is not a decimal number"))?;
                if unscaled > NUMERIC_UNSCALED_MAX || unscaled < -NUMERIC_UNSCALED_MAX {
                    return Err(format!("numeric value {value} does not fit Decimal(18, 6)"));
                }
                Cell::Scaled(unscaled)
            }
            DatapointValueType::Decimal32 => {
                let unscaled = parse_scaled(value, 4)
                    .ok_or_else(|| format!("{value:?} is not a decimal number"))?;
                let bounded = unscaled.clamp(-DECIMAL32_UNSCALED_MAX, DECIMAL32_UNSCALED_MAX);
                if bounded != unscaled {
                    self.clamped += 1;
                }
                Cell::Scaled(bounded)
            }
            DatapointValueType::Text => Cell::Text(checked_text(value)?),
            DatapointValueType::Mixed => match decimal_parts(value) {
                Some(_) => Cell::MixedNumeric(
                    value
                        .parse::<f64>()
                        .map_err(|_| format!("{value:?} is not a float"))?,
                ),
                None => Cell::MixedText(checked_text(value)?),
            },
        };
        self.push(id, timestamp_ms, cell);
        Ok(())
    }

    pub fn add_bigint(&mut self, id: u64, timestamp_ms: i64, value: i64) -> Result<(), String> {
        self.expect(DatapointValueType::Bigint)?;
        self.push(id, timestamp_ms, Cell::I64(value));
        Ok(())
    }

    pub fn add_float(&mut self, id: u64, timestamp_ms: i64, value: f64) -> Result<(), String> {
        self.expect(DatapointValueType::Float)?;
        self.push(id, timestamp_ms, Cell::F64(value));
        Ok(())
    }

    pub fn add_float32(&mut self, id: u64, timestamp_ms: i64, value: f32) -> Result<(), String> {
        self.expect(DatapointValueType::Float32)?;
        self.push(id, timestamp_ms, Cell::F32(value));
        Ok(())
    }

    pub fn add_text(&mut self, id: u64, timestamp_ms: i64, value: &str) -> Result<(), String> {
        self.expect(DatapointValueType::Text)?;
        let text = checked_text(value)?;
        self.push(id, timestamp_ms, Cell::Text(text));
        Ok(())
    }

    fn expect(&self, expected: DatapointValueType) -> Result<(), String> {
        if self.value_type != expected {
            return Err(format!(
                "writer is for {:?}, not {:?}",
                self.value_type, expected
            ));
        }
        Ok(())
    }

    fn push(&mut self, id: u64, timestamp: i64, cell: Cell) {
        if let Cell::Text(t) | Cell::MixedText(t) = &cell {
            self.text_bytes += t.len();
        }
        self.rows.push(Row {
            id: id as i64,
            timestamp,
            cell,
        });
    }

    fn key(&self, index: usize) -> (i64, i64) {
        let row = &self.rows[index];
        (row.id, row.timestamp)
    }

    /// Sorts by (id, timestamp), keeps the last value of a repeated pair, checks the caps, writes
    /// the Arrow stream, compresses it and returns the complete frame.
    pub fn build(&self, zstd_level: i32) -> Result<Frame, String> {
        if self.rows.is_empty() {
            return Err("frame has no rows".to_string());
        }
        let mut order: Vec<usize> = (0..self.rows.len()).collect();
        order.sort_by(|&a, &b| self.key(a).cmp(&self.key(b)));
        let mut kept = Vec::with_capacity(order.len());
        for (i, &index) in order.iter().enumerate() {
            let last_of_key = i + 1 == order.len() || self.key(index) != self.key(order[i + 1]);
            if last_of_key {
                kept.push(index);
            }
        }
        let rows = kept.len();
        let max_rows = self.value_type.max_rows();
        if rows > max_rows {
            return Err(format!(
                "frame has {rows} rows, the cap for {:?} is {max_rows}; split it",
                self.value_type
            ));
        }
        let mut directory: Vec<(i64, &str)> = Vec::new();
        for &index in &kept {
            let id = self.rows[index].id;
            if directory.last().map_or(true, |(last, _)| *last != id) {
                let external_id = self
                    .series
                    .get(&id)
                    .ok_or_else(|| format!("no external id registered for series {id}"))?;
                directory.push((id, external_id.as_str()));
            }
        }
        if directory.len() > MAX_SERIES_PER_FRAME {
            return Err(format!(
                "frame has {} series, the cap is {MAX_SERIES_PER_FRAME}; split it",
                directory.len()
            ));
        }

        let raw = self.write_ipc(&kept)?;
        if raw.len() > MAX_FRAME_RAW_BYTES {
            return Err(format!(
                "frame payload is {} bytes, the cap is {MAX_FRAME_RAW_BYTES}; split it",
                raw.len()
            ));
        }
        let compressed =
            zstd::bulk::compress(&raw, zstd_level).map_err(|e| format!("zstd failed: {e}"))?;

        let directory_len: usize = directory
            .iter()
            .map(|(_, external_id)| 8 + varint_size(external_id.len()) + external_id.len())
            .sum();
        let mut frame = Vec::with_capacity(HEADER_BYTES + directory_len + compressed.len());
        frame.extend_from_slice(MAGIC);
        frame.push(VERSION);
        frame.push(self.value_type.id());
        frame.push(CODEC_ARROW_IPC);
        frame.push(COMPRESSION_ZSTD);
        for n in [rows, directory.len(), directory_len, compressed.len(), raw.len()] {
            frame.extend_from_slice(&(n as u32).to_le_bytes());
        }
        for (id, external_id) in &directory {
            frame.extend_from_slice(&id.to_le_bytes());
            write_varint(&mut frame, external_id.len());
            frame.extend_from_slice(external_id.as_bytes());
        }
        frame.extend_from_slice(&compressed);
        Ok(Frame {
            bytes: frame,
            rows,
            series: directory.len(),
            raw_len: raw.len(),
            clamped: self.clamped,
        })
    }

    fn write_ipc(&self, kept: &[usize]) -> Result<Vec<u8>, String> {
        let ids: Vec<i64> = kept.iter().map(|&i| self.rows[i].id).collect();
        let timestamps: Vec<i64> = kept.iter().map(|&i| self.rows[i].timestamp).collect();
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(TimestampMillisecondArray::from(timestamps).with_timezone("UTC")),
        ];
        let cells = kept.iter().map(|&i| &self.rows[i].cell);
        match self.value_type {
            DatapointValueType::Bigint => {
                let values: Vec<i64> = cells
                    .map(|c| match c {
                        Cell::I64(v) => *v,
                        _ => unreachable!("bigint frame holds bigint cells"),
                    })
                    .collect();
                columns.push(Arc::new(Int64Array::from(values)));
            }
            DatapointValueType::Float => {
                let values: Vec<f64> = cells
                    .map(|c| match c {
                        Cell::F64(v) => *v,
                        _ => unreachable!("float frame holds float cells"),
                    })
                    .collect();
                columns.push(Arc::new(Float64Array::from(values)));
            }
            DatapointValueType::Float32 => {
                let values: Vec<f32> = cells
                    .map(|c| match c {
                        Cell::F32(v) => *v,
                        _ => unreachable!("float32 frame holds float32 cells"),
                    })
                    .collect();
                columns.push(Arc::new(Float32Array::from(values)));
            }
            DatapointValueType::Numeric | DatapointValueType::Decimal32 => {
                let values: Vec<i128> = cells
                    .map(|c| match c {
                        Cell::Scaled(v) => *v,
                        _ => unreachable!("decimal frame holds scaled cells"),
                    })
                    .collect();
                let (precision, scale) = if self.value_type == DatapointValueType::Numeric {
                    (18, 6)
                } else {
                    (9, 4)
                };
                let array = Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .map_err(|e| e.to_string())?;
                columns.push(Arc::new(array));
            }
            DatapointValueType::Text => {
                let values: Vec<&str> = cells
                    .map(|c| match c {
                        Cell::Text(v) => v.as_str(),
                        _ => unreachable!("text frame holds text cells"),
                    })
                    .collect();
                columns.push(Arc::new(StringArray::from(values)));
            }
            DatapointValueType::Mixed => {
                let mut numeric: Vec<Option<f64>> = Vec::with_capacity(kept.len());
                let mut text: Vec<Option<&str>> = Vec::with_capacity(kept.len());
                for cell in cells {
                    match cell {
                        Cell::MixedNumeric(v) => {
                            numeric.push(Some(*v));
                            text.push(None);
                        }
                        Cell::MixedText(v) => {
                            numeric.push(None);
                            text.push(Some(v.as_str()));
                        }
                        _ => unreachable!("mixed frame holds mixed cells"),
                    }
                }
                columns.push(Arc::new(Float64Array::from(numeric)));
                columns.push(Arc::new(StringArray::from(text)));
            }
        }
        let schema = self.value_type.schema();
        let batch = RecordBatch::try_new(schema.clone(), columns).map_err(|e| e.to_string())?;
        let mut raw = Vec::with_capacity(self.estimated_raw_bytes());
        let mut writer =
            StreamWriter::try_new(&mut raw, schema.as_ref()).map_err(|e| e.to_string())?;
        writer.write(&batch).map_err(|e| e.to_string())?;
        writer.finish().map_err(|e| e.to_string())?;
        drop(writer);
        Ok(raw)
    }
}

/// The server counts the character limit in UTF-16 units, so this does too.
fn checked_text(value: &str) -> Result<String, String> {
    if value.is_empty() {
        return Err("text value is empty".to_string());
    }
    let chars = value.encode_utf16().count();
    if chars > MAX_VALUE_CHARS {
        return Err(format!(
            "text value is {chars} characters, allowed {MAX_VALUE_CHARS}"
        ));
    }
    if value.len() > MAX_VALUE_BYTES {
        return Err(format!(
            "text value is {} bytes, allowed {MAX_VALUE_BYTES}",
            value.len()
        ));
    }
    Ok(value.to_string())
}

/// Splits a decimal literal (`[+-]digits[.digits][e[+-]digits]`, the grammar `BigDecimal` takes)
/// into its sign, its digit string and the number of those digits before the decimal point.
fn decimal_parts(value: &str) -> Option<(bool, String, i32)> {
    let (negative, rest) = match value.as_bytes().first()? {
        b'-' => (true, &value[1..]),
        b'+' => (false, &value[1..]),
        _ => (false, value),
    };
    let (mantissa, exponent) = match rest.find(['e', 'E']) {
        Some(at) => (&rest[..at], rest[at + 1..].parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (integer, fraction) = match mantissa.find('.') {
        Some(at) => (&mantissa[..at], &mantissa[at + 1..]),
        None => (mantissa, ""),
    };
    if integer.is_empty() && fraction.is_empty() {
        return None;
    }
    if !integer.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let point = i32::try_from(integer.len()).ok()?.checked_add(exponent)?;
    Some((negative, format!("{integer}{fraction}"), point))
}

/// Parses a decimal literal to its unscaled value at `scale` decimals, rounded half-up.
fn parse_scaled(value: &str, scale: u32) -> Option<i128> {
    let (negative, digits, point) = decimal_parts(value)?;
    let total = i32::try_from(digits.len()).ok()?;
    // Digits to keep: everything before the point plus `scale` after it.
    let keep = point.checked_add(scale as i32)?;
    let magnitude: i128 = if keep >= total {
        let shift = u32::try_from(keep - total).ok()?;
        if digits.len() as u32 + shift > 38 {
            return None;
        }
        digits.parse::<i128>().ok()?.checked_mul(10i128.checked_pow(shift)?)?
    } else if keep < 0 {
        0
    } else {
        let keep = keep as usize;
        if keep > 38 {
            return None;
        }
        let head: i128 = if keep == 0 { 0 } else { digits[..keep].parse().ok()? };
        let round_up = digits.as_bytes()[keep] >= b'5';
        if round_up {
            head + 1
        } else {
            head
        }
    };
    Some(if negative { -magnitude } else { magnitude })
}

fn varint_size(mut value: usize) -> usize {
    let mut n = 1;
    while value >= 0x80 {
        value >>= 7;
        n += 1;
    }
    n
}

fn write_varint(out: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        out.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Series the binary path has resolved, per service instance. Dropped as a whole when the server
/// says a series is unknown or renamed, so the next call re-reads every series it names.
#[derive(Debug, Default)]
pub(crate) struct SeriesCache {
    by_external_id: HashMap<String, ResolvedSeries>,
    by_id: HashMap<u64, ResolvedSeries>,
}

/// Groups the collections' datapoints into writers, one value type at a time, cutting a new
/// writer whenever the open one would pass a cap.
pub(crate) fn cut_into_writers(
    items: &[(ResolvedSeries, &[DatapointString])],
) -> Result<Vec<FrameWriter>, ResponseError> {
    let mut open: HashMap<DatapointValueType, FrameWriter> = HashMap::new();
    let mut done = Vec::new();
    for (series, datapoints) in items {
        let value_type = series.value_type;
        for dp in datapoints.iter() {
            let timestamp = dp.timestamp.parse::<i64>().map_err(|_| {
                ResponseError::bad_request(format!(
                    "timestamp {:?} of series {} is not epoch milliseconds",
                    dp.timestamp, series.external_id
                ))
            })?;
            let writer = open
                .entry(value_type)
                .or_insert_with(|| FrameWriter::new(value_type));
            if writer.is_full_for(series.id) {
                let full = std::mem::replace(writer, FrameWriter::new(value_type));
                done.push(full);
            }
            writer
                .series(series.id, &series.external_id)
                .map_err(unprocessable)?;
            writer
                .add(series.id, timestamp, &dp.value)
                .map_err(|message| unprocessable(format!("series {}: {message}", series.external_id)))?;
        }
    }
    done.extend(open.into_values().filter(|w| w.row_count() > 0));
    Ok(done)
}

/// Concatenates frames into request bodies under the per-request caps, in order.
pub(crate) fn pack_requests(frames: Vec<Frame>) -> Vec<Vec<u8>> {
    let mut requests = Vec::new();
    let mut body = Vec::new();
    let mut count = 0;
    let mut raw = 0;
    for frame in frames {
        if count > 0
            && (count == MAX_FRAMES_PER_REQUEST || raw + frame.raw_len > MAX_REQUEST_RAW_BYTES)
        {
            requests.push(std::mem::take(&mut body));
            count = 0;
            raw = 0;
        }
        body.extend_from_slice(&frame.bytes);
        count += 1;
        raw += frame.raw_len;
    }
    if count > 0 {
        requests.push(body);
    }
    requests
}

fn unprocessable(message: String) -> ResponseError {
    ResponseError {
        status: StatusCode::UNPROCESSABLE_ENTITY,
        message,
    }
}

/// The server rejected the request because the series named in a frame no longer match what the
/// client resolved: unknown after a delete, or renamed since.
fn is_stale_series_rejection(error: &ResponseError) -> bool {
    let status = error.get_status();
    (status == StatusCode::NOT_FOUND || status == StatusCode::UNPROCESSABLE_ENTITY)
        && (error.message.contains("unknown-timeseries")
            || error.message.contains("external-id-mismatch"))
}

impl TimeSeriesService {
    /// Inserts datapoints through `POST /timeseries/data/binary`, the high-throughput path.
    ///
    /// The same input as [`insert_datapoints`](Self::insert_datapoints), sent as zstd-compressed
    /// Arrow frames instead of JSON: each series is resolved once to its id and value type (through
    /// `/timeseries/byids`, which needs read access on its dataset, and cached on this service),
    /// the values are checked against that type here, sorted and de-duplicated per series, cut into
    /// frames at the contract's caps, compressed in parallel and posted in as many requests as the
    /// caps allow. A 204 means every frame was accepted; a request is all-or-nothing on the server.
    ///
    /// Errors before any request: a series that does not exist, or that the caller cannot read, is
    /// a 404 naming it; a value that does not fit its series' type, or a text value over the
    /// limits, is a 422. A 429 or a 5xx is retried per [`BinaryIngestOptions::max_retries`]. A
    /// request the server refuses because a series was deleted or renamed after it was cached is
    /// rebuilt once after re-resolving. The durable spool does not apply to this path.
    pub async fn insert_datapoints_binary(
        &self,
        json: &DataWrapper<DatapointsCollection<DatapointString>>,
        options: &BinaryIngestOptions,
    ) -> Result<DataWrapper<String>, ResponseError> {
        options.validate()?;
        match self.insert_binary_once(json, options).await {
            Err(error) if is_stale_series_rejection(&error) => {
                self.evict_binary_series_cache();
                self.insert_binary_once(json, options).await
            }
            other => other,
        }
    }

    /// Forgets every series the binary path has resolved, so the next call re-reads them.
    pub fn evict_binary_series_cache(&self) {
        let mut cache = self.binary_series.lock().unwrap();
        cache.by_external_id.clear();
        cache.by_id.clear();
    }

    async fn insert_binary_once(
        &self,
        json: &DataWrapper<DatapointsCollection<DatapointString>>,
        options: &BinaryIngestOptions,
    ) -> Result<DataWrapper<String>, ResponseError> {
        let collections = json.get_items();
        let resolved = self.resolve_series(collections).await?;
        let items: Vec<(ResolvedSeries, &[DatapointString])> = resolved
            .into_iter()
            .zip(collections.iter())
            .map(|(series, collection)| (series, collection.datapoints.as_slice()))
            .collect();
        let writers = cut_into_writers(&items)?;

        let level = options.zstd_level;
        let builds = writers
            .into_iter()
            .map(|writer| tokio::task::spawn_blocking(move || writer.build(level)));
        let mut frames = Vec::new();
        for built in join_all(builds).await {
            let frame = built
                .map_err(|e| ResponseError::bad_request(format!("frame build failed: {e}")))?
                .map_err(unprocessable)?;
            frames.push(frame);
        }

        // Concurrently, not one after another. A call of up to 3.2M points packs into a single
        // request and this changes nothing, but a larger one becomes several and the api
        // validates them independently, so there is no reason to serialise them. Bounded by
        // `request_concurrency` so a very large call cannot open an unbounded number of
        // connections.
        let path = format!("{}/data/binary", self.base_url);
        let bodies = pack_requests(frames);
        let limit = options.request_concurrency.max(1);
        for window in bodies.chunks(limit) {
            let sends = window
                .iter()
                .map(|body| self.post_frames(&path, body.clone(), options.max_retries));
            for outcome in join_all(sends).await {
                outcome?;
            }
        }
        let mut result = DataWrapper::new();
        result.set_http_status_code(204);
        Ok(result)
    }

    async fn post_frames(
        &self,
        path: &str,
        body: Vec<u8>,
        max_retries: u32,
    ) -> Result<(), ResponseError> {
        let mut attempt = 0u32;
        loop {
            match self
                .execute_post_bytes_request::<DataWrapper<String>>(path, body.clone(), MEDIA_TYPE)
                .await
            {
                Ok(_) => return Ok(()),
                Err(error)
                    if attempt < max_retries
                        && (error.get_status() == StatusCode::TOO_MANY_REQUESTS
                            || error.get_status().is_server_error()) =>
                {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_secs(attempt as u64)).await;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// One [`ResolvedSeries`] per collection, in order, from the cache or `/timeseries/byids`.
    async fn resolve_series(
        &self,
        collections: &[DatapointsCollection<DatapointString>],
    ) -> Result<Vec<ResolvedSeries>, ResponseError> {
        let mut lookups: Vec<IdAndExtId> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        {
            let cache = self.binary_series.lock().unwrap();
            for collection in collections {
                match (collection.id, &collection.external_id) {
                    (Some(id), _) => {
                        if !cache.by_id.contains_key(&id) && seen.insert(format!("id:{id}")) {
                            lookups.push(IdAndExtId::from_id(id));
                        }
                    }
                    (None, Some(external_id)) => {
                        if !cache.by_external_id.contains_key(external_id)
                            && seen.insert(format!("ext:{external_id}"))
                        {
                            lookups.push(IdAndExtId::from_external_id(external_id));
                        }
                    }
                    (None, None) => {
                        return Err(ResponseError::bad_request(
                            "a datapoint collection names neither id nor externalId".to_string(),
                        ))
                    }
                }
            }
        }
        for chunk in lookups.chunks(MAX_SERIES_PER_FRAME) {
            let found = self.by_ids(&DataWrapper::from_vec(chunk.to_vec())).await?;
            let mut cache = self.binary_series.lock().unwrap();
            for ts in found.get_items() {
                let value_type = ts.value_type.as_deref().and_then(DatapointValueType::from_name);
                if let (Some(id), Some(value_type)) = (ts.id, value_type) {
                    let series = ResolvedSeries {
                        id,
                        external_id: ts.external_id.clone(),
                        value_type,
                    };
                    cache.by_external_id.insert(series.external_id.clone(), series.clone());
                    cache.by_id.insert(id, series);
                }
            }
        }

        let cache = self.binary_series.lock().unwrap();
        let mut resolved = Vec::with_capacity(collections.len());
        let mut missing = Vec::new();
        for collection in collections {
            let hit = match (collection.id, &collection.external_id) {
                (Some(id), _) => cache.by_id.get(&id),
                (None, Some(external_id)) => cache.by_external_id.get(external_id),
                (None, None) => None,
            };
            match hit {
                Some(series) => resolved.push(series.clone()),
                None => missing.push(
                    collection
                        .id
                        .map(|id| id.to_string())
                        .or_else(|| collection.external_id.clone())
                        .unwrap_or_default(),
                ),
            }
        }
        if !missing.is_empty() {
            return Err(ResponseError {
                status: StatusCode::NOT_FOUND,
                message: format!("Could not find following timeseries: {}", missing.join(", ")),
            });
        }
        Ok(resolved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use arrow_ipc::reader::StreamReader;
    use std::io::Cursor;

    fn u32_at(frame: &[u8], offset: usize) -> u32 {
        u32::from_le_bytes(frame[offset..offset + 4].try_into().unwrap())
    }

    /// Decompresses the payload and reads the stream back with arrow-rs.
    fn read_back(frame: &Frame) -> (Vec<RecordBatch>, SchemaRef) {
        let bytes = &frame.bytes;
        let directory_len = u32_at(bytes, 16) as usize;
        let payload_len = u32_at(bytes, 20) as usize;
        let raw_len = u32_at(bytes, 24) as usize;
        let payload = &bytes[HEADER_BYTES + directory_len..];
        assert_eq!(payload.len(), payload_len);
        let raw = zstd::bulk::decompress(payload, raw_len).unwrap();
        assert_eq!(raw.len(), raw_len);
        let reader = StreamReader::try_new(Cursor::new(raw), None).unwrap();
        let schema = reader.schema();
        let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
        (batches, schema)
    }

    #[test]
    fn float_frame_is_sorted_deduplicated_and_readable_by_arrow() {
        let mut writer = FrameWriter::new(DatapointValueType::Float);
        writer.series(7, "pump_b").unwrap();
        writer.series(3, "pump_a").unwrap();
        writer.add(7, 2_000, "2.5").unwrap();
        writer.add(3, 1_000, "1.0").unwrap();
        writer.add(7, 1_000, "2.0").unwrap();
        writer.add(3, 1_000, "1.5").unwrap(); // same key as the second row: the last one wins
        let frame = writer.build(3).unwrap();

        let bytes = &frame.bytes;
        assert_eq!(&bytes[0..4], MAGIC);
        assert_eq!(bytes[4], VERSION);
        assert_eq!(bytes[5], DatapointValueType::Float.id());
        assert_eq!(bytes[6], CODEC_ARROW_IPC);
        assert_eq!(bytes[7], COMPRESSION_ZSTD);
        assert_eq!(u32_at(bytes, 8), 3, "rows after dedupe");
        assert_eq!(u32_at(bytes, 12), 2, "series");
        assert_eq!(frame.rows, 3);
        assert_eq!(frame.series, 2);

        // Directory: ascending ids, each an i64 then a varint length and the external id.
        let directory_len = u32_at(bytes, 16) as usize;
        let directory = &bytes[HEADER_BYTES..HEADER_BYTES + directory_len];
        let mut expected = Vec::new();
        expected.extend_from_slice(&3i64.to_le_bytes());
        expected.push(6);
        expected.extend_from_slice(b"pump_a");
        expected.extend_from_slice(&7i64.to_le_bytes());
        expected.push(6);
        expected.extend_from_slice(b"pump_b");
        assert_eq!(directory, expected.as_slice());

        let (batches, schema) = read_back(&frame);
        assert_eq!(schema, DatapointValueType::Float.schema());
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let ts = batch.column(1).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        let values = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(ids.values(), &[3, 7, 7]);
        assert_eq!(ts.values(), &[1_000, 1_000, 2_000]);
        assert_eq!(values.values(), &[1.5, 2.0, 2.5]);
    }

    #[test]
    fn every_value_type_writes_its_canonical_schema() {
        for value_type in [
            DatapointValueType::Bigint,
            DatapointValueType::Float,
            DatapointValueType::Float32,
            DatapointValueType::Numeric,
            DatapointValueType::Decimal32,
            DatapointValueType::Text,
            DatapointValueType::Mixed,
        ] {
            let mut writer = FrameWriter::new(value_type);
            writer.series(1, "s").unwrap();
            let value = match value_type {
                DatapointValueType::Text => "on",
                DatapointValueType::Mixed => "off",
                _ => "42",
            };
            writer.add(1, 0, value).unwrap();
            let frame = writer.build(1).unwrap();
            assert_eq!(frame.bytes[5], value_type.id());
            let (batches, schema) = read_back(&frame);
            assert_eq!(schema, value_type.schema(), "{value_type:?}");
            assert_eq!(batches[0].num_rows(), 1);
        }
    }

    #[test]
    fn decimals_are_scaled_half_up_and_decimal32_is_clamped() {
        assert_eq!(parse_scaled("123.456789", 6), Some(123_456_789));
        assert_eq!(parse_scaled("0.0000005", 6), Some(1));
        assert_eq!(parse_scaled("0.0000004", 6), Some(0));
        assert_eq!(parse_scaled("-1.5", 0), Some(-2));
        assert_eq!(parse_scaled("1e3", 2), Some(100_000));
        assert_eq!(parse_scaled("12.34E-1", 4), Some(12_340));
        assert_eq!(parse_scaled(".5", 1), Some(5));
        assert_eq!(parse_scaled("5.", 1), Some(50));
        assert_eq!(parse_scaled("", 6), None);
        assert_eq!(parse_scaled("NaN", 6), None);
        assert_eq!(parse_scaled("1.2.3", 6), None);

        let mut numeric = FrameWriter::new(DatapointValueType::Numeric);
        numeric.series(1, "s").unwrap();
        assert!(numeric.add(1, 0, "1000000000000.5").is_err(), "outside Decimal(18, 6)");

        let mut decimal32 = FrameWriter::new(DatapointValueType::Decimal32);
        decimal32.series(1, "s").unwrap();
        decimal32.add(1, 0, "123456.78").unwrap();
        decimal32.add(1, 1, "-99999.99995").unwrap();
        decimal32.add(1, 2, "99999.9999").unwrap();
        assert_eq!(decimal32.clamped_count(), 2);
        let frame = decimal32.build(1).unwrap();
        let (batches, _) = read_back(&frame);
        let values = batches[0].column(2).as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(values.values(), &[999_999_999, -999_999_999, 999_999_999]);
    }

    #[test]
    fn mixed_rows_set_exactly_one_side() {
        let mut writer = FrameWriter::new(DatapointValueType::Mixed);
        writer.series(1, "s").unwrap();
        writer.add(1, 0, "12.5").unwrap();
        writer.add(1, 1, "open").unwrap();
        writer.add(1, 2, "-3e2").unwrap();
        writer.add(1, 3, "NaN").unwrap(); // not a decimal literal, so text
        let frame = writer.build(1).unwrap();
        let (batches, _) = read_back(&frame);
        let numeric = batches[0].column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let text = batches[0].column(3).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(numeric.null_count(), 2);
        assert_eq!(text.null_count(), 2);
        assert_eq!(numeric.value(0), 12.5);
        assert!(numeric.is_null(1));
        assert_eq!(text.value(1), "open");
        assert_eq!(numeric.value(2), -300.0);
        assert_eq!(text.value(3), "NaN");
    }

    #[test]
    fn text_limits_and_series_registration_are_enforced() {
        let mut writer = FrameWriter::new(DatapointValueType::Text);
        writer.series(1, "s").unwrap();
        assert!(writer.add(1, 0, "").is_err());
        assert!(writer.add(1, 0, &"x".repeat(65)).is_err());
        // 33 astral characters are 66 UTF-16 units, over the limit as the server counts it.
        assert!(writer.add(1, 0, &"\u{1F600}".repeat(33)).unwrap_err().contains("characters"));
        writer.add(1, 0, &"\u{20ac}".repeat(64)).unwrap();
        writer.add(1, 0, &"x".repeat(64)).unwrap();
        writer.add(2, 0, "unregistered").unwrap();
        assert!(writer.build(1).unwrap_err().contains("series 2"));

        let mut typed = FrameWriter::new(DatapointValueType::Float);
        assert!(typed.add_bigint(1, 0, 1).is_err());
        assert!(typed.add(1, 0, "abc").is_err());
        assert!(FrameWriter::new(DatapointValueType::Float).build(1).is_err(), "no rows");
    }

    #[test]
    fn frames_are_cut_at_the_row_cap_and_packed_under_the_request_cap() {
        let series = ResolvedSeries {
            id: 5,
            external_id: "s".to_string(),
            value_type: DatapointValueType::Float,
        };
        let datapoints: Vec<DatapointString> = (0..250_000)
            .map(|i| DatapointString::new(&i.to_string(), "1.0"))
            .collect();
        let writers = cut_into_writers(&[(series.clone(), datapoints.as_slice())]).unwrap();
        let rows: Vec<usize> = writers.iter().map(FrameWriter::row_count).collect();
        assert_eq!(rows, vec![100_000, 100_000, 50_000]);

        let bad = vec![DatapointString::new("2025-01-01T00:00:00Z", "1.0")];
        let error = cut_into_writers(&[(series.clone(), bad.as_slice())]).unwrap_err();
        assert_eq!(error.get_status(), StatusCode::BAD_REQUEST);

        let unfit = vec![DatapointString::new("0", "warm")];
        let error = cut_into_writers(&[(series, unfit.as_slice())]).unwrap_err();
        assert_eq!(error.get_status(), StatusCode::UNPROCESSABLE_ENTITY);

        let frame = |raw_len: usize| Frame {
            bytes: vec![0xAB],
            rows: 1,
            series: 1,
            raw_len,
            clamped: 0,
        };
        let by_count = pack_requests((0..33).map(|_| frame(1)).collect());
        assert_eq!(by_count.iter().map(Vec::len).collect::<Vec<_>>(), vec![32, 1]);
        let by_bytes = pack_requests(vec![
            frame(MAX_REQUEST_RAW_BYTES / 2),
            frame(MAX_REQUEST_RAW_BYTES / 2),
            frame(1),
        ]);
        assert_eq!(by_bytes.iter().map(Vec::len).collect::<Vec<_>>(), vec![2, 1]);
    }

    #[test]
    fn varints_are_unsigned_leb128() {
        let mut out = Vec::new();
        write_varint(&mut out, 0);
        write_varint(&mut out, 127);
        write_varint(&mut out, 128);
        write_varint(&mut out, 300);
        assert_eq!(out, vec![0, 127, 0x80, 0x01, 0xAC, 0x02]);
        assert_eq!(varint_size(127), 1);
        assert_eq!(varint_size(128), 2);
        assert_eq!(varint_size(16_384), 3);
    }

    #[test]
    fn options_accept_only_the_three_levels() {
        assert!(BinaryIngestOptions::default().validate().is_ok());
        assert_eq!(BinaryIngestOptions::default().zstd_level, 9);
        assert!(BinaryIngestOptions::new().zstd_level(3).validate().is_ok());
        assert!(BinaryIngestOptions::new().zstd_level(0).validate().is_err());
        assert!(BinaryIngestOptions::new().zstd_level(22).validate().is_err());
    }
}
