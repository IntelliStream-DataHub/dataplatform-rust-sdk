//! The binary datapoint read behind `POST /timeseries/data/list/binary`.
//!
//! The request is the JSON retrieve body; the answer is the frames `POST /timeseries/data/binary`
//! takes, one series per frame, concatenated, and ended by the 4-byte marker `DHDE`. A body without
//! the marker was cut short, whether inside a frame or between two, and is an error rather than a
//! shorter answer. Codec 2 frames carry bucketed aggregates instead of values.
//!
//! [`TimeSeriesService::stream_datapoints_binary`] hands the frames over as they arrive, decoded
//! on blocking threads while the rest download; [`TimeSeriesService::retrieve_datapoints_binary`]
//! collects that stream into one [`BinaryDatapoints`] per series. [`parse_read_response`] does the
//! same for a body already in memory.

use std::io::Cursor;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{
    Array, Decimal128Array, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use arrow_ipc::reader::StreamReader;
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bytes::{Buf, Bytes, BytesMut};
use futures::stream::{FuturesOrdered, Stream, StreamExt};
use tokio::sync::mpsc;
use oauth2::http::StatusCode;

use crate::generic::{ApiServiceProvider, DataWrapper, RetrieveFilter};
use crate::http::ResponseError;
use crate::timeseries::binary::{
    DatapointValueType, CODEC_ARROW_IPC, COMPRESSION_ZSTD, HEADER_BYTES, MAGIC, MAX_FRAME_RAW_BYTES,
    MEDIA_TYPE, VERSION,
};
use crate::timeseries::TimeSeriesService;

/// Ends a binary read's response, after the last frame.
pub const END_OF_RESPONSE: &[u8; 4] = b"DHDE";
/// A response frame whose payload holds bucketed aggregates in [`aggregates_schema`].
pub const CODEC_ARROW_IPC_AGGREGATES: u8 = 2;
/// Buckets per aggregate frame, whatever the series' type.
pub const MAX_AGGREGATE_ROWS: usize = 50_000;

/// The schema of an aggregate frame: the bucket start and four Float64s, for every value type.
pub fn aggregates_schema() -> SchemaRef {
    let float = |name: &str| Field::new(name, DataType::Float64, false);
    Arc::new(Schema::new(vec![
        Field::new("timeseries_id", DataType::Int64, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        float("avg"),
        float("min"),
        float("max"),
        float("sum"),
    ]))
}

/// One series' rows from a binary read, a column per field rather than an object per point.
#[derive(Debug, Clone, PartialEq)]
pub struct BinaryDatapoints {
    pub id: u64,
    pub external_id: String,
    pub value_type: DatapointValueType,
    /// Epoch milliseconds, oldest first; for aggregates, the start of each bucket.
    pub timestamps: Vec<i64>,
    pub values: BinaryValues,
}

impl BinaryDatapoints {
    pub fn len(&self) -> usize {
        self.timestamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.timestamps.is_empty()
    }

    pub fn is_aggregates(&self) -> bool {
        matches!(self.values, BinaryValues::Aggregates { .. })
    }
}

/// The value column of a [`BinaryDatapoints`], typed by the series' value type.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum BinaryValues {
    Bigint(Vec<i64>),
    Float(Vec<f64>),
    Float32(Vec<f32>),
    /// `numeric` and `decimal32`, exact: each value is `unscaled / 10^scale`.
    Decimal { unscaled: Vec<i128>, scale: i8 },
    Text(Vec<String>),
    /// Each row sets exactly one side.
    Mixed {
        numeric: Vec<Option<f64>>,
        text: Vec<Option<String>>,
    },
    /// Time-weighted `avg`, `min`, `max` and `sum` per bucket. All four come back whichever the
    /// request named.
    Aggregates {
        avg: Vec<f64>,
        min: Vec<f64>,
        max: Vec<f64>,
        sum: Vec<f64>,
    },
}

impl BinaryValues {
    fn reserve_exact(&mut self, additional: usize) {
        match self {
            Self::Bigint(v) => v.reserve_exact(additional),
            Self::Float(v) => v.reserve_exact(additional),
            Self::Float32(v) => v.reserve_exact(additional),
            Self::Decimal { unscaled, .. } => unscaled.reserve_exact(additional),
            Self::Text(v) => v.reserve_exact(additional),
            Self::Mixed { numeric, text } => {
                numeric.reserve_exact(additional);
                text.reserve_exact(additional);
            }
            Self::Aggregates { avg, min, max, sum } => {
                for v in [avg, min, max, sum] {
                    v.reserve_exact(additional);
                }
            }
        }
    }

    /// Appends `other`, which must be the same variant; false if it is not.
    fn append(&mut self, other: BinaryValues) -> bool {
        match (self, other) {
            (Self::Bigint(a), Self::Bigint(b)) => a.extend(b),
            (Self::Float(a), Self::Float(b)) => a.extend(b),
            (Self::Float32(a), Self::Float32(b)) => a.extend(b),
            (
                Self::Decimal { unscaled: a, scale: sa },
                Self::Decimal { unscaled: b, scale: sb },
            ) if *sa == sb => a.extend(b),
            (Self::Text(a), Self::Text(b)) => a.extend(b),
            (
                Self::Mixed { numeric: an, text: at },
                Self::Mixed { numeric: bn, text: bt },
            ) => {
                an.extend(bn);
                at.extend(bt);
            }
            (
                Self::Aggregates { avg: aa, min: ai, max: ax, sum: asum },
                Self::Aggregates { avg: ba, min: bi, max: bx, sum: bsum },
            ) => {
                aa.extend(ba);
                ai.extend(bi);
                ax.extend(bx);
                asum.extend(bsum);
            }
            _ => return false,
        }
        true
    }
}

fn u32_at(body: &[u8], offset: usize) -> usize {
    u32::from_le_bytes(body[offset..offset + 4].try_into().unwrap()) as usize
}

fn read_varint(body: &[u8], pos: &mut usize) -> Option<usize> {
    let mut value = 0usize;
    for shift in (0..35).step_by(7) {
        let byte = *body.get(*pos)?;
        *pos += 1;
        value |= ((byte & 0x7F) as usize) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

/// Parses a binary read's whole response body: one entry per series in the order the frames came,
/// which is the order of the request's items, with consecutive frames of one series merged.
///
/// The envelopes are walked first, which is cheap and finds a body that was cut short; the frames
/// are then decompressed and decoded on several threads, since zstd is most of the cost.
///
/// Errors on anything short of a complete, well-formed answer: a missing end marker, bytes after
/// it, a frame that breaks the envelope, or a payload outside its canon.
pub fn parse_read_response(body: &[u8]) -> Result<Vec<BinaryDatapoints>, String> {
    let mut assembler = FrameAssembler::new();
    assembler.push(body);
    let mut frames = Vec::new();
    while let Some(frame) = assembler.next_frame()? {
        frames.push(frame);
    }
    assembler.finish()?;
    let decoded = decode_frames(&frames)?;
    merge_series(decoded.into_iter().flatten())
}

/// Merges consecutive parts of one series, the way they come off the frames, into one entry each.
fn merge_series(
    parts: impl IntoIterator<Item = BinaryDatapoints>,
) -> Result<Vec<BinaryDatapoints>, String> {
    // Grouped before merging so each series is allocated once at its final size: extending part
    // by part regrows and recopies a long series dozens of times.
    let mut groups: Vec<Vec<BinaryDatapoints>> = Vec::new();
    for part in parts {
        let continues = groups.last().and_then(|g| g.last()).is_some_and(|last| {
            last.id == part.id
                && last.is_aggregates() == part.is_aggregates()
                && last.timestamps.last() < part.timestamps.first()
        });
        if continues {
            groups.last_mut().unwrap().push(part);
        } else {
            groups.push(vec![part]);
        }
    }
    groups.into_iter().map(merge).collect()
}

/// One series from its consecutive parts, in order.
fn merge(parts: Vec<BinaryDatapoints>) -> Result<BinaryDatapoints, String> {
    let rows: usize = parts.iter().map(BinaryDatapoints::len).sum();
    let mut parts = parts.into_iter();
    let mut series = parts.next().expect("a group has at least one part");
    series.timestamps.reserve_exact(rows - series.len());
    series.values.reserve_exact(rows - series.len());
    for part in parts {
        let BinaryDatapoints { timestamps, values, .. } = part;
        series.timestamps.extend(timestamps);
        if !series.values.append(values) {
            return Err(format!("series {} changes shape between frames", series.id));
        }
    }
    Ok(series)
}

fn cut_short(index: usize) -> String {
    format!("the response ends without its end marker, at frame {index}; it was cut short")
}

/// Cuts a response into frames as its bytes arrive, in chunks of any size: a frame's header, its
/// directory and its payload may each be split anywhere.
struct FrameAssembler {
    buf: BytesMut,
    index: usize,
    ended: bool,
}

impl FrameAssembler {
    fn new() -> Self {
        FrameAssembler {
            buf: BytesMut::new(),
            index: 0,
            ended: false,
        }
    }

    fn push(&mut self, chunk: &[u8]) {
        self.buf.extend_from_slice(chunk);
    }

    /// The next complete frame; `None` until more bytes arrive, and after the end marker.
    fn next_frame(&mut self) -> Result<Option<FrameView>, String> {
        if !self.ended && self.buf.starts_with(END_OF_RESPONSE) {
            self.ended = true;
            self.buf.advance(END_OF_RESPONSE.len());
        }
        if self.ended {
            if !self.buf.is_empty() {
                return Err(format!("{} bytes after the end marker", self.buf.len()));
            }
            return Ok(None);
        }
        if self.buf.len() >= MAGIC.len() && !self.buf.starts_with(MAGIC) {
            return Err(format!("frame {}: does not start with DHDP", self.index));
        }
        if self.buf.len() < HEADER_BYTES {
            return Ok(None);
        }
        let total = check_header(&self.buf[..HEADER_BYTES], self.index)?;
        if self.buf.len() < total {
            return Ok(None);
        }
        let frame = FrameView::parse(self.buf.split_to(total).freeze(), self.index)?;
        self.index += 1;
        Ok(Some(frame))
    }

    /// After the last byte: the body has to have ended with its marker and nothing after it.
    fn finish(&mut self) -> Result<(), String> {
        if self.next_frame()?.is_some() || !self.ended {
            return Err(cut_short(self.index));
        }
        Ok(())
    }
}

/// Checks a frame's 28-byte header and returns the frame's whole length.
fn check_header(header: &[u8], index: usize) -> Result<usize, String> {
    let fail = |message: String| format!("frame {index}: {message}");
    if header[4] != VERSION {
        return Err(fail(format!("version {}", header[4])));
    }
    let value_type = DatapointValueType::from_id(header[5])
        .ok_or_else(|| fail(format!("value type id {}", header[5])))?;
    let aggregates = match header[6] {
        CODEC_ARROW_IPC => false,
        CODEC_ARROW_IPC_AGGREGATES => true,
        codec => return Err(fail(format!("codec {codec}"))),
    };
    if header[7] != COMPRESSION_ZSTD {
        return Err(fail(format!("compression {}", header[7])));
    }
    let rows = u32_at(header, 8);
    let max_rows = if aggregates {
        MAX_AGGREGATE_ROWS
    } else {
        value_type.max_rows()
    };
    if rows < 1 || rows > max_rows {
        return Err(fail(format!("{rows} rows, allowed 1 to {max_rows}")));
    }
    let raw_len = u32_at(header, 24);
    if raw_len > MAX_FRAME_RAW_BYTES {
        return Err(fail(format!(
            "payload is {raw_len} bytes raw, the cap is {MAX_FRAME_RAW_BYTES}"
        )));
    }
    Ok(HEADER_BYTES + u32_at(header, 16) + u32_at(header, 20))
}

/// One complete frame whose header passed [`check_header`], its payload still compressed. Owns its
/// bytes, so it can be decoded on another thread.
struct FrameView {
    index: usize,
    value_type: DatapointValueType,
    aggregates: bool,
    rows: usize,
    names: Vec<(i64, String)>,
    bytes: Bytes,
    payload_start: usize,
    raw_len: usize,
}

impl FrameView {
    fn parse(bytes: Bytes, index: usize) -> Result<Self, String> {
        let fail = |message: String| format!("frame {index}: {message}");
        let series_count = u32_at(&bytes, 12);
        let directory_len = u32_at(&bytes, 16);
        let directory = &bytes[HEADER_BYTES..HEADER_BYTES + directory_len];
        let mut names: Vec<(i64, String)> = Vec::with_capacity(series_count);
        let mut at = 0;
        for _ in 0..series_count {
            let id = directory
                .get(at..at + 8)
                .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
                .ok_or_else(|| fail("directory is shorter than its series count".to_string()))?;
            at += 8;
            let len = read_varint(directory, &mut at)
                .ok_or_else(|| fail("directory is shorter than its series count".to_string()))?;
            let name = directory
                .get(at..at + len)
                .ok_or_else(|| fail("directory is shorter than its series count".to_string()))?;
            at += len;
            let name = String::from_utf8(name.to_vec())
                .map_err(|_| fail(format!("external id of series {id} is not UTF-8")))?;
            names.push((id, name));
        }
        if at != directory_len {
            return Err(fail(format!("{} bytes after the directory", directory_len - at)));
        }
        let aggregates = bytes[6] == CODEC_ARROW_IPC_AGGREGATES;
        Ok(FrameView {
            index,
            value_type: DatapointValueType::from_id(bytes[5]).expect("checked with the header"),
            aggregates,
            rows: u32_at(&bytes, 8),
            names,
            payload_start: HEADER_BYTES + directory_len,
            raw_len: u32_at(&bytes, 24),
            bytes,
        })
    }
}

/// Decodes every frame, on as many threads as there are cores and frames, keeping their order.
/// On failure, the error of the earliest frame that failed.
fn decode_frames(frames: &[FrameView]) -> Result<Vec<Vec<BinaryDatapoints>>, String> {
    let threads = std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(frames.len());
    if threads <= 1 {
        return frames.iter().map(decode_frame).collect();
    }
    let next = AtomicUsize::new(0);
    let mut decoded: Vec<(usize, Result<Vec<BinaryDatapoints>, String>)> =
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..threads)
                .map(|_| {
                    scope.spawn(|| {
                        let mut done = Vec::new();
                        loop {
                            let i = next.fetch_add(1, Ordering::Relaxed);
                            let Some(frame) = frames.get(i) else { return done };
                            done.push((i, decode_frame(frame)));
                        }
                    })
                })
                .collect();
            workers
                .into_iter()
                .flat_map(|w| w.join().expect("a frame decoder panicked"))
                .collect()
        });
    decoded.sort_unstable_by_key(|(i, _)| *i);
    decoded.into_iter().map(|(_, result)| result).collect()
}

/// One frame's rows, split into one part per series it names.
fn decode_frame(frame: &FrameView) -> Result<Vec<BinaryDatapoints>, String> {
    let fail = |message: String| format!("frame {}: {message}", frame.index);
    let raw = zstd::bulk::decompress(&frame.bytes[frame.payload_start..], frame.raw_len)
        .map_err(|e| fail(format!("zstd: {e}")))?;
    if raw.len() != frame.raw_len {
        return Err(fail(format!(
            "payload decompressed to {} bytes, the envelope says {}",
            raw.len(),
            frame.raw_len
        )));
    }
    let reader =
        StreamReader::try_new(Cursor::new(raw), None).map_err(|e| fail(format!("arrow: {e}")))?;
    let expected = if frame.aggregates {
        aggregates_schema()
    } else {
        frame.value_type.schema()
    };
    let schema = reader.schema();
    let same_columns = schema.fields().len() == expected.fields().len()
        && schema
            .fields()
            .iter()
            .zip(expected.fields())
            .all(|(got, want)| got.name() == want.name() && got.data_type() == want.data_type());
    if !same_columns {
        return Err(fail(format!("schema {schema:?} is not the canon {expected:?}")));
    }

    let mut parts: Vec<BinaryDatapoints> = Vec::new();
    let mut read = 0;
    for batch in reader {
        let batch = batch.map_err(|e| fail(format!("arrow: {e}")))?;
        read += batch.num_rows();
        split_batch(&batch, frame.value_type, frame.aggregates, &frame.names, &mut parts)
            .map_err(fail)?;
    }
    if read != frame.rows {
        return Err(fail(format!(
            "{read} rows in the payload, the envelope says {}",
            frame.rows
        )));
    }
    Ok(parts)
}

type Item = Result<BinaryDatapoints, ResponseError>;

/// A binary read as it arrives: one [`BinaryDatapoints`] per frame, in the order the frames come,
/// which is the order of the request's items. Consecutive items may belong to the same series —
/// a series longer than one frame comes as several, oldest first — so an item is a *chunk* of a
/// series, not all of it.
///
/// Frames are decoded on blocking threads, several at a time, while the rest are still
/// downloading. A refusal is not an item: it is the error [`TimeSeriesService::stream_datapoints_binary`]
/// returns before there is a stream. **A response that breaks off is an error item after the
/// items that did arrive**, so a consumer that acts on items as they come has to treat what it
/// already took as incomplete when one appears; the stream ends after it. Dropping the stream
/// closes the connection.
///
/// Read it with [`futures::StreamExt::next`].
pub struct BinaryDatapointStream {
    items: mpsc::Receiver<Item>,
    task: tokio::task::JoinHandle<()>,
}

impl Stream for BinaryDatapointStream {
    type Item = Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Item>> {
        self.items.poll_recv(cx)
    }
}

impl Drop for BinaryDatapointStream {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Frames decoded at once. Past a handful the decoders only compete for memory bandwidth.
fn decoders() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get()).clamp(1, 8)
}

fn unreadable(path: &str, message: String) -> ResponseError {
    ResponseError {
        status: StatusCode::BAD_GATEWAY,
        message: format!("POST {path} returned an unreadable answer: {message}"),
        content_type: None,
    }
}

/// Reads the body, cuts it into frames, decodes up to [`decoders`] of them at once and sends their
/// parts on in frame order. Whatever fails, the parts of the frames before it are sent first.
async fn produce(response: reqwest::Response, path: String, items: mpsc::Sender<Item>) {
    let limit = decoders();
    let mut body = response.bytes_stream();
    let mut assembler = FrameAssembler::new();
    let mut pending = FuturesOrdered::new();
    let mut failure: Option<ResponseError> = None;
    let mut eof = false;
    loop {
        while failure.is_none() && pending.len() < limit {
            match assembler.next_frame() {
                Ok(Some(frame)) => {
                    pending.push_back(tokio::task::spawn_blocking(move || decode_frame(&frame)))
                }
                Ok(None) => break,
                Err(message) => failure = Some(unreadable(&path, message)),
            }
        }
        let reading = !eof && failure.is_none() && pending.len() < limit;
        if pending.is_empty() && !reading {
            let last = match failure {
                Some(error) => Some(error),
                None => assembler.finish().err().map(|m| unreadable(&path, m)),
            };
            if let Some(error) = last {
                let _ = items.send(Err(error)).await;
            }
            return;
        }
        tokio::select! {
            biased;
            Some(decoded) = pending.next(), if !pending.is_empty() => match decoded {
                Ok(Ok(parts)) => {
                    for part in parts {
                        if items.send(Ok(part)).await.is_err() {
                            return;
                        }
                    }
                }
                // The earliest frame still in flight, so its error comes before any other.
                Ok(Err(message)) => {
                    failure = Some(unreadable(&path, message));
                    pending = FuturesOrdered::new();
                }
                Err(join) => {
                    failure = Some(ResponseError {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        message: format!("decoding a frame failed: {join}"),
                        content_type: None,
                    });
                    pending = FuturesOrdered::new();
                }
            },
            chunk = body.next(), if reading => match chunk {
                Some(Ok(bytes)) => assembler.push(&bytes),
                Some(Err(error)) => {
                    failure = Some(ResponseError {
                        status: StatusCode::BAD_GATEWAY,
                        message: format!("the response was cut short while it was being read: {error}"),
                        content_type: None,
                    })
                }
                None => eof = true,
            },
        }
    }
}

/// Appends a batch's rows to `parts`, one part per run of a series id.
fn split_batch(
    batch: &RecordBatch,
    value_type: DatapointValueType,
    aggregates: bool,
    names: &[(i64, String)],
    parts: &mut Vec<BinaryDatapoints>,
) -> Result<(), String> {
    let ids = column::<Int64Array>(batch, 0)?;
    let timestamps = column::<TimestampMillisecondArray>(batch, 1)?;
    let values = batch_values(batch, value_type, aggregates)?;
    let mut start = 0;
    while start < batch.num_rows() {
        let id = ids.value(start);
        let mut end = start + 1;
        while end < batch.num_rows() && ids.value(end) == id {
            end += 1;
        }
        let external_id = names
            .iter()
            .find(|(named, _)| *named == id)
            .map(|(_, name)| name.clone())
            .ok_or_else(|| format!("series {id} is not in the directory"))?;
        let part = BinaryDatapoints {
            id: id as u64,
            external_id,
            value_type,
            timestamps: timestamps.values()[start..end].to_vec(),
            values: slice_values(&values, start, end),
        };
        match parts.last_mut() {
            Some(last) if last.id == part.id => {
                last.timestamps.extend(part.timestamps);
                last.values.append(part.values);
            }
            _ => parts.push(part),
        }
        start = end;
    }
    Ok(())
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, String> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("column {index} is not of its canonical type"))
}

fn batch_values(
    batch: &RecordBatch,
    value_type: DatapointValueType,
    aggregates: bool,
) -> Result<BinaryValues, String> {
    let floats = |index| column::<Float64Array>(batch, index).map(|a| a.values().to_vec());
    if aggregates {
        return Ok(BinaryValues::Aggregates {
            avg: floats(2)?,
            min: floats(3)?,
            max: floats(4)?,
            sum: floats(5)?,
        });
    }
    Ok(match value_type {
        DatapointValueType::Bigint => {
            BinaryValues::Bigint(column::<Int64Array>(batch, 2)?.values().to_vec())
        }
        DatapointValueType::Float => BinaryValues::Float(floats(2)?),
        DatapointValueType::Float32 => {
            BinaryValues::Float32(column::<Float32Array>(batch, 2)?.values().to_vec())
        }
        DatapointValueType::Numeric | DatapointValueType::Decimal32 => {
            let array = column::<Decimal128Array>(batch, 2)?;
            BinaryValues::Decimal {
                unscaled: array.values().to_vec(),
                scale: array.scale(),
            }
        }
        DatapointValueType::Text => {
            let array = column::<StringArray>(batch, 2)?;
            BinaryValues::Text(array.iter().map(|v| v.unwrap_or_default().to_string()).collect())
        }
        DatapointValueType::Mixed => {
            let numeric = column::<Float64Array>(batch, 2)?;
            let text = column::<StringArray>(batch, 3)?;
            BinaryValues::Mixed {
                numeric: numeric.iter().collect(),
                text: text.iter().map(|v| v.map(str::to_string)).collect(),
            }
        }
    })
}

fn slice_values(values: &BinaryValues, start: usize, end: usize) -> BinaryValues {
    match values {
        BinaryValues::Bigint(v) => BinaryValues::Bigint(v[start..end].to_vec()),
        BinaryValues::Float(v) => BinaryValues::Float(v[start..end].to_vec()),
        BinaryValues::Float32(v) => BinaryValues::Float32(v[start..end].to_vec()),
        BinaryValues::Decimal { unscaled, scale } => BinaryValues::Decimal {
            unscaled: unscaled[start..end].to_vec(),
            scale: *scale,
        },
        BinaryValues::Text(v) => BinaryValues::Text(v[start..end].to_vec()),
        BinaryValues::Mixed { numeric, text } => BinaryValues::Mixed {
            numeric: numeric[start..end].to_vec(),
            text: text[start..end].to_vec(),
        },
        BinaryValues::Aggregates { avg, min, max, sum } => BinaryValues::Aggregates {
            avg: avg[start..end].to_vec(),
            min: min[start..end].to_vec(),
            max: max[start..end].to_vec(),
            sum: sum[start..end].to_vec(),
        },
    }
}

impl TimeSeriesService {
    /// Reads datapoints through `POST /timeseries/data/list/binary`, the high-throughput twin of
    /// [`retrieve_datapoints`](Self::retrieve_datapoints): the same request, answered as
    /// zstd-compressed Arrow frames instead of JSON, and returned as one columnar
    /// [`BinaryDatapoints`] per series, in the order of the request's items.
    ///
    /// Each item needs a `start` or an `end`. With a `limit` (at most 100 000) it returns that many
    /// of the oldest points from there; without one it returns **every** point in its window,
    /// however many, so bound a long series with `end`: the whole answer is held in memory. A
    /// series with no points in its window is left out of the answer rather than returned empty.
    /// There is no cursor (sending one is a 400): to read on, send the request again with `start`
    /// one millisecond past the last timestamp received. An item with `aggregates` and a
    /// `granularity` comes back as [`BinaryValues::Aggregates`], carrying all four whichever were
    /// named; text series have none.
    ///
    /// Unlike the JSON read, a series that does not exist or sits in a dataset the caller cannot
    /// read is a 400 for the whole request, not an empty result. A response the server could not
    /// finish is a 502 whose message says it was cut short, never fewer points; sending the
    /// request again is safe.
    ///
    /// The frames are decoded as they arrive, so this costs little more than the download; for
    /// rows as they come, or a window too large to hold, use
    /// [`stream_datapoints_binary`](Self::stream_datapoints_binary).
    pub async fn retrieve_datapoints_binary(
        &self,
        json: &DataWrapper<RetrieveFilter>,
    ) -> Result<Vec<BinaryDatapoints>, ResponseError> {
        let path = format!("{}/data/list/binary", self.base_url);
        let mut stream = self.stream_datapoints_binary(json).await?;
        let mut parts = Vec::new();
        while let Some(part) = stream.next().await {
            parts.push(part?);
        }
        tokio::task::spawn_blocking(move || merge_series(parts))
            .await
            .map_err(|e| ResponseError {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                message: format!("merging the binary read failed: {e}"),
                content_type: None,
            })?
            .map_err(|message| unreadable(&path, message))
    }

    /// The same read as [`retrieve_datapoints_binary`](Self::retrieve_datapoints_binary), handed
    /// over frame by frame as the response arrives instead of collected: see
    /// [`BinaryDatapointStream`] for what an item is and how a broken-off response shows up.
    ///
    /// The request is checked before there is a stream, so a refusal (a 400 for an unknown
    /// series, a `cursor`, aggregates on a text series) is the `Err` here rather than an item.
    /// Holding the stream holds the connection, and the server's query, open: a consumer that
    /// stops reading for long can run into the server's timeouts.
    pub async fn stream_datapoints_binary(
        &self,
        json: &DataWrapper<RetrieveFilter>,
    ) -> Result<BinaryDatapointStream, ResponseError> {
        let path = format!("{}/data/list/binary", self.base_url);
        let response = self
            .execute_post_request_for_stream(&path, json, MEDIA_TYPE)
            .await?;
        let (tx, items) = mpsc::channel(2 * decoders());
        let task = tokio::spawn(produce(response, path, tx));
        Ok(BinaryDatapointStream { items, task })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timeseries::binary::FrameWriter;
    use arrow_ipc::writer::StreamWriter;

    /// Written by the platform's own frame writer (`DatapointFrameWriter.wrap`, the code that
    /// answers a binary read), from `feat/binary-datapoint-read`: one series per value type, then
    /// the end marker. What the Rust reader must accept from the Java writer.
    const JAVA_VALUES: &[u8] = include_bytes!("fixtures/read_values.bin");
    /// One codec-2 frame of three hourly buckets for series 21, `rd_aggregates`.
    const JAVA_AGGREGATES: &[u8] = include_bytes!("fixtures/read_aggregates.bin");

    const T0: i64 = 1_700_000_000_000;

    fn response(frames: &[Vec<u8>]) -> Vec<u8> {
        let mut body: Vec<u8> = frames.concat();
        body.extend_from_slice(END_OF_RESPONSE);
        body
    }

    fn float_frame(id: u64, external_id: &str, timestamps: std::ops::Range<i64>) -> Vec<u8> {
        let mut writer = FrameWriter::new(DatapointValueType::Float);
        writer.series(id, external_id).unwrap();
        for t in timestamps {
            writer.add_float(id, t, t as f64 / 2.0).unwrap();
        }
        writer.build(1).unwrap().bytes
    }

    /// A frame around any batch, with any codec byte: how a test makes a codec-2 frame.
    fn frame_around(value_type: DatapointValueType, codec: u8, id: i64, name: &str, batch: &RecordBatch) -> Vec<u8> {
        let mut raw = Vec::new();
        let mut writer = StreamWriter::try_new(&mut raw, batch.schema().as_ref()).unwrap();
        writer.write(batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let compressed = zstd::bulk::compress(&raw, 1).unwrap();
        let mut directory = id.to_le_bytes().to_vec();
        directory.push(name.len() as u8);
        directory.extend_from_slice(name.as_bytes());
        let mut frame = MAGIC.to_vec();
        frame.extend_from_slice(&[VERSION, value_type.id(), codec, COMPRESSION_ZSTD]);
        for n in [batch.num_rows(), 1, directory.len(), compressed.len(), raw.len()] {
            frame.extend_from_slice(&(n as u32).to_le_bytes());
        }
        frame.extend_from_slice(&directory);
        frame.extend_from_slice(&compressed);
        frame
    }

    #[test]
    fn reads_every_value_type_the_java_writer_answers_with() {
        let series = parse_read_response(JAVA_VALUES).unwrap();
        let names: Vec<&str> = series.iter().map(|s| s.external_id.as_str()).collect();
        assert_eq!(
            names,
            ["rd_bigint", "rd_float", "rd_float32", "rd_numeric", "rd_decimal32", "rd_text", "rd_mixed", "rd_mixed_numbers"]
        );

        let bigint = &series[0];
        assert_eq!((bigint.id, bigint.value_type), (11, DatapointValueType::Bigint));
        assert_eq!(bigint.timestamps, [T0, T0 + 1000, T0 + 2000]);
        assert_eq!(bigint.values, BinaryValues::Bigint(vec![1, 42, -9_000_000_000]));

        assert_eq!(series[1].values, BinaryValues::Float(vec![1.5, -2.25]));
        assert_eq!(series[2].values, BinaryValues::Float32(vec![0.5]));
        assert_eq!(
            series[3].values,
            BinaryValues::Decimal { unscaled: vec![123_456_789_012_345_678, -1], scale: 6 }
        );
        assert_eq!(
            series[4].values,
            BinaryValues::Decimal { unscaled: vec![123_456_789], scale: 4 }
        );
        assert_eq!(
            series[5].values,
            BinaryValues::Text(vec!["open".to_string(), "blåbær".to_string()])
        );
        assert_eq!(
            series[6].values,
            BinaryValues::Mixed {
                numeric: vec![Some(3.5), None, Some(-7.0)],
                text: vec![None, Some("fault".to_string()), None],
            }
        );
        // No text at all: the writer leaves the validity bitmap out of the numeric column.
        assert_eq!(
            series[7].values,
            BinaryValues::Mixed { numeric: vec![Some(1.0), Some(2.0)], text: vec![None, None] }
        );
    }

    #[test]
    fn reads_an_aggregate_frame_the_java_writer_answers_with() {
        let series = parse_read_response(JAVA_AGGREGATES).unwrap();
        assert_eq!(series.len(), 1);
        let s = &series[0];
        assert_eq!((s.id, s.external_id.as_str()), (21, "rd_aggregates"));
        assert_eq!(s.value_type, DatapointValueType::Float, "the series' own type, not the canon's");
        assert!(s.is_aggregates());
        assert_eq!(s.timestamps, [T0, T0 + 3_600_000, T0 + 7_200_000]);
        assert_eq!(
            s.values,
            BinaryValues::Aggregates {
                avg: vec![1.5, 2.5, 3.5],
                min: vec![1.0, 2.0, 3.0],
                max: vec![2.0, 3.0, 4.0],
                sum: vec![6.0, 10.0, 14.0],
            }
        );
    }

    #[test]
    fn consecutive_frames_of_one_series_merge_in_order() {
        let body = response(&[
            float_frame(1, "a", 0..3),
            float_frame(1, "a", 3..5),
            float_frame(2, "b", 0..2),
        ]);
        let series = parse_read_response(&body).unwrap();
        assert_eq!(series.len(), 2);
        assert_eq!(series[0].timestamps, [0, 1, 2, 3, 4]);
        assert_eq!(series[0].values, BinaryValues::Float(vec![0.0, 0.5, 1.0, 1.5, 2.0]));
        assert_eq!(series[1].external_id, "b");
    }

    /// Decoded on several threads, merged in the order the frames came, and a failure reported for
    /// the earliest frame that has one, whichever thread reached it first.
    #[test]
    fn many_frames_decode_in_parallel_and_keep_their_order() {
        let mut frames: Vec<Vec<u8>> = Vec::new();
        for id in 1..=64u64 {
            let start = id as i64 * 1_000;
            frames.push(float_frame(id, &format!("s{id}"), start..start + 3));
            frames.push(float_frame(id, &format!("s{id}"), start + 3..start + 5));
        }
        let series = parse_read_response(&response(&frames)).unwrap();
        assert_eq!(series.len(), 64);
        for (i, s) in series.iter().enumerate() {
            let id = i as u64 + 1;
            assert_eq!((s.id, s.external_id.as_str()), (id, format!("s{id}").as_str()));
            let start = id as i64 * 1_000;
            assert_eq!(s.timestamps, (start..start + 5).collect::<Vec<_>>());
        }

        let batch = RecordBatch::try_new(
            DatapointValueType::Float.schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let bad = frame_around(DatapointValueType::Float, CODEC_ARROW_IPC_AGGREGATES, 1, "a", &batch);
        frames[100] = bad.clone();
        frames[17] = bad;
        let error = parse_read_response(&response(&frames)).unwrap_err();
        assert!(error.starts_with("frame 17:"), "{error}");
    }

    fn assemble_in_chunks(body: &[u8], chunk: usize) -> Result<Vec<BinaryDatapoints>, String> {
        let mut assembler = FrameAssembler::new();
        let mut frames = Vec::new();
        for piece in body.chunks(chunk) {
            assembler.push(piece);
            while let Some(frame) = assembler.next_frame()? {
                frames.push(frame);
            }
        }
        assembler.finish()?;
        merge_series(decode_frames(&frames)?.into_iter().flatten())
    }

    #[test]
    fn a_frame_may_be_split_anywhere() {
        for body in [JAVA_VALUES, JAVA_AGGREGATES] {
            let whole = parse_read_response(body).unwrap();
            for chunk in [1, 3, 7, 28, 1000, 65536] {
                assert_eq!(assemble_in_chunks(body, chunk).unwrap(), whole, "chunks of {chunk}");
            }
        }
        // Cut inside the marker itself: "DH" then nothing.
        let mut body = response(&[float_frame(1, "a", 0..2)]);
        body.truncate(body.len() - 2);
        assert!(assemble_in_chunks(&body, 1).unwrap_err().contains("cut short"));
    }

    /// A local HTTP server answering with `chunks` as a chunked body, each written and flushed on
    /// its own; the chunk at `gate_after` waits for a permit on `permits`.
    async fn serve(chunks: Vec<Vec<u8>>, permits: tokio::sync::mpsc::Receiver<()>, gate_after: usize) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut permits = permits;
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/vnd.intellistream.datapoint-block\r\nTransfer-Encoding: chunked\r\n\r\n")
                .await
                .unwrap();
            for (i, chunk) in chunks.iter().enumerate() {
                if i == gate_after {
                    permits.recv().await;
                }
                socket.write_all(format!("{:x}\r\n", chunk.len()).as_bytes()).await.unwrap();
                socket.write_all(chunk).await.unwrap();
                socket.write_all(b"\r\n").await.unwrap();
                socket.flush().await.unwrap();
            }
            let _ = socket.write_all(b"0\r\n\r\n").await;
            let _ = socket.shutdown().await;
        });
        format!("http://{address}/")
    }

    async fn stream_of(url: &str) -> BinaryDatapointStream {
        let response = reqwest::get(url).await.unwrap();
        let (tx, items) = mpsc::channel(4);
        let task = tokio::spawn(produce(response, "test".to_string(), tx));
        BinaryDatapointStream { items, task }
    }

    #[tokio::test]
    async fn items_arrive_while_the_body_is_still_coming() {
        let first = float_frame(1, "a", 0..3);
        let rest = [float_frame(1, "a", 3..5), float_frame(2, "b", 0..2), END_OF_RESPONSE.to_vec()].concat();
        let (release, permits) = tokio::sync::mpsc::channel(1);
        let url = serve(vec![first, rest], permits, 1).await;
        let mut stream = stream_of(&url).await;

        // The server sends nothing more until this item is in hand.
        let item = tokio::time::timeout(std::time::Duration::from_secs(5), stream.next())
            .await
            .expect("the first frame did not arrive before the rest of the body")
            .unwrap()
            .unwrap();
        assert_eq!((item.id, item.timestamps.clone()), (1, vec![0, 1, 2]));
        release.send(()).await.unwrap();

        let rest: Vec<BinaryDatapoints> = stream.map(|i| i.unwrap()).collect().await;
        assert_eq!(rest.iter().map(|s| s.id).collect::<Vec<_>>(), [1, 2], "one item per frame");
        let merged = merge_series(std::iter::once(item).chain(rest)).unwrap();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].timestamps, [0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn a_stream_cut_short_ends_with_an_error_after_what_arrived() {
        let frames = vec![float_frame(1, "a", 0..2), float_frame(2, "b", 0..2)];
        // Every frame intact and the chunked body properly ended, but no end marker.
        let (_release, permits) = tokio::sync::mpsc::channel(1);
        let url = serve(frames, permits, usize::MAX).await;
        let items: Vec<Item> = stream_of(&url).await.collect().await;
        assert_eq!(items.len(), 3);
        assert!(items[0].is_ok() && items[1].is_ok());
        let error = items[2].as_ref().unwrap_err();
        assert_eq!(error.get_status(), StatusCode::BAD_GATEWAY);
        assert!(error.get_message().contains("cut short"), "{}", error.get_message());
    }

    #[tokio::test]
    async fn a_bad_frame_ends_the_stream_after_the_frames_before_it() {
        let batch = RecordBatch::try_new(
            DatapointValueType::Float.schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let bad = frame_around(DatapointValueType::Float, CODEC_ARROW_IPC_AGGREGATES, 2, "b", &batch);
        let body = response(&[float_frame(1, "a", 0..2), bad, float_frame(3, "c", 0..2)]);
        let (_release, permits) = tokio::sync::mpsc::channel(1);
        let url = serve(body.chunks(5).map(<[u8]>::to_vec).collect(), permits, usize::MAX).await;
        let items: Vec<Item> = stream_of(&url).await.collect().await;
        assert_eq!(items.len(), 2, "the good frame, then the error; nothing after it");
        assert_eq!(items[0].as_ref().unwrap().id, 1);
        assert!(items[1].as_ref().unwrap_err().get_message().contains("frame 1:"));
    }

    /// The same series asked for twice, the second window starting before the first ended: two
    /// entries, since merging them would put the timestamps out of order.
    #[test]
    fn a_series_whose_timestamps_restart_is_a_second_entry() {
        let body = response(&[float_frame(1, "a", 0..3), float_frame(1, "a", 1..2)]);
        let series = parse_read_response(&body).unwrap();
        assert_eq!(series.len(), 2);
        assert_eq!(series[1].timestamps, [1]);
    }

    #[test]
    fn the_end_marker_alone_is_no_points() {
        assert!(parse_read_response(END_OF_RESPONSE).unwrap().is_empty());
    }

    #[test]
    fn a_response_cut_short_is_an_error_not_fewer_points() {
        let first = float_frame(1, "a", 0..3);
        let full = response(&[first.clone(), float_frame(2, "b", 0..3)]);

        // Between two frames: every byte that arrived parses, only the marker is missing.
        let error = parse_read_response(&full[..first.len()]).unwrap_err();
        assert!(error.contains("cut short"), "{error}");
        // Inside a frame, in the header and in the payload.
        for cut in [first.len() + 10, first.len() + HEADER_BYTES + 12, full.len() - 5] {
            let error = parse_read_response(&full[..cut]).unwrap_err();
            assert!(error.contains("cut short"), "cut at {cut}: {error}");
        }
        // Nothing at all.
        assert!(parse_read_response(&[]).unwrap_err().contains("cut short"));
    }

    #[test]
    fn bytes_after_the_end_marker_are_refused() {
        let mut body = response(&[float_frame(1, "a", 0..2)]);
        body.push(0);
        assert!(parse_read_response(&body).unwrap_err().contains("after the end marker"));
    }

    #[test]
    fn a_payload_outside_its_canon_is_refused() {
        // Values in a codec-2 frame: right envelope, wrong schema.
        let batch = RecordBatch::try_new(
            DatapointValueType::Float.schema(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let body = response(&[frame_around(DatapointValueType::Float, CODEC_ARROW_IPC_AGGREGATES, 1, "a", &batch)]);
        assert!(parse_read_response(&body).unwrap_err().contains("canon"));

        let body = response(&[frame_around(DatapointValueType::Float, 9, 1, "a", &batch)]);
        assert!(parse_read_response(&body).unwrap_err().contains("codec 9"));
    }

    #[test]
    fn a_row_not_named_in_the_directory_is_refused() {
        let batch = RecordBatch::try_new(
            DatapointValueType::Float.schema(),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(TimestampMillisecondArray::from(vec![0]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let body = response(&[frame_around(DatapointValueType::Float, CODEC_ARROW_IPC, 1, "a", &batch)]);
        assert!(parse_read_response(&body).unwrap_err().contains("not in the directory"));
    }

    #[test]
    fn the_aggregate_row_cap_is_its_own() {
        let n = MAX_AGGREGATE_ROWS;
        let floats = || Arc::new(Float64Array::from(vec![1.0; n])) as Arc<dyn Array>;
        let batch = RecordBatch::try_new(
            aggregates_schema(),
            vec![
                Arc::new(Int64Array::from(vec![1; n])),
                Arc::new(TimestampMillisecondArray::from((0..n as i64).collect::<Vec<_>>()).with_timezone("UTC")),
                floats(),
                floats(),
                floats(),
                floats(),
            ],
        )
        .unwrap();
        // Text's value cap is 10 000; an aggregate frame of a mixed series may still hold 50 000.
        let body = response(&[frame_around(DatapointValueType::Mixed, CODEC_ARROW_IPC_AGGREGATES, 1, "a", &batch)]);
        let series = parse_read_response(&body).unwrap();
        assert_eq!(series[0].len(), n);
    }
}
