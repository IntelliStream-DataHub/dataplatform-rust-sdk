//! Durable, file-backed buffering for ingestion that can't reach the API.
//!
//! When the backend is unreachable, datapoint/event ingestion spools to disk and is flushed
//! automatically on a later ingest call. The spool is a **segmented, zstd-compressed,
//! newline-delimited-JSON log** so it stays memory-safe even near a multi-gigabyte cap:
//!
//! - The active segment is plain, append-only NDJSON (`.ndjson`); an unclean shutdown leaves at
//!   most a torn last line, which is skipped on read.
//! - At rollover (`rollover_bytes`, ~50 MiB) the active segment is zstd-sealed to `.ndjson.zst`
//!   (temp file + atomic rename), and a fresh active segment is started.
//! - Drain seals the active segment, then reads each sealed segment (oldest first) one at a time
//!   (memory bounded by a single segment), and deletes it once fully sent.
//! - Time retention (`retention_ms`) drops whole segments past the window and expired records on
//!   read; the size cap (`max_bytes`) bounds total on-disk bytes by deleting the oldest segment.
//!
//! Each on-disk line is `<epoch_millis>\t<json>`: the timestamp (for retention) followed by the
//! serialized item to resend. The spool is content-agnostic; callers serialize their own items.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

const ACTIVE_SUFFIX: &str = ".ndjson";
const SEALED_SUFFIX: &str = ".ndjson.zst";
const TEMP_SUFFIX: &str = ".ndjson.zst.tmp";
const DEFAULT_ROLLOVER_BYTES: u64 = 50 * 1024 * 1024; // 50 MiB of plain NDJSON
const MIN_ROLLOVER_BYTES: u64 = 64 * 1024;
const ZSTD_LEVEL: i32 = 9;

/// One on-disk segment file plus the stats kept in memory for retention.
struct Segment {
    path: PathBuf,
    compressed: bool,
    seq: u64,
    max_ts: i64,
    records: u64,
    bytes: u64,
}

/// A durable spool of `(timestamp_millis, json_line)` records. Not thread-safe on its own; wrap it
/// in a `Mutex`. File I/O is synchronous, so never hold that lock across an `.await`.
pub struct DurableSpool {
    dir: PathBuf,
    retention_ms: Option<i64>,
    max_bytes: Option<u64>,
    rollover_bytes: u64,
    segments: Vec<Segment>, // ordered by seq (oldest first)
    next_seq: u64,
}

impl DurableSpool {
    /// Open (and recover) a spool in `dir`. At least one of `retention_ms` / `max_bytes` should be
    /// set, otherwise the spool is unbounded.
    pub fn open(dir: PathBuf, retention_ms: Option<i64>, max_bytes: Option<u64>) -> io::Result<Self> {
        let rollover_bytes = match max_bytes {
            Some(cap) => (cap / 4).clamp(MIN_ROLLOVER_BYTES, DEFAULT_ROLLOVER_BYTES),
            None => DEFAULT_ROLLOVER_BYTES,
        };
        let mut spool = DurableSpool {
            dir,
            retention_ms,
            max_bytes,
            rollover_bytes,
            segments: Vec::new(),
            next_seq: 0,
        };
        spool.recover()?;
        Ok(spool)
    }

    /// Total records currently spooled across all segments.
    pub fn size(&self) -> u64 {
        self.segments.iter().map(|s| s.records).sum()
    }

    /// Append `(ts_millis, json)` records to the active segment, sealing at rollover and enforcing
    /// the retention bounds.
    pub fn append(&mut self, records: &[(i64, String)], now_ms: i64) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.prune(now_ms)?;
        fs::create_dir_all(&self.dir)?;
        let active_idx = self.active_for_write();
        {
            let path = self.segments[active_idx].path.clone();
            let file = OpenOptions::new().create(true).append(true).open(&path)?;
            let mut writer = BufWriter::new(file);
            for (ts, json) in records {
                writeln!(writer, "{}\t{}", ts, json)?;
                let seg = &mut self.segments[active_idx];
                seg.max_ts = seg.max_ts.max(*ts);
                seg.records += 1;
            }
            writer.flush()?;
        }
        let seg = &mut self.segments[active_idx];
        seg.bytes = fs::metadata(&seg.path)?.len();
        if seg.bytes >= self.rollover_bytes {
            self.seal(active_idx)?;
        }
        self.prune(now_ms)?;
        Ok(())
    }

    /// Seal the active segment (if any) so every existing record lives in an immutable, compressed
    /// segment. Call this before draining: it makes deletes safe (no concurrent appends to a sealed
    /// file) and bounds drain memory to one segment.
    pub fn roll(&mut self, now_ms: i64) -> io::Result<()> {
        self.prune(now_ms)?;
        if let Some(idx) = self.active_index() {
            if self.segments[idx].records > 0 {
                self.seal(idx)?;
            } else {
                let path = self.segments[idx].path.clone();
                let _ = fs::remove_file(path);
                self.segments.remove(idx);
            }
        }
        Ok(())
    }

    /// The sequence number of the oldest sealed segment, if any. Plain (active) segments are skipped
    /// so an in-progress append is never drained out from under the writer.
    pub fn oldest_sealed_seq(&self) -> Option<u64> {
        self.segments
            .iter()
            .filter(|s| s.compressed)
            .map(|s| s.seq)
            .min()
    }

    /// Read a segment's still-valid records (json lines), dropping any past the time window.
    pub fn read_segment(&self, seq: u64, now_ms: i64) -> io::Result<Vec<String>> {
        let Some(seg) = self.segments.iter().find(|s| s.seq == seq) else {
            return Ok(Vec::new());
        };
        let cutoff = self.retention_ms.map(|r| now_ms - r);
        let mut out = Vec::new();
        for_each_line(&seg.path, seg.compressed, |line| {
            if let Some((ts_str, json)) = line.split_once('\t') {
                if let Ok(ts) = ts_str.parse::<i64>() {
                    if cutoff.map_or(true, |c| ts >= c) {
                        out.push(json.to_string());
                    }
                }
            }
        })?;
        Ok(out)
    }

    /// Delete a segment (called after its records were accepted by the server).
    pub fn delete_segment(&mut self, seq: u64) -> io::Result<()> {
        if let Some(pos) = self.segments.iter().position(|s| s.seq == seq) {
            let _ = fs::remove_file(&self.segments[pos].path);
            self.segments.remove(pos);
        }
        Ok(())
    }

    /// Remove every segment.
    pub fn clear(&mut self) -> io::Result<()> {
        for seg in self.segments.drain(..) {
            let _ = fs::remove_file(&seg.path);
        }
        Ok(())
    }

    // --- internals -----------------------------------------------------------------------------

    fn prune(&mut self, now_ms: i64) -> io::Result<()> {
        if let Some(retention) = self.retention_ms {
            let cutoff = now_ms - retention;
            while let Some(first) = self.segments.first() {
                if first.max_ts < cutoff {
                    let path = first.path.clone();
                    let _ = fs::remove_file(path);
                    self.segments.remove(0);
                } else {
                    break;
                }
            }
        }
        if let Some(cap) = self.max_bytes {
            while self.total_bytes() > cap && !self.segments.is_empty() {
                let path = self.segments[0].path.clone();
                let _ = fs::remove_file(path);
                self.segments.remove(0);
            }
        }
        Ok(())
    }

    fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.bytes).sum()
    }

    fn active_index(&self) -> Option<usize> {
        self.segments.iter().rposition(|s| !s.compressed)
    }

    fn active_for_write(&mut self) -> usize {
        if let Some(idx) = self.active_index() {
            if idx == self.segments.len() - 1 {
                return idx;
            }
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.segments.push(Segment {
            path: self.dir.join(segment_name(seq, ACTIVE_SUFFIX)),
            compressed: false,
            seq,
            max_ts: i64::MIN,
            records: 0,
            bytes: 0,
        });
        self.segments.len() - 1
    }

    fn seal(&mut self, idx: usize) -> io::Result<()> {
        let seq = self.segments[idx].seq;
        let plain = self.segments[idx].path.clone();
        let sealed = self.dir.join(segment_name(seq, SEALED_SUFFIX));
        let tmp = self.dir.join(segment_name(seq, TEMP_SUFFIX));
        {
            let reader = File::open(&plain)?;
            let mut input = BufReader::new(reader);
            let out = File::create(&tmp)?;
            let mut encoder = zstd::stream::write::Encoder::new(out, ZSTD_LEVEL)?;
            io::copy(&mut input, &mut encoder)?;
            encoder.finish()?;
        }
        fs::rename(&tmp, &sealed)?;
        let _ = fs::remove_file(&plain);
        let seg = &mut self.segments[idx];
        seg.path = sealed.clone();
        seg.compressed = true;
        seg.bytes = fs::metadata(&sealed)?.len();
        Ok(())
    }

    fn recover(&mut self) -> io::Result<()> {
        if !self.dir.is_dir() {
            return Ok(());
        }
        // Collect sealed and plain segment paths by sequence; a sealed file wins a crash mid-seal.
        let mut sealed: std::collections::BTreeMap<u64, PathBuf> = Default::default();
        let mut plain: std::collections::BTreeMap<u64, PathBuf> = Default::default();
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };
            if name.ends_with(TEMP_SUFFIX) {
                let _ = fs::remove_file(&path); // leftover from a crash mid-seal
            } else if let Some(seq) = parse_seq(&name, SEALED_SUFFIX) {
                sealed.insert(seq, path);
            } else if let Some(seq) = parse_seq(&name, ACTIVE_SUFFIX) {
                plain.insert(seq, path);
            }
        }
        let seqs: std::collections::BTreeSet<u64> = sealed.keys().chain(plain.keys()).copied().collect();
        for seq in seqs {
            let (path, compressed) = if let Some(p) = sealed.get(&seq) {
                if let Some(stale) = plain.get(&seq) {
                    let _ = fs::remove_file(stale);
                }
                (p.clone(), true)
            } else {
                (plain.get(&seq).unwrap().clone(), false)
            };
            let mut seg = Segment {
                path,
                compressed,
                seq,
                max_ts: i64::MIN,
                records: 0,
                bytes: 0,
            };
            self.scan(&mut seg)?;
            self.next_seq = self.next_seq.max(seq + 1);
            self.segments.push(seg);
        }
        Ok(())
    }

    fn scan(&self, seg: &mut Segment) -> io::Result<()> {
        let mut max_ts = i64::MIN;
        let mut records = 0u64;
        for_each_line(&seg.path, seg.compressed, |line| {
            if let Some((ts_str, _)) = line.split_once('\t') {
                if let Ok(ts) = ts_str.parse::<i64>() {
                    max_ts = max_ts.max(ts);
                    records += 1;
                }
            }
        })?;
        seg.max_ts = max_ts;
        seg.records = records;
        seg.bytes = fs::metadata(&seg.path).map(|m| m.len()).unwrap_or(0);
        Ok(())
    }
}

/// Stream a segment's lines, decompressing if sealed, tolerating a torn trailing line.
fn for_each_line(path: &Path, compressed: bool, mut f: impl FnMut(&str)) -> io::Result<()> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    let reader: Box<dyn BufRead> = if compressed {
        Box::new(BufReader::new(zstd::stream::read::Decoder::new(file)?))
    } else {
        Box::new(BufReader::new(file))
    };
    for line in reader.lines() {
        match line {
            Ok(l) => {
                if !l.is_empty() {
                    f(&l);
                }
            }
            Err(_) => break, // truncated trailing line after an unclean shutdown
        }
    }
    Ok(())
}

fn segment_name(seq: u64, suffix: &str) -> String {
    format!("{:019}{}", seq, suffix)
}

fn parse_seq(name: &str, suffix: &str) -> Option<u64> {
    name.strip_suffix(suffix).and_then(|s| s.parse::<u64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_dir() -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("datahub_spool_test_{}_{}", nanos, n));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn drain_all(spool: &mut DurableSpool, now: i64) -> Vec<String> {
        spool.roll(now).unwrap();
        let mut sent = Vec::new();
        while let Some(seq) = spool.oldest_sealed_seq() {
            let lines = spool.read_segment(seq, now).unwrap();
            sent.extend(lines);
            spool.delete_segment(seq).unwrap();
        }
        sent
    }

    #[test]
    fn append_then_drain_round_trips_all_records() {
        let dir = temp_dir();
        let mut spool = DurableSpool::open(dir.clone(), Some(3_600_000), None).unwrap();
        let now = 1_000_000_000_000;
        spool
            .append(&[(now, "a".into()), (now, "b".into())], now)
            .unwrap();
        spool.append(&[(now, "c".into())], now).unwrap();
        assert_eq!(spool.size(), 3);

        let mut sent = drain_all(&mut spool, now);
        sent.sort();
        assert_eq!(sent, vec!["a", "b", "c"]);
        assert_eq!(spool.size(), 0);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn retention_drops_records_older_than_window() {
        let dir = temp_dir();
        let mut spool = DurableSpool::open(dir.clone(), Some(60_000), None).unwrap();
        let now = 1_000_000_000_000;
        spool.append(&[(now - 120_000, "old".into())], now).unwrap();
        spool.append(&[(now, "fresh".into())], now).unwrap();
        let sent = drain_all(&mut spool, now);
        assert_eq!(sent, vec!["fresh"]);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn seals_and_reads_back_compressed_segments() {
        let dir = temp_dir();
        // 4 MiB cap -> ~1 MiB rollover; lots of records cross it and seal.
        let mut spool = DurableSpool::open(dir.clone(), None, Some(4 * 1024 * 1024)).unwrap();
        let now = 1_000_000_000_000;
        let mut total = 0u64;
        for batch in 0..80 {
            let recs: Vec<(i64, String)> = (0..1000)
                .map(|i| (now, format!("{{\"n\":{}}}", batch * 1000 + i)))
                .collect();
            spool.append(&recs, now).unwrap();
            total += 1000;
        }
        assert_eq!(spool.size(), total);
        let sealed = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(SEALED_SUFFIX))
            .count();
        assert!(sealed >= 1, "expected at least one zstd-sealed segment");

        let sent = drain_all(&mut spool, now);
        assert_eq!(sent.len() as u64, total);
        assert_eq!(spool.size(), 0);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn recovers_segments_across_reopen() {
        let dir = temp_dir();
        let now = 1_000_000_000_000;
        {
            let mut spool = DurableSpool::open(dir.clone(), Some(3_600_000), None).unwrap();
            spool
                .append(&[(now, "x".into()), (now, "y".into())], now)
                .unwrap();
            assert_eq!(spool.size(), 2);
        }
        let reopened = DurableSpool::open(dir.clone(), Some(3_600_000), None).unwrap();
        assert_eq!(reopened.size(), 2);
        let _ = fs::remove_dir_all(dir);
    }
}
