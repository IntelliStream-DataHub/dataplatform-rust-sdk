# JSON against binary datapoint ingest, through the Python SDK.
#
# Not a pytest: it needs a live platform and takes minutes, so it is a script you run when you
# want the numbers. The Java equivalent lives in the platform repo's datahub-e2e module and
# reports the same columns, so the two are comparable.
#
#   python python_tests/bench_json_vs_binary.py --points 10000000
#
# Reads the same .env the SDK does. The api must have its daily quota and rate limiter off, or
# it will refuse a run of this size:
#   -Ddatahub.limits.quota.enabled=false -Ddatahub.limits.rate.enabled=false
"""Measure the two ingest paths from Python and print a comparison."""

from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import resource
import time
import urllib.request

from intellistream_datahub_sdk import DataHubClient, TimeSeries

START = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)


def generate(count: int, offset: int, series_index: int) -> tuple[list[dt.datetime], list[float]]:
    """A slow sine plus noise, one signal per series.

    Identical series would let zstd compress the repetition across them and report a wire size
    no real fleet of sensors would produce.
    """
    base = 150.0 + series_index * 0.7
    phase = series_index * 0.37
    timestamps = []
    values = []
    for i in range(count):
        t = offset + i
        timestamps.append(START + dt.timedelta(seconds=t))
        # Deterministic, so two runs generate the same bytes.
        z = (t + series_index * 6364136223846793005) * 6364136223846793005 & 0xFFFFFFFFFFFFFFFF
        z ^= z >> 33
        noise = ((z >> 40) / float(1 << 24)) - 0.5
        values.append(base + 20.0 * math.sin(t / 600.0 + phase) + noise)
    return timestamps, values


def clickhouse_count(ids: list[int]) -> int:
    url = (
        os.environ.get("CLICKHOUSE_URL", "http://localhost:18123")
        + "/?user=" + os.environ.get("CLICKHOUSE_USER", "foobar")
        + "&password=" + os.environ.get("CLICKHOUSE_PASSWORD", "changeme")
    )
    db = os.environ.get("CLICKHOUSE_DB", "foo")
    sql = (
        f"SELECT count() FROM {db}.datapoints_float WHERE timeseries_id IN "
        f"({','.join(str(i) for i in ids)})"
    )
    with urllib.request.urlopen(url, sql.encode()) as response:
        return int(response.read().decode().strip())


def peak_rss_mb() -> float:
    # ru_maxrss is kilobytes on Linux.
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def run(client, label: str, binary: bool, points: int, series: int, chunk: int) -> dict:
    run_id = f"pybench_{int(time.time())}_{'bin' if binary else 'json'}"
    external_ids = [f"{run_id}_{i}" for i in range(series)]
    internal_ids = []
    for external_id in external_ids:
        created = client.timeseries.create(
            [TimeSeries(name=external_id, external_id=external_id, value_type="float", unit="celsius")]
        )
        internal_ids.append(created[0].id)

    per_series = chunk // series
    latencies = []
    sent = 0
    offset = 0
    started = time.perf_counter()
    while sent < points:
        this_chunk = min(chunk, points - sent)
        per_series = max(1, this_chunk // series)
        for index, external_id in enumerate(external_ids):
            timestamps, values = generate(per_series, offset, index)
            call = time.perf_counter()
            if binary:
                client.timeseries.insert_from_lists_binary(timestamps, values, external_id)
            else:
                client.timeseries.insert_from_lists(timestamps, values, external_id)
            latencies.append(time.perf_counter() - call)
        sent += per_series * series
        offset += per_series
        elapsed = time.perf_counter() - started
        print(f"  {label}: {sent:,} / {points:,} points, {sent / elapsed:,.0f} pts/s", flush=True)
    ingest_seconds = time.perf_counter() - started

    settle_start = time.perf_counter()
    deadline = settle_start + 1800
    while time.perf_counter() < deadline:
        if clickhouse_count(internal_ids) >= sent:
            break
        time.sleep(1)
    else:
        raise SystemExit(f"{label}: only {clickhouse_count(internal_ids)} of {sent} rows became readable")
    settle_seconds = time.perf_counter() - settle_start

    client.timeseries.delete(external_ids)
    latencies.sort()
    return {
        "path": label,
        "points": sent,
        "ingest_seconds": ingest_seconds,
        "settle_seconds": settle_seconds,
        "points_per_second": sent / ingest_seconds,
        # Wall clock includes generating the points in interpreted Python, which dominates it.
        # This is the same points divided by the time actually spent inside the SDK calls, so
        # it says what the transport did rather than what the loop above did.
        "points_per_second_in_call": sent / sum(latencies),
        "requests": len(latencies),
        "latency_mean_ms": sum(latencies) / len(latencies) * 1000,
        "latency_p50_ms": latencies[len(latencies) // 2] * 1000,
        "latency_p99_ms": latencies[min(len(latencies) - 1, int(len(latencies) * 0.99))] * 1000,
        "client_peak_rss_mb": peak_rss_mb(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--points", type=int, default=10_000_000)
    parser.add_argument("--series", type=int, default=100)
    parser.add_argument("--chunk", type=int, default=1_000_000)
    args = parser.parse_args()

    env_file = os.environ.get("DATAHUB_ENV_FILE", ".env")
    client = DataHubClient.from_envfile(env_file)
    print(f"\n=== {args.points:,} points across {args.series:,} series, float ===", flush=True)
    results = [
        run(client, "JSON", False, args.points, args.series, args.chunk),
        run(client, "binary", True, args.points, args.series, args.chunk),
    ]

    rows = [
        ("ingest wall time (s)", "{:.1f}", "ingest_seconds"),
        ("points per second", "{:,.0f}", "points_per_second"),
        ("points per second in-call", "{:,.0f}", "points_per_second_in_call"),
        ("settle to readable (s)", "{:.1f}", "settle_seconds"),
        ("requests", "{:,.0f}", "requests"),
        ("latency mean (ms)", "{:,.0f}", "latency_mean_ms"),
        ("latency p50 (ms)", "{:,.0f}", "latency_p50_ms"),
        ("latency p99 (ms)", "{:,.0f}", "latency_p99_ms"),
        ("client peak RSS (MB)", "{:,.0f}", "client_peak_rss_mb"),
    ]
    print(f"\n=== datapoint ingest from Python: JSON against binary ===")
    print(f"{args.points:,} points across {args.series:,} series, float\n")
    print("{:<26}{:>18}{:>18}".format("metric", *[r["path"] for r in results]))
    for label, fmt, key in rows:
        print("{:<26}{:>18}{:>18}".format(label, *[fmt.format(r[key]) for r in results]))


if __name__ == "__main__":
    main()
