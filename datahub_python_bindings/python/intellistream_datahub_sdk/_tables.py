"""Polars tables from a binary read, shared by the buffered result and the streamed reader so the
two cannot drift: the long table, and its wide form with one column per series."""


def _polars(method):
    try:
        import polars
    except ImportError:
        raise ImportError(f"{method}() needs polars; install it with `pip install polars`") from None
    return polars


def to_polars(read, wide):
    """`read` is a `BinaryReadResult` or a `BinaryDatapointReader`: an Arrow stream of the long
    table, and an iterable of `BinaryDatapoints` (whole series, or a reader's per-frame chunks)."""
    pl = _polars("to_polars")
    if not wide:
        return pl.DataFrame(read)
    return _wide(pl, read)


def _wide(pl, parts):
    # Chunks of one series arrive consecutively; a dict keyed by id keeps the read's order.
    series = {}
    for part in parts:
        external_id, frames = series.setdefault(part.id, (part.external_id, []))
        frames.append(part.to_polars())
    if not series:
        raise ValueError("the read returned no rows, so there are no columns for a wide table")

    columns = []
    for external_id, frames in series.values():
        frame = pl.concat(frames, rechunk=False) if len(frames) > 1 else frames[0]
        if not frame["timestamp"].is_sorted() or frame["timestamp"].n_unique() != frame.height:
            raise ValueError(
                f"series {external_id} has repeated timestamps, as when one series is asked for "
                f"twice with overlapping windows; a wide table needs one value per timestamp"
            )
        values = [c for c in frame.columns if c != "timestamp"]
        names = {values[0]: external_id} if len(values) == 1 else {c: f"{external_id}.{c}" for c in values}
        columns.append(frame.rename(names))

    first = columns[0]["timestamp"]
    if all(c.height == first.len() and c["timestamp"].equals(first) for c in columns[1:]):
        # Every series on the same timestamps: the columns side by side, nothing to match up.
        return columns[0].hstack([s for c in columns[1:] for s in c.drop("timestamp").get_columns()])

    single_values = [c.columns[1:] for c in columns]
    if all(len(v) == 1 for v in single_values) and len({c.dtypes[1] for c in columns}) == 1:
        # One value column of one type: a pivot on a numeric key is the fastest way to match rows.
        names = [v[0] for v in single_values]
        long = pl.concat(
            [c.rename({n: "value"}).with_columns(pl.lit(i, pl.Int32).alias("series")) for i, (c, n) in enumerate(zip(columns, names))],
            rechunk=False,
        )
        wide = long.pivot(on="series", index="timestamp", values="value").sort("timestamp")
        return wide.rename({str(i): n for i, n in enumerate(names)}).select(["timestamp", *names])

    # Different value types or several value columns: matched on timestamp, each keeping its type.
    return pl.concat(columns, how="align")
