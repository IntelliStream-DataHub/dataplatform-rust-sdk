"""Shared polling helpers for the integration suite.

Backend reads go through eventually-consistent projections (ClickHouse for events,
Neo4j for the graph, search indexes, ...), so a just-created entity isn't instantly
visible to every read path. These helpers retry a fetch until it satisfies a
predicate or a timeout elapses, then return the last result for the caller to
assert on.

They deliberately never raise or skip on timeout — the caller decides what an
unsatisfied predicate means (assert, skip, or ignore). Poll for eventual
consistency, then assert; don't sleep-once-and-hope, and don't skip a test that is
meant to prove something works.
"""
import asyncio
import time

# Generous defaults: long enough to ride out normal projection lag, short enough
# that a genuinely broken read path fails in reasonable time. Override per call
# for faster (synchronous) or slower read paths.
DEFAULT_TIMEOUT = 30.0
DEFAULT_INTERVAL = 0.5


def poll_until(fetch, predicate, *, timeout=DEFAULT_TIMEOUT, interval=DEFAULT_INTERVAL):
    """Call ``fetch()`` until ``predicate(result)`` is truthy or ``timeout`` seconds
    elapse; return the last result either way.

    ``fetch`` is invoked at least once. Between attempts it sleeps ``interval``
    seconds.
    """
    deadline = time.monotonic() + timeout
    result = fetch()
    while not predicate(result) and time.monotonic() < deadline:
        time.sleep(interval)
        result = fetch()
    return result


async def poll_until_async(fetch, predicate, *, timeout=DEFAULT_TIMEOUT, interval=DEFAULT_INTERVAL):
    """Async twin of :func:`poll_until` for a coroutine-returning ``fetch``."""
    deadline = time.monotonic() + timeout
    result = await fetch()
    while not predicate(result) and time.monotonic() < deadline:
        await asyncio.sleep(interval)
        result = await fetch()
    return result
