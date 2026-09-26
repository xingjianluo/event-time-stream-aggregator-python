"""Shared fixtures: an aggregator with small, deterministic time constants
and a logger that prints input events, watermark and decision rationale."""

import logging

import pytest

from eventagg import Aggregator, AggregatorConfig

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s | %(message)s",
)
log = logging.getLogger("eventagg.test")


@pytest.fixture
def agg() -> Aggregator:
    # window=60s, watermark lags 10s behind max event time,
    # triggered windows accept corrections for another 30s.
    return Aggregator(AggregatorConfig(
        window_size=60.0, out_of_orderness=10.0,
        allowed_lateness=30.0, dedup_capacity=1000, log_capacity=8,
    ))


def ingest_logged(agg: Aggregator, event_id: str, key: str,
                  event_time: float, value: float):
    """Ingest one event and log the input, decision and watermark."""
    from eventagg import Event
    ev = Event(event_id=event_id, key=key, event_time=event_time, value=value)
    result = agg.ingest(ev)
    log.info(
        "ingest event_id=%s key=%s event_time=%.1f value=%.1f -> "
        "status=%s reason=%s detail=%r | watermark=%.1f",
        event_id, key, event_time, value,
        result.status.value, result.reason.value, result.detail,
        agg.watermark,
    )
    return result


def drain_logged(agg: Aggregator, sub, note: str = ""):
    """Poll a subscription and log every emitted result with its rationale."""
    results = sub.poll(100)
    for r in results:
        log.info(
            "emit seq=%d key=%s window=[%.1f, %.1f) count=%d total=%.1f "
            "state=%s watermark=%.1f reason=%r %s",
            r.seq, r.key, r.window_start, r.window_end, r.count, r.total,
            r.state.value, r.watermark, r.reason, note,
        )
    if results:
        sub.ack(results[-1].seq)
    return results
