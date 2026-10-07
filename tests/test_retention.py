"""Bounded result retention and explicit refusal of expired resume points."""

import logging

import pytest

from eventagg import (
    Aggregator,
    AggregatorConfig,
    ResumePointExpired,
    SubscriptionClosed,
)
from tests.conftest import ingest_logged

log = logging.getLogger("eventagg.test")


def _make_agg(retention: int, log_capacity: int = 1000) -> Aggregator:
    return Aggregator(AggregatorConfig(
        window_size=60.0, out_of_orderness=10.0, allowed_lateness=30.0,
        dedup_capacity=1000, log_capacity=log_capacity,
        retention_capacity=retention,
    ))


def _emit(agg: Aggregator, n_events: int, start: int = 0) -> None:
    """Ingest events in fresh windows so each advances the watermark and
    forces TRIGGERED/FINAL emissions of older windows."""
    for i in range(start, start + n_events):
        ingest_logged(agg, f"ret-{i}", "k", 100.0 + i * 60.0, 1.0)


def test_retention_is_bounded_and_oldest_evicted():
    agg = _make_agg(retention=5)
    _emit(agg, 10)
    stats = agg.stats()
    log.info("retention stats: %s", stats)
    assert stats["retained_results"] == 5          # hard upper bound
    assert stats["evicted_results"] > 0            # oldest were dropped
    # The retained suffix is exactly the last 5 seqs, contiguous.
    sub = agg.subscribe(after_seq=stats["evicted_results"])
    seqs = [r.seq for r in sub.poll(100)]
    log.info("retained seqs: %s (evicted_upto=%d)", seqs, stats["evicted_results"])
    assert seqs == list(range(stats["evicted_results"] + 1,
                              stats["evicted_results"] + 6))


def test_resume_point_older_than_retention_is_explicitly_refused():
    agg = _make_agg(retention=4)
    _emit(agg, 8)
    evicted = agg.stats()["evicted_results"]
    assert evicted >= 2
    log.info("evicted_upto=%d; client returns with stale resume point 0", evicted)

    # A client that disconnected long ago comes back with seq 0: the gap
    # (0, evicted] is gone, so the server must refuse, not silently skip.
    with pytest.raises(ResumePointExpired) as exc:
        agg.subscribe(after_seq=0)
    log.info("explicit refusal: %s", exc.value)
    # Distinguishable from backpressure: different exception type.
    assert not isinstance(exc.value, SubscriptionClosed)
    assert agg.stats()["resume_expired"] == 1

    # Any position at/after the eviction frontier still resumes cleanly.
    sub = agg.subscribe(after_seq=evicted)
    batch = sub.poll(100)
    assert batch and batch[0].seq == evicted + 1   # no loss, no duplication


def test_idle_subscription_overtaken_by_eviction_is_refused_on_poll():
    agg = _make_agg(retention=4)
    _emit(agg, 2)
    sub = agg.subscribe(after_seq=0)               # valid at creation time
    first = sub.poll(2)
    log.info("client read seqs %s then went idle", [r.seq for r in first])

    _emit(agg, 10, start=100)                      # eviction overtakes cursor
    log.info("after more emissions: %s", agg.stats())
    with pytest.raises(ResumePointExpired) as exc:
        sub.poll(4)
    log.info("poll refused instead of silently skipping: %s", exc.value)


def test_backpressure_and_expired_resume_are_distinguishable():
    # Backpressure: unacked deliveries exceed the subscription capacity.
    agg = _make_agg(retention=100, log_capacity=2)
    _emit(agg, 3)
    slow = agg.subscribe(after_seq=0)
    delivered = slow.poll(2)                       # 2 unacked == capacity
    assert len(delivered) == 2
    with pytest.raises(SubscriptionClosed) as bp_exc:
        slow.poll(1)
    log.info("backpressure refusal: %s", bp_exc.value)

    # Expired resume: different failure, different exception, own counter.
    agg2 = _make_agg(retention=2)
    _emit(agg2, 6)
    with pytest.raises(ResumePointExpired) as exp_exc:
        agg2.subscribe(after_seq=0)
    log.info("expired-resume refusal: %s", exp_exc.value)

    stats, stats2 = agg.stats(), agg2.stats()
    log.info("counters: backpressure=%d resume_expired=%d",
             stats["backpressure"], stats2["resume_expired"])
    assert stats["backpressure"] == 1 and stats["resume_expired"] == 0
    assert stats2["resume_expired"] == 1 and stats2["backpressure"] == 0
