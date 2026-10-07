"""Bounded result retention, explicit resume-too-old refusal, reclamation."""

import logging

import pytest

from eventagg import (
    Aggregator,
    AggregatorConfig,
    ResumeTooOld,
    SubscriptionClosed,
)

from tests.conftest import drain_logged, ingest_logged

log = logging.getLogger("eventagg.test")


@pytest.fixture
def agg_retained() -> Aggregator:
    # Tiny retention so eviction happens within a few emissions.
    return Aggregator(AggregatorConfig(
        window_size=60.0, out_of_orderness=10.0, allowed_lateness=30.0,
        dedup_capacity=1000, log_capacity=8, result_retention=6,
    ))


def _gen(agg, n: int, start: int = 0) -> None:
    """Each event lands 200s after the previous one: it triggers and
    finalizes the previous window, emitting 2 results per event."""
    for i in range(start, start + n):
        ingest_logged(agg, f"ret-{i}", "k", 100.0 + i * 200.0, 1.0)


def test_result_log_retention_is_bounded(agg_retained):
    _gen(agg_retained, 10)
    stats = agg_retained.stats()
    log.info("stats after 10 events: %s", stats)
    # Memory bound: never more than `result_retention` results retained.
    assert stats["retained_results"] <= 6
    # 10 events -> 18 emissions (9 windows x triggered+final); most evicted.
    assert stats["results_evicted"] == 18 - stats["retained_results"]
    assert stats["results_evicted"] > 0


def test_resume_point_too_old_is_explicit_refusal(agg_retained):
    _gen(agg_retained, 10)
    stats = agg_retained.stats()
    evicted = stats["results_evicted"]
    log.info("evicted=%d; subscribing from an evicted position", evicted)
    # A long-disconnected client comes back with a stale resume point:
    # explicit, distinguishable refusal instead of a silent gap.
    with pytest.raises(ResumeTooOld) as exc:
        agg_retained.subscribe(after_seq=evicted - 1)
    log.info("resume_too_old refusal: %s", exc.value)
    assert exc.value.reason == "resume_too_old"
    # Distinct from the backpressure refusal type.
    assert not isinstance(exc.value, SubscriptionClosed)
    assert agg_retained.stats()["resume_too_old"] == 1


def test_resume_within_retention_has_no_gap_no_duplication(agg_retained):
    _gen(agg_retained, 10)
    evicted = agg_retained.stats()["results_evicted"]
    # Oldest still-valid resume point: exactly the evicted count.
    sub = agg_retained.subscribe(after_seq=evicted)
    replayed = drain_logged(agg_retained, sub, note="(resumed at boundary)")
    assert [r.seq for r in replayed] == list(range(evicted + 1, 19))
    log.info("resumed from seq=%d without gap: %d results", evicted, len(replayed))


def test_connected_subscriber_sees_too_old_when_retention_passes_it(agg_retained):
    sub = agg_retained.subscribe(after_seq=0)
    _gen(agg_retained, 10)  # evicts results this subscriber never read
    with pytest.raises(ResumeTooOld):
        sub.poll(10)
    log.info("slow connected subscriber got resume_too_old; stats=%s",
             agg_retained.stats())


def test_window_state_reclaimed_after_final(agg_retained):
    _gen(agg_retained, 5)
    stats = agg_retained.stats()
    log.info("after finals: open_windows=%d windows_reclaimed=%d",
             stats["open_windows"], stats["windows_reclaimed"])
    # Only the newest, not-yet-triggered window is still accumulating.
    assert stats["open_windows"] == 1
    assert stats["windows_reclaimed"] == 4
    # Semantics preserved: an event for a reclaimed window is still
    # judged too_late (not silently re-opened, not double counted).
    r = ingest_logged(agg_retained, "late-1", "k", 100.0, 99.0)
    assert r.reason.value == "too_late"
    assert agg_retained.stats()["too_late"] == 1
    assert agg_retained.stats()["open_windows"] == 1  # no window re-created


def test_stats_snapshot_reports_live_sizes_and_counters():
    # Larger retention here so backpressure (not eviction) is what refuses.
    agg = Aggregator(AggregatorConfig(
        window_size=60.0, out_of_orderness=10.0, allowed_lateness=30.0,
        dedup_capacity=1000, log_capacity=8, result_retention=100,
    ))
    _gen(agg, 6)  # 10 emissions, exceeds log_capacity=8
    ingest_logged(agg, "ret-5", "k", 1100.0, 1.0)   # duplicate id, open window
    ingest_logged(agg, "bad", "", 1100.0, 1.0)       # invalid key
    sub = agg.subscribe(after_seq=0)
    sub.poll(8)
    with pytest.raises(SubscriptionClosed):
        sub.poll(1)  # backpressure refusal is counted too
    stats = agg.stats()
    log.info("snapshot: %s", stats)
    assert stats["accepted"] == 6
    assert stats["duplicate"] == 1
    assert stats["invalid"] == 1
    assert stats["backpressure"] == 1
    assert stats["open_windows"] >= 1
    assert stats["retained_results"] == 10
