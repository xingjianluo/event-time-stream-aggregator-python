"""Window state reclamation after FINAL and the runtime stats snapshot."""

import logging

from eventagg import Aggregator, AggregatorConfig, Event, IngestStatus, RejectReason
from tests.conftest import ingest_logged

log = logging.getLogger("eventagg.test")


def _make_agg() -> Aggregator:
    return Aggregator(AggregatorConfig(
        window_size=60.0, out_of_orderness=10.0, allowed_lateness=30.0,
        dedup_capacity=1000, log_capacity=1000, retention_capacity=1000,
    ))


def test_finalized_window_state_is_reclaimed():
    agg = _make_agg()
    # One event opens window [60, 120); a far-future event pushes the
    # watermark past end + allowed_lateness, finalizing it.
    ingest_logged(agg, "w1", "k", 100.0, 5.0)
    assert agg.stats()["open_windows"] >= 1
    r = ingest_logged(agg, "w2", "k", 100.0 + 60 + 40 + 1, 1.0)  # wm >= 151
    assert r.ok
    stats = agg.stats()
    log.info("after finalization: %s", stats)
    assert stats["open_windows"] <= 1   # the FINAL window's state is gone

    # Decisions are unchanged by reclamation: an event for the reclaimed
    # window is still ignored as too_late with the same rationale.
    late = ingest_logged(agg, "w3", "k", 100.0, 9.0)
    assert late.status is IngestStatus.IGNORED
    assert late.reason is RejectReason.TOO_LATE
    log.info("reclaimed-window event still decided: %s/%s",
             late.status.value, late.reason.value)


def test_open_windows_stay_bounded_over_long_uptime():
    agg = _make_agg()
    worst = 0
    for i in range(500):
        ingest_logged(agg, f"lt-{i}", "k", 100.0 + i * 60.0, 1.0)
        worst = max(worst, agg.stats()["open_windows"])
    # A window lives only until watermark >= end + allowed_lateness, i.e.
    # ~2 window lengths here; it must not grow with the number of events.
    log.info("max live windows over 500 events: %d", worst)
    assert worst <= 2
    assert agg.stats()["accepted"] == 500


def test_stats_snapshot_reports_decision_counters():
    agg = _make_agg()
    ingest_logged(agg, "s1", "k", 100.0, 1.0)                    # accepted
    ingest_logged(agg, "s1", "k", 100.0, 1.0)                    # duplicate
    ingest_logged(agg, "s2", "k", 1000.0, 1.0)                   # accepted, advances wm
    ingest_logged(agg, "s3", "k", 100.0, 1.0)                    # too_late
    bad = agg.ingest(Event(event_id="", key="k", event_time=1.0, value=1.0))
    log.info("invalid event -> %s/%s", bad.status.value, bad.reason.value)

    stats = agg.stats()
    log.info("stats snapshot: %s", stats)
    assert stats["accepted"] == 2
    assert stats["ignored_duplicate"] == 1
    assert stats["ignored_too_late"] == 1
    assert stats["rejected_invalid"] == 1
    assert stats["backpressure"] == 0
    assert stats["resume_expired"] == 0
    # Snapshot carries the memory-relevant gauges as well.
    assert stats["open_windows"] >= 1
    assert stats["retained_results"] >= 1
    assert stats["evicted_results"] == 0
