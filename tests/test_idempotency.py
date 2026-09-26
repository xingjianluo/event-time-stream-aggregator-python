"""Duplicate delivery, invalid payloads and concurrent ingestion."""

import threading

from eventagg import IngestStatus, RejectReason
from eventagg.models import WindowState

from tests.conftest import drain_logged, ingest_logged


def test_duplicate_delivery_applied_once(agg):
    sub = agg.subscribe()
    assert ingest_logged(agg, "e1", "k", 65.0, 1.0).ok
    # Retry of the same event id: ignored with a distinguishable reason.
    r = ingest_logged(agg, "e1", "k", 65.0, 1.0)
    assert r.status is IngestStatus.IGNORED
    assert r.reason is RejectReason.DUPLICATE
    # Trigger the window: the retried event must be counted exactly once.
    ingest_logged(agg, "e2", "k", 130.0, 5.0)
    trig = [x for x in drain_logged(agg, sub) if x.state is WindowState.TRIGGERED]
    assert (trig[0].count, trig[0].total) == (1, 1.0)


def test_invalid_events_rejected_with_reason(agg):
    r = ingest_logged(agg, "", "k", 65.0, 1.0)
    assert (r.status, r.reason) is not None
    assert r.status is IngestStatus.REJECTED and r.reason is RejectReason.INVALID
    r = ingest_logged(agg, "e1", "", 65.0, 1.0)
    assert r.reason is RejectReason.INVALID
    r = ingest_logged(agg, "e2", "k", float("nan"), 1.0)
    assert r.reason is RejectReason.INVALID
    r = ingest_logged(agg, "e3", "k", 65.0, float("inf"))
    assert r.reason is RejectReason.INVALID
    # Invalid events neither advance the watermark nor open windows.
    assert agg.watermark == float("-inf")
    assert agg.subscribe(0).poll(100) == []


def test_concurrent_duplicate_submissions_applied_once(agg):
    """Many threads submit the same event id concurrently: exactly one wins."""
    results = []

    def run():
        from eventagg import Event
        results.append(agg.ingest(Event("dup", "k", 65.0, 1.0)))

    threads = [threading.Thread(target=run) for _ in range(64)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [r for r in results if r.status is IngestStatus.ACCEPTED]
    duplicates = [r for r in results if r.reason is RejectReason.DUPLICATE]
    assert len(accepted) == 1
    assert len(duplicates) == 63


def test_concurrent_distinct_events_all_counted_once(agg):
    """Concurrent writes of distinct ids: final window total equals sum."""
    n_threads, n_events = 8, 50

    def run(tid):
        from eventagg import Event
        for i in range(n_events):
            r = agg.ingest(Event(f"t{tid}-e{i}", "k", 65.0, 1.0))
            assert r.status is IngestStatus.ACCEPTED

    threads = [threading.Thread(target=run, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ingest_logged(agg, "closer", "k", 500.0, 0.0)  # finalize window [60,120)
    finals = [r for r in agg.subscribe(0).poll(1000)
              if r.state is WindowState.FINAL and r.window_start == 60.0]
    assert len(finals) == 1
    assert finals[0].count == n_threads * n_events
    assert finals[0].total == float(n_threads * n_events)
