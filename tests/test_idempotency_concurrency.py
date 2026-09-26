"""Duplicate delivery, invalid events, and concurrent writes."""

from __future__ import annotations

import logging
import threading

from aggregator.engine import Engine, EngineConfig
from aggregator.types import Decision, Event, Trigger

from .conftest import submit_and_log

logger = logging.getLogger("aggregator.tests")


def test_duplicate_delivery_ignored_exactly_once(engine: Engine) -> None:
    event = Event("e1", "a", 10_000, 3.0)
    s1 = submit_and_log(engine, event)
    assert s1.decision is Decision.ACCEPTED
    # Retries of the same event_id are ignored with a distinct reason.
    for _ in range(3):
        s = submit_and_log(engine, event)
        assert s.decision is Decision.DUPLICATE
        assert s.emitted == []
    # The aggregate reflects the event exactly once.
    assert engine.poll(0, 10)[-1].count == 1


def test_invalid_events_rejected_with_reasons(engine: Engine) -> None:
    cases = [
        (Event("", "a", 1_000, 1.0), "event_id"),
        (Event("e1", "", 1_000, 1.0), "key"),
        (Event("e2", "a", -1, 1.0), "ts_ms"),
        (Event("e3", "a", 1_000, float("nan")), "finite"),
        (Event("e4", "a", 1_000, float("inf")), "finite"),
    ]
    for event, needle in cases:
        s = submit_and_log(engine, event)
        assert s.decision is Decision.INVALID
        assert needle in s.reason
        assert s.emitted == []
    # Invalid events are not recorded as seen: fixing the payload lets the
    # same id through exactly once.
    assert submit_and_log(engine, Event("e3", "a", 1_000, 2.0)).decision is Decision.ACCEPTED


def test_decisions_are_distinguishable(engine: Engine) -> None:
    submit_and_log(engine, Event("e1", "a", 70_000, 1.0))  # wm -> 65s
    outcomes = {
        "accepted": submit_and_log(engine, Event("e2", "a", 66_000, 1.0)).decision,
        "duplicate": submit_and_log(engine, Event("e1", "a", 70_000, 1.0)).decision,
        "late": submit_and_log(engine, Event("e3", "a", 1_000, 1.0)).decision,
        "invalid": submit_and_log(engine, Event("", "a", 1_000, 1.0)).decision,
    }
    logger.info("decision outcomes=%s", outcomes)
    assert outcomes == {
        "accepted": Decision.ACCEPTED,
        "duplicate": Decision.DUPLICATE,
        "late": Decision.LATE,
        "invalid": Decision.INVALID,
    }


def test_concurrent_same_event_counted_once(engine: Engine) -> None:
    event = Event("hot", "a", 10_000, 1.0)
    decisions: list[Decision] = []
    lock = threading.Lock()

    def worker() -> None:
        sub = engine.submit(event)
        with lock:
            decisions.append(sub.decision)

    threads = [threading.Thread(target=worker) for _ in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logger.info("concurrent same-event decisions=%s",
                [d.value for d in decisions])
    assert decisions.count(Decision.ACCEPTED) == 1
    assert decisions.count(Decision.DUPLICATE) == 31
    assert engine.poll(0, 10)[-1].count == 1


def test_concurrent_distinct_events_all_counted() -> None:
    engine = Engine(EngineConfig(window_size_ms=60_000, grace_ms=0,
                                 dedup_capacity=10_000, log_capacity=100_000))
    n_threads, per_thread = 8, 50

    def worker(tid: int) -> None:
        for i in range(per_thread):
            engine.submit(Event(f"t{tid}-e{i}", "k", 1_000 + i, 1.0))

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    finals = engine.advance_watermark(60_000)
    total = sum(r.count for r in finals if r.trigger is Trigger.FINAL)
    logger.info("concurrent distinct events: expected=%d final_count=%d",
                n_threads * per_thread, total)
    assert total == n_threads * per_thread
