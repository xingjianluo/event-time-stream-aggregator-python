"""Out-of-order and late events: deterministic trigger/correction/final rules."""

from __future__ import annotations

import logging

from aggregator.engine import Engine
from aggregator.types import Decision, Event, Trigger

from .conftest import submit_and_log

logger = logging.getLogger("aggregator.tests")


def test_out_of_order_event_updates_partial(engine: Engine) -> None:
    # In-order event opens window [0, 60s).
    s1 = submit_and_log(engine, Event("e1", "a", 10_000, 1.0))
    assert s1.decision is Decision.ACCEPTED
    assert s1.emitted[0].trigger is Trigger.ON_EVENT
    assert s1.emitted[0].count == 1

    # Out-of-order (earlier ts, same window, still >= watermark): ON_EVENT.
    s2 = submit_and_log(engine, Event("e2", "a", 5_000, 2.0))
    assert s2.decision is Decision.ACCEPTED
    assert s2.emitted[0].trigger is Trigger.ON_EVENT
    assert (s2.emitted[0].count, s2.emitted[0].total) == (2, 3.0)


def test_late_event_within_grace_emits_correction(engine: Engine) -> None:
    submit_and_log(engine, Event("e1", "a", 10_000, 1.0))
    # Push watermark to 65s -> window [0,60s) FINAL, window [60s,120s) open.
    s2 = submit_and_log(engine, Event("e2", "a", 70_000, 1.0))
    assert any(r.trigger is Trigger.FINAL for r in s2.emitted)
    assert engine.watermark_ms == 65_000

    # ts=62s < watermark 65s but its window [60s,120s) is still open:
    # accepted as a CORRECTION, not silently dropped.
    s3 = submit_and_log(engine, Event("e3", "a", 62_000, 5.0))
    assert s3.decision is Decision.ACCEPTED
    assert s3.emitted[0].trigger is Trigger.CORRECTION
    assert (s3.emitted[0].count, s3.emitted[0].total) == (2, 6.0)


def test_event_for_finalized_window_rejected_late(engine: Engine) -> None:
    submit_and_log(engine, Event("e1", "a", 10_000, 1.0))
    submit_and_log(engine, Event("e2", "a", 70_000, 1.0))  # wm -> 65s
    # Window [0,60s) already finalized at wm=65s: rejected LATE, not counted.
    s3 = submit_and_log(engine, Event("e3", "a", 20_000, 9.0))
    assert s3.decision is Decision.LATE
    assert "finalized" in s3.reason
    assert s3.emitted == []
    # The finalized window's FINAL result still says count=1 (no double count).
    finals = [r for r in engine.poll(0, 100) if r.trigger is Trigger.FINAL]
    assert len(finals) == 1 and finals[0].count == 1


def test_watermark_is_monotone(engine: Engine) -> None:
    submit_and_log(engine, Event("e1", "a", 100_000, 1.0))
    assert engine.watermark_ms == 95_000
    # A much older event must not move the watermark backwards.
    submit_and_log(engine, Event("e2", "a", 50_000, 1.0))
    assert engine.watermark_ms == 95_000
    # Explicit advance below current watermark is a no-op.
    assert engine.advance_watermark(10_000) == []
    assert engine.watermark_ms == 95_000


def test_explicit_watermark_advance_finalizes_windows(engine: Engine) -> None:
    submit_and_log(engine, Event("e1", "a", 10_000, 1.0))
    # e2 pushes wm to 65s, which already finalizes a's window [0, 60s).
    s2 = submit_and_log(engine, Event("e2", "b", 70_000, 2.0))
    assert any(r.trigger is Trigger.FINAL and r.key == "a" for r in s2.emitted)
    # Explicit advance to 120s finalizes the remaining open window (b).
    finals = engine.advance_watermark(120_000)
    logger.info("explicit advance -> wm=%s finals=%s", engine.watermark_ms,
                [(r.key, r.window_start_ms, r.count, r.total) for r in finals])
    assert engine.watermark_ms == 120_000
    assert {(r.key, r.window_start_ms) for r in finals} == {("b", 60_000)}
    assert engine.open_window_count == 0
