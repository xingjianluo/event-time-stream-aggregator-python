"""Shared fixtures: a small-config engine and a verdict logger.

Test logs print every input event, the watermark before/after, and the
decision rationale so results are reproducible and explainable.
"""

from __future__ import annotations

import logging

import pytest

from aggregator.engine import Engine, EngineConfig
from aggregator.types import Event, Submission

logger = logging.getLogger("aggregator.tests")


@pytest.fixture()
def engine() -> Engine:
    # 60s windows, 5s grace, small buffers so backpressure is easy to hit.
    return Engine(EngineConfig(window_size_ms=60_000, grace_ms=5_000,
                               dedup_capacity=1_000, log_capacity=1_000))


def submit_and_log(engine: Engine, event: Event) -> Submission:
    """Submit one event and log event + watermark + decision rationale."""
    wm_before = engine.watermark_ms
    sub = engine.submit(event)
    logger.info(
        "event=%s key=%s ts=%d value=%s | wm %s -> %s | decision=%s reason=%s | emitted=%s",
        event.event_id, event.key, event.ts_ms, event.value,
        wm_before, engine.watermark_ms, sub.decision.value, sub.reason,
        [(r.seq, r.key, r.window_start_ms, r.count, r.total, r.trigger.value)
         for r in sub.emitted],
    )
    return sub
