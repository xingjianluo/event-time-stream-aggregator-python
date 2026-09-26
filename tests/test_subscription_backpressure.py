"""Resumable subscriptions and bounded-memory backpressure."""

from __future__ import annotations

import logging

import pytest

from aggregator.engine import Engine, EngineConfig
from aggregator.result_log import GapError
from aggregator.types import Event

from .conftest import submit_and_log

logger = logging.getLogger("aggregator.tests")


def _make_engine(log_capacity: int = 8) -> Engine:
    return Engine(EngineConfig(window_size_ms=60_000, grace_ms=5_000,
                               dedup_capacity=10_000, log_capacity=log_capacity))


def test_resume_from_checkpoint_no_loss_no_dup() -> None:
    engine = _make_engine(log_capacity=1_000)
    for i in range(10):
        submit_and_log(engine, Event(f"e{i}", "a", 1_000 + i, 1.0))

    first = engine.poll(after_seq=0, limit=4)
    assert [r.seq for r in first] == [1, 2, 3, 4]
    # Client checkpoints at last seen seq and reconnects: gets exactly the
    # remaining entries, no gaps, no duplicates.
    resumed = engine.poll(after_seq=first[-1].seq, limit=100)
    logger.info("resume after seq=%d -> seqs=%s", first[-1].seq,
                [r.seq for r in resumed])
    assert [r.seq for r in resumed] == [5, 6, 7, 8, 9, 10]
    assert engine.poll(after_seq=10, limit=100) == []


def test_slow_consumer_gets_explicit_gap_signal() -> None:
    engine = _make_engine(log_capacity=8)
    # 20 events -> 20 results, but the log only keeps the last 8.
    for i in range(20):
        engine.submit(Event(f"e{i}", "a", 1_000 + i, 1.0))
    logger.info("log bounded: oldest_seq=%d latest_seq=%d",
                engine._log.oldest_seq, engine._log.latest_seq)
    assert engine._log.latest_seq == 20
    assert engine._log.oldest_seq == 13  # memory stayed bounded at capacity 8

    # A consumer resuming from an evicted checkpoint gets a GapError with
    # the oldest available seq, instead of silent data loss.
    with pytest.raises(GapError) as exc_info:
        engine.poll(after_seq=5, limit=100)
    logger.info("gap detected: %s", exc_info.value)
    assert exc_info.value.oldest_seq == 13

    # It can resubscribe from the oldest available seq and continue.
    resumed = engine.poll(after_seq=exc_info.value.oldest_seq - 1, limit=100)
    assert [r.seq for r in resumed] == list(range(13, 21))


def test_bounded_log_memory_stays_flat_under_flood() -> None:
    engine = _make_engine(log_capacity=16)
    for i in range(5_000):
        engine.submit(Event(f"e{i}", "a", 1_000 + (i % 50), 1.0))
    # Internal buffers never exceed their configured capacity.
    assert len(engine._log._entries) <= 16
    logger.info("after 5000 events: log_size=%d open_windows=%d",
                len(engine._log._entries), engine.open_window_count)


def test_sequenced_results_are_totally_ordered() -> None:
    engine = _make_engine(log_capacity=1_000)
    for i in range(50):
        engine.submit(Event(f"e{i}", "k", 1_000 + i, 1.0))
    seqs = [r.seq for r in engine.poll(0, 1_000)]
    assert seqs == list(range(1, len(seqs) + 1))  # contiguous, monotone
