"""Incremental subscription: resume without loss/duplication, backpressure."""

import logging

import pytest

from eventagg import SubscriptionClosed

from tests.conftest import drain_logged, ingest_logged

log = logging.getLogger("eventagg.test")


def _emit_results(agg, n: int, start: int = 0) -> int:
    """Ingest events until at least `n` NEW results have been emitted.

    Returns the total number of results in the log afterwards.
    """
    probe = agg.subscribe(after_seq=0)
    before = _count(probe)
    i = start
    while _count(agg.subscribe(after_seq=0)) - before < n:
        # Each event lands in a fresh window and pushes the watermark past
        # earlier windows, producing TRIGGERED/FINAL emissions.
        ingest_logged(agg, f"gen-{i}", "k", 100.0 + i * 60.0, 1.0)
        i += 1
    return _count(agg.subscribe(after_seq=0))


def _count(sub) -> int:
    return len(sub.poll(10_000))


def test_resume_after_reconnect_no_loss_no_duplication(agg):
    total = _emit_results(agg, 6)
    # Consumer reads everything, then "disconnects" remembering its ack.
    sub1 = agg.subscribe(after_seq=0)
    first = drain_logged(agg, sub1, note="(before reconnect)")
    assert [r.seq for r in first] == list(range(1, total + 1))
    acked = sub1.acked_seq
    log.info("client disconnects at acked_seq=%d", acked)

    # More results are emitted while the client is away.
    total2 = _emit_results(agg, 3, start=100)
    assert total2 > total

    # Reconnect with the remembered position: exactly the missed results.
    sub2 = agg.subscribe(after_seq=acked)
    replayed = drain_logged(agg, sub2, note="(after reconnect)")
    assert [r.seq for r in replayed] == list(range(total + 1, total2 + 1))
    # No duplication: seqs already acked are never delivered again.
    assert all(r.seq > acked for r in replayed)


def test_backpressure_raises_when_consumer_falls_behind(agg):
    # Fixture config: log_capacity = 8.
    sub = agg.subscribe(after_seq=0)
    total = _emit_results(agg, 10)
    assert total >= 10
    # Consumer polls without acking: buffer exceeds capacity -> explicit refusal.
    sub.poll(8)
    with pytest.raises(SubscriptionClosed) as exc:
        sub.poll(1)
    log.info("backpressure refusal: %s", exc.value)
    # Resume from the acked position still works after the refusal.
    sub2 = agg.subscribe(after_seq=sub.acked_seq)
    assert [r.seq for r in sub2.poll(100)] == list(range(1, total + 1))


def test_slow_consumer_with_acks_never_blocked(agg):
    """A consumer that acks as it goes stays within capacity."""
    sub = agg.subscribe(after_seq=0)
    total = _emit_results(agg, 20)
    seen = []
    while True:
        batch = sub.poll(4)  # small batches, ack each time
        if not batch:
            break
        seen.extend(r.seq for r in batch)
        sub.ack(batch[-1].seq)
    assert seen == list(range(1, total + 1))
    assert sub.acked_seq == total


def test_ack_beyond_delivered_rejected(agg):
    sub = agg.subscribe(after_seq=0)
    _emit_results(agg, 2)
    delivered = sub.poll(1)
    with pytest.raises(ValueError):
        sub.ack(delivered[-1].seq + 1)  # never delivered to this subscription
