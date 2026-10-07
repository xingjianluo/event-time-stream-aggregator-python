"""Result log and subscriptions: bounded retention, resume, backpressure.

Two explicitly distinguishable refusal modes:

- ``SubscriptionClosed`` (reason ``backpressure``): the consumer polls
  without acking and its unacked deliveries exceed the subscription
  capacity. It may reconnect and resume from its last acked seq.
- ``ResumeTooOld`` (reason ``resume_too_old``): the requested resume
  point has already been evicted from the bounded result log, so a
  loss-free replay is impossible. The client must re-sync from scratch
  instead of silently receiving a gap.
"""

from __future__ import annotations

import threading
from collections import deque

from eventagg.models import WindowResult
from eventagg.stats import Stats


class SubscriptionError(Exception):
    """Base class for subscription-level refusals."""

    reason = "subscription_error"


class SubscriptionClosed(SubscriptionError):
    """Unacked deliveries exceeded capacity (consumer too slow)."""

    reason = "backpressure"


class ResumeTooOld(SubscriptionError):
    """Resume point evicted from the bounded log; full re-sync required."""

    reason = "resume_too_old"


class ResultLog:
    """Append-only, thread-safe log of emitted window results.

    Retention is bounded: once more than ``retention`` results are held,
    the oldest are evicted (FIFO). ``min_available_seq`` always tells
    callers which is the oldest seq still retained; asking to resume
    from anything older raises :class:`ResumeTooOld` instead of silently
    skipping the evicted range.
    """

    def __init__(self, retention: int = 10_000, stats: Stats | None = None) -> None:
        if retention <= 0:
            raise ValueError("retention must be > 0")
        self.retention = retention
        self._stats = stats
        self._results: deque[WindowResult] = deque()
        self._evicted = 0  # count of results evicted from the front
        self._lock = threading.Lock()

    def append(self, result: WindowResult) -> None:
        with self._lock:
            self._results.append(result)
            evicted_now = 0
            while len(self._results) > self.retention:
                self._results.popleft()
                self._evicted += 1
                evicted_now += 1
        if evicted_now and self._stats is not None:
            self._stats.incr("results_evicted", evicted_now)

    @property
    def evicted_count(self) -> int:
        """How many results have been evicted so far (monotone)."""
        with self._lock:
            return self._evicted

    @property
    def min_available_seq(self) -> int:
        """Oldest seq still retained (seqs are contiguous from 1)."""
        with self._lock:
            return self._evicted + 1

    def check_resumable(self, after_seq: int) -> None:
        """Raise ResumeTooOld if a resume from `after_seq` would skip data."""
        with self._lock:
            if after_seq < self._evicted:
                raise ResumeTooOld(
                    f"resume point seq {after_seq} is older than the oldest "
                    f"retained seq {self._evicted + 1}; {self._evicted} results "
                    f"already evicted (retention={self.retention}); "
                    f"a full re-sync is required"
                )

    def read_from(self, after_seq: int, limit: int = 100) -> list[WindowResult]:
        """Return up to `limit` results with seq > after_seq, in order.

        Raises ResumeTooOld if part of the requested range was evicted.
        """
        self.check_resumable(after_seq)
        with self._lock:
            skip = max(0, after_seq - self._evicted)
            out = []
            for i in range(skip, len(self._results)):
                out.append(self._results[i])
                if len(out) >= limit:
                    break
            return out

    def __len__(self) -> int:
        with self._lock:
            return len(self._results)


class Subscription:
    """A consumer view over the ResultLog with resume and backpressure.

    - Resume: construct with `after_seq` equal to the last acked seq; every
      result after it is replayed exactly once, nothing is lost or repeated.
      If that position has already been evicted, ResumeTooOld is raised
      immediately (no silent gap).
    - Backpressure: results delivered via `poll` must be confirmed via
      `ack`. If unacked deliveries exceed `capacity`, `poll` raises
      SubscriptionClosed (explicit refusal) instead of buffering without
      bound. The client may reconnect and resume from `acked_seq`.
    """

    def __init__(self, log: ResultLog, after_seq: int = 0, capacity: int = 10_000,
                 stats: Stats | None = None) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._stats = stats
        try:
            log.check_resumable(after_seq)
        except ResumeTooOld:
            if self._stats is not None:
                self._stats.incr("resume_too_old")
            raise
        self._log = log
        self._capacity = capacity
        self._read_seq = after_seq    # next seq to deliver is read_seq + ...
        self._acked_seq = after_seq
        self._lock = threading.Lock()

    def poll(self, limit: int = 100) -> list[WindowResult]:
        """Fetch up to `limit` results after the read cursor.

        Raises SubscriptionClosed if unacked deliveries already exceed the
        subscription capacity (consumer slower than producer), and
        ResumeTooOld if retention evicted results this subscription has
        not delivered yet.
        """
        with self._lock:
            pending = self._read_seq - self._acked_seq
            if pending >= self._capacity:
                if self._stats is not None:
                    self._stats.incr("backpressure")
                raise SubscriptionClosed(
                    f"unacked results {pending} >= capacity {self._capacity}; "
                    f"reconnect and resume from seq {self._acked_seq}"
                )
            try:
                batch = self._log.read_from(self._read_seq, limit)
            except ResumeTooOld:
                if self._stats is not None:
                    self._stats.incr("resume_too_old")
                raise
            if batch:
                self._read_seq = batch[-1].seq
            return batch

    def ack(self, upto_seq: int) -> None:
        """Mark results up to `upto_seq` as consumed; enables resume."""
        with self._lock:
            if upto_seq > self._read_seq:
                raise ValueError(
                    f"cannot ack seq {upto_seq} beyond delivered {self._read_seq}"
                )
            if upto_seq > self._acked_seq:
                self._acked_seq = upto_seq

    @property
    def acked_seq(self) -> int:
        with self._lock:
            return self._acked_seq
