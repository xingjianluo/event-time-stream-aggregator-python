"""Result log and subscriptions with resume, bounded retention and backpressure."""

from __future__ import annotations

import threading
from collections import deque

from eventagg.models import WindowResult
from eventagg.stats import Counters


class SubscriptionClosed(Exception):
    """Raised when a subscription overflows its unacked buffer (backpressure).

    The consumer is too slow *right now*; it may reconnect and resume from
    its last acked position.
    """


class ResumePointExpired(Exception):
    """Raised when a resume position points at results already evicted.

    Distinct from SubscriptionClosed: the retained log no longer covers the
    requested position, so the client must do a full resync instead of
    silently receiving a partial, gap-ridden stream.
    """


class ResultLog:
    """Append-only, thread-safe log of emitted window results.

    Retention is bounded: once more than `retention` results are held, the
    oldest are evicted (FIFO). A subscriber whose resume position falls
    behind the oldest retained seq is refused explicitly via
    ResumePointExpired, never silently skipped.
    """

    def __init__(self, retention: int = 10_000,
                 counters: Counters | None = None) -> None:
        if retention <= 0:
            raise ValueError("retention must be > 0")
        self._retention = retention
        self._counters = counters
        self._results: deque[WindowResult] = deque()
        self._evicted_upto = 0     # seqs <= this have been evicted
        self._lock = threading.Lock()

    def append(self, result: WindowResult) -> None:
        with self._lock:
            self._results.append(result)
            while len(self._results) > self._retention:
                self._evicted_upto = self._results.popleft().seq

    def read_from(self, after_seq: int, limit: int = 100) -> list[WindowResult]:
        """Return up to `limit` results with seq > after_seq, in order.

        Raises ResumePointExpired if results in (after_seq, evicted_upto]
        have already been evicted: resuming there would silently skip data.
        """
        with self._lock:
            self._check_resume_locked(after_seq)
            out = [r for r in self._results if r.seq > after_seq]
        return out[:limit]

    def check_resume(self, after_seq: int) -> None:
        """Validate a resume position without reading (fail fast at subscribe)."""
        with self._lock:
            self._check_resume_locked(after_seq)

    def _check_resume_locked(self, after_seq: int) -> None:
        if after_seq < self._evicted_upto:
            if self._counters is not None:
                self._counters.incr("resume_expired")
            raise ResumePointExpired(
                f"resume point seq {after_seq} is older than the oldest "
                f"retained result; seqs up to {self._evicted_upto} have been "
                f"evicted (retention={self._retention}); a full resync is "
                f"required"
            )

    @property
    def evicted_upto(self) -> int:
        """Highest seq no longer retained (0 if nothing evicted yet)."""
        with self._lock:
            return self._evicted_upto

    def __len__(self) -> int:
        with self._lock:
            return len(self._results)


class Subscription:
    """A consumer view over the ResultLog with resume and backpressure.

    - Resume: construct with `after_seq` equal to the last acked seq; every
      result after it is replayed exactly once, nothing is lost or repeated.
      If that position has already been evicted from the log, construction
      fails fast with ResumePointExpired (full resync needed).
    - Backpressure: results delivered via `poll` must be confirmed via
      `ack`. If unacked deliveries exceed `capacity`, `poll` raises
      SubscriptionClosed (explicit refusal) instead of buffering without
      bound. The client may reconnect and resume from `acked_seq`.
    """

    def __init__(self, log: ResultLog, after_seq: int = 0, capacity: int = 10_000,
                 counters: Counters | None = None) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        log.check_resume(after_seq)
        self._log = log
        self._capacity = capacity
        self._counters = counters
        self._read_seq = after_seq    # next seq to deliver is read_seq + ...
        self._acked_seq = after_seq
        self._lock = threading.Lock()

    def poll(self, limit: int = 100) -> list[WindowResult]:
        """Fetch up to `limit` results after the read cursor.

        Raises SubscriptionClosed if unacked deliveries already exceed the
        subscription capacity (consumer slower than producer), and
        ResumePointExpired if retention eviction has overtaken the read
        cursor while this subscription was idle.
        """
        with self._lock:
            pending = self._read_seq - self._acked_seq
            if pending >= self._capacity:
                if self._counters is not None:
                    self._counters.incr("backpressure")
                raise SubscriptionClosed(
                    f"unacked results {pending} >= capacity {self._capacity}; "
                    f"reconnect and resume from seq {self._acked_seq}"
                )
            batch = self._log.read_from(self._read_seq, limit)
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
