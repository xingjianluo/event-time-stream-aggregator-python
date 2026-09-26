"""Result log and subscriptions with resume and bounded buffering."""

from __future__ import annotations

import threading

from eventagg.models import WindowResult


class SubscriptionClosed(Exception):
    """Raised when a subscription overflows its unacked buffer (backpressure)."""


class ResultLog:
    """Append-only, thread-safe log of emitted window results.

    Results are retained so that a reconnecting subscriber can resume from
    its last acked sequence without loss or duplication.
    """

    def __init__(self) -> None:
        self._results: list[WindowResult] = []
        self._lock = threading.Lock()

    def append(self, result: WindowResult) -> None:
        with self._lock:
            self._results.append(result)

    def read_from(self, after_seq: int, limit: int = 100) -> list[WindowResult]:
        """Return up to `limit` results with seq > after_seq, in order."""
        with self._lock:
            out = [r for r in self._results if r.seq > after_seq]
        return out[:limit]

    def __len__(self) -> int:
        with self._lock:
            return len(self._results)


class Subscription:
    """A consumer view over the ResultLog with resume and backpressure.

    - Resume: construct with `after_seq` equal to the last acked seq; every
      result after it is replayed exactly once, nothing is lost or repeated.
    - Backpressure: results delivered via `poll` must be confirmed via
      `ack`. If unacked deliveries exceed `capacity`, `poll` raises
      SubscriptionClosed (explicit refusal) instead of buffering without
      bound. The client may reconnect and resume from `acked_seq`.
    """

    def __init__(self, log: ResultLog, after_seq: int = 0, capacity: int = 10_000) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._log = log
        self._capacity = capacity
        self._read_seq = after_seq    # next seq to deliver is read_seq + ...
        self._acked_seq = after_seq
        self._lock = threading.Lock()

    def poll(self, limit: int = 100) -> list[WindowResult]:
        """Fetch up to `limit` results after the read cursor.

        Raises SubscriptionClosed if unacked deliveries already exceed the
        subscription capacity (consumer slower than producer).
        """
        with self._lock:
            pending = self._read_seq - self._acked_seq
            if pending >= self._capacity:
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
