"""Bounded, totally ordered result log with resumable reads."""

from __future__ import annotations

import dataclasses
from collections import deque

from .types import WindowResult


class GapError(Exception):
    """Raised when the requested resume point has been evicted.

    The client asked for seq > after_seq, but entries up to and including
    after_seq are gone, so continuity cannot be guaranteed. The client
    receives an explicit signal instead of silent data loss and should
    resubscribe from the oldest available seq.
    """

    def __init__(self, after_seq: int, oldest_seq: int) -> None:
        super().__init__(
            f"resume point {after_seq} evicted; oldest available seq is {oldest_seq}"
        )
        self.after_seq = after_seq
        self.oldest_seq = oldest_seq


class ResultLog:
    """Append-only log of emitted results with monotonically increasing seq.

    Bounded by `capacity`; oldest entries are evicted, which is the
    backpressure mechanism for slow consumers: memory never grows without
    bound, and a consumer that falls too far behind gets a GapError.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._capacity = capacity
        self._entries: deque[WindowResult] = deque()
        self._next_seq = 1

    def append(self, result: WindowResult) -> WindowResult:
        """Assign the next seq, store, and return the sequenced result."""
        sequenced = dataclasses.replace(result, seq=self._next_seq)
        self._next_seq += 1
        self._entries.append(sequenced)
        while len(self._entries) > self._capacity:
            self._entries.popleft()
        return sequenced

    def subscribe(self, after_seq: int, limit: int) -> list[WindowResult]:
        """Return up to `limit` entries with seq > after_seq (resumable)."""
        if limit <= 0:
            return []
        if not self._entries:
            if after_seq >= self._next_seq:
                return []
            raise GapError(after_seq, self._next_seq)
        oldest = self._entries[0].seq
        if after_seq < oldest - 1:
            raise GapError(after_seq, oldest)
        out: list[WindowResult] = []
        for entry in self._entries:
            if entry.seq > after_seq:
                out.append(entry)
                if len(out) >= limit:
                    break
        return out

    @property
    def oldest_seq(self) -> int:
        return self._entries[0].seq if self._entries else self._next_seq

    @property
    def latest_seq(self) -> int:
        return self._next_seq - 1
