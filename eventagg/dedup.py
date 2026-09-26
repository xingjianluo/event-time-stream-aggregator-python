"""Idempotency store: remembers which event ids have been applied."""

from __future__ import annotations

import threading
from collections import OrderedDict


class DedupStore:
    """Thread-safe set of applied event ids with bounded memory.

    `check_and_mark` is atomic, so concurrent submissions of the same id
    are applied exactly once. Ids are evicted in FIFO order once
    `capacity` is exceeded; callers must treat evicted (unknown) ids
    according to their lateness rules rather than silently re-applying.
    """

    def __init__(self, capacity: int = 100_000) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self.capacity = capacity
        self._ids: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.Lock()

    def check_and_mark(self, event_id: str) -> bool:
        """Atomically check-and-mark. Returns True if newly applied."""
        with self._lock:
            if event_id in self._ids:
                return False
            self._ids[event_id] = None
            while len(self._ids) > self.capacity:
                self._ids.popitem(last=False)
            return True

    def __contains__(self, event_id: str) -> bool:
        with self._lock:
            return event_id in self._ids

    def __len__(self) -> int:
        with self._lock:
            return len(self._ids)
