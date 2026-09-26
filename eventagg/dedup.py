"""Idempotency store: remembers which event ids have been applied."""

from __future__ import annotations

import threading


class DedupStore:
    """Thread-safe set of applied event ids with bounded memory.

    Ids are evicted in FIFO order once `capacity` is exceeded; evicted ids
    are treated as TOO_LATE rather than silently re-applied.
    """

    def __init__(self, capacity: int = 100_000) -> None:
        self.capacity = capacity

    def check_and_mark(self, event_id: str) -> bool:
        """Atomically check-and-mark. Returns True if newly applied."""
        raise NotImplementedError

    def __contains__(self, event_id: str) -> bool:
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError
