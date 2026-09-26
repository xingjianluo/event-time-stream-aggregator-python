"""Bounded idempotency store for accepted event ids."""

from __future__ import annotations

from collections import OrderedDict


class DedupStore:
    """Remembers ids of accepted events so retries are ignored exactly once.

    Bounded by `capacity`; oldest ids are evicted (FIFO) so memory stays
    bounded. An evicted id would be treated as new again — a documented
    trade-off; size the capacity above the expected retry horizon.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        self._capacity = capacity
        self._ids: OrderedDict[str, None] = OrderedDict()

    def check_and_record(self, event_id: str) -> bool:
        """Return True if the id is new (and record it); False if duplicate."""
        if event_id in self._ids:
            # Refresh recency so hot retried ids are not evicted early.
            self._ids.move_to_end(event_id)
            return False
        self._ids[event_id] = None
        while len(self._ids) > self._capacity:
            self._ids.popitem(last=False)
        return True

    def __len__(self) -> int:
        return len(self._ids)
