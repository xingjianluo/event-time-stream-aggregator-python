"""Tumbling event-time windows and per-window aggregation state."""

from __future__ import annotations

import math
from dataclasses import dataclass


def window_of(event_time: float, size: float) -> tuple[float, float]:
    """Return (start, end) of the tumbling window containing event_time.

    Windows are half-open: [start, end). Deterministic for any event_time.
    """
    if size <= 0:
        raise ValueError("window size must be > 0")
    start = math.floor(event_time / size) * size
    return start, start + size


@dataclass
class WindowAgg:
    """Aggregation state for one (key, window)."""

    key: str
    start: float
    end: float
    count: int = 0
    total: float = 0.0

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
