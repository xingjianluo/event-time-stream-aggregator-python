"""Watermark tracking: monotone max event time minus allowed lateness."""

from __future__ import annotations

import threading


class WatermarkTracker:
    """Tracks the maximum observed event time and derives the watermark.

    watermark = max_event_time - out_of_orderness

    The watermark never moves backwards, so downstream window triggers are
    deterministic regardless of arrival order.
    """

    def __init__(self, out_of_orderness: float) -> None:
        if out_of_orderness < 0:
            raise ValueError("out_of_orderness must be >= 0")
        self.out_of_orderness = out_of_orderness
        self.max_event_time: float | None = None
        self.current: float = float("-inf")
        self._lock = threading.Lock()

    def observe(self, event_time: float) -> float:
        """Observe an event time; returns the (possibly advanced) watermark."""
        with self._lock:
            if self.max_event_time is None or event_time > self.max_event_time:
                self.max_event_time = event_time
            candidate = self.max_event_time - self.out_of_orderness
            if candidate > self.current:
                self.current = candidate
            return self.current
