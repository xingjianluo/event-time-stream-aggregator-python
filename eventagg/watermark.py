"""Watermark tracking: monotone max event time minus allowed lateness."""

from __future__ import annotations


class WatermarkTracker:
    """Tracks the maximum observed event time and derives the watermark.

    watermark = max_event_time - out_of_orderness
    The watermark never moves backwards.
    """

    def __init__(self, out_of_orderness: float) -> None:
        self.out_of_orderness = out_of_orderness
        self.max_event_time: float | None = None
        self.current: float = float("-inf")

    def observe(self, event_time: float) -> float:
        """Observe an event time; returns the (possibly advanced) watermark."""
        raise NotImplementedError
