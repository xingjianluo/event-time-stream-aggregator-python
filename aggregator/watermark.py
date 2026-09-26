"""Watermark tracking: monotone max-observed-event-time minus grace."""

from __future__ import annotations


class WatermarkTracker:
    """Tracks the max event time seen; watermark = max_ts - grace_ms.

    The watermark is monotone: it never moves backwards. Explicit advances
    via `advance_to` are also clamped to be non-decreasing.
    """

    def __init__(self, grace_ms: int) -> None:
        if grace_ms < 0:
            raise ValueError("grace_ms must be >= 0")
        self._grace_ms = grace_ms
        self._max_ts: int | None = None
        self._explicit: int | None = None

    @property
    def watermark_ms(self) -> int | None:
        """Current watermark, or None before any event/watermark input."""
        derived = None if self._max_ts is None else self._max_ts - self._grace_ms
        candidates = [c for c in (derived, self._explicit) if c is not None]
        return max(candidates) if candidates else None

    def observe_event(self, ts_ms: int) -> int | None:
        """Observe an accepted event's timestamp; returns new watermark."""
        if self._max_ts is None or ts_ms > self._max_ts:
            self._max_ts = ts_ms
        return self.watermark_ms

    def advance_to(self, watermark_ms: int) -> int | None:
        """Explicitly advance the watermark (monotone); returns new value."""
        current = self.watermark_ms
        if current is None or watermark_ms > current:
            self._explicit = watermark_ms
        return self.watermark_ms
