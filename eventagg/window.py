"""Tumbling event-time windows and per-window aggregation state."""

from __future__ import annotations

from dataclasses import dataclass, field


def window_of(event_time: float, size: float) -> tuple[float, float]:
    """Return (start, end) of the tumbling window containing event_time."""
    raise NotImplementedError


@dataclass
class WindowAgg:
    """Aggregation state for one (key, window)."""

    key: str
    start: float
    end: float
    count: int = 0
    total: float = 0.0

    def add(self, value: float) -> None:
        raise NotImplementedError
