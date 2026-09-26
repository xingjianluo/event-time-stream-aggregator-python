"""Core data types: events, decisions, window results."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class Decision(enum.Enum):
    """Outcome of submitting one event."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    LATE = "late"
    INVALID = "invalid"


class Trigger(enum.Enum):
    """Why a window result was emitted."""

    ON_EVENT = "on_event"
    CORRECTION = "correction"
    FINAL = "final"


@dataclass(frozen=True)
class Event:
    """One input event. `ts_ms` is event time in epoch milliseconds."""

    event_id: str
    key: str
    ts_ms: int
    value: float


@dataclass(frozen=True)
class Window:
    """A fixed event-time window [start_ms, end_ms)."""

    start_ms: int
    end_ms: int

    def contains(self, ts_ms: int) -> bool:
        return self.start_ms <= ts_ms < self.end_ms


@dataclass(frozen=True)
class WindowResult:
    """One emitted aggregate result for a (key, window)."""

    seq: int
    key: str
    window_start_ms: int
    window_end_ms: int
    count: int
    total: float
    trigger: Trigger
    watermark_ms: int


@dataclass
class Submission:
    """Per-event ingestion verdict."""

    event_id: str
    decision: Decision
    reason: str
    emitted: list[WindowResult] = field(default_factory=list)
