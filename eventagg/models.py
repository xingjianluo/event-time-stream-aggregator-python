"""Core data types: events, ingest results, window results."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class IngestStatus(enum.Enum):
    """Outcome classification of a single event submission."""

    ACCEPTED = "accepted"
    IGNORED = "ignored"
    REJECTED = "rejected"


class RejectReason(enum.Enum):
    """Machine-distinguishable reasons for ignored/rejected events."""

    NONE = "none"
    DUPLICATE = "duplicate"            # same event_id already applied
    TOO_LATE = "too_late"              # event time behind allowed lateness horizon
    INVALID = "invalid"                # malformed payload (bad timestamp/value)
    BACKPRESSURE = "backpressure"      # downstream too slow, explicitly refused


class WindowState(enum.Enum):
    """Lifecycle of a window result."""

    OPEN = "open"            # still accepting events, result is partial
    TRIGGERED = "triggered"  # watermark passed window end, result emitted
    CORRECTED = "corrected"  # late event arrived within allowed lateness
    FINAL = "final"          # lateness horizon passed, immutable conclusion


@dataclass(frozen=True)
class Event:
    """A single input event identified by event_id."""

    event_id: str
    key: str
    event_time: float          # epoch seconds (event time)
    value: float
    ingest_time: float = 0.0   # set by the aggregator on arrival


@dataclass(frozen=True)
class IngestResult:
    """Per-event decision returned to the caller."""

    status: IngestStatus
    reason: RejectReason
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status is IngestStatus.ACCEPTED


@dataclass(frozen=True)
class WindowResult:
    """An emitted (possibly revised) aggregate for one (key, window)."""

    seq: int                   # monotonically increasing emission sequence
    key: str
    window_start: float
    window_end: float
    count: int
    total: float
    state: WindowState
    watermark: float           # watermark at emission time
    reason: str                # human-readable explanation of this emission
