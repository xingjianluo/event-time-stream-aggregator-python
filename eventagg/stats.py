"""Runtime counters for observability: rejections, ignores, backpressure."""

from __future__ import annotations

import threading


class Counters:
    """Thread-safe named counters shared by the engine and subscriptions.

    Every distinguishable refusal/ignore gets its own counter so operators
    can tell "consumer too slow" apart from "resume point expired" and from
    per-event ignores (duplicate / too_late / invalid).
    """

    NAMES = (
        "accepted",            # events folded into a window
        "ignored_duplicate",   # same event_id applied again
        "ignored_too_late",    # event behind the lateness horizon
        "rejected_invalid",    # malformed payload
        "backpressure",        # poll refused: unacked buffer full
        "resume_expired",      # resume refused: position already evicted
    )

    def __init__(self) -> None:
        self._values = {name: 0 for name in self.NAMES}
        self._lock = threading.Lock()

    def incr(self, name: str, by: int = 1) -> None:
        with self._lock:
            self._values[name] += by

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._values)
