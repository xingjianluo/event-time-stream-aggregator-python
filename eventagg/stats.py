"""Runtime statistics: thread-safe counters for observability."""

from __future__ import annotations

import threading


class Stats:
    """Monotone counters describing aggregator behaviour since start.

    Used for the runtime snapshot: how many events were accepted, how
    many were ignored/rejected per reason, how many subscription
    refusals happened per kind, and how much state was reclaimed or
    evicted. All counters are thread-safe and never decrease.
    """

    _FIELDS = (
        "accepted",          # events applied to a window
        "duplicate",         # events ignored: event_id already applied
        "too_late",          # events ignored: window already FINAL
        "invalid",           # events rejected: malformed payload
        "backpressure",      # poll refusals: consumer too slow
        "resume_too_old",    # subscribe/poll refusals: resume point evicted
        "results_evicted",   # results dropped by bounded retention
        "windows_reclaimed",  # FINAL windows whose state was freed
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters = {name: 0 for name in self._FIELDS}

    def incr(self, name: str, n: int = 1) -> None:
        with self._lock:
            self._counters[name] += n

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counters)
