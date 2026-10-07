"""Core engine: ingest -> validate -> dedup -> watermark -> window -> emit."""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass

from eventagg.dedup import DedupStore
from eventagg.models import (
    Event,
    IngestResult,
    IngestStatus,
    RejectReason,
    WindowResult,
    WindowState,
)
from eventagg.stats import Stats
from eventagg.subscription import ResultLog, Subscription
from eventagg.watermark import WatermarkTracker
from eventagg.window import WindowAgg, window_of


@dataclass(frozen=True)
class AggregatorConfig:
    """Tunable semantics of the aggregator."""

    window_size: float = 60.0          # seconds, tumbling window size
    out_of_orderness: float = 10.0     # watermark lag behind max event time
    allowed_lateness: float = 30.0     # extra time a triggered window accepts corrections
    dedup_capacity: int = 100_000      # max remembered event ids
    log_capacity: int = 10_000         # max unacked results per subscription
    result_retention: int = 10_000     # max results retained for replay/resume


class Aggregator:
    """Thread-safe event-time window aggregator.

    Deterministic rules:
    - watermark = max(event_time seen) - out_of_orderness, never recedes.
    - A window is TRIGGERED once watermark >= window_end.
    - A TRIGGERED window still accepts events (CORRECTED emissions) until
      watermark >= window_end + allowed_lateness, then it is FINAL.
    - An event whose window is already FINAL is ignored as TOO_LATE.
    - An event id already applied is ignored as DUPLICATE (exactly-once).
    """

    def __init__(self, config: AggregatorConfig | None = None) -> None:
        self.config = config or AggregatorConfig()
        self._wm = WatermarkTracker(self.config.out_of_orderness)
        self._dedup = DedupStore(self.config.dedup_capacity)
        self._windows: dict[tuple[str, float], WindowAgg] = {}
        self._states: dict[tuple[str, float], WindowState] = {}
        self._stats = Stats()
        self._log = ResultLog(retention=self.config.result_retention,
                              stats=self._stats)
        self._seq = 0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ API

    def ingest(self, event: Event) -> IngestResult:
        """Validate, dedup, and fold one event into its window."""
        invalid = self._validate(event)
        if invalid is not None:
            self._stats.incr("invalid")
            return invalid

        start, end = window_of(event.event_time, self.config.window_size)
        wkey = (event.key, start)

        with self._lock:
            wm = self._wm.current
            # Lateness horizon: window already finalized -> too late.
            if wm >= end + self.config.allowed_lateness:
                self._stats.incr("too_late")
                return IngestResult(
                    IngestStatus.IGNORED,
                    RejectReason.TOO_LATE,
                    f"watermark {wm} >= window end {end} + allowed_lateness "
                    f"{self.config.allowed_lateness}; window is FINAL",
                )
            # Idempotency: atomic check-and-mark under the same lock.
            if not self._dedup.check_and_mark(event.event_id):
                self._stats.incr("duplicate")
                return IngestResult(
                    IngestStatus.IGNORED,
                    RejectReason.DUPLICATE,
                    f"event_id {event.event_id!r} already applied",
                )

            agg = self._windows.get(wkey)
            if agg is None:
                agg = WindowAgg(event.key, start, end)
                self._windows[wkey] = agg
                self._states[wkey] = WindowState.OPEN
            agg.add(event.value)

            state = self._states[wkey]
            if state is WindowState.TRIGGERED or state is WindowState.CORRECTED:
                # Late-but-allowed event revises an already-triggered window.
                self._states[wkey] = WindowState.CORRECTED
                self._emit(agg, WindowState.CORRECTED,
                           f"late event {event.event_id!r} arrived while "
                           f"watermark {wm} < window end {end} + allowed_lateness "
                           f"{self.config.allowed_lateness}")

            # Advance watermark, then fire triggers/finalizations in order.
            wm = self._wm.observe(event.event_time)
            self._advance(wm)

            self._stats.incr("accepted")
            return IngestResult(IngestStatus.ACCEPTED, RejectReason.NONE,
                                f"applied to window [{start}, {end})")

    @property
    def watermark(self) -> float:
        return self._wm.current

    def subscribe(self, after_seq: int = 0) -> Subscription:
        """Open a subscription over emitted results, resuming after `after_seq`.

        Raises ResumeTooOld if `after_seq` has already been evicted from
        the bounded result log; the caller must then re-sync from scratch.
        """
        return Subscription(self._log, after_seq=after_seq,
                            capacity=self.config.log_capacity,
                            stats=self._stats)

    def stats(self) -> dict:
        """Runtime snapshot: live state sizes plus monotone counters.

        Includes the number of windows still accumulating, results still
        retained for replay, and per-kind accept/ignore/refuse counts so
        operators can verify memory is bounded and see why events or
        consumers were refused.
        """
        with self._lock:
            open_windows = len(self._windows)
        snap = self._stats.snapshot()
        return {
            "open_windows": open_windows,
            "retained_results": len(self._log),
            "watermark": self._wm.current,
            **snap,
        }

    # ------------------------------------------------------------- internal

    @staticmethod
    def _validate(event: Event) -> IngestResult | None:
        if not event.event_id:
            return IngestResult(IngestStatus.REJECTED, RejectReason.INVALID,
                                "event_id must be non-empty")
        if not event.key:
            return IngestResult(IngestStatus.REJECTED, RejectReason.INVALID,
                                "key must be non-empty")
        if not math.isfinite(event.event_time):
            return IngestResult(IngestStatus.REJECTED, RejectReason.INVALID,
                                f"event_time {event.event_time!r} is not finite")
        if not math.isfinite(event.value):
            return IngestResult(IngestStatus.REJECTED, RejectReason.INVALID,
                                f"value {event.value!r} is not finite")
        return None

    def _advance(self, wm: float) -> None:
        """Fire TRIGGERED and FINAL emissions for windows crossed by `wm`.

        Once a window is FINAL its per-window state is reclaimed: the
        watermark never recedes, so any later event for that window is
        rejected as TOO_LATE before touching window state, and keeping
        the aggregate around would only leak memory.
        """
        for wkey in sorted(self._windows):
            agg = self._windows[wkey]
            state = self._states[wkey]
            if state is WindowState.OPEN and wm >= agg.end:
                self._states[wkey] = WindowState.TRIGGERED
                self._emit(agg, WindowState.TRIGGERED,
                           f"watermark {wm} >= window end {agg.end}")
            if self._states[wkey] is not WindowState.FINAL and \
                    wm >= agg.end + self.config.allowed_lateness:
                self._states[wkey] = WindowState.FINAL
                self._emit(agg, WindowState.FINAL,
                           f"watermark {wm} >= window end {agg.end} + "
                           f"allowed_lateness {self.config.allowed_lateness}; "
                           f"window closed permanently")
        # Reclaim state of permanently closed windows.
        closed = [k for k, s in self._states.items() if s is WindowState.FINAL]
        for k in closed:
            del self._windows[k]
            del self._states[k]
        if closed:
            self._stats.incr("windows_reclaimed", len(closed))

    def _emit(self, agg: WindowAgg, state: WindowState, reason: str) -> None:
        self._seq += 1
        self._log.append(WindowResult(
            seq=self._seq,
            key=agg.key,
            window_start=agg.start,
            window_end=agg.end,
            count=agg.count,
            total=agg.total,
            state=state,
            watermark=self._wm.current,
            reason=reason,
        ))
