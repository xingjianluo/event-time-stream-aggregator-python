"""Aggregation engine: ties watermark, dedup, windows and result log together."""

from __future__ import annotations

import threading

from .dedup import DedupStore
from .result_log import ResultLog
from .types import Decision, Event, Submission, WindowResult
from .watermark import WatermarkTracker
from .windows import WindowManager


class EngineConfig:
    """Tunable knobs (see README for semantics)."""

    def __init__(
        self,
        window_size_ms: int = 60_000,
        grace_ms: int = 5_000,
        dedup_capacity: int = 100_000,
        log_capacity: int = 10_000,
    ) -> None:
        self.window_size_ms = window_size_ms
        self.grace_ms = grace_ms
        self.dedup_capacity = dedup_capacity
        self.log_capacity = log_capacity


class Engine:
    """Thread-safe aggregation engine.

    A single lock serializes submissions, so concurrent and/or repeated
    delivery of the same event affects the aggregate exactly once.

    Submission decision order (deterministic, distinguishable):
    1. INVALID   - malformed event (empty id/key, negative ts, non-finite value).
    2. DUPLICATE - event_id already accepted before; ignored, no state change.
    3. LATE      - event's window was already finalized (window_end <= watermark);
                   rejected, never silently dropped nor double counted.
    4. ACCEPTED  - aggregated; emits an ON_EVENT partial (or CORRECTION if the
                   event is late-but-still-open), plus FINAL results for any
                   windows the advancing watermark closes.
    """

    def __init__(self, config: EngineConfig | None = None) -> None:
        self._config = config or EngineConfig()
        self._lock = threading.RLock()
        self._watermark = WatermarkTracker(self._config.grace_ms)
        self._dedup = DedupStore(self._config.dedup_capacity)
        self._windows = WindowManager(self._config.window_size_ms)
        self._log = ResultLog(self._config.log_capacity)

    def submit(self, event: Event) -> Submission:
        """Validate, dedup, aggregate and emit; returns the verdict."""
        with self._lock:
            invalid_reason = self._validate(event)
            if invalid_reason is not None:
                return Submission(event.event_id, Decision.INVALID, invalid_reason)

            if not self._dedup.check_and_record(event.event_id):
                return Submission(
                    event.event_id,
                    Decision.DUPLICATE,
                    "event_id already accepted; ignored to keep exactly-once",
                )

            window = self._windows.window_for(event.ts_ms)
            watermark = self._watermark.watermark_ms
            if self._windows.is_expired(window, watermark):
                return Submission(
                    event.event_id,
                    Decision.LATE,
                    f"window [{window.start_ms}, {window.end_ms}) already finalized "
                    f"at watermark {watermark}",
                )

            watermark = self._watermark.observe_event(event.ts_ms)
            assert watermark is not None
            emitted = [self._log.append(self._windows.add(event.key, event, watermark))]
            emitted.extend(self._log.append(r) for r in self._windows.advance(watermark))
            return Submission(event.event_id, Decision.ACCEPTED, "aggregated", emitted)

    def advance_watermark(self, watermark_ms: int) -> list[WindowResult]:
        """Explicitly advance the watermark; emits FINAL results."""
        with self._lock:
            new_watermark = self._watermark.advance_to(watermark_ms)
            if new_watermark is None:
                return []
            return [self._log.append(r) for r in self._windows.advance(new_watermark)]

    def poll(self, after_seq: int, limit: int) -> list[WindowResult]:
        """Resumable read of the result log (entries with seq > after_seq)."""
        with self._lock:
            return self._log.subscribe(after_seq, limit)

    @property
    def watermark_ms(self) -> int | None:
        return self._watermark.watermark_ms

    @property
    def open_window_count(self) -> int:
        return self._windows.open_window_count()

    @staticmethod
    def _validate(event: Event) -> str | None:
        if not event.event_id:
            return "event_id must be non-empty"
        if not event.key:
            return "key must be non-empty"
        if event.ts_ms < 0:
            return "ts_ms must be >= 0"
        if event.value != event.value or event.value in (float("inf"), float("-inf")):
            return "value must be finite"
        return None
