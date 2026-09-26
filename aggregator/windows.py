"""Per-key fixed event-time window state and emission rules."""

from __future__ import annotations

from .types import Event, Trigger, Window, WindowResult


class WindowManager:
    """Maintains open windows per key and decides what to emit.

    Deterministic emission rules (watermark W = max_event_ts - grace):
    - ACCEPTED event with ts >= W        -> ON_EVENT partial for its window.
    - ACCEPTED late event (ts < W) whose window is still open (end > W)
      -> CORRECTION for its window.
    - When W reaches/passes a window end -> FINAL, window state purged.
    - Event whose window end <= W (already finalized) -> rejected as LATE
      by the caller (`is_expired`), never silently dropped or double counted.

    Emitted results carry seq=0; the ResultLog assigns the real seq.
    """

    def __init__(self, window_size_ms: int) -> None:
        if window_size_ms <= 0:
            raise ValueError("window_size_ms must be > 0")
        self._window_size_ms = window_size_ms
        # (key, window_start_ms) -> (count, total)
        self._state: dict[tuple[str, int], tuple[int, float]] = {}

    def window_for(self, ts_ms: int) -> Window:
        """Compute the fixed window [start, start+size) containing `ts_ms`."""
        start = (ts_ms // self._window_size_ms) * self._window_size_ms
        return Window(start_ms=start, end_ms=start + self._window_size_ms)

    def is_expired(self, window: Window, watermark_ms: int | None) -> bool:
        """True if the window was already finalized (end <= watermark)."""
        return watermark_ms is not None and window.end_ms <= watermark_ms

    def add(self, key: str, event: Event, watermark_ms: int) -> WindowResult:
        """Add an accepted event; returns the partial/correction to emit."""
        window = self.window_for(event.ts_ms)
        state_key = (key, window.start_ms)
        count, total = self._state.get(state_key, (0, 0.0))
        count, total = count + 1, total + event.value
        self._state[state_key] = (count, total)
        trigger = Trigger.CORRECTION if event.ts_ms < watermark_ms else Trigger.ON_EVENT
        return WindowResult(
            seq=0,
            key=key,
            window_start_ms=window.start_ms,
            window_end_ms=window.end_ms,
            count=count,
            total=total,
            trigger=trigger,
            watermark_ms=watermark_ms,
        )

    def advance(self, watermark_ms: int) -> list[WindowResult]:
        """Finalize and purge all windows with end <= watermark."""
        finals: list[WindowResult] = []
        expired = [k for k in self._state if k[1] + self._window_size_ms <= watermark_ms]
        for state_key in sorted(expired):
            key, start = state_key
            count, total = self._state.pop(state_key)
            finals.append(
                WindowResult(
                    seq=0,
                    key=key,
                    window_start_ms=start,
                    window_end_ms=start + self._window_size_ms,
                    count=count,
                    total=total,
                    trigger=Trigger.FINAL,
                    watermark_ms=watermark_ms,
                )
            )
        return finals

    def open_window_count(self) -> int:
        return len(self._state)
