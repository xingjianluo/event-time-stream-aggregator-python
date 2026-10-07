"""Server-side subscription sessions for stateful incremental consumption.

Each session wraps one Subscription (its own read/ack cursors), so
clients are fully independent: a slow consumer's backpressure never
affects other sessions. Sessions expire after `ttl_seconds` of
inactivity; an expired session is forgotten, and the client may simply
open a new one resuming from its last acked seq (still loss-free while
that position is within retention).
"""

from __future__ import annotations

import threading
import time
import uuid
from typing import Callable

from eventagg.aggregator import Aggregator
from eventagg.subscription import Subscription


class UnknownSession(KeyError):
    """The session id is unknown (never existed, closed, or expired)."""


class SessionRegistry:
    """Thread-safe registry of per-client subscription sessions."""

    def __init__(self, aggregator: Aggregator, ttl_seconds: float = 300.0,
                 clock: Callable[[], float] = time.monotonic) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._agg = aggregator
        self._ttl = ttl_seconds
        self._clock = clock
        self._sessions: dict[str, tuple[Subscription, float]] = {}
        self._lock = threading.Lock()

    def create(self, after_seq: int = 0) -> str:
        """Open a session resuming after `after_seq`; returns session id.

        Raises ResumeTooOld if the resume point was already evicted.
        """
        sub = self._agg.subscribe(after_seq=after_seq)
        session_id = uuid.uuid4().hex
        with self._lock:
            self._sweep_locked()
            self._sessions[session_id] = (sub, self._clock())
        return session_id

    def get(self, session_id: str) -> Subscription:
        """Return the session's subscription, refreshing its activity."""
        with self._lock:
            self._sweep_locked()
            entry = self._sessions.get(session_id)
            if entry is None:
                raise UnknownSession(session_id)
            self._sessions[session_id] = (entry[0], self._clock())
            return entry[0]

    def close(self, session_id: str) -> None:
        with self._lock:
            if self._sessions.pop(session_id, None) is None:
                raise UnknownSession(session_id)

    def _sweep_locked(self) -> None:
        now = self._clock()
        expired = [sid for sid, (_, last) in self._sessions.items()
                   if now - last > self._ttl]
        for sid in expired:
            del self._sessions[sid]

    def __len__(self) -> int:
        with self._lock:
            self._sweep_locked()
            return len(self._sessions)
