"""Server-side pull sessions: stateful incremental subscriptions over HTTP.

Each session wraps one Subscription (its own read/ack cursors), so clients
are fully independent: a slow consumer overflows only its own session.
Sessions are lightweight cursors (no per-session buffering); the registry
bounds their count with LRU eviction. A client whose session was evicted
simply creates a new one with `resume_from` set to its last acked seq.
"""

from __future__ import annotations

import threading
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field

from eventagg.aggregator import Aggregator
from eventagg.models import WindowResult
from eventagg.subscription import Subscription


class UnknownSession(Exception):
    """Raised when polling/acking a session id that does not (or no longer)
    exists. The client should create a new session with its last acked seq."""


@dataclass
class _Session:
    sub: Subscription
    lock: threading.Lock = field(default_factory=threading.Lock)


class SessionRegistry:
    """Thread-safe registry of named pull sessions over one Aggregator."""

    def __init__(self, aggregator: Aggregator, max_sessions: int = 1024) -> None:
        if max_sessions <= 0:
            raise ValueError("max_sessions must be > 0")
        self._agg = aggregator
        self._max_sessions = max_sessions
        self._sessions: OrderedDict[str, _Session] = OrderedDict()
        self._lock = threading.Lock()

    def create(self, resume_from: int = 0) -> str:
        """Open a session resuming after `resume_from` (last acked seq).

        Raises ResumePointExpired if that position is no longer retained:
        the client must do a full resync (resume_from=0 within retention).
        """
        sub = self._agg.subscribe(after_seq=resume_from)
        session_id = uuid.uuid4().hex
        with self._lock:
            self._sessions[session_id] = _Session(sub)
            self._sessions.move_to_end(session_id)
            while len(self._sessions) > self._max_sessions:
                self._sessions.popitem(last=False)  # evict least recently used
        return session_id

    def poll(self, session_id: str, limit: int = 100) -> list[WindowResult]:
        """Deliver the next batch for this session (and only this session).

        Raises UnknownSession, SubscriptionClosed (backpressure) or
        ResumePointExpired (retention overtook the cursor while idle).
        """
        session = self._get(session_id)
        with session.lock:
            return session.sub.poll(limit)

    def ack(self, session_id: str, upto_seq: int) -> int:
        """Confirm consumption up to `upto_seq`; returns the acked position."""
        session = self._get(session_id)
        with session.lock:
            session.sub.ack(upto_seq)
            return session.sub.acked_seq

    def close(self, session_id: str) -> None:
        with self._lock:
            self._sessions.pop(session_id, None)

    def _get(self, session_id: str) -> _Session:
        with self._lock:
            session = self._sessions.get(session_id)
            if session is None:
                raise UnknownSession(
                    f"session {session_id!r} not found or evicted; create a "
                    f"new session with resume_from set to your last acked seq"
                )
            self._sessions.move_to_end(session_id)
            return session

    def __len__(self) -> int:
        with self._lock:
            return len(self._sessions)
