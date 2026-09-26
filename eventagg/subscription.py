"""Result log and subscriptions with resume and bounded buffering."""

from __future__ import annotations

import threading

from eventagg.models import WindowResult


class SubscriptionClosed(Exception):
    """Raised when a subscription was closed due to backpressure overflow."""


class ResultLog:
    """Append-only, thread-safe log of emitted window results."""

    def __init__(self) -> None:
        pass

    def append(self, result: WindowResult) -> None:
        raise NotImplementedError

    def read_from(self, after_seq: int, limit: int = 100) -> list[WindowResult]:
        raise NotImplementedError


class Subscription:
    """A consumer view over the ResultLog with resume and backpressure.

    - Resume: `after_seq` replays everything emitted after that sequence.
    - Backpressure: a subscriber must call `poll` / `ack`; if unacked
      buffered results exceed `capacity`, `poll` raises SubscriptionClosed
      instead of growing memory without bound.
    """

    def __init__(self, log: ResultLog, after_seq: int = 0, capacity: int = 10_000) -> None:
        self._log = log

    def poll(self, limit: int = 100) -> list[WindowResult]:
        """Fetch up to `limit` results after the last acked sequence."""
        raise NotImplementedError

    def ack(self, upto_seq: int) -> None:
        """Mark results up to `upto_seq` as consumed; enables resume."""
        raise NotImplementedError

    @property
    def acked_seq(self) -> int:
        raise NotImplementedError
