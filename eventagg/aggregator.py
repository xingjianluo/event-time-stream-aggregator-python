"""Core engine: ingest -> validate -> dedup -> watermark -> window -> emit."""

from __future__ import annotations

import threading
from dataclasses import dataclass

from eventagg.dedup import DedupStore
from eventagg.models import Event, IngestResult, WindowResult
from eventagg.subscription import ResultLog
from eventagg.watermark import WatermarkTracker


@dataclass(frozen=True)
class AggregatorConfig:
    """Tunable semantics of the aggregator."""

    window_size: float = 60.0          # seconds, tumbling window size
    out_of_orderness: float = 10.0     # watermark lag behind max event time
    allowed_lateness: float = 30.0     # extra time a triggered window accepts corrections
    dedup_capacity: int = 100_000      # max remembered event ids
    log_capacity: int = 10_000         # max buffered results per subscription


class Aggregator:
    """Thread-safe event-time window aggregator."""

    def __init__(self, config: AggregatorConfig | None = None) -> None:
        self.config = config or AggregatorConfig()

    def ingest(self, event: Event) -> IngestResult:
        """Validate, dedup, and fold one event into its window."""
        raise NotImplementedError

    @property
    def watermark(self) -> float:
        raise NotImplementedError

    def subscribe(self, after_seq: int = 0):
        """Open a subscription over emitted results, resuming after `after_seq`."""
        raise NotImplementedError
