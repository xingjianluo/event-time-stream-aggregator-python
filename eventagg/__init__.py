"""Event-time stream aggregator: windowing, watermark, idempotency, subscription."""

from eventagg.models import (
    Event,
    IngestResult,
    IngestStatus,
    RejectReason,
    WindowResult,
    WindowState,
)
from eventagg.aggregator import Aggregator, AggregatorConfig
from eventagg.stats import Stats
from eventagg.subscription import (
    ResumeTooOld,
    Subscription,
    SubscriptionClosed,
    SubscriptionError,
)

__all__ = [
    "Event",
    "IngestResult",
    "IngestStatus",
    "RejectReason",
    "WindowResult",
    "WindowState",
    "Aggregator",
    "AggregatorConfig",
    "Stats",
    "Subscription",
    "SubscriptionError",
    "SubscriptionClosed",
    "ResumeTooOld",
]
