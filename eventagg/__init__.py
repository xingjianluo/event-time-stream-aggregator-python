"""Event-time stream aggregator: windowing, watermark, idempotency, subscription."""

from eventagg.aggregator import Aggregator, AggregatorConfig
from eventagg.models import (
    Event,
    IngestResult,
    IngestStatus,
    RejectReason,
    WindowResult,
    WindowState,
)
from eventagg.subscription import (
    ResumePointExpired,
    Subscription,
    SubscriptionClosed,
)

__all__ = [
    "Aggregator",
    "AggregatorConfig",
    "Event",
    "IngestResult",
    "IngestStatus",
    "RejectReason",
    "ResumePointExpired",
    "Subscription",
    "SubscriptionClosed",
    "WindowResult",
    "WindowState",
]
