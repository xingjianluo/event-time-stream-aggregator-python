"""End-to-end HTTP behaviour through the FastAPI surface."""

from __future__ import annotations

import logging

import pytest
from starlette.testclient import TestClient

from aggregator.api import create_app
from aggregator.engine import Engine, EngineConfig

logger = logging.getLogger("aggregator.tests")


@pytest.fixture()
def client() -> TestClient:
    engine = Engine(EngineConfig(window_size_ms=60_000, grace_ms=5_000,
                                 dedup_capacity=100, log_capacity=8))
    return TestClient(create_app(engine))


def _post_event(client: TestClient, **kw) -> dict:
    resp = client.post("/events", json=kw)
    assert resp.status_code == 200
    body = resp.json()
    logger.info("POST /events %s -> decision=%s reason=%s emitted=%s",
                kw, body["decision"], body["reason"],
                [(e["seq"], e["trigger"], e["count"]) for e in body["emitted"]])
    return body


def test_health_and_watermark(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/watermark").json() == {"watermark_ms": None}


def test_http_full_flow(client: TestClient) -> None:
    accepted = _post_event(client, event_id="e1", key="a", ts_ms=10_000, value=1.0)
    assert accepted["decision"] == "accepted"
    assert accepted["emitted"][0]["trigger"] == "on_event"

    duplicate = _post_event(client, event_id="e1", key="a", ts_ms=10_000, value=1.0)
    assert duplicate["decision"] == "duplicate"
    assert duplicate["emitted"] == []

    invalid = client.post("/events", json={"event_id": "x", "key": "a",
                                           "ts_ms": -1, "value": 1.0})
    assert invalid.status_code == 422  # schema-level validation

    # Explicit watermark control finalizes window [0, 60s).
    final = client.post("/watermark", json={"watermark_ms": 60_000}).json()
    logger.info("POST /watermark -> %s", final)
    assert final["watermark_ms"] == 60_000
    assert [f["trigger"] for f in final["finals"]] == ["final"]

    # A late event after finalization is rejected with a distinct reason.
    late = _post_event(client, event_id="e2", key="a", ts_ms=20_000, value=1.0)
    assert late["decision"] == "late"


def test_http_resumable_results_and_gap_409(client: TestClient) -> None:
    # First six events fit inside the 8-entry log: exercise checkpoint paging.
    for i in range(6):
        _post_event(client, event_id=f"e{i}", key="a",
                    ts_ms=1_000 + i, value=1.0)
    page1 = client.get("/results", params={"after_seq": 0, "limit": 4}).json()
    assert [r["seq"] for r in page1["results"]] == [1, 2, 3, 4]
    page2 = client.get("/results", params={"after_seq": 4, "limit": 4}).json()
    assert [r["seq"] for r in page2["results"]] == [5, 6]
    logger.info("resume pages: 1-4 then 5-6, no loss/dup")

    # Flood past log capacity (8), evicting the early checkpoints.
    for i in range(6, 20):
        _post_event(client, event_id=f"e{i}", key="a",
                    ts_ms=1_000 + (i % 50), value=1.0)
    # Resuming from an evicted checkpoint yields an explicit 409.
    resp = client.get("/results", params={"after_seq": 5})
    assert resp.status_code == 409
    body = resp.json()
    logger.info("GET /results after_seq=5 -> 409 %s", body)
    assert body["error"] == "resume_gap"
    assert body["oldest_seq"] == 13
