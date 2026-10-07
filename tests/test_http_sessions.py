"""HTTP session-based subscription: per-client progress, acks, backpressure,
expired resume points, and client independence — all over the HTTP API."""

import logging

from fastapi.testclient import TestClient

from eventagg import Aggregator, AggregatorConfig
from main import create_app

log = logging.getLogger("eventagg.test")


def _make_client(log_capacity=1000, retention=1000) -> TestClient:
    agg = Aggregator(AggregatorConfig(
        window_size=60.0, out_of_orderness=10.0, allowed_lateness=30.0,
        dedup_capacity=1000, log_capacity=log_capacity,
        retention_capacity=retention,
    ))
    return TestClient(create_app(agg))


def _emit(client: TestClient, n: int, start: int = 0) -> None:
    for i in range(start, start + n):
        resp = client.post("/events", json={
            "event_id": f"http-{i}", "key": "k",
            "event_time": 100.0 + i * 60.0, "value": 1.0,
        })
        body = resp.json()
        log.info("POST /events http-%d -> %s/%s | watermark=%s",
                 i, body["status"], body["reason"],
                 client.get("/watermark").json()["watermark"])
        assert resp.status_code == 200


def _create_session(client: TestClient, resume_from: int = 0) -> str:
    resp = client.post("/sessions", json={"resume_from": resume_from})
    assert resp.status_code == 201, resp.json()
    sid = resp.json()["session_id"]
    log.info("session created: %s (resume_from=%d)", sid, resume_from)
    return sid


def _poll(client: TestClient, sid: str, limit: int = 100):
    resp = client.get(f"/sessions/{sid}/results", params={"limit": limit})
    return resp


def _ack(client: TestClient, sid: str, upto_seq: int) -> int:
    resp = client.post(f"/sessions/{sid}/ack", json={"upto_seq": upto_seq})
    assert resp.status_code == 200, resp.json()
    acked = resp.json()["acked_seq"]
    log.info("session %s acked up to %d", sid, acked)
    return acked


def test_session_progress_and_only_acked_counts_as_consumed():
    client = _make_client()
    _emit(client, 3)
    sid = _create_session(client)

    batch = _poll(client, sid).json()
    seqs = [r["seq"] for r in batch]
    log.info("polled seqs %s without ack", seqs)
    assert seqs

    # Not acked => not consumed: a fresh session at the same resume point
    # sees the very same results again.
    sid2 = _create_session(client, resume_from=0)
    assert [r["seq"] for r in _poll(client, sid2).json()] == seqs

    # After ack, a resumed session continues exactly after the acked seq.
    _ack(client, sid, seqs[-1])
    _emit(client, 2, start=100)
    sid3 = _create_session(client, resume_from=seqs[-1])
    rest = [r["seq"] for r in _poll(client, sid3).json()]
    log.info("resumed after ack %d -> seqs %s", seqs[-1], rest)
    assert rest and min(rest) > seqs[-1]           # no duplication
    assert rest == list(range(seqs[-1] + 1, seqs[-1] + 1 + len(rest)))  # no loss


def test_slow_consumer_gets_explicit_backpressure_then_recovers():
    client = _make_client(log_capacity=4)
    _emit(client, 4)                               # >= 4 results emitted
    sid = _create_session(client)
    first = _poll(client, sid, limit=4).json()     # 4 unacked == capacity
    assert len(first) == 4

    resp = _poll(client, sid, limit=1)             # still nothing acked
    log.info("slow consumer refused: %d %s", resp.status_code, resp.json())
    assert resp.status_code == 409
    assert resp.json()["detail"]["reason"] == "backpressure"

    # It recovers by acking what it processed...
    _ack(client, sid, first[-1]["seq"])
    assert _poll(client, sid, limit=1).status_code == 200
    # ...or by reconnecting from its acked position.
    sid2 = _create_session(client, resume_from=first[-1]["seq"])
    assert _poll(client, sid2, limit=4).status_code == 200


def test_clients_are_independent():
    client = _make_client(log_capacity=2)
    _emit(client, 3)
    slow = _create_session(client)
    fast = _create_session(client)

    _poll(client, slow, limit=2)                   # slow one fills its buffer
    resp = _poll(client, slow, limit=1)
    assert resp.status_code == 409                 # only the slow one is refused
    log.info("slow session refused with 409; fast session unaffected")

    fast_batch = _poll(client, fast, limit=100)
    assert fast_batch.status_code == 200
    seqs = [r["seq"] for r in fast_batch.json()]
    _ack(client, fast, seqs[-1])
    log.info("fast session consumed seqs %s while slow one was stalled", seqs)
    assert seqs == sorted(seqs) and seqs


def test_expired_resume_point_returns_410_distinct_from_backpressure():
    client = _make_client(retention=4)
    _emit(client, 8)                               # evicts the oldest results
    stats = client.get("/stats").json()
    log.info("stats after eviction: %s", stats)
    assert stats["evicted_results"] > 0

    resp = client.post("/sessions", json={"resume_from": 0})
    log.info("stale resume refused: %d %s", resp.status_code, resp.json())
    assert resp.status_code == 410                 # Gone: permanently unavailable
    assert resp.json()["detail"]["reason"] == "resume_expired"
    assert client.get("/stats").json()["resume_expired"] == 1

    # A resume point inside retention still works: no loss, no duplication.
    frontier = stats["evicted_results"]
    sid = _create_session(client, resume_from=frontier)
    batch = _poll(client, sid).json()
    assert batch and batch[0]["seq"] == frontier + 1


def test_unknown_session_and_invalid_ack_are_distinguishable():
    client = _make_client()
    _emit(client, 2)
    resp = _poll(client, "no-such-session")
    assert resp.status_code == 404
    assert resp.json()["detail"]["reason"] == "unknown_session"

    sid = _create_session(client)
    batch = _poll(client, sid, limit=1).json()
    resp = client.post(f"/sessions/{sid}/ack",
                       json={"upto_seq": batch[-1]["seq"] + 1})
    log.info("ack beyond delivered refused: %d %s", resp.status_code, resp.json())
    assert resp.status_code == 400
    assert resp.json()["detail"]["reason"] == "invalid_ack"


def test_stats_endpoint_reports_gauges_and_counters():
    client = _make_client()
    _emit(client, 3)
    stats = client.get("/stats").json()
    log.info("GET /stats -> %s", stats)
    assert stats["accepted"] == 3
    assert stats["open_windows"] >= 1
    assert stats["retained_results"] >= 1
    assert stats["backpressure"] == 0
    assert stats["resume_expired"] == 0
