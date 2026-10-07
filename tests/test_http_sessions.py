"""HTTP session-based subscriptions: stateful progress, backpressure,
client independence, resume within/beyond retention."""

import logging

import pytest
from fastapi.testclient import TestClient

import main
from eventagg import Aggregator, AggregatorConfig
from eventagg.sessions import SessionRegistry

log = logging.getLogger("eventagg.test")


@pytest.fixture
def client():
    # Rebuild app state with small, deterministic limits per test.
    main.aggregator = Aggregator(AggregatorConfig(
        window_size=60.0, out_of_orderness=10.0, allowed_lateness=30.0,
        dedup_capacity=1000, log_capacity=4, result_retention=8,
    ))
    main.sessions = SessionRegistry(main.aggregator)
    return TestClient(main.app)


def _gen(client, n: int, start: int = 0) -> None:
    for i in range(start, start + n):
        r = client.post("/events", json={
            "event_id": f"http-{i}", "key": "k",
            "event_time": 100.0 + i * 200.0, "value": 1.0,
        })
        body = r.json()
        log.info("POST /events http-%d -> %s/%s", i, body["status"], body["reason"])


def _open(client, after_seq: int = 0) -> str:
    r = client.post("/subscriptions", json={"after_seq": after_seq})
    assert r.status_code == 201, r.json()
    sid = r.json()["session_id"]
    log.info("opened session %s after_seq=%d", sid[:8], after_seq)
    return sid


def _poll(client, sid: str, limit: int = 4):
    r = client.post(f"/subscriptions/{sid}/poll", params={"limit": limit})
    if r.status_code == 200:
        seqs = [x["seq"] for x in r.json()["results"]]
        log.info("session %s poll -> seqs=%s acked=%d",
                 sid[:8], seqs, r.json()["acked_seq"])
    else:
        log.info("session %s poll refused: %d %s", sid[:8], r.status_code, r.json())
    return r


def test_session_tracks_progress_and_only_acked_counts(client):
    sid = _open(client)
    _gen(client, 3)  # 4 emissions
    r = _poll(client, sid, limit=4).json()
    assert [x["seq"] for x in r["results"]] == [1, 2, 3, 4]
    assert r["acked_seq"] == 0  # delivered but not yet consumed
    client.post(f"/subscriptions/{sid}/ack", json={"upto_seq": 4})
    r = _poll(client, sid).json()
    assert r["results"] == [] and r["acked_seq"] == 4
    # New emissions continue from the session cursor, not from scratch.
    _gen(client, 2, start=10)
    r = _poll(client, sid).json()
    assert [x["seq"] for x in r["results"]] == [5, 6, 7, 8]


def test_slow_consumer_gets_distinguishable_backpressure(client):
    sid = _open(client)
    _gen(client, 3)  # 4 emissions == log_capacity
    _poll(client, sid, limit=4)  # delivered, unacked
    r = _poll(client, sid)
    assert r.status_code == 409
    assert r.json()["reason"] == "backpressure"  # distinct from resume_too_old
    # Acking relieves the pressure; the same session continues.
    client.post(f"/subscriptions/{sid}/ack", json={"upto_seq": 4})
    assert _poll(client, sid).status_code == 200


def test_slow_client_does_not_affect_others(client):
    slow = _open(client)
    fast = _open(client)
    _gen(client, 3)
    _poll(client, slow, limit=4)          # slow: takes 4, never acks
    assert _poll(client, slow).json()["reason"] == "backpressure"
    # Fast client on its own session consumes everything unhindered.
    r = _poll(client, fast, limit=4).json()
    assert [x["seq"] for x in r["results"]] == [1, 2, 3, 4]
    client.post(f"/subscriptions/{fast}/ack", json={"upto_seq": 4})
    stats = client.get("/stats").json()
    log.info("stats with one slow one fast: %s", stats)
    assert stats["active_sessions"] == 2
    assert stats["backpressure"] == 1


def test_reconnect_with_acked_position_no_loss_no_duplication(client):
    sid = _open(client)
    _gen(client, 3)
    r = _poll(client, sid, limit=4).json()
    acked = r["results"][1]["seq"]  # ack only the first two
    client.post(f"/subscriptions/{sid}/ack", json={"upto_seq": acked})
    log.info("client disconnects at acked_seq=%d", acked)
    _gen(client, 2, start=10)  # more results while away
    # Reconnect as a NEW session from the last acked position.
    sid2 = _open(client, after_seq=acked)
    r = _poll(client, sid2, limit=10).json()
    seqs = [x["seq"] for x in r["results"]]
    assert seqs == list(range(acked + 1, 9))  # nothing lost, nothing repeated


def test_resume_point_too_old_over_http(client):
    _gen(client, 10)  # 18 emissions, retention 8 -> 10 evicted
    r = client.post("/subscriptions", json={"after_seq": 1})
    log.info("stale resume refused: %d %s", r.status_code, r.json())
    assert r.status_code == 409
    assert r.json()["reason"] == "resume_too_old"
    # Legacy stateless pull reports the same distinguishable refusal.
    r = client.get("/results", params={"after_seq": 1})
    assert r.status_code == 409 and r.json()["reason"] == "resume_too_old"
    # A resume point still within retention connects fine.
    evicted = client.get("/stats").json()["results_evicted"]
    assert client.post("/subscriptions",
                       json={"after_seq": evicted}).status_code == 201


def test_unknown_session_and_invalid_ack(client):
    r = client.post("/subscriptions/nope/poll")
    assert r.status_code == 404 and r.json()["reason"] == "unknown_session"
    sid = _open(client)
    _gen(client, 2)
    r = client.post(f"/subscriptions/{sid}/ack", json={"upto_seq": 99})
    assert r.status_code == 400 and r.json()["reason"] == "invalid_ack"


def test_stats_endpoint_reports_sessions_and_counters(client):
    sid = _open(client)
    _gen(client, 10)
    client.post("/subscriptions", json={"after_seq": 1})  # resume_too_old
    stats = client.get("/stats").json()
    log.info("GET /stats -> %s", stats)
    assert stats["active_sessions"] == 1
    assert stats["retained_results"] <= 8
    assert stats["results_evicted"] > 0
    assert stats["windows_reclaimed"] > 0
    assert stats["resume_too_old"] == 1
    client.delete(f"/subscriptions/{sid}")
    assert client.get("/stats").json()["active_sessions"] == 0
