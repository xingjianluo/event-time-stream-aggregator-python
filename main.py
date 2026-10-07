"""HTTP entrypoint: exposes the aggregator over FastAPI.

Endpoints:
- POST /events                      ingest one event (per-event decision)
- GET  /watermark                   current watermark
- GET  /results?after_seq=&limit=   stateless incremental pull (legacy)
- POST /subscriptions               open a stateful session (resume point)
- POST /subscriptions/{sid}/poll    pull a batch for that session
- POST /subscriptions/{sid}/ack     confirm consumption up to a seq
- DELETE /subscriptions/{sid}       close the session
- GET  /stats                       runtime snapshot (memory + counters)

Refusals are machine-distinguishable: HTTP 409 with reason
"backpressure" (consumer too slow) vs "resume_too_old" (resume point
evicted from bounded retention; full re-sync required).
"""

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from eventagg import (
    Aggregator,
    AggregatorConfig,
    Event,
    ResumeTooOld,
    SubscriptionClosed,
)
from eventagg.models import WindowResult
from eventagg.sessions import SessionRegistry, UnknownSession

app = FastAPI(title="event-time-stream-aggregator")
aggregator = Aggregator(AggregatorConfig())
sessions = SessionRegistry(aggregator)


class EventIn(BaseModel):
    event_id: str
    key: str
    event_time: float
    value: float


class IngestOut(BaseModel):
    status: str
    reason: str
    detail: str


class WindowResultOut(BaseModel):
    seq: int
    key: str
    window_start: float
    window_end: float
    count: int
    total: float
    state: str
    watermark: float
    reason: str


class SubscriptionCreateIn(BaseModel):
    after_seq: int = 0


class AckIn(BaseModel):
    upto_seq: int


def _to_out(r: WindowResult) -> WindowResultOut:
    return WindowResultOut(seq=r.seq, key=r.key, window_start=r.window_start,
                           window_end=r.window_end, count=r.count, total=r.total,
                           state=r.state.value, watermark=r.watermark, reason=r.reason)


def _refusal(status_code: int, reason: str, detail: str) -> JSONResponse:
    return JSONResponse(status_code=status_code,
                        content={"reason": reason, "detail": detail})


@app.post("/events", response_model=IngestOut)
def submit_event(body: EventIn):
    """Ingest one event; the response carries a distinguishable decision."""
    result = aggregator.ingest(Event(event_id=body.event_id, key=body.key,
                                     event_time=body.event_time, value=body.value))
    return IngestOut(status=result.status.value, reason=result.reason.value,
                     detail=result.detail)


@app.get("/watermark")
def get_watermark():
    return {"watermark": aggregator.watermark}


@app.get("/stats")
def get_stats():
    """Runtime snapshot: live window/result counts and refusal counters."""
    snap = aggregator.stats()
    snap["active_sessions"] = len(sessions)
    return snap


@app.get("/results", response_model=list[WindowResultOut])
def get_results(after_seq: int = 0, limit: int = 100):
    """Stateless incremental pull (legacy): pass the last seen seq."""
    try:
        sub = aggregator.subscribe(after_seq=after_seq)
        return [_to_out(r) for r in sub.poll(limit)]
    except ResumeTooOld as exc:
        return _refusal(409, "resume_too_old", str(exc))


@app.post("/subscriptions", status_code=201)
def create_subscription(body: SubscriptionCreateIn):
    """Open a stateful session resuming after `after_seq` (default: from now)."""
    try:
        session_id = sessions.create(after_seq=body.after_seq)
    except ResumeTooOld as exc:
        return _refusal(409, "resume_too_old", str(exc))
    return {"session_id": session_id}


@app.post("/subscriptions/{session_id}/poll")
def poll_subscription(session_id: str, limit: int = 100):
    """Pull a batch for this session; acked positions stay consumed."""
    try:
        sub = sessions.get(session_id)
    except UnknownSession:
        return _refusal(404, "unknown_session",
                        f"session {session_id!r} not found or expired")
    try:
        batch = sub.poll(limit)
    except SubscriptionClosed as exc:
        return _refusal(409, "backpressure", str(exc))
    except ResumeTooOld as exc:
        return _refusal(409, "resume_too_old", str(exc))
    return {"results": [_to_out(r) for r in batch], "acked_seq": sub.acked_seq}


@app.post("/subscriptions/{session_id}/ack")
def ack_subscription(session_id: str, body: AckIn):
    """Confirm consumption up to `upto_seq`; only acked positions count."""
    try:
        sub = sessions.get(session_id)
    except UnknownSession:
        return _refusal(404, "unknown_session",
                        f"session {session_id!r} not found or expired")
    try:
        sub.ack(body.upto_seq)
    except ValueError as exc:
        return _refusal(400, "invalid_ack", str(exc))
    return {"acked_seq": sub.acked_seq}


@app.delete("/subscriptions/{session_id}")
def close_subscription(session_id: str):
    try:
        sessions.close(session_id)
    except UnknownSession:
        return _refusal(404, "unknown_session",
                        f"session {session_id!r} not found or expired")
    return {"closed": session_id}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
