"""HTTP entrypoint: exposes the aggregator over FastAPI.

Pull model: session-based incremental subscription.
  POST   /sessions                  create a session (optionally resume_from)
  GET    /sessions/{sid}/results    poll the next batch for this session
  POST   /sessions/{sid}/ack        confirm consumption up to a seq
  DELETE /sessions/{sid}            drop the session
Only acked positions count as consumed; a session that falls more than
`log_capacity` results behind its acks is refused with 409/backpressure,
and a resume point older than retention is refused with 410/resume_expired.
"""

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from eventagg import (
    Aggregator,
    AggregatorConfig,
    Event,
    ResumePointExpired,
    SubscriptionClosed,
)
from eventagg.models import WindowResult
from eventagg.sessions import SessionRegistry, UnknownSession


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


class SessionCreateIn(BaseModel):
    resume_from: int = Field(default=0, ge=0)


class SessionOut(BaseModel):
    session_id: str
    resume_from: int


class AckIn(BaseModel):
    upto_seq: int = Field(ge=0)


class AckOut(BaseModel):
    acked_seq: int


def _to_out(r: WindowResult) -> WindowResultOut:
    return WindowResultOut(seq=r.seq, key=r.key, window_start=r.window_start,
                           window_end=r.window_end, count=r.count, total=r.total,
                           state=r.state.value, watermark=r.watermark, reason=r.reason)


def _resume_expired(exc: ResumePointExpired) -> HTTPException:
    # 410 Gone: the requested position is permanently unavailable.
    return HTTPException(status_code=410, detail={
        "error": str(exc),
        "reason": "resume_expired",
        "action": "do a full resync: create a new session with resume_from=0",
    })


def _backpressure(exc: SubscriptionClosed) -> HTTPException:
    # 409 Conflict: transient, retry after acking or reconnecting.
    return HTTPException(status_code=409, detail={
        "error": str(exc),
        "reason": "backpressure",
        "action": "ack delivered results, or reconnect and resume from acked_seq",
    })


def _unknown_session(exc: UnknownSession) -> HTTPException:
    return HTTPException(status_code=404, detail={
        "error": str(exc),
        "reason": "unknown_session",
        "action": "create a new session with resume_from set to your last acked seq",
    })


def create_app(aggregator: Aggregator | None = None,
               max_sessions: int = 1024) -> FastAPI:
    """Build the HTTP app around one aggregator (isolated per instance)."""
    agg = aggregator or Aggregator(AggregatorConfig())
    sessions = SessionRegistry(agg, max_sessions=max_sessions)
    app = FastAPI(title="event-time-stream-aggregator")

    @app.post("/events", response_model=IngestOut)
    def submit_event(body: EventIn):
        """Ingest one event; the response carries a distinguishable decision."""
        result = agg.ingest(Event(event_id=body.event_id, key=body.key,
                                  event_time=body.event_time, value=body.value))
        return IngestOut(status=result.status.value, reason=result.reason.value,
                         detail=result.detail)

    @app.get("/watermark")
    def get_watermark():
        return {"watermark": agg.watermark}

    @app.get("/stats")
    def get_stats():
        """Runtime snapshot: live windows, retained results, decision counters."""
        return agg.stats()

    @app.post("/sessions", response_model=SessionOut, status_code=201)
    def create_session(body: SessionCreateIn | None = None):
        """Open a pull session, optionally resuming after a last acked seq."""
        resume_from = body.resume_from if body else 0
        try:
            session_id = sessions.create(resume_from=resume_from)
        except ResumePointExpired as exc:
            raise _resume_expired(exc) from exc
        return SessionOut(session_id=session_id, resume_from=resume_from)

    @app.get("/sessions/{session_id}/results", response_model=list[WindowResultOut])
    def poll_session(session_id: str, limit: int = 100):
        """Fetch the next batch for this session (progress is per-session)."""
        try:
            batch = sessions.poll(session_id, limit)
        except UnknownSession as exc:
            raise _unknown_session(exc) from exc
        except SubscriptionClosed as exc:
            raise _backpressure(exc) from exc
        except ResumePointExpired as exc:
            raise _resume_expired(exc) from exc
        return [_to_out(r) for r in batch]

    @app.post("/sessions/{session_id}/ack", response_model=AckOut)
    def ack_session(session_id: str, body: AckIn):
        """Confirm consumption up to `upto_seq`; only acked positions count."""
        try:
            acked = sessions.ack(session_id, body.upto_seq)
        except UnknownSession as exc:
            raise _unknown_session(exc) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={
                "error": str(exc), "reason": "invalid_ack",
            }) from exc
        return AckOut(acked_seq=acked)

    @app.delete("/sessions/{session_id}", status_code=204)
    def close_session(session_id: str):
        sessions.close(session_id)

    @app.get("/results", response_model=list[WindowResultOut])
    def get_results(after_seq: int = 0, limit: int = 100):
        """Stateless one-shot pull (kept for compatibility).

        Prefer session-based pulls for backpressure and tracked progress.
        """
        try:
            sub = agg.subscribe(after_seq=after_seq)
            return [_to_out(r) for r in sub.poll(limit)]
        except ResumePointExpired as exc:
            raise _resume_expired(exc) from exc

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
