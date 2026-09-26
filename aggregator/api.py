"""FastAPI surface: ingestion, watermark control, resumable result reads."""

from __future__ import annotations

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .engine import Engine, EngineConfig
from .result_log import GapError
from .types import Event, Submission, WindowResult


class EventIn(BaseModel):
    event_id: str
    key: str
    ts_ms: int = Field(ge=0)
    value: float


class WatermarkIn(BaseModel):
    watermark_ms: int = Field(ge=0)


def _result_to_dict(r: WindowResult) -> dict:
    return {
        "seq": r.seq,
        "key": r.key,
        "window_start_ms": r.window_start_ms,
        "window_end_ms": r.window_end_ms,
        "count": r.count,
        "total": r.total,
        "trigger": r.trigger.value,
        "watermark_ms": r.watermark_ms,
    }


def _submission_to_dict(s: Submission) -> dict:
    return {
        "event_id": s.event_id,
        "decision": s.decision.value,
        "reason": s.reason,
        "emitted": [_result_to_dict(r) for r in s.emitted],
    }


def create_app(engine: Engine | None = None) -> FastAPI:
    """Build the HTTP app around an engine instance."""
    app = FastAPI(title="event-time-stream-aggregator")
    app.state.engine = engine or Engine(EngineConfig())

    @app.exception_handler(GapError)
    def _gap_handler(_request: Request, exc: GapError) -> JSONResponse:
        # 409: the resume point is gone; client must resubscribe from
        # oldest_seq. Explicit signal instead of silent data loss.
        return JSONResponse(
            status_code=409,
            content={
                "error": "resume_gap",
                "detail": str(exc),
                "oldest_seq": exc.oldest_seq,
            },
        )

    def _engine() -> Engine:
        return app.state.engine

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/watermark")
    def get_watermark() -> dict:
        return {"watermark_ms": _engine().watermark_ms}

    @app.post("/watermark", status_code=200)
    def advance_watermark(body: WatermarkIn) -> dict:
        finals = _engine().advance_watermark(body.watermark_ms)
        return {
            "watermark_ms": _engine().watermark_ms,
            "finals": [_result_to_dict(r) for r in finals],
        }

    @app.post("/events", status_code=200)
    def submit_event(body: EventIn) -> dict:
        event = Event(
            event_id=body.event_id,
            key=body.key,
            ts_ms=body.ts_ms,
            value=body.value,
        )
        return _submission_to_dict(_engine().submit(event))

    @app.get("/results")
    def get_results(
        after_seq: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        entries = _engine().poll(after_seq, limit)
        return {
            "results": [_result_to_dict(r) for r in entries],
            "latest_seq": entries[-1].seq if entries else after_seq,
        }

    return app
