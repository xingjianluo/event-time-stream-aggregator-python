"""HTTP entrypoint: exposes the aggregator over FastAPI."""

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from eventagg import Aggregator, AggregatorConfig, Event, IngestStatus
from eventagg.models import WindowResult

app = FastAPI(title="event-time-stream-aggregator")
aggregator = Aggregator(AggregatorConfig())


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


def _to_out(r: WindowResult) -> WindowResultOut:
    return WindowResultOut(seq=r.seq, key=r.key, window_start=r.window_start,
                           window_end=r.window_end, count=r.count, total=r.total,
                           state=r.state.value, watermark=r.watermark, reason=r.reason)


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


@app.get("/results", response_model=list[WindowResultOut])
def get_results(after_seq: int = 0, limit: int = 100):
    """Incremental pull with resume: pass the last seen seq as `after_seq`."""
    sub = aggregator.subscribe(after_seq=after_seq)
    return [_to_out(r) for r in sub.poll(limit)]


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
