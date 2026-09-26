"""HTTP entrypoint: exposes the aggregator over FastAPI."""

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from eventagg import Aggregator, AggregatorConfig

app = FastAPI(title="event-time-stream-aggregator")
aggregator = Aggregator(AggregatorConfig())


class EventIn(BaseModel):
    event_id: str
    key: str
    event_time: float
    value: float


@app.post("/events")
def submit_event(body: EventIn):
    raise NotImplementedError


@app.get("/watermark")
def get_watermark():
    raise NotImplementedError


@app.get("/results")
def get_results(after_seq: int = 0, limit: int = 100):
    raise NotImplementedError


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
