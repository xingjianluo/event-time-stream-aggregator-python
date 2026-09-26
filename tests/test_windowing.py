"""Out-of-order and late events: deterministic trigger/correct/final rules."""

from eventagg import IngestStatus, RejectReason
from eventagg.models import WindowState

from tests.conftest import drain_logged, ingest_logged


def test_out_of_order_events_same_window_accumulate(agg):
    sub = agg.subscribe()
    # Arrive out of order; both belong to window [60, 120).
    assert ingest_logged(agg, "e1", "k", 95.0, 2.0).ok
    assert ingest_logged(agg, "e2", "k", 65.0, 1.0).ok  # earlier event arrives later
    # Watermark is driven by max event time only: 95 - 10 = 85.
    assert agg.watermark == 85.0
    # Push watermark past window end 120 -> trigger with both events counted once.
    assert ingest_logged(agg, "e3", "k", 130.0, 5.0).ok
    results = drain_logged(agg, sub)
    trig = [r for r in results if r.state is WindowState.TRIGGERED
            and r.window_start == 60.0]
    assert len(trig) == 1
    assert (trig[0].count, trig[0].total) == (2, 3.0)


def test_late_event_within_allowed_lateness_corrects_window(agg):
    sub = agg.subscribe()
    ingest_logged(agg, "e1", "k", 65.0, 1.0)
    ingest_logged(agg, "e2", "k", 130.0, 5.0)   # wm=120 -> trigger [60,120)
    drain_logged(agg, sub)
    # Late but allowed: wm=120 < 120 + 30 -> accepted, window corrected.
    r = ingest_logged(agg, "e3", "k", 70.0, 10.0)
    assert r.status is IngestStatus.ACCEPTED
    results = drain_logged(agg, sub)
    corr = [x for x in results if x.state is WindowState.CORRECTED]
    assert len(corr) == 1
    assert (corr[0].count, corr[0].total) == (2, 11.0)


def test_event_after_finalization_is_ignored_too_late(agg):
    sub = agg.subscribe()
    ingest_logged(agg, "e1", "k", 65.0, 1.0)
    # wm = 160 - 10 = 150 >= 120 + 30 -> window [60,120) becomes FINAL.
    ingest_logged(agg, "e2", "k", 160.0, 1.0)
    results = drain_logged(agg, sub)
    assert any(r.state is WindowState.FINAL and r.window_start == 60.0
               for r in results)
    # Same window now rejects with a distinguishable reason.
    r = ingest_logged(agg, "e3", "k", 70.0, 99.0)
    assert r.status is IngestStatus.IGNORED
    assert r.reason is RejectReason.TOO_LATE
    # The final conclusion is unchanged by the too-late event.
    results = drain_logged(agg, sub)
    finals = [x for x in agg.subscribe(after_seq=0).poll(100)
              if x.state is WindowState.FINAL and x.window_start == 60.0]
    assert (finals[-1].count, finals[-1].total) == (1, 1.0)


def test_results_reproducible_within_lateness_contract(agg):
    """Arrival orders that respect the lateness contract (no event arrives
    after its window is FINAL) yield identical FINAL conclusions."""
    from eventagg import Aggregator, AggregatorConfig, Event
    events = [Event(f"e{i}", "k", t, v) for i, (t, v) in enumerate(
        [(65, 1.0), (95, 2.0), (70, 3.0), (130, 5.0), (160, 7.0)])]

    def run(order):
        a = Aggregator(AggregatorConfig(window_size=60, out_of_orderness=10,
                                        allowed_lateness=30))
        decisions = []
        for i in order:
            decisions.append(a.ingest(events[i]).status)
        finals = [(r.window_start, r.count, r.total)
                  for r in a.subscribe(0).poll(100)
                  if r.state is WindowState.FINAL]
        return decisions, finals

    # Both orders keep every event admissible (max-time events arrive last).
    fwd_decisions, forward = run(range(len(events)))
    shf_decisions, shuffled = run([2, 0, 1, 3, 4])
    assert all(s is IngestStatus.ACCEPTED for s in fwd_decisions + shf_decisions)
    assert forward == shuffled
