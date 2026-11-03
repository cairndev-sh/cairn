"""The mid-flight observability projection of a run (#572): the per-step trace and
the pending gate, sourced only from the run's own event log. Public core: ``GET
/runs/{id}`` returns it while a run has no terminal ``results`` rollup yet. It used
to live in ``cairn_platform.pilot`` (a product module); it is the record's own story,
so it stays in core when pilot leaves (#2421)."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional


def _duration_ms(start: str | None, end: str | None) -> Optional[float]:
    if not start or not end:
        return None
    try:
        return (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds() * 1000.0
    except (ValueError, TypeError):
        return None


def _trace_from_events(events: list[dict]) -> list[dict]:
    """Derive a per-step trace from the captured event log: each ``node`` event is
    a step; its duration is the gap to the next lifecycle event (real timestamps,
    not invented). Falls back to empty when no node events were recorded."""
    steps = []
    nodes = [e for e in events if e.get("type") == "node"]
    ordered = [e for e in events if e.get("type") in ("node", "done", "failed", "cancelled")]
    for e in nodes:
        nxt = next((o for o in ordered if (o.get("seq") or 0) > (e.get("seq") or 0)), None)
        ms = _duration_ms(e.get("ts"), nxt.get("ts")) if nxt else None
        data = e.get("data") or {}
        steps.append({
            "step": data.get("node", "step"),
            "status": data.get("state") if data.get("state") in ("failed", "interrupted") else "ok",
            "ms": round(ms) if ms is not None else None,
            # what the node produced this step (bounded at capture; #172). Absent
            # for older runs whose node events predate output capture.
            "output": data.get("output"),
        })
    return steps


def run_progress(events: list[dict]) -> dict[str, Any]:
    """#572 — mid-flight observability projection for a run whose terminal
    ``results`` rollup doesn't exist yet.

    Under ``CAIRN_RUNTIME=temporal`` a composed run pauses INSIDE the durable
    workflow, so the dispatcher's post-run block (which persists ``results``/
    ``result_payloads``) never runs while paused and ``GET /runs/{id}`` was
    observability-dark. The live event log IS recorded, though (per-node ``node``
    events from the worker sink; the ``awaiting_approval`` event from #456's
    ``surface_approval``), so project it: the same per-step trace + pending gate
    the finished pilot report shows, sourced only from what the run recorded —
    nothing invented. Returns ``{"steps": [...], "pending": <ApprovalRequest|None>}``.
    """
    pending = None
    for e in events:
        data = e.get("data") or {}
        if e.get("type") == "awaiting_approval" and isinstance(data.get("approval"), dict):
            pending = data["approval"]  # latest wins (multi-gate runs)
    return {"steps": _trace_from_events(events), "pending": pending}


__all__ = ["run_progress"]
