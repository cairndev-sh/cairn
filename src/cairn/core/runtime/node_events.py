"""Per-node run-event payloads, shared across runtimes (#172 / #415).

A ``node`` event marks one completed graph node in the run's append-only log so
the console/Pilot per-node trace can show progress + what each step produced.

Three runtimes record these, and they must produce IDENTICAL payloads:

  - **in-process** — the dispatcher's node sink (``_node_sink``) fires from the
    ``adrive`` streaming helper (``node_stream.py``) in the server process.
  - **Temporal** — the worker wraps activity-backed graph nodes dispatched by
    the LangGraph plugin, then appends events from the worker process (#415).
  - **DBOS** — the workflow drives the graph through ``adrive`` on its
    background loop and writes events through its node sink (#1865).

This module owns the shared, size-capped payload builder so all three paths stay
byte-identical. It depends on nothing but the stdlib, so the dispatcher and
either worker runtime can import it without dragging in each other.
"""
from __future__ import annotations

import json
from typing import Any


def bound_node_output(
    value: Any, *, max_str: int = 300, max_items: int = 20, max_chars: int = 2000
) -> Any:
    """A JSON-safe, SIZE-CAPPED summary of a node's output delta for the run trace
    (#172). A node's real output can be huge (a dataset profile, model logs), and it
    rides the event log + every SSE frame — so strings/lists/dicts are truncated and
    the whole thing is hard-capped. Returns None for an empty delta (no ``output``
    key emitted). PII/secret redaction is a separate gate (#247)."""
    if not value:  # None / {} / [] / "" → nothing worth recording
        return None

    def trim(v: Any, depth: int = 0) -> Any:
        if isinstance(v, str):
            return v if len(v) <= max_str else v[:max_str] + f"… (+{len(v) - max_str} chars)"
        if isinstance(v, bool) or isinstance(v, (int, float)) or v is None:
            return v
        if isinstance(v, dict):
            if depth >= 4:
                return "{…}"
            return {str(k): trim(val, depth + 1) for k, val in list(v.items())[:max_items]}
        if isinstance(v, (list, tuple)):
            if depth >= 4:
                return "[…]"
            items = [trim(x, depth + 1) for x in list(v)[:max_items]]
            if len(v) > max_items:
                items.append(f"… (+{len(v) - max_items} more)")
            return items
        return str(v)[:max_str]

    summary = trim(value)
    try:
        s = json.dumps(summary, default=str)
    except (TypeError, ValueError):
        return {"_repr": str(value)[:max_chars]}
    if len(s) > max_chars:
        return {"_truncated": s[:max_chars] + "…"}
    return summary


def node_event_data(
    node_name: str,
    output: Any = None,
    *,
    llm_cost_usd: float | None = None,
) -> dict[str, Any]:
    """Build the bounded ``node`` event payload and any measured LLM cost.

    ``output`` is the node's state-update delta; it's included (bounded) only when
    non-empty. A supplied cost is included even when zero so a later retry can
    replace an earlier attempt's cost."""
    data: dict[str, Any] = {"node": node_name, "state": "done"}
    summary = bound_node_output(output)
    if summary is not None:
        data["output"] = summary
    if llm_cost_usd is not None:
        data["llm_cost_usd"] = round(float(llm_cost_usd), 6)
    return data


def llm_cost_from_node_events(events: list[Any]) -> float:
    """Sum measured LLM spend, retaining only the last event for each node."""
    last: dict[str, float] = {}
    for event in events:
        if getattr(event, "type", None) != "node":
            continue
        data = getattr(event, "data", None)
        if isinstance(data, dict) and isinstance(data.get("llm_cost_usd"), (int, float)):
            last[data.get("node") or ""] = float(data["llm_cost_usd"])
    return round(sum(last.values()), 6)


__all__ = ["bound_node_output", "llm_cost_from_node_events", "node_event_data"]
