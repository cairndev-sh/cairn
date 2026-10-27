"""Per-request observability context.

A ContextVar holds the in-flight ``RequestCtx``. Helpers in this module
mutate it; surrounding application code stays oblivious.

Lifecycle (one cycle per ``POST /ask``):

    start(request_id, session_id, query)   # at endpoint entry
    ... graph runs, helpers record signals ...
    finish(answer)                         # at endpoint exit; flushes event

Why a ContextVar rather than threading ``ctx`` through every call site:
LangGraph runs nodes as asyncio tasks, and ``asyncio.create_task`` snapshots
the current Context, so child tasks see the same ctx without us passing it.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from time import time
from typing import Iterator

_cur: ContextVar["RequestCtx | None"] = ContextVar("obs_ctx", default=None)
_cost_parents: ContextVar[tuple["RequestCtx", ...]] = ContextVar("obs_cost_parents", default=())


@dataclass
class RequestCtx:
    """All telemetry for one request. Serialized verbatim into events.jsonl."""

    request_id: str
    session_id: str
    query: str
    t0: float = field(default_factory=time)
    route: str | None = None
    answer: str | None = None
    total_ms: float | None = None
    llm_calls: list[dict] = field(default_factory=list)
    mcp_calls: list[dict] = field(default_factory=list)
    cost_usd: float = 0.0
    # Set when an agent-node hit its latency budget. Dashboards filter on
    # this to separate "slow answer" from "incomplete answer".
    degraded: bool = False
    # Node names that timed out; usually 0 or 1 entries per request.
    timeouts: list[str] = field(default_factory=list)
    # #1441 -- False in a scope that MEASURES without capping (the dataflow
    # runner's per-stage scope: a leg's spend is governed by its reservation and
    # spec.budget, not by the chat session backstop ``settings.max_cost_usd``).
    enforce_cap: bool = True
    # A template run can provide a tighter cap than the process-wide setting.
    # Durable runtimes inherit this field when their task context is copied.
    max_cost_usd: float | None = None
    # ── #476 capture additions (node level) ───────────────────────────
    # Per-node retry counts are NOT surfaced here: retries are owned by
    # ``core/runtime/retry.py`` / the Temporal activity layer, both outside
    # ``core/obs/`` (out of scope for this ticket — see ADR-0059 #476). The
    # request-level ``degraded`` flag above already covers "something in this
    # request was unhappy" (set by ``record_timeout``); a per-node retry count
    # would need the runtime layer to call into obs on each retry attempt,
    # which is a follow-up, not a field guess added here.


def start(request_id: str, session_id: str, query: str) -> RequestCtx:
    """Open a new request scope. Returns the ctx (callers rarely need it)."""
    ctx = RequestCtx(request_id=request_id, session_id=session_id, query=query)
    _cur.set(ctx)
    return ctx


def get() -> RequestCtx | None:
    """Current request ctx, or ``None`` if outside a request scope."""
    return _cur.get()


def cost_parents() -> tuple[RequestCtx, ...]:
    """Scopes that receive calls from the current cost meter."""
    return _cost_parents.get()


def record_mcp(
    tool: str,
    ms: float,
    n_bytes: int,
    *,
    # ── #476 capture additions — all additive, all optional ──────────
    success: bool | None = None,
    error_type: str | None = None,
    tool_result_used: bool | None = None,
) -> None:
    """Append an MCP-call record. No-op outside a request scope.

    ``success``/``error_type`` are best-effort per caller (#476) — pass them
    when the call site already knows the outcome (e.g. the ``mcp/call``
    operator distinguishes ``McpNotConfigured`` / unknown-tool / transport
    error today via ``audit_emit``, but does not yet route through
    ``record_mcp``; wiring that is a separate follow-up, not this ticket).

    ``tool_result_used`` is normally left ``None`` here and filled in later by
    :func:`compute_tool_result_used`, since whether the result was used can
    only be known once the *next* LLM call has happened.
    """
    ctx = _cur.get()
    if ctx is not None:
        ctx.mcp_calls.append(
            {
                "tool": tool,
                "ms": ms,
                "bytes": n_bytes,
                "success": success,
                "error_type": error_type,
                "tool_result_used": tool_result_used,
            }
        )


def compute_tool_result_used(
    tool_result_text: str | None,
    next_llm_prompt_text: str | None,
) -> bool | None:
    """Cheap heuristic (#476): did the LLM call immediately following a tool
    call reference that tool's output?

    Conservative by design — a **substring match** of the (stringified) tool
    result inside the next LLM call's serialized prompt. This is intentionally
    crude: it will under-count (a paraphrased/summarized result won't match)
    but it will not confidently claim "used" when it wasn't literally present.

    Returns ``None`` (not "unknown == False") when either side is missing —
    e.g. there was no following LLM call at all, or the tool produced no
    capturable result text — so callers can distinguish "we don't know" from
    "we checked and it wasn't used".

    Call-site wiring note: today's ``mcp_calls`` entries only store a byte
    count (``n_bytes``), not the actual result text, and ``record_mcp`` has no
    live caller (see its docstring) — so this heuristic is exercised directly
    in tests against explicit text, not yet wired end-to-end from a real tool
    call + real LLM call. Wiring it live requires the ``mcp/call`` operator
    (or whichever future call site) to capture a short result snippet and pass
    both snippets in here after the next ``llm_complete`` — left as a
    follow-up rather than guessed at.
    """
    if not tool_result_text or not next_llm_prompt_text:
        return None
    return tool_result_text in next_llm_prompt_text


def set_route(route: str) -> None:
    """Set the route taken by the router. No-op outside a request scope or
    when ``route`` is empty (router hasn't decided yet)."""
    ctx = _cur.get()
    if ctx is not None and route:
        ctx.route = route


def record_timeout(node: str) -> None:
    """Flag the current request as degraded and append the offending node.
    No-op outside a request scope."""
    ctx = _cur.get()
    if ctx is not None:
        ctx.degraded = True
        ctx.timeouts.append(node)


def finish(answer: str) -> None:
    """Close the request scope, write the event row, save the replay snapshot.

    Local imports of ``events``/``replay`` avoid an import cycle (both
    modules import ``RequestCtx`` from this one). The ctx is cleared even
    if a sink fails, so a corrupt sink can't keep stale state alive.

    Each sink wraps its own work in try/except — one failing doesn't
    prevent the other from running.
    """
    from cairn.core.obs import events, replay

    ctx = _cur.get()
    if ctx is None:
        return
    ctx.answer = answer
    ctx.total_ms = round((time() - ctx.t0) * 1000, 1)
    try:
        events.write(ctx)
        replay.save(ctx)
    finally:
        _cur.set(None)


@contextmanager
def cost_scope(
    label: str = "investigation",
    *,
    enforce_cap: bool = True,
    max_cost_usd: float | None = None,
    propagate_cost: bool = False,
) -> Iterator["RequestCtx"]:
    """Open a lightweight obs scope for cost accumulation only.

    Unlike ``start``/``finish`` (the full ``/ask`` request lifecycle), this does
    NOT flush an event row or replay snapshot — it just establishes a ``RequestCtx``
    so ``record_llm`` can tally per-call spend (and enforce the global
    ``max_cost_usd`` backstop) for the duration of the block, then restores the
    previous scope. Used by ``templates_v2`` to measure per-investigation LLM cost
    without inheriting the chat-path side effects.

    Nests safely: the prior ctx (if any) is reset on exit via the ContextVar token.

    ``enforce_cap=False`` (#1441) records every call but never raises a cap. A
    ``max_cost_usd`` can tighten the process-wide backstop. When requested,
    calls also accrue to the enclosing scope and enforce its cap immediately.
    """
    ctx = RequestCtx(
        request_id=label,
        session_id=label,
        query="",
        enforce_cap=enforce_cap,
        max_cost_usd=max_cost_usd,
    )
    parent = _cur.get()
    parents = (parent, *_cost_parents.get()) if propagate_cost and parent is not None else ()
    parent_token = _cost_parents.set(parents)
    token = _cur.set(ctx)
    try:
        yield ctx
    finally:
        _cur.reset(token)
        _cost_parents.reset(parent_token)
