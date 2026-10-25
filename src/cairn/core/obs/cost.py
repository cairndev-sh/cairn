"""Per-request LLM cost tracking + session budget cap.

Price source (#1441), first answer wins:

1. the cost the call itself REPORTED (``record_llm(usd=...)``: the LiteLLM
   gateway's ``x-litellm-response-cost`` header, else litellm's own per-call
   ``response_cost`` -- see ``core/llm/client.py``);
2. the model-role registry (``core/llm/models.yaml`` + operator overrides);
3. ``litellm.model_cost`` -- the price map of litellm, a hard dependency (the
   exact id, then the id without its ``<provider>/`` prefix).

A model none of them knows is **unpriced**, never free: the call is recorded
with ``usd: 0.0`` AND ``priced: False``, so a reader can tell "cost nothing"
from "cost unknown" (the #1664 lesson; ``operator_compute``'s ``tokens.py``
refuses the same way). There is no hand-kept price table here any more.

Budget cap: when ``ctx.cost_usd`` exceeds the scope's ``max_cost_usd`` (or the
process setting when no scope cap is supplied), ``record_llm`` raises
``BudgetExceeded``. The graph node bubbles it up and the stream emits an SSE
``error`` frame. Set the cap to ``0`` to disable. A scope opened with
``enforce_cap=False`` (the dataflow runner and durable node meters, #1441)
records without a cap so its spend can be folded into an enforcing parent.
"""
from __future__ import annotations

import math
from typing import Optional

from cairn.core.provider_settings import get_settings
from cairn.core.llm.models import get_cost
from cairn.core.obs.base import RequestCtx, get


class BudgetExceeded(RuntimeError):
    """Raised by ``record_llm`` when the per-session cost cap is breached."""


def _cap_for(ctx: RequestCtx) -> float:
    """Return the scope-specific cap, falling back to the process setting."""
    return float(
        ctx.max_cost_usd
        if ctx.max_cost_usd is not None
        else (get_settings().max_cost_usd or 0.0)
    )


def merge_cost(parent: RequestCtx, child: RequestCtx) -> None:
    """Fold a non-enforcing child meter into its enforcing parent.

    DBOS and dataflow execute work in nested scopes so a node can be measured
    without turning a run cap into a node cap. The parent receives every call,
    then enforces its own cap once the child has been paid for.
    """
    if not child.llm_calls:
        return
    parent.llm_calls.extend(child.llm_calls)
    parent.cost_usd = round(parent.cost_usd + child.cost_usd, 6)
    cap = _cap_for(parent) if parent.enforce_cap else 0.0
    if cap and parent.cost_usd > cap:
        raise BudgetExceeded(
            f"session {parent.session_id} over budget: "
            f"${parent.cost_usd:.3f} > ${cap:.3f}"
        )


def _litellm_price(model: str) -> Optional[tuple[float, float]]:
    """$ per token (in, out) from litellm's price map; None when it has no entry."""
    try:
        import litellm  # noqa: PLC0415 - heavy; only paid when the registry misses
    except Exception:  # noqa: BLE001 - no map is "unknown", never "free"
        return None
    names = [model]
    if "/" in model:
        names.append(model.split("/", 1)[1])
    for name in names:
        entry = litellm.model_cost.get(name)
        if isinstance(entry, dict) and ("input_cost_per_token" in entry or "output_cost_per_token" in entry):
            return (float(entry.get("input_cost_per_token") or 0.0),
                    float(entry.get("output_cost_per_token") or 0.0))
    return None


def price_usd(model: str, in_tok: int, out_tok: int) -> Optional[float]:
    """What ``in_tok``/``out_tok`` tokens on ``model`` cost, or ``None`` when no
    price source knows the model (unpriced -- never silently $0)."""
    # Registry: $ per 1K tokens -> $ per token = divide by 1000.
    pi, po = get_cost(model)
    if pi or po:
        return in_tok * (pi / 1000) + out_tok * (po / 1000)
    per_token = _litellm_price(model)
    if per_token is None:
        return None
    return in_tok * per_token[0] + out_tok * per_token[1]


def _price(model: str, in_tok: int, out_tok: int) -> float:
    """``price_usd`` with an unpriced model read as 0.0 -- for callers that only
    compare or scale (``core/eval/cost_baseline.py``)."""
    return price_usd(model, in_tok, out_tok) or 0.0


def record_llm(
    model: str,
    in_tok: int,
    out_tok: int,
    ms: float,
    *,
    # ── #476 capture additions — all additive, all optional ──────────
    # A field stays None when the provider/call path doesn't supply it;
    # existing callers (positional 4-arg form) are unaffected.
    finish_reason: str | None = None,
    cached_tokens: int | None = None,  # best-effort per provider (prompt cache hit tokens)
    time_to_first_token_ms: float | None = None,  # best-effort per provider (needs streaming)
    schema_valid: bool | None = None,  # only set when response_format was requested
    # #1441 -- the cost the call itself reported (gateway header / litellm
    # response_cost). None = not reported -> priced from the registry / litellm map.
    usd: float | None = None,
) -> None:
    """Append an LLM-call record to the current ctx and enforce the budget.

    No-op outside a request scope — unit tests that call ``llm_complete``
    directly without ``obs.start`` won't crash.
    """
    ctx = get()
    if ctx is None:
        return
    if usd is None or not math.isfinite(usd) or usd < 0:
        usd = price_usd(model, in_tok, out_tok)
    priced = usd is not None
    usd = float(usd or 0.0)
    call: dict = {
        "model": model,
        "in": in_tok,
        "out": out_tok,
        "ms": round(ms, 1),
        "usd": round(usd, 6),
        # #1441 -- False: no price source knew the model; `usd` 0.0 means UNKNOWN.
        "priced": priced,
        # Additive #476 fields — always present (None when not observed) so
        # downstream consumers can rely on the key existing.
        "finish_reason": finish_reason,
        "cached_tokens": cached_tokens,
        "time_to_first_token_ms": time_to_first_token_ms,
        "schema_valid": schema_valid,
    }
    ctx.llm_calls.append(call)
    ctx.cost_usd = round(ctx.cost_usd + usd, 6)
    cap = _cap_for(ctx) if ctx.enforce_cap else 0.0
    if cap and ctx.cost_usd > cap:
        raise BudgetExceeded(
            f"session {ctx.session_id} over budget: "
            f"${ctx.cost_usd:.3f} > ${cap:.3f}"
        )
