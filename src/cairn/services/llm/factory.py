"""
Single construction point for every LLM client templates_v2 builds.

Two jobs the four scattered ``ChatOpenAI(...)`` call sites in ``compiler.py`` did
NOT do, now centralized here:

1. **Gateway routing** — route through the pluggable LLM gateway (``cairn.services.llm.gateway``)
   so an operator can point the whole engine at Bifrost / LiteLLM Proxy / Portkey /
   direct OpenAI via ``LLM_GATEWAY`` env, instead of always hitting OpenAI directly.
   In the default ``direct`` mode the params are returned unchanged, so behavior is
   byte-for-byte what it was before.

2. **Cost accounting** — attach a :class:`BudgetCallback` that tallies token spend
   per LLM call into the active ``obs`` cost scope (reusing the existing pricing +
   ``BudgetExceeded`` machinery) and enforces a per-investigation ``max_cost_usd``
   ceiling. This is what turns ``budget.max_cost_usd`` from a declared-but-dead field
   into an enforced one. Enforcement granularity is per-LLM-call: when the running
   tally crosses the cap, the *next* call raises ``BudgetExceeded`` and unwinds the
   agent loop (the wall-clock budget remains the hard mid-call abort).

The cost tally only happens inside an ``obs.cost_scope`` (opened by the runner);
outside one, ``record_llm`` is a no-op, so unit tests that build a model without a
scope don't accumulate or raise.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from langchain_core.callbacks import BaseCallbackHandler
from langchain_openai import ChatOpenAI

from cairn.core import obs
from cairn.services.llm.gateway import get_gateway
from cairn.services.llm.redacting import COMPILE_PLACEHOLDER_KEY, RedactingChatOpenAI

_log = logging.getLogger("cairn.llm_factory")


def _extract_usage(response: Any) -> tuple[int, int]:
    """Best-effort (prompt_tokens, completion_tokens) from a LangChain LLMResult.

    Handles both the legacy ``llm_output['token_usage']`` shape and the newer
    per-message ``usage_metadata``. Returns (0, 0) when usage is unavailable.
    """
    # 1. Aggregate token_usage on llm_output (OpenAI-style).
    llm_output = getattr(response, "llm_output", None)
    if isinstance(llm_output, dict):
        usage = llm_output.get("token_usage") or llm_output.get("usage") or {}
        if usage:
            return (
                int(usage.get("prompt_tokens", 0) or 0),
                int(usage.get("completion_tokens", 0) or 0),
            )
    # 2. Per-generation usage_metadata on the message.
    in_tok = out_tok = 0
    for gen_list in getattr(response, "generations", []) or []:
        for gen in gen_list:
            msg = getattr(gen, "message", None)
            um = getattr(msg, "usage_metadata", None)
            if isinstance(um, dict):
                in_tok += int(um.get("input_tokens", 0) or 0)
                out_tok += int(um.get("output_tokens", 0) or 0)
    return (in_tok, out_tok)


class BudgetCallback(BaseCallbackHandler):
    """Feeds LLM token usage into the obs cost scope and enforces a cost ceiling.

    ``model`` is the *bare* model name (pre gateway provider-prefix) so the pricing
    table in ``obs.cost`` can resolve it. ``max_cost_usd`` is the per-investigation
    cap from ``budget.max_cost_usd`` (``None`` = rely only on the global backstop).
    """

    def __init__(self, model: str, max_cost_usd: Optional[float] = None):
        self.model = model
        self.cap = max_cost_usd

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        in_tok, out_tok = _extract_usage(response)
        if not (in_tok or out_tok):
            return
        # Accumulate into the active obs ctx (no-op outside a cost_scope). This
        # also enforces the GLOBAL settings.max_cost_usd backstop via record_llm.
        obs.record_llm(self.model, in_tok, out_tok, 0.0)
        # Per-investigation ceiling (tighter than the global one).
        ctx = obs.get()
        if self.cap and ctx is not None and ctx.enforce_cap and ctx.cost_usd > self.cap:
            raise obs.BudgetExceeded(
                f"investigation over budget.max_cost_usd: "
                f"${ctx.cost_usd:.4f} > ${self.cap:.4f}"
            )


def build_chat_model(
    model: str,
    *,
    temperature: float = 0.0,
    max_tokens: Optional[int] = None,
    max_cost_usd: Optional[float] = None,
    job_tag: str = "",
    extra_callbacks: Optional[list[BaseCallbackHandler]] = None,
) -> ChatOpenAI:
    """Build a ``ChatOpenAI`` routed through the gateway with cost tracking.

    Pure construction — never invokes the model — so offline compile/validation
    is unaffected: ``ChatOpenAI`` validates its key *eagerly at construction*, so
    when no key is configured we inject a placeholder (see below). The real key is
    only needed at *invocation* (run-time).
    """
    params: dict[str, Any] = {"model": model, "temperature": temperature}
    if max_tokens is not None:
        params["max_tokens"] = max_tokens
    # #530: request token usage on STREAMING responses too. Agent nodes
    # (create_react_agent) stream, and OpenAI omits usage from a streamed
    # response unless stream_options.include_usage is set — without this the
    # BudgetCallback's _extract_usage sees (0,0) and Run.cost comes back 0.0 for
    # every agent-driven run (the incident-rca symptom). Harmless for .ainvoke().
    params["stream_usage"] = True

    # Route through the configured gateway (direct = identity).
    params = get_gateway().apply(params, job_tag=job_tag)
    # The gateway speaks the litellm/OpenAI `extra_headers` key; ChatOpenAI wants
    # `default_headers`.
    if "extra_headers" in params:
        params["default_headers"] = params.pop("extra_headers")

    callbacks: list[BaseCallbackHandler] = [BudgetCallback(model, max_cost_usd)]
    if extra_callbacks:
        callbacks.extend(extra_callbacks)
    params["callbacks"] = callbacks

    # Compile must not require live credentials (#412): the Temporal worker
    # compiles EVERY template at startup, and ChatOpenAI validates its key
    # eagerly at construction — so a worker booted before OPENAI_API_KEY is
    # present compiles 0 templates → empty graph registry → every run fails
    # "Graph not found". Inject a placeholder when no key is configured (and the
    # gateway didn't set one); a genuinely missing key still fails loudly at
    # *invocation* (run-time), which is the correct place to require it: the
    # model refuses to send the placeholder to a provider and says "no model
    # configured" instead (RedactingChatOpenAI).
    if not (
        params.get("api_key")
        or params.get("openai_api_key")
        or os.environ.get("OPENAI_API_KEY")
    ):
        params["api_key"] = COMPILE_PLACEHOLDER_KEY

    # #1376 — THE redaction chokepoint for LangChain-driven calls. Every
    # agentic LLM surface funnels through this constructor (template prompt
    # nodes, the chat router, ask's ReAct loop), and none of them call
    # `llm_complete`, where ADR-0028's redaction actually lived. Returning the
    # redacting subclass here is what makes the documented guarantee true on
    # the path the product actually runs. Identity behaviour when no
    # `redact.text` binding is active.
    return RedactingChatOpenAI(**params)
