"""Tests for the templates_v2 LLM factory: gateway routing + cost accounting.

These pin the two behaviors that turn `budget.max_cost_usd` from a dead field
into an enforced one, and that let an operator point the engine at a proxy.
"""
from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-validate-offline")

import pytest

from cairn.core import obs
from cairn.services.llm.gateway import DirectGateway, ProxyGateway
from cairn.services.llm import factory as llm_factory
from cairn.services.llm.factory import BudgetCallback, build_chat_model, _extract_usage


# ─── usage extraction (both LangChain shapes) ───────────────────────


def test_extract_usage_from_llm_output():
    resp = SimpleNamespace(
        llm_output={"token_usage": {"prompt_tokens": 120, "completion_tokens": 30}},
        generations=[],
    )
    assert _extract_usage(resp) == (120, 30)


def test_extract_usage_from_usage_metadata():
    msg = SimpleNamespace(usage_metadata={"input_tokens": 10, "output_tokens": 5})
    gen = SimpleNamespace(message=msg)
    resp = SimpleNamespace(llm_output=None, generations=[[gen]])
    assert _extract_usage(resp) == (10, 5)


def test_extract_usage_absent_is_zero():
    resp = SimpleNamespace(llm_output=None, generations=[])
    assert _extract_usage(resp) == (0, 0)


# ─── cost accumulation + ceiling enforcement ────────────────────────


def _big_usage_response():
    # 1M in + 1M out tokens → measurable cost (~$0.75 for gpt-4o-mini) regardless
    # of whether pricing comes from the registry or the fallback table.
    return SimpleNamespace(
        llm_output={"token_usage": {"prompt_tokens": 1_000_000, "completion_tokens": 1_000_000}},
        generations=[],
    )


def test_cost_accumulates_inside_scope():
    cb = BudgetCallback("gpt-4o-mini", max_cost_usd=None)
    with obs.cost_scope("t") as ctx:
        assert ctx.cost_usd == 0.0
        cb.on_llm_end(_big_usage_response())
        assert ctx.cost_usd > 0.0


def test_no_scope_is_noop():
    # Outside a cost_scope, record_llm is a no-op — must not raise.
    cb = BudgetCallback("gpt-4o-mini", max_cost_usd=0.0000001)
    cb.on_llm_end(_big_usage_response())  # no active ctx → nothing happens


def test_ceiling_raises_budget_exceeded():
    cb = BudgetCallback("gpt-4o-mini", max_cost_usd=0.5)  # ~$0.75 spend > $0.50 cap
    with obs.cost_scope("t"):
        with pytest.raises(obs.BudgetExceeded):
            cb.on_llm_end(_big_usage_response())


def test_scope_resets_after_exit():
    cb = BudgetCallback("gpt-4o-mini")
    with obs.cost_scope("a") as ctx_a:
        cb.on_llm_end(_big_usage_response())
        assert ctx_a.cost_usd > 0
    # outside, no ctx leaks
    assert obs.get() is None


def test_non_enforcing_child_scope_defers_cap_to_parent():
    cb = BudgetCallback("gpt-4o-mini", max_cost_usd=0.5)
    with obs.cost_scope("run", max_cost_usd=0.5) as parent:
        with obs.cost_scope("node", enforce_cap=False) as child:
            cb.on_llm_end(_big_usage_response())
            assert child.cost_usd > 0.5
        with pytest.raises(obs.BudgetExceeded):
            obs.merge_cost(parent, child)
        assert parent.cost_usd == child.cost_usd


# ─── gateway routing ────────────────────────────────────────────────


def test_direct_gateway_builds_plain_model(monkeypatch):
    monkeypatch.setattr(llm_factory, "get_gateway", lambda: DirectGateway())
    llm = build_chat_model("gpt-4o-mini", temperature=0.2)
    assert llm.model_name == "gpt-4o-mini"
    assert any(isinstance(c, BudgetCallback) for c in (llm.callbacks or []))


def test_proxy_gateway_sets_base_url(monkeypatch):
    monkeypatch.setattr(
        llm_factory,
        "get_gateway",
        lambda: ProxyGateway(url="http://bifrost:8080", add_provider_prefix=False),
    )
    llm = build_chat_model("gpt-4o-mini")
    assert str(llm.openai_api_base) == "http://bifrost:8080"
    assert llm.model_name == "gpt-4o-mini"


# ─── offline/keyless construction (#412) ────────────────────────────


def test_build_chat_model_constructs_without_credentials(monkeypatch):
    """#412: template compile must not require live creds. ``ChatOpenAI``
    validates its key eagerly at construction, and the Temporal worker compiles
    every template at startup — so with no key configured, construction must
    still succeed (a placeholder is injected). Otherwise the worker registers 0
    graphs → every run fails 'Graph not found'. The real key is required only at
    invocation."""
    monkeypatch.setattr(llm_factory, "get_gateway", lambda: DirectGateway())
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    llm = build_chat_model("gpt-4o-mini")  # must NOT raise on the missing key
    assert llm.model_name == "gpt-4o-mini"
