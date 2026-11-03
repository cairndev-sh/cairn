"""#129 / #63 — a ``task:`` leg's spend is measured, recorded and reserved.

A task leg ran its operator with no cost scope open, so an operator's LLM calls
(``build_chat_model``'s ``BudgetCallback`` → ``obs.record_llm``) recorded nothing:
``Run.cost == {}``, gate cost lines and later legs' admission read $0, and the global
``max_cost_usd`` backstop never applied. Under a capped series it also never reserved.
It is now accounted like a flow leg (``execute_leg``) and a template run.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from pydantic import BaseModel, ConfigDict

from cairn.core import obs
from cairn.core.budget_reservations import held_for_run, reset_reservations
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.leg_hooks import pipeline_spent_usd
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.operator import Operation, register_operator
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner

CALLS: list[str] = []


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t129/llm", version="0.1.0")
class _Llm(Operation[_P, _P]):
    """Spends what one model call reports, the way ``BudgetCallback.on_llm_end`` does."""
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        CALLS.append("llm")
        obs.record_llm("gpt-4o", int(getattr(inp, "tokens_in", 200_000)), 50_000, 0.0)
        if getattr(inp, "fail_after", False):
            raise RuntimeError("the model answered, then the operator failed")
        return _P(ok=True)


@pytest.fixture(autouse=True)
def _registered():
    from cairn.core.operator.registry import default_registry
    try:
        default_registry.register(_Llm, strict=False)
    except Exception:  # noqa: BLE001 — already registered is fine
        pass
    default_registry.attach_metadata(OperatorEntry(name="_t129/llm", risk="read_only"))
    reset_reservations()
    CALLS.clear()
    yield
    reset_reservations()


async def _run(yaml_steps: str, governance: str = ""):
    d = parse_pipeline_def(
        f"name: r\nkind: pipeline\n{governance}steps:\n{yaml_steps}", pack="t129", source="<test>")
    disp = RunDispatcher(FakeRunner(), backend=build_storage())
    pstore = InMemoryPipelineStore()
    bridge = PipelineBridge(disp, store=pstore)
    await bridge.start()
    rec = await bridge.start_pipeline(plan_pipeline(d), inputs={})
    deadline = asyncio.get_event_loop().time() + 8.0
    while True:
        r = await pstore.get(rec.pipeline_id)
        if r.status in ("done", "failed", "cancelled"):
            break
        assert asyncio.get_event_loop().time() < deadline, f"stuck: {r.status}"
        await asyncio.sleep(0.05)
    bridge.stop()
    return disp, r


async def test_a_task_legs_llm_spend_is_recorded_on_its_run():
    disp, rec = await _run("  - id: ask\n    task: _t129/llm\n")
    run = await disp.store.get(rec.step_state["ask"].run_id)
    assert rec.status == "done"
    assert run.cost["llm_usd"] > 0 and run.cost["total_usd"] == run.cost["llm_usd"]
    assert run.result_payloads[0]["llm_cost_usd"] == run.cost["llm_usd"]
    assert await pipeline_spent_usd(rec, disp.store) == run.cost["total_usd"]


async def test_a_failed_task_leg_keeps_what_it_spent():
    disp, rec = await _run("  - id: ask\n    task: _t129/llm\n    with: {fail_after: true}\n")
    run = await disp.store.get(rec.step_state["ask"].run_id)
    assert rec.status == "failed" and run.status == "failed"
    assert run.cost["total_usd"] > 0


async def test_under_a_capped_series_the_leg_reserves_and_settles_its_spend(monkeypatch):
    from cairn.core import budget_reservations as br
    seen: dict = {}
    real_reserve, real_settle = br.reserve, br.settle

    async def reserve(series_id, **kw):
        seen["reserve"] = (series_id, kw["est_usd"], kw["budget_usd"])
        return await real_reserve(series_id, **kw)

    async def settle(reservation, actual_usd=0.0):
        seen["settle"] = (reservation.run_id, actual_usd)
        await real_settle(reservation, actual_usd)

    monkeypatch.setattr(br, "reserve", reserve)
    monkeypatch.setattr(br, "settle", settle)
    disp, rec = await _run("  - id: ask\n    task: _t129/llm\n",
                           "governance: {series: s-129, budget: {usd: 100}, estimated_usd: 2}\n")
    run_id = rec.step_state["ask"].run_id
    run = await disp.store.get(run_id)
    assert rec.status == "done"
    assert seen["reserve"] == ("s-129", 2.0, 100.0)
    assert seen["settle"] == (run_id, pytest.approx(run.cost["total_usd"]))
    assert await held_for_run(run_id) == []


async def test_a_capped_series_refuses_the_next_leg_once_spend_passes_the_cap():
    # the first leg spends ~$0.90 against a $0.50 cap; the second must not start
    disp, rec = await _run(
        "  - id: first\n    task: _t129/llm\n"
        "  - id: second\n    task: _t129/llm\n    depends_on: [first]\n",
        "governance: {series: s-129-cap, budget: {usd: 0.5}, estimated_usd: 0.01}\n")
    assert rec.status == "failed"
    assert CALLS == ["llm"], "the second leg's operator must never run"
    second = await disp.store.get(rec.step_state["second"].run_id)
    assert "budget" in (second.error or "").lower()
