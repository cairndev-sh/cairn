"""#258 — a pipeline gate's evidence is on its step, where every reader of the record has it.

The bridge built the approver's context, then sent it only to the audit event and the review
record. `step_state` had none of it, so `GET /pipelines/{id}`, which is all the CLI reads, showed
a person the gate's name and nothing to decide on.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.operator import Operation, register_operator
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState
from cairn.core.triggers.server import build_app
from tests.core.test_trigger_server_dispatch import FakeRunner


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t258/measure", version="0.1.0")
class _Measure(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        return _P(score=0.97)


@pytest.fixture(autouse=True)
def _registered():
    from cairn.core.operator.registry import default_registry
    try:
        default_registry.register(_Measure, strict=False)
    except Exception:  # noqa: BLE001 — already registered is fine
        pass
    default_registry.attach_metadata(OperatorEntry(name="_t258/measure", risk="read_only"))


PIPELINE = (
    "name: g\nkind: pipeline\n"
    "governance: {series: s-258, budget: {usd: 5}, approvers: ml-owners}\n"
    "steps:\n"
    "  - id: measure\n    task: _t258/measure\n"
    "  - gate: review\n    depends_on: [measure]\n    show: [measure.score]\n"
    "  - id: after\n    task: _t258/measure\n    depends_on: [review]\n"
)


async def _until(store, pid, pred, timeout=8.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await store.get(pid)
        if pred(rec):
            return rec
        assert asyncio.get_event_loop().time() < deadline, rec.status
        await asyncio.sleep(0.02)


async def test_a_paused_gate_carries_its_evidence_and_keeps_it_after_the_decision():
    bridge = PipelineBridge(RunDispatcher(FakeRunner(), backend=build_storage()), store=InMemoryPipelineStore())
    await bridge.start()
    try:
        plan = plan_pipeline(parse_pipeline_def(PIPELINE, pack="t258", source="<test>"))
        rec = await bridge.start_pipeline(plan, inputs={}, admission={"verdict": "admitted"})
        rec = await _until(bridge.store, rec.pipeline_id,
                           lambda r: r.step_state["review"].status == "awaiting_approval")
        ctx = rec.step_state["review"].gate_context
        assert ctx["fields"] == {"measure.score": 0.97}
        assert ctx["approvers"] == "ml-owners"
        assert ctx["evidence"]["cost_cap_usd"] == 5.0 and ctx["evidence"]["cost_usd"] == 0.0
        assert ctx["admission"]["verdict"] == "admitted"
        assert rec.step_state["measure"].gate_context is None, "a leg that is no gate carries none"

        await bridge.advance_step(rec.pipeline_id, "review", approver="vibhu")
        rec = await _until(bridge.store, rec.pipeline_id, lambda r: r.status == "done")
        assert rec.step_state["review"].gate_context["fields"] == {"measure.score": 0.97}
    finally:
        bridge.stop()


def test_the_pipeline_route_returns_it(monkeypatch):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    store = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: store)
    ctx = {"gate": "review", "fields": {"measure.score": 0.97}, "evidence": {"cost_usd": 1.5}}
    asyncio.run(store.put(PipelineRecord(
        pipeline_id="pl_258", pipeline_name="g", status="paused", workspace_id="default",
        step_state={"review": StepState(status="awaiting_approval", gate_context=ctx)})))
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        body = c.get("/pipelines/pl_258").json()
    assert body["step_state"]["review"]["gate_context"] == ctx


async def test_a_later_pause_by_another_authority_does_not_show_the_gates_evidence():
    """A step released from `approval: true` can pause again for an operator or policy
    approval. That pause was not produced by the gate, so its evidence must not appear."""
    from types import SimpleNamespace

    bridge = PipelineBridge(RunDispatcher(FakeRunner(), backend=build_storage()), store=InMemoryPipelineStore())
    step = SimpleNamespace(id="train", ref="task:_t258/measure")
    rec = PipelineRecord(pipeline_name="g", status="running", step_state={
        "train": StepState(status="running", gate_context={"gate": "train", "fields": {"a.b": 1}})})
    req = SimpleNamespace(operator="_t258/measure", risk="paid_compute", sources=("policy",), reason="needs a person")
    await bridge._pause_leg_for_approval(rec, step, SimpleNamespace(id="r-1"), req)
    assert rec.step_state["train"].status == "awaiting_approval"
    assert rec.step_state["train"].gate_context is None
