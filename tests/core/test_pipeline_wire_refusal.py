"""A step whose wire cannot resolve stops its caller (real bridge paths).

A non-optional ``$steps.<id>.<field>`` wire that resolves to nothing fails the
pipeline with the cited reason (#1540). The submit returns the same refusal a
record without a workspace does, so the caller stops there: no later leg
starts on the failed record, ``advance_step`` records no approval, and the
failure is announced once. Hermetic: in-memory pipeline store, stubbed run
dispatch, no network.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from types import SimpleNamespace

import pytest

from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState

#: `a` completed, but its output has no `missing` field.
_A_OUTPUT = {"a": {"present": 1}}


class _Runs:
    async def get(self, run_id, *a, **kw):
        return None

    async def update(self, *a, **kw):
        return None


@pytest.fixture
def world(monkeypatch):
    seen = SimpleNamespace(audits=[], notified=[], reviews=[], calls=[])

    async def _audit(bridge, rec, step_id, event, detail):
        seen.audits.append((step_id, event))

    async def _notify(bridge, rec, reason):
        seen.notified.append(reason)

    async def _decide(bridge, pipeline_id, step_id, verdict, *, reviewer="", rationale=""):
        seen.reviews.append((step_id, verdict, rationale))

    async def _outputs(bridge, rec):
        return dict(_A_OUTPUT)

    monkeypatch.setattr(PipelineBridge, "_audit", _audit)
    monkeypatch.setattr(PipelineBridge, "_notify_pipeline_failed", _notify)
    monkeypatch.setattr(PipelineBridge, "_decide_review", _decide)
    monkeypatch.setattr(PipelineBridge, "_open_review", lambda *a, **k: _none())
    monkeypatch.setattr(PipelineBridge, "_collected_outputs", _outputs)

    async def _run_template(ref, inputs, **kw):
        seen.calls.append(ref)
        return SimpleNamespace(id=f"run-{len(seen.calls)}")

    async def _emit(*a, **kw):
        return None

    disp = SimpleNamespace(store=_Runs(), _emit=_emit, run_template=_run_template)
    seen.bridge = PipelineBridge(disp, store=InMemoryPipelineStore())
    return seen


async def _none():
    return None


def _steps(b_status: str = "pending", **b_kw):
    """`a` done; `b` wired from a field `a` never produced; `c` ready beside it."""
    plan = PipelinePlan(name="wired", steps=[
        PipelinePlanStep(id="a", ref="demo/a"),
        PipelinePlanStep(id="b", ref="demo/b", depends_on=["a"],
                         inputs={"x": "$steps.a.missing"}, **b_kw),
        PipelinePlanStep(id="c", ref="demo/c", depends_on=["a"]),
    ])
    state = {"a": StepState(status="done", run_id="run-a"),
             "b": StepState(status=b_status), "c": StepState(status="pending")}
    return plan, state


async def _put(bridge, plan, state, status="running"):
    return await bridge.store.put(PipelineRecord(
        pipeline_name="wired", status=status, workspace_id="default", inputs={},
        plan=plan, step_state=state))


async def test_advance_stops_at_an_unresolved_wire(world):
    plan, state = _steps()
    rec = await _put(world.bridge, plan, state)
    await world.bridge._advance(rec.pipeline_id)
    stored = await world.bridge.store.get(rec.pipeline_id)
    assert stored.status == "failed"
    assert "resolved to NOTHING" in stored.trigger["failure_reason"]
    assert world.calls == [], "no leg starts after the pipeline failed"
    assert stored.step_state["c"].status == "pending"
    assert ("b", "wire_unresolved") in world.audits
    assert len(world.notified) == 1


async def test_held_back_release_stops_at_an_unresolved_wire(world):
    plan, state = _steps()
    rec = await _put(world.bridge, plan, state)
    await world.bridge._advance_held_back(rec.pipeline_id, {"b", "c"})
    stored = await world.bridge.store.get(rec.pipeline_id)
    assert stored.status == "failed"
    assert world.calls == []
    assert stored.step_state["c"].status == "pending"
    assert len(world.notified) == 1


async def test_advance_step_on_an_unresolved_wire_records_no_approval(world):
    plan, state = _steps(b_status="awaiting_approval", approval=True)
    rec = await _put(world.bridge, plan, state, status="paused")
    out = await world.bridge.advance_step(rec.pipeline_id, "b", approver="alice")
    stored = await world.bridge.store.get(rec.pipeline_id)
    assert out.status == stored.status == "failed"
    assert not any(e in ("approved", "advanced") for _, e in world.audits)
    (step, verdict, rationale), = world.reviews
    assert (step, verdict) == ("b", "rejected")
    assert "resolved to NOTHING" in rationale, "the review cites the wire, not a workspace"
    assert world.calls == []
    assert len(world.notified) == 1


async def test_a_gate_released_pass_stops_at_an_unresolved_wire(world):
    """The pass a passed gate leg runs from inside the submit stops too; the
    outer pass does not start a leg on the record it failed."""
    plan = PipelinePlan(name="wired", steps=[
        PipelinePlanStep(id="a", ref="demo/a"),
        PipelinePlanStep(id="g", ref="gate:ok", depends_on=["a"]),
        PipelinePlanStep(id="b", ref="demo/b", depends_on=["g"], inputs={"x": "$steps.a.missing"}),
        PipelinePlanStep(id="c", ref="demo/c", depends_on=["a"]),
    ])
    state = {"a": StepState(status="done", run_id="run-a"), "g": StepState(status="pending"),
             "b": StepState(status="pending"), "c": StepState(status="pending")}
    rec = await _put(world.bridge, plan, state)
    await world.bridge._advance(rec.pipeline_id)
    stored = await world.bridge.store.get(rec.pipeline_id)
    assert stored.status == "failed"
    assert world.calls == []
    assert len(world.notified) == 1
