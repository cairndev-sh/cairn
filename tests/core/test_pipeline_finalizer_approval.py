"""#218 — a gated ``finalizer:`` can be decided while the pipeline finalizes.

A finalizer runs after the terminal intent is set. When a profile or policy gates its
operator (``compute/release`` is ``external_write``), it parks at ``awaiting_approval``,
and ``advance_step`` / ``reject_step`` refused every step once the intent was set
(#1787), so the pipeline stayed ``running`` forever and the box it releases kept billing.
A person may now approve or reject that finalizer; every other step stays refused.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from pydantic import BaseModel, ConfigDict

from cairn.core.approval import enforce
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.operator import Operation, register_operator
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner

RELEASED: list[bool] = []


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t218/work", version="0.1.0")
class _Work(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        return _P(ok=True)


@register_operator(name="_t218/release", version="0.1.0")
class _Release(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        RELEASED.append(True)
        return _P(released=True)


@pytest.fixture(autouse=True)
def _gated_release(monkeypatch):
    from cairn.core.operator.registry import default_registry
    for cls in (_Work, _Release):
        try:
            default_registry.register(cls, strict=False)
        except Exception:  # noqa: BLE001 — already registered is fine
            pass
    default_registry.attach_metadata(OperatorEntry(name="_t218/work", risk="read_only"))
    default_registry.attach_metadata(OperatorEntry(name="_t218/release", risk="external_write"))
    # what `[profile.approval] mode = "enforce", default_on_risk = ["external_write"]` yields
    monkeypatch.setattr(enforce, "_profile_approval",
                        lambda: {"mode": "enforce", "default_on_risk": ["external_write"]})
    RELEASED.clear()


async def _wait(pstore, pid, pred, what, timeout=8.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await pstore.get(pid)
        if pred(rec):
            return rec
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"timed out waiting for {what}: status={rec.status} "
                                 f"steps={ {k: v.status for k, v in rec.step_state.items()} }")
        await asyncio.sleep(0.05)


async def _parked_finalizer():
    d = parse_pipeline_def(
        "name: r\nkind: pipeline\nsteps:\n"
        "  - id: work\n    task: _t218/work\n"
        "  - id: release\n    task: _t218/release\n    finalizer: true\n",
        pack="t218", source="<test>")
    pstore = InMemoryPipelineStore()
    bridge = PipelineBridge(RunDispatcher(FakeRunner(), backend=build_storage()), store=pstore)
    await bridge.start()
    rec = await bridge.start_pipeline(plan_pipeline(d), inputs={})
    parked = await _wait(pstore, rec.pipeline_id,
                         lambda r: r.step_state.get("release") is not None
                         and r.step_state["release"].status == "awaiting_approval",
                         "the gated finalizer to park")
    assert parked.terminal_intent == "done" and parked.status == "running"
    return bridge, pstore, rec.pipeline_id


async def test_approving_a_gated_finalizer_runs_it_and_settles_the_pipeline():
    bridge, pstore, pid = await _parked_finalizer()
    try:
        await bridge.advance_step(pid, "release", approver="ops@example.com")
        rec = await _wait(pstore, pid, lambda r: r.status == "done", "the pipeline to settle")
        assert rec.step_state["release"].status == "done"
        assert rec.step_state["release"].approver == "ops@example.com"
        assert RELEASED == [True]
    finally:
        bridge.stop()


async def test_rejecting_a_gated_finalizer_settles_the_pipeline_failed():
    bridge, pstore, pid = await _parked_finalizer()
    try:
        await bridge.reject_step(pid, "release", decider="ops@example.com", reason="keep the box")
        rec = await _wait(pstore, pid, lambda r: r.status in ("done", "failed", "cancelled"),
                          "the pipeline to settle")
        assert rec.status == "failed"
        assert rec.step_state["release"].verdict == "rejected"
        assert RELEASED == []
    finally:
        bridge.stop()


async def test_no_other_step_can_advance_while_finalizing():
    bridge, pstore, pid = await _parked_finalizer()
    try:
        with pytest.raises(ValueError, match="is finalizing"):
            await bridge.advance_step(pid, "work", approver="ops@example.com")
        rec = await pstore.get(pid)
        assert not PipelineBridge._finalizer_awaiting_approval(rec, "work")
        assert PipelineBridge._finalizer_awaiting_approval(rec, "release")
    finally:
        bridge.stop()
