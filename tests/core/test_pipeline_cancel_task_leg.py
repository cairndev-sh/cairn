"""ADR-0159 (#219, #207) — a pipeline cancel stops its running task legs before the finalizers.

A cancel used to leave a ``running`` task leg alone, and the finalizers ran at once. A
``compute/acquire`` still starting the box then raced ``compute/release``: the release saw a
stopped box, did nothing, and the start landed after it. Now the bridge cancels the legs it
executes, waits for them (bounded), settles each ``cancelled``, and only then runs the
teardown.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from pydantic import BaseModel, ConfigDict

from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.operator import Operation, register_operator
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner

TIMELINE: list[str] = []
CLEANUP_S = {"slow": 0.1}


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t219/slow", version="0.1.0")
class _Slow(Operation[_P, _P]):
    """Runs until cancelled, then takes a moment to clean up, as `acquire` stopping a box does."""
    input_schema = _P
    output_schema = _P

    async def run(self, inp, ctx):
        TIMELINE.append("started")
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            TIMELINE.append("cancelled")
            await asyncio.sleep(CLEANUP_S["slow"])
            TIMELINE.append("cleaned")
            raise
        return _P(ok=True)


@register_operator(name="_t219/final", version="0.1.0")
class _Final(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        TIMELINE.append("final")
        return _P(ok=True)


@pytest.fixture(autouse=True)
def _registered():
    from cairn.core.operator.registry import default_registry
    for cls, name in ((_Slow, "_t219/slow"), (_Final, "_t219/final")):
        try:
            default_registry.register(cls, strict=False)
        except Exception:  # noqa: BLE001 — already registered is fine
            pass
        default_registry.attach_metadata(OperatorEntry(name=name, risk="read_only"))
    TIMELINE.clear()
    CLEANUP_S["slow"] = 0.1
    yield


async def _wait(store, pid, pred, timeout=8.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await store.get(pid)
        if pred(rec):
            return rec
        assert asyncio.get_event_loop().time() < deadline, f"stuck: {rec.status}"
        await asyncio.sleep(0.02)


async def _cancel(steps: str):
    d = parse_pipeline_def(f"name: c\nkind: pipeline\nsteps:\n{steps}", pack="t219", source="<test>")
    disp = RunDispatcher(FakeRunner(), backend=build_storage())
    bridge = PipelineBridge(disp, store=InMemoryPipelineStore())
    await bridge.start()
    try:
        rec = await bridge.start_pipeline(plan_pipeline(d), inputs={})
        await _wait(bridge.store, rec.pipeline_id, lambda r: "started" in TIMELINE)
        await bridge.cancel_pipeline(rec.pipeline_id)
        rec = await _wait(bridge.store, rec.pipeline_id, lambda r: r.status in ("done", "failed", "cancelled"))
        run = await disp.store.get(rec.step_state["work"].run_id)
        return rec, run
    finally:
        bridge.stop()


async def test_a_cancel_stops_a_running_task_leg_and_settles_it_cancelled():
    rec, run = await _cancel("  - id: work\n    task: _t219/slow\n")

    assert TIMELINE == ["started", "cancelled", "cleaned"]
    assert rec.status == "cancelled"
    assert (rec.step_state["work"].status, rec.step_state["work"].verdict) == ("skipped", "cancelled")
    assert run.status == "cancelled" and "cancelled" in (run.error or "")


async def test_the_finalizer_runs_only_after_the_cancelled_leg_has_wound_down():
    rec, _ = await _cancel("  - id: work\n    task: _t219/slow\n"
                           "  - id: teardown\n    task: _t219/final\n    finalizer: true\n")

    assert rec.status == "cancelled" and rec.step_state["teardown"].status == "done"
    assert TIMELINE == ["started", "cancelled", "cleaned", "final"], TIMELINE


async def test_a_leg_slower_than_the_bound_does_not_hold_the_teardown(monkeypatch):
    monkeypatch.setattr(PipelineBridge, "_TASK_LEG_CANCEL_WAIT_S", 0.2)
    CLEANUP_S["slow"] = 1.0
    rec, run = await _cancel("  - id: work\n    task: _t219/slow\n"
                             "  - id: teardown\n    task: _t219/final\n    finalizer: true\n")

    assert TIMELINE.index("final") < TIMELINE.index("cleaned") if "cleaned" in TIMELINE else "final" in TIMELINE
    assert rec.status == "cancelled" and rec.step_state["teardown"].status == "done"
    assert rec.step_state["work"].verdict == "cancelled" and run.status == "cancelled"
