"""Temporal pipeline-record writes share the bridge's per-pipeline section."""
from __future__ import annotations

import asyncio
import types
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

pytest.importorskip("temporalio")

from cairn.core.dispatch.coordination import CriticalSections
from cairn.core.dispatch.leg_hooks import FlowLegHooks
from cairn.core.runtime.temporal import pipeline_activities
from cairn.core.runtime.temporal.pipeline_activities import (
    _pin_step_run,
    pipeline_mirror_state,
)
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState


async def _put_stale_record_while_sectioned_update_waits(
    pstore: InMemoryPipelineStore,
    sections: CriticalSections,
    write: Callable[[], Awaitable[None]],
) -> None:
    async with sections.hold("pl1"):
        stale = (await pstore.get("pl1")).model_copy(deep=True)
        task = asyncio.create_task(write())
        for _ in range(5):
            await asyncio.sleep(0)
        await pstore.put(stale)
    await task


async def _no_gate(*args: Any, **kwargs: Any) -> None:
    return None


async def test_pin_step_run_waits_for_a_stale_pipeline_put():
    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(
        pipeline_id="pl1", status="running",
        step_state={"prep": StepState(status="running")},
    ))
    sections = pipeline_activities._SECTIONS

    await _put_stale_record_while_sectioned_update_waits(
        pstore,
        sections,
        lambda: _pin_step_run(pstore, "pl1", "prep", "run-1", section=sections.hold),
    )

    step = (await pstore.get("pl1")).step_state["prep"]
    assert step.run_id == "run-1" and step.status == "running" and step.started_at


async def test_temporal_submit_handle_waits_for_a_stale_pipeline_put():
    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(
        pipeline_id="pl1", status="running",
        step_state={"prep": StepState(status="running")},
    ))
    dispatcher = types.SimpleNamespace()
    sections = pipeline_activities._SECTIONS

    async def emit(run_id: str, event: str, data: dict[str, Any]) -> None:
        return None

    hooks = FlowLegHooks(
        pipeline_id="pl1", step_id="prep", leg_ref="flow:demo/clean", run_id="run-1",
        pipeline_store=pstore, run_store=types.SimpleNamespace(), emit=emit,
        artifact_store=None, section=sections.hold,
    )
    handle = types.SimpleNamespace(job_id="job-1", engine="builtin", meta={})

    await _put_stale_record_while_sectioned_update_waits(
        pstore, sections, lambda: hooks.on_submit(handle),
    )

    step = (await pstore.get("pl1")).step_state["prep"]
    assert step.run_id == "run-1" and step.handle == {
        "job_id": "job-1", "engine": "builtin", "meta": {},
    }


async def test_temporal_mirror_waits_for_a_stale_pipeline_put(monkeypatch):
    """The workflow mirror and activity-owned handle writes share one section.

    Without the mirror's section, it can read the record before a concurrent
    handle write and put that stale snapshot afterward, erasing the re-attach
    token and the approver that the activity-owned writer persisted.
    """
    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(
        pipeline_id="pl1", status="running",
        step_state={"prep": StepState(
            status="running", run_id="run-1",
            handle={"job_id": "job-1", "engine": "builtin", "meta": {}},
            approver="alice",
        )},
    ))
    dispatcher = types.SimpleNamespace()
    sections = pipeline_activities._SECTIONS
    monkeypatch.setattr(pipeline_activities, "_DISPATCHER", dispatcher)
    monkeypatch.setattr(
        "cairn.core.storage.pipeline_store.default_pipeline_store",
        lambda: pstore,
    )

    await _put_stale_record_while_sectioned_update_waits(
        pstore,
        sections,
        lambda: pipeline_mirror_state("pl1", {"prep": {"status": "done"}}, "done"),
    )

    rec = await pstore.get("pl1")
    step = rec.step_state["prep"]
    assert rec.status == "done"
    assert step.run_id == "run-1"
    assert step.handle == {"job_id": "job-1", "engine": "builtin", "meta": {}}
    assert step.approver == "alice"


async def test_run_flow_leg_wires_sectioned_hooks_into_execute(monkeypatch):
    """Drive the Temporal owner through ``_run_flow_leg`` to exercise its wiring.

    The fake executor calls the real hook callback. A stale whole-record put is
    held in the same section while that callback runs, so a missing ``section``
    in the active path would erase the persisted handle.
    """
    pytest.importorskip("fcntl")
    from cairn.core.dispatch.leg_executor import LegOutcome
    from cairn.core.storage.memory.runs import InMemoryRunStore
    from cairn.core.storage.protocols import Run
    from cairn.core.engine.schema import TriggerEvent

    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(
        pipeline_id="pl1", status="running",
        step_state={"prep": StepState(status="running", run_id="run-1")},
    ))
    run_store = InMemoryRunStore()
    await run_store.create(Run(
        id="run-1",
        trigger=TriggerEvent(source="manual", payload={}),
        status="running",
        bundle_pipeline_id="pl1",
        step_id="prep",
    ))

    emitted: list[tuple[str, str, dict]] = []

    async def emit(run_id: str, event: str, data: dict) -> None:
        emitted.append((run_id, event, data))

    dispatcher = types.SimpleNamespace(store=run_store, _emit=emit)
    sections = pipeline_activities._SECTIONS
    monkeypatch.setattr(pipeline_activities, "_DISPATCHER", dispatcher)
    monkeypatch.setattr(
        "cairn.core.storage.pipeline_store.default_pipeline_store",
        lambda: pstore,
    )
    monkeypatch.setattr(pipeline_activities, "_gate_flow_leg", _no_gate)
    monkeypatch.setattr(pipeline_activities.activity, "heartbeat", lambda: None)
    monkeypatch.setattr(
        "cairn.core.dataflow.defs.flow_def_for",
        lambda ref: types.SimpleNamespace(kind="data"),
    )
    engine = types.SimpleNamespace(name="builtin")
    monkeypatch.setattr(
        "cairn.core.dataflow.engine_registry.resolve_engine",
        lambda ctx=None, kind="data": engine,
    )
    monkeypatch.setattr(
        "cairn.core.storage.artifacts.build_artifact_store",
        lambda: None,
    )

    async def fake_execute_leg(ref: str, **kwargs):
        await kwargs["on_submit"](
            types.SimpleNamespace(job_id="job-2", engine="builtin", meta={})
        )
        return LegOutcome(state="succeeded")

    monkeypatch.setattr(
        "cairn.core.dispatch.leg_executor.execute_leg",
        fake_execute_leg,
    )

    async def run_leg() -> None:
        await pipeline_activities._run_flow_leg(
            "flow:demo/clean", {}, "pl1", "prep", None, {"produces": []},
        )

    await _put_stale_record_while_sectioned_update_waits(pstore, sections, run_leg)

    step = (await pstore.get("pl1")).step_state["prep"]
    assert step.run_id == "run-1"
    assert step.handle == {"job_id": "job-2", "engine": "builtin", "meta": {}}
    assert emitted and emitted[-1][1] == "done"


def test_pipeline_bridge_owns_process_local_pipeline_sections():
    pytest.importorskip("fcntl")
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge

    dispatcher = types.SimpleNamespace()
    bridge = PipelineBridge(dispatcher)

    assert bridge._sections.namespace == "pipeline"
    assert not hasattr(dispatcher, "_pipeline_sections")
