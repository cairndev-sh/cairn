"""Temporal pipeline-record writes share the bridge's per-pipeline section."""
from __future__ import annotations

import asyncio
import types
from collections.abc import Awaitable, Callable
from typing import Any

import pytest

pytest.importorskip("temporalio")

from cairn.core.dispatch.coordination import CriticalSections, pipeline_sections_for
from cairn.core.dispatch.leg_hooks import FlowLegHooks
from cairn.core.runtime.temporal.pipeline_activities import _pin_step_run
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


async def test_pin_step_run_waits_for_a_stale_pipeline_put():
    pstore = InMemoryPipelineStore()
    await pstore.put(PipelineRecord(
        pipeline_id="pl1", status="running",
        step_state={"prep": StepState(status="running")},
    ))
    sections = pipeline_sections_for(types.SimpleNamespace())

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
    sections = pipeline_sections_for(dispatcher)
    assert pipeline_sections_for(dispatcher) is sections

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


def test_pipeline_bridge_reuses_dispatcher_pipeline_sections():
    pytest.importorskip("fcntl")
    from cairn.core.dispatch.pipeline_bridge import PipelineBridge

    dispatcher = types.SimpleNamespace()
    sections = pipeline_sections_for(dispatcher)
    bridge = PipelineBridge(dispatcher)

    assert bridge._sections is sections
