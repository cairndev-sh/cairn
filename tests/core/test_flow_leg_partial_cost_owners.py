"""#131 — failed spend is persisted by both flow-leg owners and governs later legs."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from cairn.core.dispatch.leg_executor import LegOutcome
from cairn.core.dispatch.leg_hooks import pipeline_spent_usd
from cairn.core.engine.schema import TriggerEvent
from cairn.core.storage.memory.runs import InMemoryRunStore
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState
from cairn.core.storage.protocols import Run


class _NoopHooks:
    def __init__(self, **kwargs):
        pass

    def execute_kwargs(self, **kwargs):
        return {}

    async def relay_checkpoints(self):
        return None


class _Dispatcher:
    def __init__(self, store):
        self.store = store
        self.events: list[tuple[str, str, dict]] = []

    async def _emit(self, run_id, event, payload, **kwargs):
        self.events.append((run_id, event, payload))


def _run(run_id: str, step_id: str) -> Run:
    return Run(
        id=run_id,
        trigger=TriggerEvent(source="manual", payload={}),
        status="running",
        template_ref="flow:demo/clean",
        bundle_pipeline_id="pl_partial_cost",
        step_id=step_id,
    )


def _pipeline(*, bridge_owner: bool) -> PipelineRecord:
    return PipelineRecord(
        pipeline_id="pl_partial_cost",
        status="running",
        governance={"series": "series-1", "budget": {"usd": 2.0}, "estimated_usd": 0.5},
        step_state={
            "first": StepState(run_id="run_first" if bridge_owner else None),
            "next": StepState(run_id="run_next" if bridge_owner else None),
        },
    )


def _paid_outcomes() -> tuple[LegOutcome, LegOutcome]:
    return (
        LegOutcome(state="failed", error="failed after paid call", cost_usd=0.4),
        LegOutcome(state="succeeded", cost_usd=0.1),
    )


async def test_bridge_persists_failed_cost_for_next_leg_reservation(monkeypatch):
    from cairn.core.dispatch import pipeline_bridge as bridge_module

    run_store = InMemoryRunStore()
    await run_store.create(_run("run_first", "first"))
    await run_store.create(_run("run_next", "next"))
    dispatcher = _Dispatcher(run_store)
    pipeline_store = InMemoryPipelineStore()
    rec = _pipeline(bridge_owner=True)
    await pipeline_store.put(rec)
    bridge = bridge_module.PipelineBridge(dispatcher, store=pipeline_store, bus=object())
    monkeypatch.setattr(bridge_module, "FlowLegHooks", _NoopHooks)
    monkeypatch.setattr(bridge, "_leg_engine", lambda step: object())
    monkeypatch.setattr(bridge, "_checkpoint_store", lambda: None)

    outcomes = iter(_paid_outcomes())
    admitted_spend: list[float] = []

    async def execute(ref, **kwargs):
        admitted_spend.append(kwargs["spent_usd"])
        return next(outcomes)

    monkeypatch.setattr(bridge_module, "execute_leg", execute)
    await bridge._execute_and_settle_flow_leg(
        rec, SimpleNamespace(id="first", ref="flow:demo/clean"),
        await run_store.get("run_first"), {},
    )
    failed = await run_store.get("run_first")
    assert failed.status == "failed"
    assert failed.cost["total_usd"] == pytest.approx(0.4)

    await bridge._execute_and_settle_flow_leg(
        rec, SimpleNamespace(id="next", ref="flow:demo/clean"),
        await run_store.get("run_next"), {},
    )
    assert admitted_spend == [0.0, 0.4]
    assert (await run_store.get("run_next")).status == "done"
    assert await pipeline_spent_usd(rec, run_store, exclude_step="next") == 0.4


async def test_temporal_owner_persists_failed_cost_for_next_leg_reservation(monkeypatch):
    pytest.importorskip("temporalio")
    from cairn.core.runtime.temporal import pipeline_activities as activities
    from cairn.core.dispatch import dispatcher as dispatcher_module
    from cairn.core.dispatch import leg_executor, leg_hooks
    from cairn.core.dataflow import defs, engine_registry
    from cairn.core.storage import artifacts, pipeline_store as pipeline_store_module

    run_store = InMemoryRunStore()
    dispatcher = _Dispatcher(run_store)
    pipeline_store = InMemoryPipelineStore()
    rec = _pipeline(bridge_owner=False)
    await pipeline_store.put(rec)
    monkeypatch.setattr(activities, "_dispatcher", lambda: dispatcher)
    monkeypatch.setattr(pipeline_store_module, "default_pipeline_store", lambda: pipeline_store)
    monkeypatch.setattr(activities, "_gate_flow_leg", _allow_leg)
    monkeypatch.setattr(leg_hooks, "FlowLegHooks", _NoopHooks)
    monkeypatch.setattr(artifacts, "build_artifact_store", lambda: None)
    monkeypatch.setattr(activities.activity, "heartbeat", lambda: None)
    monkeypatch.setattr(defs, "flow_def_for", lambda ref: SimpleNamespace(kind="data"))
    monkeypatch.setattr(engine_registry, "resolve_engine", lambda engine, *, kind: object())
    ids = iter(("run_first", "run_next"))
    monkeypatch.setattr(dispatcher_module, "new_run_id", lambda ref: next(ids))

    outcomes = iter(_paid_outcomes())
    admitted_spend: list[float] = []

    async def execute(ref, **kwargs):
        admitted_spend.append(kwargs["spent_usd"])
        return next(outcomes)

    monkeypatch.setattr(leg_executor, "execute_leg", execute)
    step = {"produces": [], "timeout_s": 1}
    await activities._run_flow_leg(
        "flow:demo/clean", {}, rec.pipeline_id, "first", None, step)
    failed = await run_store.get("run_first")
    assert failed.status == "failed"
    assert failed.cost["total_usd"] == pytest.approx(0.4)

    await activities._run_flow_leg(
        "flow:demo/clean", {}, rec.pipeline_id, "next", None, step)
    assert admitted_spend == [0.0, 0.4]
    assert (await run_store.get("run_next")).status == "done"
    persisted = await pipeline_store.get(rec.pipeline_id)
    assert await pipeline_spent_usd(persisted, run_store, exclude_step="next") == 0.4


async def _allow_leg(*args, **kwargs):
    return None
