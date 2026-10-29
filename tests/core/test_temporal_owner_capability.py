"""#199 drift guard — the Temporal owner's declared capability matches what it runs.

The launch door refuses a plan the Temporal pipeline owner cannot finish by reading
``cairn.core.runtime.temporal.PIPELINE_OWNER_LEG_KINDS`` and
``PIPELINE_OWNER_RUNS_FINALIZERS``. These tests derive both facts from the owner
itself, so a change to its routing or scheduling that does not update the constants
(or the reverse) fails here instead of silently loosening or tightening the door:

* leg kinds — the real ``pipeline_submit_step`` activity is called with a ref of each
  submitted kind and the executor it reaches is observed (``flow:`` → the engine,
  a template → ``run_template``; a ``task:`` leg routed to ``run_template`` is a
  template call on a task ref, i.e. not run); ``gate:`` legs are completed by the
  workflow itself without SUBMIT;
* finalizers — the real ``PipelineWorkflow`` runs a plan whose leg fails, and the
  finalizer after it either runs (finalizer semantics) or is skipped.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("temporalio")

from temporalio import activity  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions  # noqa: E402

from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep  # noqa: E402
from cairn.core.runtime import temporal as owner  # noqa: E402
from cairn.core.runtime.temporal import pipeline_activities as PA  # noqa: E402
from cairn.core.runtime.temporal.pipeline_workflow import PipelineWorkflow  # noqa: E402

#: A ref of each kind SUBMIT receives, and the executor that kind is meant to reach.
_SUBMITTED = {
    "flow": ("flow:somepack/someflow", "engine"),
    "template": ("somepack/sometemplate", "run_template"),
    "task": ("task:monitor/threshold", "task"),
}


async def _route(ref: str, monkeypatch) -> str:
    """Which executor the real SUBMIT activity sends ``ref`` to."""
    import cairn.core.storage.pipeline_store as store_mod

    seen: list[str] = []

    async def _flow(*a, **kw):
        seen.append("engine")
        return "run_flow"

    async def _run_template(r, *a, **kw):
        seen.append("run_template")
        return SimpleNamespace(id="run_tpl")

    async def _record(*a, **kw):
        return SimpleNamespace(workspace_id="default")

    monkeypatch.setattr(PA, "_run_flow_leg", _flow)
    monkeypatch.setattr(PA, "_leg_pipeline_record", _record)
    monkeypatch.setattr(PA, "_dispatcher", lambda: SimpleNamespace(run_template=_run_template))
    monkeypatch.setattr(store_mod, "default_pipeline_store", lambda: None)
    try:
        await PA.pipeline_submit_step(ref, {}, "pl_x", "leg", None, {})
    except Exception:  # noqa: BLE001 — a path this test does not stub is "some other executor"
        pass
    return seen[0] if seen else "other"


@pytest.mark.parametrize("kind", sorted(_SUBMITTED))
async def test_the_declared_leg_kinds_are_the_kinds_submit_routes(kind, monkeypatch):
    ref, meant = _SUBMITTED[kind]
    reached = await _route(ref, monkeypatch)
    # a task leg is run only when SUBMIT sends it somewhere that is neither the
    # engine nor the template door (today it goes to run_template and fails, #59)
    routed = reached == meant if meant != "task" else reached not in ("engine", "run_template")
    assert (kind in owner.PIPELINE_OWNER_LEG_KINDS) is routed, (
        f"SUBMIT sends a {kind} leg to {reached!r}; update PIPELINE_OWNER_LEG_KINDS "
        "in cairn/core/runtime/temporal/__init__.py with the owner")


# ── the workflow itself: gate legs and finalizers ────────────────────────────

_SUBMITS: list[str] = []


@activity.defn(name="cairn.pipeline_prepare_step")
async def _prepare(step: dict, inputs: dict, outputs: dict) -> dict:
    return {"gate_passed": True, "routed_on": True, "resolved": {}}


@activity.defn(name="cairn.pipeline_submit_step")
async def _submit(ref: str, resolved: dict, pipeline_id: str, step_id: str,
                  environment, step: dict | None = None) -> str:
    _SUBMITS.append(ref)
    return f"run_{step_id}"


@activity.defn(name="cairn.pipeline_await_run")
async def _await(run_id: str) -> dict:
    if run_id == "run_train":
        return {"status": "failed", "verdict": "error", "output": {}}
    return {"status": "done", "verdict": "pass", "output": {}}


@activity.defn(name="cairn.pipeline_mirror_state")
async def _mirror(pipeline_id: str, step_state: dict, status: str) -> None:
    return None


@activity.defn(name="cairn.pipeline_emit_event")
async def _emit(event: str, output: dict, pipeline_id: str, step_id: str) -> None:
    return None


async def _run(plan: PipelinePlan) -> dict:
    _SUBMITS.clear()
    runner = SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("cairn"))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="owner-tq", workflows=[PipelineWorkflow],
                          activities=[_prepare, _submit, _await, _mirror, _emit],
                          workflow_runner=runner):
            return await env.client.execute_workflow(
                PipelineWorkflow.run, args=[plan.model_dump(), {}, "pl_owner"],
                id="wf-owner-capability", task_queue="owner-tq")


async def test_gate_legs_are_run_by_the_workflow_without_submit():
    out = await _run(PipelinePlan(name="p", steps=[
        PipelinePlanStep(id="check", ref="gate:check", gate="`true`"),
        PipelinePlanStep(id="run", ref="somepack/sometemplate", depends_on=["check"]),
    ]))
    ran = out["step_state"]["check"]["status"] == "done" and "gate:check" not in _SUBMITS
    assert ("gate" in owner.PIPELINE_OWNER_LEG_KINDS) is ran


async def test_the_declared_finalizer_support_is_what_the_workflow_does():
    out = await _run(PipelinePlan(name="p", steps=[
        PipelinePlanStep(id="train", ref="flow:somepack/train"),
        PipelinePlanStep(id="release", ref="somepack/release", depends_on=["train"], finalizer=True),
    ]))
    assert out["step_state"]["train"]["status"] == "failed"
    ran_after_failure = out["step_state"]["release"]["status"] == "done"
    assert owner.PIPELINE_OWNER_RUNS_FINALIZERS is ran_after_failure, (
        "the workflow's finalizer behaviour changed; update PIPELINE_OWNER_RUNS_FINALIZERS "
        "(ADR-0151 D10)")
