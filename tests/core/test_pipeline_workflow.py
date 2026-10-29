"""ADR-0069 Phase 4 Slice ① — the durable PipelineWorkflow spine, tested hermetically
under ``temporalio.testing.WorkflowEnvironment`` (time-skipping, no real cluster). The
activities are mocked (same names) so the test exercises the workflow's orchestration +
gate enforcement, not the real dispatcher/store.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("temporalio")

from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from temporalio.worker.workflow_sandbox import SandboxedWorkflowRunner, SandboxRestrictions

from cairn.core.engine.bundle import BundlePipeline, BundlePipelineStep
from cairn.core.engine.bundle_pipeline import plan_bundle
from cairn.core.runtime.temporal.pipeline_activities import pipeline_next_tick  # real Slice-② activity
from cairn.core.runtime.temporal.pipeline_workflow import PipelineWorkflow

_CANNED = {"prep_verdict": "pass"}


@activity.defn(name="cairn.pipeline_prepare_step")
async def mock_prepare(step: dict, inputs: dict, outputs: dict) -> dict:
    from cairn.core.engine.bundle import eval_gate_predicate, resolve_step_inputs
    gate = step.get("gate")
    passed = True if not gate else eval_gate_predicate(gate, bundle_input=inputs, steps=outputs)
    resolved = resolve_step_inputs(SimpleNamespace(inputs=step.get("inputs", {})), bundle_input=inputs, steps=outputs)
    return {"gate_passed": bool(passed), "resolved": resolved}


@activity.defn(name="cairn.pipeline_submit_step")
async def mock_submit(ref: str, resolved: dict, pipeline_id: str, step_id: str,
                      environment, step: dict | None = None) -> str:
    # `step` landed with the leg-kind routing (#1198) — the missing param made
    # this activity TypeError, which Temporal RETRIES indefinitely, so every
    # test in this file hung silently from #1198 until #1361's parity run
    # noticed. A mock drifting from the real signature fails as a hang, not a
    # failure — keep these signatures in lockstep with pipeline_activities.py.
    return f"run_{step_id}"


@activity.defn(name="cairn.pipeline_await_run")
async def mock_await(run_id: str) -> dict:
    sid = run_id[len("run_"):]
    if sid == "prep":
        v = _CANNED["prep_verdict"]
        return {"status": "done", "verdict": v, "output": {"verdict": v, "staged_dataset_uri": "s3://staged"}}
    return {"status": "done", "verdict": "pass", "output": {}}


@activity.defn(name="cairn.pipeline_mirror_state")
async def mock_mirror(pipeline_id: str, step_state: dict, status: str) -> None:
    return None


_EMITTED: list[tuple[str, str]] = []


@activity.defn(name="cairn.pipeline_emit_event")
async def mock_emit(event: str, output: dict, pipeline_id: str, step_id: str) -> None:
    _EMITTED.append((event, step_id))


def _plan() -> dict:
    return plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="prep", pack="p", template="prep"),
        BundlePipelineStep(id="train", pack="p", template="train", depends_on=["prep"],
                           gate="$steps.prep.verdict == 'pass'",
                           inputs={"dataset": "$steps.prep.staged_dataset_uri"}),
    ])).model_dump()


_ACTIVITIES = [mock_prepare, mock_submit, mock_await, mock_mirror, mock_emit, pipeline_next_tick]


async def _run_pipeline(plan: dict) -> dict:
    # Same sandbox config as the prod worker: pass cairn modules through so importing
    # the workflow module (which imports cairn) doesn't trip the deterministic sandbox.
    runner = SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("cairn"))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="pl-tq",
            workflows=[PipelineWorkflow],
            activities=_ACTIVITIES,
            workflow_runner=runner,
        ):
            return await env.client.execute_workflow(
                PipelineWorkflow.run,
                args=[plan, {"path": "s3://ds"}, "pl_test"],
                id="wf-pl-test", task_queue="pl-tq",
            )


async def test_temporal_pipeline_gate_pass_completes():
    _CANNED["prep_verdict"] = "pass"
    out = await _run_pipeline(_plan())
    assert out["status"] == "done"
    assert out["step_state"]["prep"]["status"] == "done"
    assert out["step_state"]["train"]["status"] == "done"


async def test_temporal_pipeline_gate_fail_terminates():
    _CANNED["prep_verdict"] = "fail"
    out = await _run_pipeline(_plan())
    assert out["status"] == "failed"                       # gate blocked → pipeline terminates
    assert out["step_state"]["train"]["status"] == "skipped"


# ── Slice ②: deterministic cron next-tick ───────────────────────────────────


def _plan_schedule() -> dict:
    return plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="prep", pack="p", template="prep"),
        BundlePipelineStep(id="publish", pack="p", template="publish", depends_on=["prep"],
                           trigger="schedule:* * * * *"),
    ])).model_dump()


async def test_temporal_pipeline_schedule_advances():
    # The schedule step sleeps to the next cron tick (delay from the real pipeline_next_tick
    # activity); the time-skipping env fast-forwards the durable timer, then it submits.
    _CANNED["prep_verdict"] = "pass"
    out = await _run_pipeline(_plan_schedule())
    assert out["status"] == "done"
    assert out["step_state"]["publish"]["status"] == "done"


# ── Slice ③: on:<event> step waits for a deliver_event signal ────────────────


async def test_temporal_pipeline_on_event_advances_on_signal():
    # The on:<event> step blocks until a deliver_event signal (the bus→signal bridge's job);
    # then it submits + completes. Buffered signals are safe: deliver_event before the wait
    # still satisfies it.
    _CANNED["prep_verdict"] = "pass"
    plan = plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="prep", pack="p", template="prep"),
        BundlePipelineStep(id="serve", pack="p", template="serve", depends_on=["prep"],
                           trigger="on:model.trained"),
    ])).model_dump()
    runner = SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("cairn"))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client, task_queue="pl-tq",
            workflows=[PipelineWorkflow],
            activities=_ACTIVITIES,
            workflow_runner=runner,
        ):
            handle = await env.client.start_workflow(
                PipelineWorkflow.run, args=[plan, {"path": "s3://ds"}, "pl_evt"],
                id="wf-pl-evt", task_queue="pl-tq",
            )
            await handle.signal(PipelineWorkflow.deliver_event, "model.trained")
            out = await handle.result()
    assert out["status"] == "done"
    assert out["step_state"]["serve"]["status"] == "done"


# ── a step's emits event is published on completion (ADR-0069) ───────────────


async def test_temporal_pipeline_emits_event_on_done():
    _CANNED["prep_verdict"] = "pass"
    _EMITTED.clear()
    plan = plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="prep", pack="p", template="prep", emits="dataset.ready"),
        BundlePipelineStep(id="train", pack="p", template="train", depends_on=["prep"],
                           gate="$steps.prep.verdict == 'pass'"),
    ])).model_dump()
    out = await _run_pipeline(plan)
    assert out["status"] == "done"
    assert ("dataset.ready", "prep") in _EMITTED           # emits published on step completion


# ── #1409 live finding: a PASSED predicate `gate:` leg must never reach SUBMIT ──

_SUBMITTED_REFS: list[str] = []


@activity.defn(name="cairn.pipeline_submit_step")
async def mock_submit_recording(ref: str, resolved: dict, pipeline_id: str, step_id: str,
                                environment, step: dict | None = None) -> str:
    _SUBMITTED_REFS.append(ref)
    if ref.startswith("gate:"):
        # the real activity does run_template(ref) here → FileNotFoundError; make it loud
        raise RuntimeError(f"gate leg reached SUBMIT: {ref}")
    return f"run_{step_id}"


async def test_temporal_predicate_gate_leg_completes_without_submit():
    """The e2e-fullstack shape under the Temporal owner: flow → `gate:` leg
    carrying only a predicate (no approval) → flow. Found live 2026-08-18: the
    passed predicate gate fell through to SUBMIT → run_template('gate:...') →
    FileNotFoundError, failing the whole pipeline. The bridge never submits gate
    legs; the owner must not either."""
    _CANNED["prep_verdict"] = "pass"
    _SUBMITTED_REFS.clear()
    plan = _plan()
    # splice a standalone predicate gate leg between prep and train, as plan_bundle
    # emits for `- gate: <name>` + `predicate:` in a pipeline YAML
    steps = plan["steps"]
    steps[1]["depends_on"] = ["data-clean"]
    steps.insert(1, {"id": "data-clean", "ref": "gate:data-clean", "trigger": "auto",
                     "depends_on": ["prep"], "gate": "$steps.prep.verdict == 'pass'",
                     "inputs": {}, "approval": None})
    runner = SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("cairn"))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="pl-tq2", workflows=[PipelineWorkflow],
                          activities=[mock_prepare, mock_submit_recording, mock_await, mock_mirror,
                                      mock_emit, pipeline_next_tick], workflow_runner=runner):
            out = await env.client.execute_workflow(
                PipelineWorkflow.run, args=[plan, {"path": "s3://ds"}, "pl_gate"],
                id="wf-pl-gate", task_queue="pl-tq2")
    assert out["status"] == "done"
    assert out["step_state"]["data-clean"]["status"] == "done"
    assert out["step_state"]["data-clean"]["verdict"] == "passed"
    assert not any(r.startswith("gate:") for r in _SUBMITTED_REFS), _SUBMITTED_REFS
    assert _SUBMITTED_REFS == ["p/prep", "p/train"]      # only executables submitted


# ── ADR-0156 D5: a person rejects an approval gate (the `reject_step` signal) ──


async def test_temporal_reject_step_signal_ends_the_workflow_failed():
    """The twin of `approve_step`: `reject_step` ends the gate `skipped` / `rejected`, the
    workflow ends failed, and the gated leg is never submitted."""
    _CANNED["prep_verdict"] = "pass"
    _SUBMITTED_REFS.clear()
    plan = plan_bundle(BundlePipeline(name="pl", step=[
        BundlePipelineStep(id="prep", pack="p", template="prep"),
        BundlePipelineStep(id="train", pack="p", template="train", depends_on=["prep"],
                           approval=True),
    ])).model_dump()
    runner = SandboxedWorkflowRunner(restrictions=SandboxRestrictions.default.with_passthrough_modules("cairn"))
    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(env.client, task_queue="pl-tq3", workflows=[PipelineWorkflow],
                          activities=[mock_prepare, mock_submit_recording, mock_await, mock_mirror,
                                      mock_emit, pipeline_next_tick], workflow_runner=runner):
            handle = await env.client.start_workflow(
                PipelineWorkflow.run, args=[plan, {"path": "s3://ds"}, "pl_rej"],
                id="wf-pl-rej", task_queue="pl-tq3")
            await handle.signal(PipelineWorkflow.reject_step, "train")
            out = await handle.result()
    assert out["status"] == "failed"
    assert out["step_state"]["train"]["status"] == "skipped"
    assert out["step_state"]["train"]["verdict"] == "rejected"
    assert _SUBMITTED_REFS == ["p/prep"]                 # the rejected leg never ran


async def test_reject_signal_bookkeeping_needs_no_server():
    """The offline half of the twin test above: the signal records the gate, and the DAG
    path's `_rejected_at_gate` ends the step `skipped` / `rejected`, stops launching and
    mirrors `failed` — a reject wins over an approve that raced it."""
    wf = PipelineWorkflow()
    mirrored: list = []

    async def act(name, *args, **kw):
        mirrored.append((name, args[2]))

    state = {"train": {"status": "awaiting_approval", "run_id": None, "verdict": None}}
    assert await wf._rejected_at_gate("train", state, "pl_x", act) is False    # nothing yet
    wf.approve_step("train")
    wf.reject_step("train")
    assert await wf._rejected_at_gate("train", state, "pl_x", act) is True
    assert (state["train"]["status"], state["train"]["verdict"]) == ("skipped", "rejected")
    assert wf._failed and wf._abort
    assert mirrored == [("cairn.pipeline_mirror_state", "failed")]
