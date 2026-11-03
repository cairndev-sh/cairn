"""#233 — a governed pipeline run's hash-chained audit trail.

With an ``audit.sink`` bound (``audit/store``, a tmp ``CAIRN_AUDIT_LOG``) a pipeline run writes
``pipeline.*`` / ``leg.*`` rows through the one ``audit_emit`` chokepoint: the launch door's
admission and refusals, each gate decision with its actor, each leg's submit and terminal
state with cost, the lifecycle rows of the paid-box and promote operators, the per-leg
budget refusal, approval expiry and dismissal, cancel with its actor, and the pipeline's
terminal state with its total spend. The resulting log verifies with the shared verifier.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-validate-offline")

import pytest  # noqa: E402

from cairn.core.admission_door import REFUSED, AdmissionRefusedError  # noqa: E402
from cairn.core.dispatch.leg_hooks import audit_flow_leg_finished, audit_leg_finished  # noqa: E402
from cairn.core.dispatch.pipeline_bridge import PipelineBridge  # noqa: E402
from cairn.core.dispatch.pipeline_launch import launch_pipeline  # noqa: E402
from cairn.core.obs import audit as audit_mod  # noqa: E402
from cairn.core.obs.audit import verify_chain  # noqa: E402
from cairn.core.pack import run_hooks as RH  # noqa: E402
from cairn.core.storage.pipeline_store import InMemoryPipelineStore  # noqa: E402
from operator_audit_store.operators.audit_store import AuditStoreOp  # noqa: E402
from tests.core.test_pipeline_bridge import (  # noqa: E402
    FakeDispatcher,
    _NullBus,
    _complete,
    _prep_then,
)
from tests.core.test_pipeline_launching_hook import (  # noqa: E402,F401 — fixtures + fakes
    PIPE,
    _Bridge,
    _Hooks,
    _refuse,
    installed,
)


class Trail:
    def __init__(self, path):
        self.path = path

    def lines(self) -> list[str]:
        return self.path.read_text().splitlines() if self.path.exists() else []

    def rows(self) -> list[dict]:
        return [json.loads(x) for x in self.lines()]

    def actions(self) -> list[str]:
        return [r["action"] for r in self.rows()]

    def of(self, action: str) -> list[dict]:
        return [r for r in self.rows() if r["action"] == action]

    def assert_verifies(self):
        report = verify_chain(self.lines())
        assert report.ok and report.rows == len(self.rows()), report


@pytest.fixture
def trail(tmp_path, monkeypatch):
    """The real chained writer bound as the audit sink, on a tmp log."""
    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    audit_mod.reset_audit_sink()
    monkeypatch.setattr(audit_mod, "_CACHE", AuditStoreOp())
    yield Trail(log)
    audit_mod.reset_audit_sink()


class PricedDispatcher(FakeDispatcher):
    """FakeDispatcher whose runs carry a ref, a target and a settled cost, like real legs."""

    async def run_template(self, template_ref, inputs, **kw):
        run = await super().run_template(template_ref, inputs, **kw)
        run.template_ref, run.status, run.compute_target = template_ref, "running", ""
        run.cost, run.error = {}, ""
        return run


def _bridge():
    disp = PricedDispatcher()
    return PipelineBridge(disp, store=InMemoryPipelineStore(), bus=_NullBus()), disp


async def _finish(bridge, disp, run_id, usd):
    disp.store.runs[run_id].cost = {"llm_usd": usd, "gpu_usd": 0.0, "storage_usd": 0.0, "total_usd": usd}
    await _complete(bridge, disp, run_id, output={"staged_dataset_uri": "s3://s"})


# ── the run: legs, gate, terminal ───────────────────────────────────────────


async def test_a_pipeline_run_writes_its_legs_gate_and_terminal_to_the_chain(trail):
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train", approval=True), inputs={})
    await _finish(bridge, disp, "run_1", 0.25)
    await bridge.advance_step(rec.pipeline_id, "train", approver="alice")
    await _finish(bridge, disp, "run_2", 1.5)

    assert (await bridge.store.get(rec.pipeline_id)).status == "done"
    assert trail.actions() == [
        "leg.submitted", "leg.finished",                          # prep
        "pipeline.gate_approved", "leg.submitted",                # train released by alice
        "leg.finished", "pipeline.finished",
    ]
    sub = trail.of("leg.submitted")[0]
    assert sub["metadata"]["step_id"] == "prep" and sub["run_id"] == "run_1"
    assert sub["metadata"]["pipeline_id"] == rec.pipeline_id and sub["metadata"]["ref"]
    fin = trail.of("leg.finished")
    assert [r["metadata"]["state"] for r in fin] == ["done", "done"]
    assert [r["metadata"]["settled_usd"] for r in fin] == [0.25, 1.5]
    gate = trail.of("pipeline.gate_approved")[0]
    assert gate["actor"] == "alice" and gate["metadata"]["step_id"] == "train"
    end = trail.of("pipeline.finished")[0]
    assert end["metadata"]["state"] == "done" and end["metadata"]["spent_usd"] == 1.75
    assert end["outcome"] == "ok"
    trail.assert_verifies()


async def test_a_failed_leg_and_pipeline_are_recorded_with_the_reason(trail):
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train"), inputs={})
    disp.store.runs["run_1"].error = "RuntimeError: engine exploded"
    await _complete(bridge, disp, "run_1", topic="run.failed")

    fin = trail.of("leg.finished")[0]
    assert fin["outcome"] == "error" and fin["metadata"]["state"] == "failed"
    assert "engine exploded" in fin["metadata"]["reason"]
    end = trail.of("pipeline.finished")[0]
    assert end["outcome"] == "error" and end["metadata"]["state"] == "failed"
    assert rec.pipeline_id == end["metadata"]["pipeline_id"]
    trail.assert_verifies()


async def test_a_reject_records_the_decider_and_the_failed_terminal(trail):
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train", approval=True), inputs={})
    await _finish(bridge, disp, "run_1", 0.0)
    await bridge.reject_step(rec.pipeline_id, "train", decider="bob", reason="dataset looks wrong")

    rej = trail.of("pipeline.gate_rejected")[0]
    assert rej["actor"] == "bob" and rej["outcome"] == "deny"
    assert rej["metadata"]["reason"] == "dataset looks wrong" and rej["metadata"]["step_id"] == "train"
    assert trail.of("pipeline.finished")[0]["metadata"]["state"] == "failed"
    trail.assert_verifies()


async def test_a_cancel_records_who_cancelled(trail):
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train", approval=True), inputs={})
    await bridge.cancel_pipeline(rec.pipeline_id, actor="carol")

    c = trail.of("pipeline.cancelled")[0]
    assert c["actor"] == "carol" and c["metadata"]["pipeline_id"] == rec.pipeline_id
    end = trail.of("pipeline.finished")[0]
    assert end["metadata"]["state"] == "cancelled"
    trail.assert_verifies()


async def test_a_gate_waiting_when_the_pipeline_is_cancelled_ends_as_a_cancelled_leg(trail):
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train", approval=True), inputs={})
    await _finish(bridge, disp, "run_1", 0.0)            # train now awaits approval
    await bridge.cancel_pipeline(rec.pipeline_id, actor="carol")
    cancelled = [r for r in trail.of("leg.finished") if r["metadata"]["state"] == "cancelled"]
    assert [r["metadata"]["step_id"] for r in cancelled] == ["train"]
    trail.assert_verifies()


async def test_an_unbound_sink_drops_silently_and_changes_nothing(tmp_path, monkeypatch):
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(tmp_path / "never.jsonl"))
    audit_mod.reset_audit_sink()
    monkeypatch.setattr(audit_mod, "_CACHE", audit_mod.NullAuditSink())
    bridge, disp = _bridge()
    rec = await bridge.start_pipeline(_prep_then("train"), inputs={})
    await _finish(bridge, disp, "run_1", 0.1)
    await _finish(bridge, disp, "run_2", 0.1)
    assert (await bridge.store.get(rec.pipeline_id)).status == "done"
    assert not (tmp_path / "never.jsonl").exists()
    audit_mod.reset_audit_sink()


# ── flow legs, lifecycle operators, budget ──────────────────────────────────


async def test_a_flow_leg_row_carries_engine_target_attempts_and_cost(trail, monkeypatch):
    from cairn.core.dispatch import leg_hooks

    monkeypatch.setattr(leg_hooks, "engine_and_target_for_kind", lambda kind: ("builtin", "gpu-box"))
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.flow_kind_of", lambda ref: "ml")
    outcome = SimpleNamespace(ok=False, error="train crashed", cost_usd=0.4, gpu_usd=1.1, attempts=2)
    run = SimpleNamespace(id="run_f", compute_target="gpu-box")
    await audit_flow_leg_finished("pl_1", "train", ref="flow:ft/sft", run=run, outcome=outcome,
                                  status="failed", engine="builtin")
    row = trail.of("leg.finished")[0]
    assert row["outcome"] == "error" and row["run_id"] == "run_f"
    m = row["metadata"]
    assert (m["engine"], m["compute_target"], m["attempts"], m["state"]) == ("builtin", "gpu-box", 2, "failed")
    assert (m["llm_usd"], m["gpu_usd"], m["settled_usd"]) == (0.4, 1.1, 1.5)
    assert m["reason"] == "train crashed"
    trail.assert_verifies()


@pytest.mark.parametrize("op,capability,row", [
    ("compute/acquire", "compute.acquire", "leg.compute_acquired"),
    ("compute/release", "compute.release", "leg.compute_released"),
    ("artifact/promote", "artifact.promote", "leg.promoted"),
])
async def test_paid_box_and_promote_legs_write_their_lifecycle_row(trail, monkeypatch, op, capability, row):
    monkeypatch.setattr("cairn.core.dispatch.leg_hooks._task_capabilities", lambda ref: [capability])
    await audit_leg_finished("pl_1", "step", ref=f"task:{op}", state="done", run=SimpleNamespace(id="run_t"))
    assert trail.actions() == ["leg.finished", row]
    assert trail.of(row)[0]["metadata"]["operator"] == op
    trail.assert_verifies()


async def test_a_failed_lifecycle_leg_writes_no_lifecycle_row(trail, monkeypatch):
    monkeypatch.setattr("cairn.core.dispatch.leg_hooks._task_capabilities", lambda ref: ["compute.acquire"])
    await audit_leg_finished("pl_1", "step", ref="task:compute/acquire", state="failed",
                             run=SimpleNamespace(id="run_t"), reason="no capacity")
    assert trail.actions() == ["leg.finished"]


async def test_a_per_leg_budget_refusal_is_recorded_as_a_deny(trail, monkeypatch):
    from cairn.core import budget_reservations as budget
    from cairn.core.dispatch.leg_executor import execute_leg
    from tests.core.test_leg_retry import _FlakyEngine, _stub_resolution

    monkeypatch.setattr(budget, "_backend_instance", budget._MemoryBackend())
    _stub_resolution(monkeypatch)
    engine = _FlakyEngine(failures=0)
    out = await execute_leg("flow:demo/clean", inputs={}, engine=engine, run_id="run_b", series_id="s1",
                            estimated_usd=50.0, spent_usd=0.0, budget_usd=10.0)
    assert not out.ok and engine.submits == 0
    row = trail.of("leg.budget_refused")[0]
    assert row["outcome"] == "deny" and row["run_id"] == "run_b" and row["target"] == "flow:demo/clean"
    assert row["metadata"]["estimated_usd"] == 50.0 and row["metadata"]["budget_usd"] == 10.0
    trail.assert_verifies()


async def test_a_leg_outcome_counts_its_attempts_and_each_retry_is_recorded(trail, monkeypatch):
    from cairn.core import budget_reservations as budget  # noqa: F401
    from cairn.core.dispatch.leg_executor import execute_leg
    from cairn.core.dispatch.leg_hooks import FlowLegHooks
    from tests.core.test_leg_retry import _FlakyEngine, _stub_resolution

    _stub_resolution(monkeypatch)
    hooks = FlowLegHooks(pipeline_id="pl_1", step_id="clean", leg_ref="flow:demo/clean", run_id="run_r",
                         pipeline_store=None, run_store=None, emit=_noop_emit, artifact_store=None)
    out = await execute_leg("flow:demo/clean", inputs={}, engine=_FlakyEngine(failures=1), run_id="run_r",
                            on_retry=hooks.on_retry)
    assert out.ok and out.attempts == 2
    retry = trail.of("leg.retrying")[0]
    assert retry["metadata"]["attempt"] == 1 and "connection reset" in retry["metadata"]["reason"]
    trail.assert_verifies()


async def _noop_emit(*_a, **_k):
    return None


# ── approval expiry / dismissal ─────────────────────────────────────────────


class _RunStore:
    def __init__(self):
        self.updated, self.events = [], []

    async def update(self, rid, **kw):
        self.updated.append((rid, kw))

    async def append_event(self, rid, kind, data):
        self.events.append((rid, kind))


async def test_expiry_and_dismissal_are_recorded_apart(trail):
    from cairn.core.approval.reaper import expire_run

    run = SimpleNamespace(id="run_g", status="awaiting_approval", bundle_pipeline_id="pl_1",
                          step_id="review", template_ref="task:x")
    now = datetime.now(timezone.utc)
    assert await expire_run(_RunStore(), run, now=now, reason="approval expired: no decision within 24h")
    assert await expire_run(_RunStore(), run, now=now, reason="approval dismissed by dave", actor="dave")
    exp, dis = trail.of("approval.expired")[0], trail.of("approval.dismissed")[0]
    assert exp["actor"] is None and exp["run_id"] == "run_g" and exp["metadata"]["pipeline_id"] == "pl_1"
    assert dis["actor"] == "dave"
    trail.assert_verifies()


async def test_a_run_cancel_names_the_actor(trail, monkeypatch):
    from cairn.core.dispatch.dispatcher import RunDispatcher

    run = SimpleNamespace(id="run_c", status="running", bundle_pipeline_id="pl_1", step_id="train")

    class _Store:
        async def get(self, _rid):
            return run

    async def _no_close(*a, **k):
        return None

    d = RunDispatcher.__new__(RunDispatcher)
    d.store, d._tasks = _Store(), {}
    monkeypatch.setattr(d, "_close_reservations", _no_close, raising=False)
    monkeypatch.setattr("cairn.core.operator.job_control.cancel_remote_jobs_for_run", _no_close)
    await d.cancel("run_c", actor="erin")
    row = trail.of("leg.cancelled")[0]
    assert row["actor"] == "erin" and row["run_id"] == "run_c"
    trail.assert_verifies()


# ── the launch door ─────────────────────────────────────────────────────────


async def test_an_admitted_launch_writes_the_admission_row(trail, installed, monkeypatch):  # noqa: F811
    monkeypatch.setattr(RH, "load_run_hooks", lambda: {})
    bridge = _Bridge()

    async def _start(plan, **kw):
        return SimpleNamespace(pipeline_id="pl_test", status="running", step_state={}, workspace_id="default",
                               admission=kw["admission"], governance={})

    bridge.start_pipeline = _start
    await launch_pipeline(PIPE, inputs={"path": "x"}, bridge=bridge, trigger={"source": "cli"})
    row = trail.of("pipeline.admitted")[0]
    assert row["metadata"]["pipeline_id"] == "pl_test" and row["metadata"]["source"] == "cli"
    assert row["metadata"]["verdict"] == "admitted"
    assert "path" not in json.dumps(row["metadata"])        # no input values on the trail
    trail.assert_verifies()


async def test_a_refused_launch_writes_a_refusal_row_and_no_admission(trail, installed, monkeypatch):  # noqa: F811
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"deny": hooks.recorder(_refuse("series cap reached"))}
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError) as ei:
        await launch_pipeline(PIPE, inputs={"path": "x"}, bridge=bridge)
    assert ei.value.decision.verdict == REFUSED and bridge.calls == []
    row = trail.of("pipeline.refused")[0]
    assert row["outcome"] == "deny" and "series cap reached" in row["metadata"]["reasons"]
    assert row["metadata"]["refusal"] == "AdmissionRefusedError"
    assert trail.of("pipeline.admitted") == []
    trail.assert_verifies()


# ── the Temporal twin writes the same rows, from activities ──────────────────


async def test_the_temporal_mirror_activity_writes_the_leg_and_pipeline_terminal_rows(trail, monkeypatch):
    pytest.importorskip("temporalio")
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
    from cairn.core.runtime.temporal import pipeline_activities as act
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    store = InMemoryPipelineStore()
    plan = PipelinePlan(name="pl", steps=[
        PipelinePlanStep(id="prep", ref="p/prep"),
        PipelinePlanStep(id="train", ref="flow:ft/sft", depends_on=["prep"]),
    ])
    await store.put(PipelineRecord(
        pipeline_id="pl_t", pipeline_name="pl", status="running", plan=plan, inputs={},
        step_state={"prep": StepState(status="running", run_id="run_p"),
                    "train": StepState(status="pending")},
        governance={"budget": {"usd": 5.0}}, trigger={"owner": "temporal"}))
    runs = {"run_p": SimpleNamespace(id="run_p", error="", cost={"total_usd": 0.5, "llm_usd": 0.5, "gpu_usd": 0.0}),
            "run_t": SimpleNamespace(id="run_t", error="", cost={"total_usd": 2.0, "llm_usd": 0.0, "gpu_usd": 2.0})}

    class _Runs:
        async def get(self, rid):
            return runs.get(rid)

    monkeypatch.setattr("cairn.core.storage.pipeline_store.default_pipeline_store", lambda: store)
    monkeypatch.setattr(act, "_dispatcher", lambda: SimpleNamespace(store=_Runs()))
    done = lambda rid: {"status": "done", "run_id": rid}  # noqa: E731
    await act.pipeline_mirror_state("pl_t", {"prep": done("run_p"), "train": {"status": "running", "run_id": "run_t"}}, "running")
    await act.pipeline_mirror_state("pl_t", {"prep": done("run_p"), "train": done("run_t")}, "done")

    fin = trail.of("leg.finished")
    # the template leg is written by the mirror; the flow leg that ran writes its own row in its activity
    assert [(r["metadata"]["step_id"], r["metadata"]["settled_usd"]) for r in fin] == [("prep", 0.5)]
    end = trail.of("pipeline.finished")[0]
    assert end["metadata"]["state"] == "done" and end["metadata"]["spent_usd"] == 2.5
    assert end["metadata"]["budget_usd"] == 5.0
    await act.pipeline_mirror_state("pl_t", {"prep": done("run_p"), "train": done("run_t")}, "done")
    assert len(trail.of("pipeline.finished")) == 1                 # not written twice
    trail.assert_verifies()
