"""#1289 — risk-aware restart resume for task legs.

A task leg is in-process: a control-plane restart kills its op mid-run and
its run can never terminalize (nothing to poll, no heartbeat). The adopt
path terminalizes the dead run and decides by the op's manifest RISK:
read_only → auto-resubmit (a re-read is free); anything else — including
NO metadata, the fail-closed default — parks the step ``awaiting_advance``
for the EXISTING human door. Restart is simulated with two real bridges
sharing one durable store.
"""
from __future__ import annotations

import asyncio
import os
import time

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


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t1289/reader", version="0.1.0")
class _Reader(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        time.sleep(float(getattr(inp, "sleep_s", 0.0)))
        return _P(read=True)


@register_operator(name="_t1289/writer", version="0.1.0")
class _Writer(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        time.sleep(float(getattr(inp, "sleep_s", 0.0)))
        return _P(wrote=True)


@pytest.fixture(autouse=True)
def _ops_registered():
    # the full suite resets the default registry (seen on #1278); re-pin ops
    # AND the manifest metadata the resume policy reads.
    from cairn.core.operator.registry import default_registry
    for cls in (_Reader, _Writer):
        try:
            default_registry.register(cls, strict=False)
        except Exception:  # noqa: BLE001 — already registered is fine
            pass
    default_registry.attach_metadata(OperatorEntry(name="_t1289/reader", risk="read_only"))
    default_registry.attach_metadata(OperatorEntry(name="_t1289/writer", risk="external_write"))


def _def(op: str) -> object:
    return parse_pipeline_def(
        f"name: r\nkind: pipeline\nsteps:\n"
        f"  - id: work\n    task: {op}\n    with: {{sleep_s: 2.0}}\n",
        pack="t1289", source="<test>")


async def _start_and_kill_mid_leg(op: str):
    """Bridge A starts the pipeline and dies mid-op; returns the shared stores
    plus the dead run's id. stop() cancels the leg task without terminalizing
    the run — exactly what a process death leaves behind."""
    backend = build_storage()
    pstore = InMemoryPipelineStore()
    d1 = RunDispatcher(FakeRunner(), backend=backend)
    b1 = PipelineBridge(d1, store=pstore)
    await b1.start()
    rec = await b1.start_pipeline(plan_pipeline(_def(op)), inputs={})
    for _ in range(100):
        r = await pstore.get(rec.pipeline_id)
        if (r.step_state.get("work") or object()) and \
                getattr(r.step_state.get("work"), "status", "") == "running":
            break
        await asyncio.sleep(0.02)
    b1.stop()
    await asyncio.sleep(0.05)          # let the cancelled leg task unwind
    r = await pstore.get(rec.pipeline_id)
    old_run_id = r.step_state["work"].run_id
    run = await d1.store.get(old_run_id)
    assert run.status == "running"     # the lie a restart leaves behind
    return backend, pstore, rec.pipeline_id, old_run_id


async def _wait_for(store, pid, states, timeout=8.0):
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        rec = await store.get(pid)
        if rec.status in states:
            return rec
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(
                f"pipeline stuck in {rec.status!r}; steps="
                + str({k: v.status for k, v in rec.step_state.items()}))
        await asyncio.sleep(0.05)


async def test_read_only_leg_auto_resubmits_and_completes():
    backend, pstore, pid, old_run = await _start_and_kill_mid_leg("_t1289/reader")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    b2 = PipelineBridge(d2, store=pstore)
    await b2.start()                    # adoption runs the resume policy
    try:
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "done"     # completed unattended
        dead = await d2.store.get(old_run)
        assert dead.status == "failed"
        assert "died with the control plane restart" in (dead.error or "")
        assert rec.step_state["work"].run_id != old_run   # a FRESH governed run
    finally:
        b2.stop()


async def test_external_write_leg_parks_for_a_human_then_advances():
    backend, pstore, pid, old_run = await _start_and_kill_mid_leg("_t1289/writer")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    b2 = PipelineBridge(d2, store=pstore)
    await b2.start()
    try:
        rec = await _wait_for(pstore, pid, ("paused",))
        assert rec.step_state["work"].status == "awaiting_advance"
        dead = await d2.store.get(old_run)
        assert dead.status == "failed"

        # the EXISTING advance door resumes it — no new surface
        await b2.advance_step(pid, "work", approver="oncall")
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "done"
    finally:
        b2.stop()


async def test_unknown_risk_is_fail_closed():
    """No manifest metadata (the #1293 reality for installed packs) must park,
    never re-fire."""
    from cairn.core.operator.registry import default_registry
    default_registry._metadata.pop("_t1289/writer", None)   # simulate no manifest
    backend, pstore, pid, _ = await _start_and_kill_mid_leg("_t1289/writer")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    b2 = PipelineBridge(d2, store=pstore)
    await b2.start()
    try:
        rec = await _wait_for(pstore, pid, ("paused",))
        assert rec.step_state["work"].status == "awaiting_advance"
    finally:
        b2.stop()


# ── #2540: the step was persisted `pending` while its run was `running` ─────
#
# A leg's run is created and its op started before the record that says the
# step is `running` is put (`_advance` puts at its end). A process killed in
# between — or whose loop the leg itself blocks — leaves exactly this state,
# and adoption used to leave it alone: the pipeline read `running` forever.


async def _kill_before_the_step_was_recorded(op: str):
    """The observed state: the step reads `pending` (no run id) while its run
    row reads `running` — the record put after the submit never happened."""
    backend, pstore, pid, old_run = await _start_and_kill_mid_leg(op)
    rec = await pstore.get(pid)
    rec.step_state["work"].status = "pending"
    rec.step_state["work"].run_id = None
    rec.step_state["work"].started_at = None
    rec.cursor = []
    await pstore.put(rec)
    return backend, pstore, pid, old_run


async def test_pending_step_with_a_running_read_only_run_is_settled_and_refired():
    backend, pstore, pid, old_run = await _kill_before_the_step_was_recorded("_t1289/reader")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    b2 = PipelineBridge(d2, store=pstore)
    await b2.start()
    try:
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "done"
        dead = await d2.store.get(old_run)
        assert dead.status == "failed"
        assert "died with the control plane restart" in (dead.error or "")
        assert rec.step_state["work"].run_id not in (None, old_run)   # re-fired as a FRESH run
    finally:
        b2.stop()


async def test_pending_step_with_a_running_write_run_parks_for_a_human():
    backend, pstore, pid, old_run = await _kill_before_the_step_was_recorded("_t1289/writer")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    b2 = PipelineBridge(d2, store=pstore)
    await b2.start()
    try:
        rec = await _wait_for(pstore, pid, ("paused",))
        assert rec.step_state["work"].status == "awaiting_advance"     # never silently re-fired
        assert rec.step_state["work"].run_id == old_run
        assert (await d2.store.get(old_run)).status == "failed"
        assert [r for r in await d2.store.list(limit=50)
                if r.bundle_pipeline_id == pid and r.id != old_run] == []
    finally:
        b2.stop()


async def _fresh_record(pstore, plan, **kw):
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    return await pstore.put(PipelineRecord(
        pipeline_name="r", status="running", plan=plan, inputs={},
        step_state={s.id: StepState(status="pending") for s in plan.steps}, **kw))


async def _runs_of(store, pid):
    return [r for r in await store.list(limit=200) if r.bundle_pipeline_id == pid]


async def test_a_ready_pending_read_only_step_with_no_run_is_submitted_on_adopt():
    """The process died after the record was created but before its first leg
    was recorded: `plan_resume` says RESUBMIT, and a read_only task leg is the
    one leg #1289 re-fires unattended — so adoption submits it."""
    pstore = InMemoryPipelineStore()
    rec = await _fresh_record(pstore, plan_pipeline(_def("_t1289/reader")))
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        done = await _wait_for(pstore, rec.pipeline_id, ("done", "failed"))
        assert done.status == "done"
        assert done.step_state["work"].run_id
    finally:
        b.stop()


async def test_a_ready_pending_write_step_with_no_run_is_parked_on_adopt():
    """Same state, write op: it may already have run before the record was put,
    so a person decides — the pipeline does not sit `running` and nothing fires."""
    pstore = InMemoryPipelineStore()
    rec = await _fresh_record(pstore, plan_pipeline(_def("_t1289/writer")))
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        parked = await _wait_for(pstore, rec.pipeline_id, ("paused",))
        assert parked.step_state["work"].status == "awaiting_advance"
        assert await _runs_of(d.store, rec.pipeline_id) == []
        await b.advance_step(rec.pipeline_id, "work", approver="oncall")   # the existing door
        assert (await _wait_for(pstore, rec.pipeline_id, ("done", "failed"))).status == "done"
    finally:
        b.stop()


@pytest.mark.parametrize("finished", ["done", "failed"])
async def test_pending_step_whose_write_run_already_finished_is_parked_not_repeated(finished):
    """The leg ran to the end before the record was put: its run is terminal,
    so no scan finds it — the step must still not be re-fired silently."""
    backend, pstore, pid, old_run = await _kill_before_the_step_was_recorded("_t1289/writer")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    await d2.store.update(old_run, status=finished)
    b2 = PipelineBridge(d2, store=pstore)
    await b2.start()
    try:
        rec = await _wait_for(pstore, pid, ("paused",))
        assert rec.step_state["work"].status == "awaiting_advance"
        assert [r.id for r in await _runs_of(d2.store, pid)] == [old_run]   # no new run
        assert (await d2.store.get(old_run)).status == finished             # untouched
    finally:
        b2.stop()


@pytest.mark.parametrize("finished", ["done", "failed"])
async def test_pending_step_whose_read_only_run_already_finished_is_resubmitted(finished):
    backend, pstore, pid, old_run = await _kill_before_the_step_was_recorded("_t1289/reader")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    await d2.store.update(old_run, status=finished)
    b2 = PipelineBridge(d2, store=pstore)
    await b2.start()
    try:
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "done"
        assert rec.step_state["work"].run_id not in (None, old_run)
    finally:
        b2.stop()


async def test_a_pending_unrecorded_flow_leg_is_parked_on_adopt():
    """A flow leg may have submitted paid engine work before the record was put."""
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep

    plan = PipelinePlan(name="f", steps=[PipelinePlanStep(id="clean", ref="flow:demo/clean")])
    pstore = InMemoryPipelineStore()
    rec = await _fresh_record(pstore, plan)
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        parked = await _wait_for(pstore, rec.pipeline_id, ("paused",))
        assert parked.step_state["clean"].status == "awaiting_advance"
        assert await _runs_of(d.store, rec.pipeline_id) == []
    finally:
        b.stop()


# ── #2540: cancel settles the child runs nothing will finish ────────────────


@register_operator(name="_t2540/gated", version="0.1.0")
class _Gated(Operation[_P, _P]):
    input_schema = _P
    output_schema = _P

    def run(self, inp, ctx):
        return _P(ran=True)


async def test_cancel_settles_a_policy_paused_run_and_leaves_a_running_one():
    """The policy-paused run can never finish, so cancel settles it and the
    approvals badge stops counting it. A `running` run is left to its own path:
    liveness cannot be judged across replicas (a leg that is still executing
    finishes; one whose process died after the cancel stays `running` — a known
    gap documented on `cancel_pipeline`)."""
    import httpx
    from fastapi import FastAPI

    from cairn.core.engine.schema import TriggerEvent
    from cairn.core.operator.registry import default_registry
    from cairn.core.storage.protocols import Run
    from cairn.core.triggers.routers import runs as runs_mod

    try:
        default_registry.register(_Gated, strict=False)
    except Exception:  # noqa: BLE001 — already registered is fine
        pass
    default_registry.attach_metadata(
        OperatorEntry(name="_t2540/gated", risk="external_write", requires_approval=True))
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    await b.start()
    try:
        plan = plan_pipeline(parse_pipeline_def(
            "name: c\nkind: pipeline\nsteps:\n"
            "  - id: gated\n    task: _t2540/gated\n"
            "  - id: after\n    task: _t1289/reader\n    depends_on: [gated]\n",
            pack="t2540", source="<test>"))
        rec = await b.start_pipeline(plan, inputs={})
        pid = rec.pipeline_id
        rec = await _wait_for(b.store, pid, ("paused",))
        gated_run = rec.step_state["gated"].run_id
        assert (await d.store.get(gated_run)).status == "awaiting_approval"
        # a leg of this pipeline left `running` by an earlier process: no step names it
        orphan = Run(id="run-orphan-2540", trigger=TriggerEvent(source="manual", payload={}),
                     status="running", template_ref="task:_t1289/reader",
                     bundle_pipeline_id=pid, step_id="after", workspace_id=rec.workspace_id)
        await d.store.create(orphan)

        app = FastAPI()
        app.include_router(runs_mod.build_runs_router(d, b, review_store=None))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url="http://t") as c:
            before = (await c.get("/runs/pending-approvals")).json()
            assert before["runs"] == 1, before

            await b.cancel_pipeline(pid)

            assert (await b.store.get(pid)).status == "cancelled"
            assert (await d.store.get(gated_run)).status == "cancelled"
            assert (await d.store.get(orphan.id)).status == "running"
            after = (await c.get("/runs/pending-approvals")).json()
            assert after["runs"] == 0 and after["awaiting_runs"] == [], after
            assert after["pipelines"] == 0, after
    finally:
        b.stop()


def _child_run(pid, step_id, status, *, rid, ref="task:_t1289/writer", ws="default"):
    from cairn.core.engine.schema import TriggerEvent
    from cairn.core.storage.protocols import Run
    return Run(id=rid, trigger=TriggerEvent(source="manual", payload={}), status=status,
               template_ref=ref, bundle_pipeline_id=pid, step_id=step_id, workspace_id=ws)


async def test_cancel_stops_a_live_local_running_leg():
    """ADR-0159 — a pipeline cancel cancels a running task leg this process
    executes (it used to leave it running to finish) and settles it cancelled;
    the worker thread a sync op runs in may finish, but its result is discarded."""
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    await b.start()
    try:
        rec = await b.start_pipeline(plan_pipeline(_def("_t1289/reader")), inputs={})
        run_id = (await b.store.get(rec.pipeline_id)).step_state["work"].run_id
        await b.cancel_pipeline(rec.pipeline_id)
        assert (await d.store.get(run_id)).status == "cancelled"
        await asyncio.sleep(2.2)                                    # the op's thread has returned
        assert (await d.store.get(run_id)).status == "cancelled"   # and wrote nothing
        settled = await b.store.get(rec.pipeline_id)
        assert settled.status == "cancelled"
        assert (settled.step_state["work"].status, settled.step_state["work"].verdict) == ("skipped", "cancelled")
    finally:
        b.stop()


async def test_cancel_never_touches_a_finalizer_run():
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    plan = PipelinePlan(name="fz", steps=[
        PipelinePlanStep(id="work", ref="task:_t1289/writer"),
        PipelinePlanStep(id="release", ref="task:_t1289/writer", finalizer=True)])
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    rec = await b.store.put(PipelineRecord(
        pipeline_name="fz", status="paused", plan=plan, inputs={},
        step_state={"work": StepState(status="awaiting_approval", run_id="r-work"),
                    "release": StepState(status="awaiting_approval", run_id="r-rel")}))
    await d.store.create(_child_run(rec.pipeline_id, "work", "awaiting_approval", rid="r-work"))
    await d.store.create(_child_run(rec.pipeline_id, "release", "awaiting_approval", rid="r-rel"))
    await b.cancel_pipeline(rec.pipeline_id)
    assert (await d.store.get("r-work")).status == "cancelled"
    assert (await d.store.get("r-rel")).status == "awaiting_approval"   # the teardown's own business


async def test_cancel_of_a_temporal_owned_pipeline_settles_its_policy_paused_child_run(monkeypatch):
    """#2555 (8) — cancelling the workflow cancels only the activity awaiting the
    run; nothing in the workflow settles a policy-paused run, so the cancel does,
    after the workflow cancel. A `running` leg is still the workflow's business."""
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    plan = PipelinePlan(name="t", steps=[PipelinePlanStep(id="work", ref="task:_t1289/writer"),
                                         PipelinePlanStep(id="side", ref="task:_t1289/writer")])
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    cancelled: list[str] = []

    async def _no_temporal(pid):
        cancelled.append(pid)
    monkeypatch.setattr(b, "_cancel_temporal", _no_temporal)
    rec = await b.store.put(PipelineRecord(
        pipeline_name="t", status="running", plan=plan, inputs={}, trigger={"owner": "temporal"},
        step_state={"work": StepState(status="awaiting_approval", run_id="r-t"),
                    "side": StepState(status="running", run_id="r-live")}))
    await d.store.create(_child_run(rec.pipeline_id, "work", "awaiting_approval", rid="r-t"))
    await d.store.create(_child_run(rec.pipeline_id, "side", "running", rid="r-live"))
    await b.cancel_pipeline(rec.pipeline_id)
    assert cancelled == [rec.pipeline_id]                                # the workflow first
    assert (await b.store.get(rec.pipeline_id)).status == "cancelled"
    assert (await d.store.get("r-t")).status == "cancelled"
    assert (await d.store.get("r-live")).status == "running"


async def test_cancel_does_not_announce_a_run_whose_update_was_refused(monkeypatch):
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    plan = PipelinePlan(name="g", steps=[PipelinePlanStep(id="work", ref="task:_t1289/writer")])
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    rec = await b.store.put(PipelineRecord(
        pipeline_name="g", status="paused", plan=plan, inputs={},
        step_state={"work": StepState(status="awaiting_approval", run_id="r-g")}))
    await d.store.create(_child_run(rec.pipeline_id, "work", "awaiting_approval", rid="r-g"))

    real_update = d.store.update

    async def _refuse_cancel(run_id, **fields):
        if fields.get("status") == "cancelled":
            return await d.store.get(run_id)       # what the guard does: apply nothing
        return await real_update(run_id, **fields)
    monkeypatch.setattr(d.store, "update", _refuse_cancel)
    emitted: list[tuple[str, str]] = []
    real_emit = d._emit

    async def _spy(run_id, type, data, **kw):
        emitted.append((run_id, type))
        return await real_emit(run_id, type, data, **kw)
    monkeypatch.setattr(d, "_emit", _spy)

    await b.cancel_pipeline(rec.pipeline_id)
    assert (await d.store.get("r-g")).status == "awaiting_approval"
    assert ("r-g", "cancelled") not in emitted


def _routed_def(op: str) -> object:
    return parse_pipeline_def(
        "name: w\nkind: pipeline\n"
        "inputs:\n  go: {type: bool, default: false}\n"
        "steps:\n"
        "  - id: u\n    task: _t1289/reader\n    when: $input.go\n"
        f"  - id: d\n    task: {op}\n    depends_on: [u]\n",
        pack="t2540", source="<test>")


async def _routed_off_then_d_ran(op: str):
    """One pre-crash pass routed `u` off and submitted `d`, and `d`'s run
    finished — but the record was never put: both still read `pending`."""
    pstore = InMemoryPipelineStore()
    rec = await _fresh_record(pstore, plan_pipeline(_routed_def(op)))
    rec.inputs = {"go": False}
    await pstore.put(rec)
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    await d.store.create(_child_run(rec.pipeline_id, "d", "running", rid="r-d-ran",
                                    ref=f"task:{op}", ws=rec.workspace_id))
    await d.store.update("r-d-ran", status="done")
    return pstore, d, rec.pipeline_id


async def test_a_write_leg_made_ready_by_a_route_on_adopt_is_parked_not_repeated():
    pstore, d, pid = await _routed_off_then_d_ran("_t1289/writer")
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        rec = await _wait_for(pstore, pid, ("paused",))
        assert rec.step_state["u"].status == "skipped"
        assert rec.step_state["d"].status == "awaiting_advance"
        assert [r.id for r in await _runs_of(d.store, pid)] == ["r-d-ran"]   # no new run
        await b.advance_step(pid, "d", approver="oncall")                    # a person decides
        assert (await _wait_for(pstore, pid, ("done", "failed"))).status == "done"
    finally:
        b.stop()


async def test_a_read_only_leg_made_ready_by_a_route_on_adopt_is_submitted():
    pstore, d, pid = await _routed_off_then_d_ran("_t1289/reader")
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "done"
        assert rec.step_state["d"].run_id not in (None, "r-d-ran")
    finally:
        b.stop()


# ── #2555: what restart and cancel still left behind ───────────────────────


def _plan2555(*steps):
    from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
    return PipelinePlan(name="r2555", steps=[
        PipelinePlanStep(id=i, ref=ref, depends_on=list(deps)) for i, ref, deps in steps])


async def _running_leg(pstore, runs, plan, sid, ref, *, run_status="running",
                       handle=None, output=None):
    """A record whose step `sid` is `running` on its run, as a dead process left it."""
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    rid = f"run-{sid}"
    states = {s.id: StepState(status="pending") for s in plan.steps}
    states[sid] = StepState(status="running", run_id=rid, handle=handle)
    rec = await pstore.put(PipelineRecord(pipeline_name="r", status="running", plan=plan,
                                          inputs={}, step_state=states, cursor=[sid]))
    await runs.create(_child_run(rec.pipeline_id, sid, "running", rid=rid, ref=ref))
    if run_status != "running":
        await runs.update(rid, status=run_status,
                          result_payloads=[{"output": output}] if output else [],
                          error="" if run_status == "done" else "op raised")
    return rec.pipeline_id, rid


@pytest.mark.parametrize("ref", ["flow:demo/clean", "demo/tmpl"])
async def test_a_running_leg_nothing_can_finish_is_failed_and_parked(ref, monkeypatch, _job_store):
    """(1) A flow leg without an engine handle, or an in-process template leg:
    nothing re-attaches to or resumes it, so its run is settled and a person
    decides — it is neither left `running` nor re-run."""
    from cairn.core import settings
    monkeypatch.setattr(settings, "runtime", lambda: "inprocess")
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    pid, rid = await _running_leg(pstore, d.store, _plan2555(("leg", ref, ())), "leg", ref)
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        rec = await _wait_for(pstore, pid, ("paused",))
        assert rec.step_state["leg"].status == "awaiting_advance"
        dead = await d.store.get(rid)
        assert dead.status == "failed"
        assert "died with the control plane restart" in (dead.error or "")
        assert [r.id for r in await _runs_of(d.store, pid)] == [rid]       # no new run
    finally:
        b.stop()


async def test_a_template_leg_under_a_durable_runtime_is_left_to_it(monkeypatch):
    """DBOS recovers the template run and the reconciler reports its terminal."""
    from cairn.core import settings
    monkeypatch.setattr(settings, "runtime", lambda: "dbos")
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    pid, rid = await _running_leg(pstore, d.store, _plan2555(("leg", "demo/tmpl", ())),
                                  "leg", "demo/tmpl")
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        assert (await pstore.get(pid)).step_state["leg"].status == "running"
        assert (await d.store.get(rid)).status == "running"
        assert b._run_to_pipeline[rid] == pid                              # re-mapped as before
    finally:
        b.stop()


async def test_a_running_flow_leg_with_a_handle_is_still_readopted(monkeypatch):
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    pid, rid = await _running_leg(pstore, d.store, _plan2555(("leg", "flow:demo/clean", ())),
                                  "leg", "flow:demo/clean",
                                  handle={"job_id": "j1", "engine": "builtin", "meta": {}})
    b = PipelineBridge(d, store=pstore)
    readopted: list[str] = []

    async def _readopt(rec, s, st):
        readopted.append(s.id)
    monkeypatch.setattr(b, "_readopt_flow_leg", _readopt)
    await b.start()
    try:
        assert readopted == ["leg"]
        assert (await pstore.get(pid)).step_state["leg"].status == "running"
        assert (await d.store.get(rid)).status == "running"
    finally:
        b.stop()


async def test_a_running_task_step_whose_run_is_done_completes_with_its_output():
    """(4) The leg finished before the crash; only its terminal was never applied.
    Its persisted output is the step's, the pipeline advances, nothing re-runs."""
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    plan = _plan2555(("work", "task:_t1289/writer", ()),
                     ("after", "task:_t1289/reader", ("work",)))
    pid, rid = await _running_leg(pstore, d.store, plan, "work", "task:_t1289/writer",
                                  run_status="done", output={"wrote": "once"})
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "done"
        assert rec.step_state["work"].status == "done"
        assert rec.step_state["work"].run_id == rid
        assert (await b._collected_outputs(rec))["work"] == {"wrote": "once"}
        assert [r.id for r in await _runs_of(d.store, pid) if r.step_id == "work"] == [rid]
        assert (await d.store.get(rid)).status == "done"
    finally:
        b.stop()


async def test_a_running_task_step_whose_run_failed_fails_the_pipeline():
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    plan = _plan2555(("work", "task:_t1289/writer", ()),
                     ("after", "task:_t1289/reader", ("work",)))
    pid, rid = await _running_leg(pstore, d.store, plan, "work", "task:_t1289/writer",
                                  run_status="failed")
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "failed"
        assert rec.step_state["work"].status == "failed"
        assert rec.step_state["after"].status == "pending"
        assert "step 'work' failed" in str((rec.trigger or {}).get("failure_reason", ""))
        assert [r.id for r in await _runs_of(d.store, pid)] == [rid]
        assert (await d.store.get(rid)).error == "op raised"               # not rewritten
    finally:
        b.stop()


@pytest.fixture
def _captured(monkeypatch):
    from cairn.core import gate_notify
    from cairn.core.notifier import NotifierRegistry
    from tests.core.test_gate_notifications import _CapturingNotifier

    gate_notify.reset_gate_registry()
    monkeypatch.setattr(gate_notify, "_profile_notify", lambda: {})
    sink, reg = _CapturingNotifier(), NotifierRegistry()
    reg.register(sink)
    yield reg, sink.events
    gate_notify.reset_gate_registry()


async def test_a_task_leg_parked_by_restart_notifies_once(_captured):
    """(5) #1289's park is a human wait: one `awaiting_advance` notification."""
    reg, events = _captured
    backend, pstore, pid, _ = await _start_and_kill_mid_leg("_t1289/writer")
    d2 = RunDispatcher(FakeRunner(), backend=backend)
    b2 = PipelineBridge(d2, store=pstore)
    b2._notify_registry = reg
    await b2.start()
    try:
        await _wait_for(pstore, pid, ("paused",))
        assert [(e.kind, e.step_id) for e in events] == [("awaiting_advance", "work")]
        assert "restart" in events[0].reason
    finally:
        b2.stop()
    b3 = PipelineBridge(RunDispatcher(FakeRunner(), backend=backend), store=pstore)
    b3._notify_registry = reg
    await b3.start()                    # a second restart re-announces nothing
    b3.stop()
    assert len(events) == 1


async def test_a_step_parked_by_the_adopt_pass_notifies_once(_captured):
    reg, events = _captured
    pstore = InMemoryPipelineStore()
    rec = await _fresh_record(pstore, plan_pipeline(_def("_t1289/writer")))
    b = PipelineBridge(RunDispatcher(FakeRunner(), backend=build_storage()), store=pstore)
    b._notify_registry = reg
    await b.start()
    try:
        await _wait_for(pstore, rec.pipeline_id, ("paused",))
        assert [(e.kind, e.step_id) for e in events] == [("awaiting_advance", "work")]
        assert "may already have run" in events[0].reason
    finally:
        b.stop()


# ── #2555 review: remote-job pauses, handled flow legs, late terminals ─────


@pytest.fixture
def _job_store(tmp_path, monkeypatch):
    """An empty, private remote-job store (never the developer's sqlite)."""
    from cairn.core.operator.job_store import get_remote_job_store, reset_remote_job_store

    monkeypatch.delenv("CAIRN_JOB_STORE", raising=False)
    monkeypatch.setenv("CAIRN_JOB_STORE_DB", str(tmp_path / "jobs.sqlite"))
    reset_remote_job_store()
    yield get_remote_job_store()
    reset_remote_job_store()


def _terminal_msg(pid, rid, status="done"):
    from types import SimpleNamespace

    from cairn.core.storage.protocols import PIPELINE_ID_KEY
    return SimpleNamespace(topic=f"run.{status}", correlation_id=rid,
                           payload={PIPELINE_ID_KEY: pid})


@pytest.fixture
def _leg_finished(monkeypatch):
    """Every `leg.finished` the bridge announces, as (step_id, status)."""
    from cairn.core.pack import run_hooks

    seen: list[tuple] = []
    real = run_hooks.fire

    async def _fire(event, **payload):
        if event == run_hooks.LEG_FINISHED:
            seen.append((payload.get("step_id"), payload.get("status")))
        return await real(event, **payload)
    monkeypatch.setattr(run_hooks, "fire", _fire)
    return seen


async def test_an_inprocess_template_leg_paused_on_a_remote_job_is_left_to_the_control_loop(
        monkeypatch, _job_store):
    """The dispatcher keeps a run paused on a remote job `running`, and the
    remote-job control loop resumes it — so adopt only re-maps it, and its
    later `done` advances the pipeline."""
    from cairn.core import settings
    from cairn.core.operator.job_store import RUNNING, RemoteJobRecord
    monkeypatch.setattr(settings, "runtime", lambda: "inprocess")
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    plan = _plan2555(("train", "demo/tmpl", ()), ("after", "task:_t1289/reader", ("train",)))
    pid, rid = await _running_leg(pstore, d.store, plan, "train", "demo/tmpl")
    await _job_store.register(RemoteJobRecord(run_id=rid, node_name="fit", provider="runpod",
                                              job_id="rp-1", backend="remote_job", status=RUNNING))
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        assert (await pstore.get(pid)).step_state["train"].status == "running"
        assert (await d.store.get(rid)).status == "running"                # the GPU job is untouched
        assert b._run_to_pipeline[rid] == pid
        await d.store.update(rid, status="done", result_payloads=[{"output": {"model": "m1"}}])
        await b._on_run_terminal(_terminal_msg(pid, rid))                # the control loop's resume
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert rec.status == "done"
        assert (await b._collected_outputs(rec))["train"] == {"model": "m1"}
    finally:
        b.stop()


@pytest.mark.parametrize("finished", ["done", "failed"])
async def test_a_running_flow_leg_with_a_handle_whose_run_finished_is_settled(finished, monkeypatch):
    """The leg's terminal was written but its event never applied: re-adopting
    would watch nothing, so the run's own terminal decides the step."""
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    plan = _plan2555(("leg", "flow:demo/clean", ()), ("after", "task:_t1289/reader", ("leg",)))
    pid, rid = await _running_leg(pstore, d.store, plan, "leg", "flow:demo/clean",
                                  run_status=finished, output={"rows": 7},
                                  handle={"job_id": "j1", "engine": "builtin", "meta": {}})
    b = PipelineBridge(d, store=pstore)
    readopted: list[str] = []

    async def _readopt(rec, s, st):
        readopted.append(s.id)
    monkeypatch.setattr(b, "_readopt_flow_leg", _readopt)
    await b.start()
    try:
        rec = await _wait_for(pstore, pid, ("done", "failed"))
        assert readopted == []
        assert rec.status == finished
        assert rec.step_state["leg"].status == finished
        if finished == "done":
            assert (await b._collected_outputs(rec))["leg"] == {"rows": 7}
        assert [r.id for r in await _runs_of(d.store, pid) if r.step_id == "leg"] == [rid]
    finally:
        b.stop()


async def test_a_late_done_for_a_parked_step_is_ignored(_leg_finished):
    """A refused store update can still be announced (a remote-job resume, a flow
    leg's emit): it must not un-park a step a person now owns, nor advance."""
    from cairn.core.storage.pipeline_store import PipelineRecord, StepState

    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    plan = _plan2555(("work", "task:_t1289/writer", ()), ("after", "task:_t1289/reader", ("work",)))
    rec = await pstore.put(PipelineRecord(
        pipeline_name="r", status="paused", plan=plan, inputs={},
        step_state={"work": StepState(status="awaiting_advance", run_id="run-work"),
                    "after": StepState(status="pending")}))
    await d.store.create(_child_run(rec.pipeline_id, "work", "done", rid="run-work"))
    b = PipelineBridge(d, store=pstore)
    await b._on_run_terminal(_terminal_msg(rec.pipeline_id, "run-work"))
    cur = await pstore.get(rec.pipeline_id)
    assert cur.status == "paused"
    assert cur.step_state["work"].status == "awaiting_advance"
    assert cur.step_state["after"].status == "pending"
    assert await _runs_of(d.store, rec.pipeline_id) == [await d.store.get("run-work")]
    assert _leg_finished == []


async def test_leg_finished_fires_once_when_adopt_and_a_live_delivery_race(_leg_finished):
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    plan = _plan2555(("work", "task:_t1289/writer", ()))
    pid, rid = await _running_leg(pstore, d.store, plan, "work", "task:_t1289/writer",
                                  run_status="done", output={"wrote": "once"})
    b = PipelineBridge(d, store=pstore)
    await b.start()                                       # adopt applies the terminal
    try:
        await b._on_run_terminal(_terminal_msg(pid, rid))  # the bus delivers it too
        assert (await _wait_for(pstore, pid, ("done", "failed"))).status == "done"
        assert _leg_finished == [("work", "done")]
    finally:
        b.stop()


async def test_a_claimed_paused_template_run_resumed_to_done_completes_its_step():
    """A claimed step waits `awaiting_approval` on its paused run; the run-level
    door can resume that run to `done`. That `done` is the step's — releasing
    the step later would start the paid work again."""
    pstore = InMemoryPipelineStore()
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    plan = _plan2555(("tmpl", "demo/tmpl", ()), ("after", "task:_t1289/reader", ("tmpl",)))
    rec = await _fresh_record(pstore, plan)
    await d.store.create(_child_run(rec.pipeline_id, "tmpl", "awaiting_approval",
                                    rid="run-tmpl", ref="demo/tmpl"))
    b = PipelineBridge(d, store=pstore)
    await b.start()
    try:
        cur = await _wait_for(pstore, rec.pipeline_id, ("paused",))
        assert cur.step_state["tmpl"].status == "awaiting_approval"
        assert cur.step_state["tmpl"].run_id == "run-tmpl"
        await d.store.update("run-tmpl", status="done",
                             result_payloads=[{"output": {"answer": 42}}])
        await b._on_run_terminal(_terminal_msg(rec.pipeline_id, "run-tmpl"))   # resume_run's emit
        done = await _wait_for(pstore, rec.pipeline_id, ("done", "failed"))
        assert done.status == "done"
        assert (await b._collected_outputs(done))["tmpl"] == {"answer": 42}
        assert [r.id for r in await _runs_of(d.store, rec.pipeline_id)
                if r.step_id == "tmpl"] == ["run-tmpl"]                       # no new run
    finally:
        b.stop()


async def test_a_terminal_for_a_missing_pipeline_announces_nothing(_leg_finished):
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    await d.store.create(_child_run("pl-gone", "work", "done", rid="run-gone"))
    b = PipelineBridge(d, store=InMemoryPipelineStore())
    await b._on_run_terminal(_terminal_msg("pl-gone", "run-gone"))
    assert _leg_finished == []


async def test_an_active_remote_job_older_than_the_newest_finished_ones_is_found():
    """The lookup asks for active jobs by status, so newer finished jobs of
    other runs cannot push an active one out of the listing."""
    from cairn.core.operator.job_control import active_remote_jobs_for_run
    from cairn.core.operator.job_store import (
        DONE,
        RUNNING,
        InMemoryRemoteJobStore,
        RemoteJobRecord,
    )

    store = InMemoryRemoteJobStore()
    await store.register(RemoteJobRecord(run_id="run-old", node_name="fit", provider="runpod",
                                         job_id="j-old", status=RUNNING,
                                         created_at="2020-01-01T00:00:00+00:00"))
    for i in range(501):
        await store.register(RemoteJobRecord(run_id=f"run-{i}", node_name="fit",
                                             provider="runpod", job_id=f"j-{i}", status=DONE))
    found = await active_remote_jobs_for_run("run-old", store=store)
    assert [r.job_id for r in found] == ["j-old"]
