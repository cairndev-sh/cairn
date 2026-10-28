"""#127 / ADR-0038 — the remote-job control worker (claim → drive → resume).

poll/resume are injected fakes, so the loop is exercised with no provider/dispatcher.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from pydantic import BaseModel

from cairn.core.operator.job_control import build_default_poll, build_default_resume, drive_once
from cairn.core.operator.job_store import DONE, FAILED, RUNNING, InMemoryRemoteJobStore, RemoteJobRecord
from cairn.core.operator.jobs import JobHandle, JobStatus


def _rec(run_id, node="train"):
    return RemoteJobRecord(run_id=run_id, node_name=node, provider="runpod", job_id=f"j-{run_id}", backend="remote_job")


async def _seed(store, *run_ids):
    for r in run_ids:
        await store.register(_rec(r))


async def test_succeeded_marks_done_and_resumes():
    store = InMemoryRemoteJobStore()
    await _seed(store, "r1")
    resumed = []

    async def poll(rec):
        return JobStatus(state="succeeded", output={"adapter_uri": "s3://b/a"})

    async def resume(rec, status):
        resumed.append((rec.run_id, status.output))

    tally = await drive_once(store, poll, resume)
    assert tally == {"done": 1, "failed": 0, "cancelled": 0, "aborted": 0, "relaunched": 0,
                     "running": 0, "claimed": 1}
    assert resumed == [("r1", {"adapter_uri": "s3://b/a"})]
    rec = await store.get("r1:train")
    assert rec.status == DONE and rec.result == {"adapter_uri": "s3://b/a"}
    assert await store.claim_active() == []  # no longer active


async def test_running_stays_active_and_no_resume():
    store = InMemoryRemoteJobStore()
    await _seed(store, "r1")
    resumed = []

    async def poll(rec):
        return JobStatus(state="running")

    async def resume(rec, status):
        resumed.append(rec.run_id)

    tally = await drive_once(store, poll, resume)
    assert tally["running"] == 1 and tally["done"] == 0
    assert resumed == []
    assert len(await store.claim_active()) == 1  # still active for the next tick


async def test_failed_marks_failed_and_resumes():
    store = InMemoryRemoteJobStore()
    await _seed(store, "r1")
    resumed = []

    async def poll(rec):
        return JobStatus(state="failed", error="OOM")

    async def resume(rec, status):
        resumed.append((rec.run_id, status.state, status.error))

    tally = await drive_once(store, poll, resume)
    assert tally["failed"] == 1
    assert resumed == [("r1", "failed", "OOM")]
    assert (await store.get("r1:train")).status == FAILED


async def test_poll_raising_fails_the_job_not_the_loop():
    store = InMemoryRemoteJobStore()
    await _seed(store, "r1", "r2")
    seen = []

    async def poll(rec):
        if rec.run_id == "r1":
            raise RuntimeError("provider 500")
        return JobStatus(state="succeeded", output={})

    async def resume(rec, status):
        seen.append((rec.run_id, status.state))

    tally = await drive_once(store, poll, resume)
    # r1's poll blew up → failed; r2 still processed → done. Loop survived.
    assert tally["failed"] == 1 and tally["done"] == 1
    assert (await store.get("r1:train")).status == FAILED
    assert "RuntimeError" in (await store.get("r1:train")).error
    assert (await store.get("r2:train")).status == DONE


async def test_resume_failure_does_not_crash_the_loop():
    store = InMemoryRemoteJobStore()
    await _seed(store, "r1")

    async def poll(rec):
        return JobStatus(state="succeeded", output={})

    async def resume(rec, status):
        raise RuntimeError("dispatcher down")

    tally = await drive_once(store, poll, resume)  # must not raise
    assert tally["done"] == 1
    assert (await store.get("r1:train")).status == DONE  # job still recorded done


# ── default wiring: poll resolves the operator; resume calls the dispatcher ──


class _Inp(BaseModel):
    x: int = 1


class _Out(BaseModel):
    ok: bool = True


async def test_build_default_poll_resolves_op_and_threads_compute(monkeypatch):
    seen = {}

    class StubOp:
        name = "stub/gpu"
        input_schema = _Inp
        output_schema = _Out

        async def poll(self, handle, inp, ctx):
            seen.update(job_id=handle.job_id, x=inp.x, compute=ctx.compute)
            return JobStatus(state="succeeded", output={"ok": True})

    class FakeReg:
        def get(self, name):
            assert name == "stub/gpu"
            return StubOp

    from cairn.core.connections import ResolvedConnection

    async def fake_resolve(name, *, store=None, account_id=None):
        return ResolvedConnection(name=name, kind="compute.runpod", config={"gpu": "A100"}, secrets={})

    monkeypatch.setattr("cairn.core.connections.resolve_connection", fake_resolve)

    poll = build_default_poll(FakeReg())
    rec = RemoteJobRecord(run_id="r1", node_name="train", provider="stub/gpu", job_id="j1", backend="remote_job", compute_target="rp", input={"x": 7})
    status = await poll(rec)
    assert status.state == "succeeded"
    assert seen["job_id"] == "j1" and seen["x"] == 7 and seen["compute"].config["gpu"] == "A100"


async def test_build_default_resume_calls_dispatcher():
    calls = []

    class FakeDispatcher:
        async def resume_run(self, run_id, decision):
            calls.append((run_id, decision))

    resume = build_default_resume(FakeDispatcher())
    rec = RemoteJobRecord(run_id="r1", node_name="train", provider="p", job_id="j", backend="remote_job")
    await resume(rec, JobStatus(state="succeeded", output={"adapter": "s3://a"}))
    await resume(rec, JobStatus(state="failed", error="OOM"))
    assert calls[0] == ("r1", {"status": "succeeded", "result": {"adapter": "s3://a"}})
    assert calls[1][1] == {"status": "failed", "error": "OOM"}


# ── durable runtime path: idempotent submit → pause → return-on-resume ──


async def test_durable_path_idempotent_submit_then_returns_on_resume(monkeypatch):
    from cairn.core.operator import job_store as JS
    from cairn.core.operator import runtime as RT
    from cairn.core.operator.binding import ExecutionField, OperationBinding
    from cairn.core.operator.protocol import OperationContext

    shared = InMemoryRemoteJobStore()
    monkeypatch.setattr(JS, "get_remote_job_store", lambda: shared)

    submits = []

    class StubOp:
        name = "stub/gpu"
        input_schema = _Inp
        output_schema = _Out

        async def submit(self, inp, ctx):
            submits.append(1)
            return JobHandle(job_id="j1", backend="remote_job")

    # mock LangGraph interrupt() to raise (simulate the pause)
    import langgraph.types as LT

    class _Pause(Exception):
        pass

    def _interrupt(payload):
        raise _Pause()

    monkeypatch.setattr(LT, "interrupt", _interrupt)

    op = StubOp()
    ctx = OperationContext(pipeline_id="r1", node_name="train")
    binding = OperationBinding(operator="stub/gpu", input={}, output={}, execution=ExecutionField(compute_target="rp"))

    # first pass: no record → submit + register + pause
    with pytest.raises(_Pause):
        await RT._invoke_remote_job_durable(op, _Inp(), ctx, binding, "stub/gpu", run_id="r1", node_name="train", resolved_input={"x": 1})
    assert submits == [1]
    rec = await shared.get("r1:train")
    assert rec is not None and rec.compute_target == "rp" and rec.input == {"x": 1}

    # worker finishes → mark done; resume re-runs the node → returns, NO re-submit
    await shared.mark_done("r1:train", {"ok": True})
    out = await RT._invoke_remote_job_durable(op, _Inp(), ctx, binding, "stub/gpu", run_id="r1", node_name="train", resolved_input={"x": 1})
    assert submits == [1] and isinstance(out, dict)  # idempotent — not re-dispatched


# ── the job's workspace is the one in scope ───────────────────────────────


def _durable_world(monkeypatch):
    from cairn.core.operator import job_store as JS

    shared = InMemoryRemoteJobStore()
    monkeypatch.setattr(JS, "get_remote_job_store", lambda: shared)
    submits: list = []
    pins: list = []

    class StubOp:
        name = "stub/gpu"
        input_schema = _Inp
        output_schema = _Out

        async def submit(self, inp, ctx):
            submits.append(1)
            return JobHandle(job_id="j1", backend="remote_job")

    async def _pins(*a, **kw):
        pins.append(1)

    import langgraph.types as LT

    class _Pause(Exception):
        pass

    def _interrupt(payload):
        raise _Pause()

    monkeypatch.setattr(LT, "interrupt", _interrupt)
    monkeypatch.setattr("cairn.core.operator.pins.record_train_pins", _pins)
    return shared, StubOp(), submits, pins, _Pause


@pytest.mark.parametrize("target", ["worker:gpu", "rp"])
async def test_durable_job_with_auth_on_and_no_workspace_in_scope_is_refused(monkeypatch, target):
    """A pull-lane (``worker:<pool>``) or pushed job registered from an operation
    node outside any workspace scope, with auth on, is refused before anything is
    registered or dispatched, instead of being stamped with the process default."""
    from cairn.core.operator import runtime as RT
    from cairn.core.operator.binding import ExecutionField, OperationBinding
    from cairn.core.operator.protocol import OperationContext
    from cairn.core.workspace import WorkspaceScopeRequired

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    monkeypatch.setenv("CAIRN_WORKSPACE", "default")
    shared, op, submits, pins, _ = _durable_world(monkeypatch)
    binding = OperationBinding(operator="stub/gpu", input={}, output={},
                               execution=ExecutionField(compute_target=target))
    with pytest.raises(WorkspaceScopeRequired):
        await RT._invoke_remote_job_durable(op, _Inp(), OperationContext(pipeline_id="r1", node_name="train"),
                                            binding, "stub/gpu", run_id="r1", node_name="train",
                                            resolved_input={"x": 1})
    assert await shared.get("r1:train") is None
    assert submits == [] and pins == []


@pytest.mark.parametrize("target", ["worker:gpu", "rp"])
async def test_durable_job_is_stamped_with_the_workspace_in_scope(monkeypatch, target):
    from cairn.core.operator import runtime as RT
    from cairn.core.operator.binding import ExecutionField, OperationBinding
    from cairn.core.operator.protocol import OperationContext
    from cairn.core.workspace import workspace_scope

    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    shared, op, submits, pins, _Pause = _durable_world(monkeypatch)
    binding = OperationBinding(operator="stub/gpu", input={}, output={},
                               execution=ExecutionField(compute_target=target))
    with workspace_scope("ws-a"), pytest.raises(_Pause):
        await RT._invoke_remote_job_durable(op, _Inp(), OperationContext(pipeline_id="r1", node_name="train"),
                                            binding, "stub/gpu", run_id="r1", node_name="train",
                                            resolved_input={"x": 1})
    rec = await shared.get("r1:train")
    assert rec is not None and rec.workspace_id == "ws-a"


# ── #552 — relaunch-from-checkpoint on a resumable death ──────────────────


async def _seed_ck(store, run_id, uri="s3://b/ck/r/", attempts=0):
    rec = RemoteJobRecord(
        run_id=run_id, node_name="train", provider="runpod", job_id=f"j-{run_id}",
        backend="remote_job", checkpoint_uri=uri, attempts=attempts,
    )
    await store.register(rec)
    return rec


async def test_retriable_death_with_checkpoint_relaunches():
    store = InMemoryRemoteJobStore()
    await _seed_ck(store, "r1")
    resumed, relaunched = [], []

    async def poll(rec):
        return JobStatus(state="failed", retriable=True, error="box gone")

    async def resume(rec, status):
        resumed.append(rec.run_id)

    async def relaunch(rec):
        relaunched.append(rec.run_id)
        return JobHandle(job_id="j-new", backend="ssh", meta={"host": "box2"})

    tally = await drive_once(store, poll, resume, relaunch=relaunch, max_attempts=2)
    assert tally["relaunched"] == 1 and tally["failed"] == 0
    assert relaunched == ["r1"] and resumed == []  # run stays paused
    rec = await store.get("r1:train")
    assert rec.status == "submitted" and rec.attempts == 1 and rec.job_id == "j-new"
    assert len(await store.claim_active()) == 1  # still active for the relaunched job


async def test_retriable_death_without_checkpoint_fails():
    store = InMemoryRemoteJobStore()
    await store.register(_rec("r1"))  # no checkpoint_uri → nothing to resume from
    resumed = []

    async def poll(rec):
        return JobStatus(state="failed", retriable=True, error="box gone")

    async def resume(rec, status):
        resumed.append(rec.run_id)

    async def relaunch(rec):
        raise AssertionError("must not relaunch without a durable checkpoint")

    tally = await drive_once(store, poll, resume, relaunch=relaunch, max_attempts=2)
    assert tally["failed"] == 1 and tally["relaunched"] == 0 and resumed == ["r1"]


async def test_clean_error_not_relaunched_even_with_checkpoint():
    store = InMemoryRemoteJobStore()
    await _seed_ck(store, "r1")
    resumed = []

    async def poll(rec):
        return JobStatus(state="failed", retriable=False, error="loss=nan")

    async def resume(rec, status):
        resumed.append(rec.run_id)

    async def relaunch(rec):
        raise AssertionError("a clean training error is not retriable")

    tally = await drive_once(store, poll, resume, relaunch=relaunch, max_attempts=2)
    assert tally["failed"] == 1 and tally["relaunched"] == 0 and resumed == ["r1"]


async def test_relaunch_budget_exhausted_fails():
    store = InMemoryRemoteJobStore()
    await _seed_ck(store, "r1", attempts=2)  # already at max_attempts

    async def poll(rec):
        return JobStatus(state="failed", retriable=True, error="box gone")

    async def resume(rec, status):
        pass

    async def relaunch(rec):
        raise AssertionError("attempt budget is spent")

    tally = await drive_once(store, poll, resume, relaunch=relaunch, max_attempts=2)
    assert tally["failed"] == 1 and tally["relaunched"] == 0


async def test_checkpoint_uri_persisted_from_running_metrics():
    store = InMemoryRemoteJobStore()
    await store.register(_rec("r1"))

    async def poll(rec):
        return JobStatus(state="running", metrics={"checkpoint_uri": "s3://b/ck/r1/"})

    async def resume(rec, status):
        pass

    await drive_once(store, poll, resume)
    rec = await store.get("r1:train")
    assert rec.checkpoint_uri == "s3://b/ck/r1/"


async def test_log_tail_persisted_from_poll():
    # #553: drive_once records the latest stdout tail so the console can show it.
    store = InMemoryRemoteJobStore()
    await store.register(_rec("r1"))

    async def poll(rec):
        return JobStatus(state="running", log_tail="step 5 loss 1.2")

    async def resume(rec, status):
        pass

    await drive_once(store, poll, resume)
    rec = await store.get("r1:train")
    assert rec.last_log_tail == "step 5 loss 1.2"


# ── ADR-0151 D9: `cancelled` is a terminal JobStatus state ────────────────────
async def test_cancelled_closes_the_record_and_resumes_as_cancelled():
    """A job stopped by a cancel (remote/exec's marker) is closed cancelled and
    resumed as such — never counted running, never fed to the learning gate."""
    from cairn.core.operator.job_store import CANCELLED

    store = InMemoryRemoteJobStore()
    await _seed(store, "r1")
    resumed, cancels = [], []

    async def poll(rec):
        return JobStatus(state="cancelled", error="remote job cancelled",
                         metrics={"eval_losses": [3.0, 3.1, 3.2]})

    async def resume(rec, status):
        resumed.append((rec.run_id, status.state))

    async def cancel(rec):
        cancels.append(rec.run_id)

    from cairn.core.operator.learning import LearningConfig

    tally = await drive_once(store, poll, resume, cancel=cancel,
                             learning=LearningConfig(patience=1, warmup=0))
    assert tally["running"] == 0 and tally["aborted"] == 0, tally
    assert cancels == [], "the learning gate must not run on a cancelled job"
    assert tally["cancelled"] == 1
    assert resumed == [("r1", "cancelled")]
    assert (await store.get("r1:train")).status == CANCELLED
    assert await store.claim_active() == []


async def test_default_resume_keeps_cancelled_distinct_from_failed():
    seen: list[tuple[str, dict]] = []

    class _D:
        async def resume_run(self, run_id, decision):
            seen.append((run_id, decision))

    resume = build_default_resume(_D())
    await resume(_rec("r1"), JobStatus(state="cancelled", error="remote job cancelled"))
    assert seen == [("r1", {"status": "cancelled", "error": "remote job cancelled"})]
