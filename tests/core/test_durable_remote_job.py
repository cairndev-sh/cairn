"""#564 / #536 — the durable remote-job path COMPLETES a run on ``in_process``.

The dress-rehearsal proved training succeeds on the box but the *run* didn't
complete under Temporal (record not tracked, resume never finished — #564). The
supported, tested durable path is ``OBS_RUNTIME=in_process`` + a shared job store
+ a durable checkpointer. This test drives that path end-to-end, hermetically,
with a fake remote-job operator (no SSH/provider):

    compile → invoke → pause at the remote-job interrupt (kind=remote_job, ADR-0065)
           → control loop polls `succeeded` + marks the record DONE
           → graph resumes → the node re-runs, reads the terminal record, returns
             its result → the run COMPLETES with the output binding applied.

It also proves the #564 run-identity guard: a node reached with no run_id fails
LOUD instead of silently registering a colliding ``:node`` record.

Requires the ``runtime-sqlite`` extra (durable checkpointer) — skipped otherwise.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

# runtime-sqlite extra — the durable checkpointer the resume rehydrates from.
pytest.importorskip("langgraph.checkpoint.sqlite.aio")

from pydantic import BaseModel

from cairn.core.engine.compiler import compile_template
from cairn.core.interrupts import KIND_REMOTE_JOB, interrupt_kind
from cairn.core.operator import (
    Operation,
    OperationContext,
    default_registry,
    register_operator,
)
from cairn.core.operator.job_control import drive_once
from cairn.core.operator.job_store import (
    DONE,
    get_remote_job_store,
    reset_remote_job_store,
)
from cairn.core.operator.jobs import JobHandle, JobStatus
from cairn.core.pack.manifest import OperatorEntry
from cairn.core.runtime import InProcessRuntime
from cairn.core.runtime.checkpointer import reset_checkpointer


class _RIn(BaseModel):
    pass


class _ROut(BaseModel):
    adapter_uri: str = ""


@register_operator(
    name="test/remote_durable",
    version="0.1.0",
    capabilities=["test.remote"],
)
class _RemoteDurableOp(Operation):
    """A remote-job operator: submit/poll/cancel make it drive through the
    ``remote_job`` executor (is_remote_job_operator == True)."""

    input_schema = _RIn
    output_schema = _ROut

    async def run(self, inp, ctx: OperationContext) -> _ROut:  # in-process fallback
        return _ROut(adapter_uri="local")

    async def submit(self, inp, ctx: OperationContext) -> JobHandle:
        return JobHandle(job_id="job-1", backend="remote_job")

    async def poll(self, handle: JobHandle, inp, ctx: OperationContext) -> JobStatus:
        return JobStatus(state="succeeded", output=_ROut(adapter_uri="s3://bucket/adapter"))

    async def cancel(self, handle: JobHandle) -> None:
        return None


def _ensure() -> None:
    name = "test/remote_durable"
    if not default_registry.has(name):
        default_registry.register(_RemoteDurableOp)
    # executors=["remote_job"] → pick_executor routes to the job driver, which
    # (with OBS_REMOTE_JOB_DURABLE=1) takes the durable submit+pause path.
    default_registry.attach_metadata(
        OperatorEntry(
            name=name,
            executors=["remote_job"],
            provides_capabilities=["test.remote"],
        )
    )


def _remote_template(tmp_path: Path) -> Path:
    (tmp_path / "templates").mkdir()
    (tmp_path / "operations").mkdir()
    (tmp_path / "operations" / "remote.yaml").write_text(textwrap.dedent("""\
        operator: test/remote_durable
        input: {}
        output:
          adapter_uri: $state.findings.remote_uri
    """))
    tmpl = tmp_path / "templates" / "remote.md"
    tmpl.write_text(textwrap.dedent("""\
        ---
        name: remote-durable
        version: 0.1.0
        flow:
          type: state_machine
          source: behavior_tree
          root: { type: operation, ref: remote }
          nodes: {}
        tools:
          required: []
        ---
        # remote durable
    """))
    return tmpl


def _durable_env(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OBS_REMOTE_JOB_DURABLE", "1")
    monkeypatch.setenv("OBS_CHECKPOINTER", "sqlite")
    monkeypatch.setenv("OBS_CHECKPOINTER_PATH", str(tmp_path / "ckpt.sqlite"))
    # Shared, durable remote-job store (sqlite singleton) — the node writes the
    # record and the control loop reads it, exactly as server↔node share it in prod.
    monkeypatch.delenv("OBS_STORAGE", raising=False)  # force non-postgres selector
    monkeypatch.delenv("OBS_JOB_STORE", raising=False)  # sqlite default (not memory)
    monkeypatch.setenv("OBS_JOB_STORE_DB", str(tmp_path / "jobs.sqlite"))
    reset_checkpointer()
    reset_remote_job_store()
    _ensure()


@pytest.mark.asyncio
async def test_durable_remote_job_completes_in_process(tmp_path, monkeypatch):
    _durable_env(tmp_path, monkeypatch)
    tmpl = _remote_template(tmp_path)

    # ── invoke → the node submits the job + PAUSES at the remote-job interrupt ──
    compiled = compile_template(tmpl)
    assert compiled.runnable._compiled_graph.checkpointer is not None
    rt = InProcessRuntime()
    graph = compiled.runnable._compiled_graph
    # The runner injects run_id into initial_state (runner.py:326 — "compiler.run
    # reads initial_state.run_id"); mirror that, since this test drives the graph
    # directly rather than through the runner.
    paused = await rt.invoke(graph, {"run_id": "run-remote-1"}, run_id="run-remote-1")
    assert "__interrupt__" in paused

    # ADR-0065: this pause is a RUNNING remote job, NOT a human-approval gate.
    intr = paused["__interrupt__"]
    items = list(intr) if isinstance(intr, (list, tuple)) else [intr]
    assert interrupt_kind(getattr(items[0], "value", None)) == KIND_REMOTE_JOB

    # The record was registered under the run's identity (#564: run_id:node) and
    # is NOT yet terminal — it's active, waiting for the control loop.
    store = get_remote_job_store()
    rec = await store.get("run-remote-1:remote")
    assert rec is not None and rec.status != DONE
    assert await store.claim_active()  # the loop has something to drive

    # ── control loop: poll → succeeded → mark DONE → resume the paused run ──────
    resumed: list[dict] = []

    async def poll(_rec):
        return JobStatus(state="succeeded", output={"adapter_uri": "s3://bucket/adapter"})

    async def resume(_rec, status):
        # What build_default_resume→dispatcher.resume_run does under in_process:
        # recompile/resume from the durable checkpoint. Here the graph object is
        # in hand, so resume it directly (same thread_id == run_id).
        decision = {"status": status.state, "result": status.output}
        resumed.append(await rt.resume(graph, decision, run_id=_rec.run_id))

    tally = await drive_once(store, poll, resume)
    assert tally["done"] == 1 and tally["failed"] == 0

    # The record is terminal, and the resumed run COMPLETED (no re-pause) with the
    # operator output lowered into state via the output binding.
    rec = await store.get("run-remote-1:remote")
    assert rec.status == DONE
    assert len(resumed) == 1
    final = resumed[0]
    assert "__interrupt__" not in final
    assert final.get("findings", {}).get("remote_uri") == "s3://bucket/adapter"

    reset_remote_job_store()
    reset_checkpointer()


@pytest.mark.asyncio
async def test_remote_job_without_run_identity_fails_loud(tmp_path, monkeypatch):
    """#564 guard: a remote-job node reached with no run_id must NOT silently
    register a colliding ``:node`` record — it fails loud. With no run_id/symptom
    in state, the compiler's fallback yields the ``p?`` sentinel, which the guard
    rejects. #568: because this is a remote_job (action) operator, the failure now
    PROPAGATES (→ the dispatcher marks the run failed) rather than degrading to a
    finding — a dispatch that never happened must not report a completed run."""
    _durable_env(tmp_path, monkeypatch)
    tmpl = _remote_template(tmp_path)
    compiled = compile_template(tmpl)
    rt = InProcessRuntime()
    # run_id="" → state has no usable identity → compiler falls back to "p?" →
    # the guard raises, and (#568) a remote_job failure fails the run loud.
    with pytest.raises(Exception, match="run identity"):
        await rt.invoke(compiled.runnable._compiled_graph, {}, run_id="")

    # Crucially: NO colliding record was written to the store.
    store = get_remote_job_store()
    assert await store.get("p?:remote") is None
    assert await store.get(":remote") is None

    reset_remote_job_store()
    reset_checkpointer()


@pytest.mark.asyncio
async def test_a_cancelled_remote_job_ends_the_run_instead_of_pausing_again(tmp_path, monkeypatch):
    """ADR-0151 D9: a job the control loop polls as `cancelled` is closed CANCELLED
    and the run resumed; the re-run node reads the terminal record and raises
    JobCancelled — it does not register again or pause on the interrupt forever."""
    from cairn.core.operator.job_store import CANCELLED

    _durable_env(tmp_path, monkeypatch)
    compiled = compile_template(_remote_template(tmp_path))
    rt = InProcessRuntime()
    graph = compiled.runnable._compiled_graph
    paused = await rt.invoke(graph, {"run_id": "run-remote-c"}, run_id="run-remote-c")
    assert "__interrupt__" in paused
    store = get_remote_job_store()

    async def poll(_rec):
        return JobStatus(state="cancelled", error="remote job cancelled")

    outcomes: list = []

    async def resume(_rec, status):
        try:
            outcomes.append(await rt.resume(graph, {"status": status.state, "error": status.error},
                                            run_id=_rec.run_id))
        except Exception as e:  # noqa: BLE001 — the node's terminal raise is the outcome
            outcomes.append(e)

    tally = await drive_once(store, poll, resume)
    assert tally.get("running") == 0, f"a cancelled job was counted running: {tally}"
    assert tally["cancelled"] == 1
    assert (await store.get("run-remote-c:remote")).status == CANCELLED
    assert len(outcomes) == 1
    out = outcomes[0]
    assert not (isinstance(out, dict) and "__interrupt__" in out), "must not pause again"
    from cairn.core.operator.jobs import JobCancelled

    assert isinstance(out, JobCancelled), out

    reset_remote_job_store()
    reset_checkpointer()
