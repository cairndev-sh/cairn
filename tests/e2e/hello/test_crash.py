"""The crash cells (ADR-0127 D1; the outcomes are ADR-0125 D3/D4 and ADR-0126).

Each cell SIGKILLs the real server — at the person gate, or in the middle of a flow leg —
starts it again on the same store, and asserts what the records say afterwards, per
runtime. The pipeline is the harness's own ``hello-crash/slow-publish`` (``crash-pack/``):
check-and-publish with a ``noop.slow`` flow leg long enough to die inside.

What a restart does to an in-process leg (``pipeline_bridge._adopt_in_flight``):

- the new process's adopt pass probes the dead owner's lock (ADR-0125 D3): free ONCE is
  UNDETERMINED, so the leg is only re-mapped (ADR-0126 D2: never re-owned);
- the reconciler's sweep probes again more than one re-acquire interval later (15 s,
  ``liveness.REACQUIRE_SECONDS``): free TWICE is DEAD, and the leg is handed to the bridge
  (D4): a flow leg WITH a handle is re-attached (which rewrites its owner, ADR-0126 D1) and
  watched through its engine; the builtin engine's job table died with the old process, so
  the re-attached poll fails and the leg settles ``failed`` — the pipeline fails closed.
  Nothing is re-submitted: the leg keeps its one Run, and no write leg after it fires;
- a step parked ``awaiting_approval`` has no process to lose: it is still waiting after the
  restart and is decided then.

Under DBOS a TEMPLATE leg is the runtime's, not the bridge's: its run is a DBOS workflow
(no in-process owner, ADR-0125 D1) that DBOS recovers at the next boot. The DBOS resume cell
kills the server mid-way through ask-the-docs' ``assistant`` leg and asserts that recovery.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from tests.e2e.conftest import HELLO, Api, Cell, cell_params, dbos_workflow
from tests.e2e.hello._receipts import record

pytestmark = [pytest.mark.e2e, pytest.mark.crash]

CRASH_PACK = Path(__file__).parent / "crash-pack"
PACK_PATH = os.pathsep.join([str(HELLO), str(CRASH_PACK)])
REF = "hello-crash/slow-publish"
ASK = "hello-cairn/ask-the-docs"
LEGS = ("profile", "slow", "stage", "publish")
#: one sweep every 2 s (conftest), DEAD after two probes > 15 s apart, then the re-attach
DEAD_VERDICT_BUDGET = 90.0


def _run_ids(api: Api, pid: str) -> dict[str, str]:
    return {sid: st.get("run_id") for sid, st in api.pipeline(pid)["step_state"].items() if st.get("run_id")}


def _files(root: Path, store: str) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file() and store in p.parts)


@pytest.mark.parametrize("cell", cell_params())
def test_killed_at_the_person_gate(cell: Cell, server_factory) -> None:
    """Kill the server while ``review`` waits for a person; after the restart the gate is
    still waiting, is decided through the same door, and every leg before and after it has
    exactly one Run — the published artifact is written once."""
    srv = server_factory(cell, "gate", pack_path=PACK_PATH)
    api = srv.api()
    pid = api.start(REF)
    api.wait_step(pid, "review", "awaiting_approval", timeout=120)
    before = _run_ids(api, pid)
    assert set(before) == {"profile", "slow", "stage"}, before

    srv.kill()
    srv.start()
    api = srv.api()

    rec = api.pipeline(pid)
    assert rec["step_state"]["review"]["status"] == "awaiting_approval", rec["step_state"]["review"]
    assert _run_ids(api, pid) == before, "a restart must not re-run a settled leg"
    api.approve(pid, "review", approver="proof", rationale="after the crash")
    rec = api.wait_terminal(pid, timeout=120)
    assert rec["status"] == "done", (rec["status"], rec.get("trigger", {}).get("failure_reason"))
    assert rec["step_state"]["review"].get("approver") == "proof", rec["step_state"]["review"]
    after = _run_ids(api, pid)
    assert {k: after[k] for k in before} == before and set(after) == set(LEGS), after
    for sid in LEGS:
        assert api.run(after[sid])["status"] == "done", sid
    mine = [r for r in api.runs() if r.get("bundle_pipeline_id") == pid]
    assert sorted(r["step_id"] for r in mine) == sorted(LEGS), [(r["step_id"], r["status"]) for r in mine]
    assert len(_files(srv.artifacts, "crash-published")) == 1 and len(_files(srv.artifacts, "crash-staging")) == 1
    record("crash.gate", cell.id, "slow-publish",
           f"killed at review, restarted, approved; runs {after}; artifact written once")


@pytest.mark.parametrize("cell", cell_params())
def test_killed_mid_flow_leg(cell: Cell, server_factory) -> None:
    """Kill the server while the ``slow`` flow leg is running; after the restart the
    ADR-0125 outcome for an in-process flow leg with a persisted handle: re-attached by the
    reconciler's DEAD verdict, settled by what the engine says, never re-submitted, and the
    write legs behind it never fire. Under DBOS too: flow legs are the bridge's in-process
    poll loop on every runtime (DBOS wraps template runs only)."""
    srv = server_factory(cell, "midleg", pack_path=PACK_PATH)
    api = srv.api()
    pid = api.start(REF)
    # the step reads `running` a moment before its engine handle is persisted (the leg
    # hooks write it right after submit): wait for the handle, the re-attach token
    rec = api.wait(pid, lambda r: bool(((r["step_state"].get("slow") or {}).get("handle") or {}).get("job_id")),
                   timeout=120, what="the slow leg's engine handle")
    slow = rec["step_state"]["slow"]
    assert slow["status"] == "running" and slow["run_id"], slow
    run_id = slow["run_id"]
    owner_before = api.run(run_id).get("owner")
    assert owner_before, "an in-process flow leg names its owner process (ADR-0125 D1)"
    time.sleep(1.0)   # well inside the leg

    srv.kill()
    srv.start()
    api = srv.api()

    # ADR-0125 D3 row 5 / D4: the new process's adopt pass found the dead owner's lock free
    # ONCE → UNDETERMINED → the leg is only re-mapped: still running, same Run, not settled
    rec = api.pipeline(pid)
    st = rec["step_state"]["slow"]
    assert st["status"] == "running" and st["run_id"] == run_id, st
    assert api.run(run_id)["status"] == "running" and rec["status"] == "running"
    # D3 row 4: the reconciler's second probe, more than one re-acquire interval later, says
    # DEAD → D4 handoff → a flow leg with a handle is RE-ATTACHED; the builtin engine's job
    # table is process-local, so the re-attached poll fails and the leg settles `failed`.
    # No other state is acceptable: not done, not parked, not re-submitted.
    deadline = time.monotonic() + DEAD_VERDICT_BUDGET
    while time.monotonic() < deadline:
        st = api.pipeline(pid)["step_state"]["slow"]
        if st["status"] != "running":
            break
        time.sleep(1.0)
    assert st["status"] == "failed", f"the slow leg must settle `failed` after the DEAD verdict: {st}"
    assert st["run_id"] == run_id, f"the leg was re-submitted as a new Run: {st}"
    run = api.run(run_id)
    assert run["status"] == "failed", run.get("status")
    assert "EngineError" in (run.get("error") or "") and "process table lost" in run["error"], run.get("error")
    # ADR-0126 D1: re-attaching rewrote the owner to the process that took the leg over
    assert run.get("owner") and run["owner"] != owner_before, (owner_before, run.get("owner"))
    final = api.wait(pid, lambda r: r["status"] == "failed", timeout=60, what="the pipeline failed closed")
    ids = _run_ids(api, pid)
    assert set(ids) == {"profile", "slow"}, ids   # stage and publish never fired
    mine = [r for r in api.runs() if r.get("bundle_pipeline_id") == pid]
    assert sorted(r["step_id"] for r in mine) == ["profile", "slow"], [(r["step_id"], r["status"]) for r in mine]
    assert not _files(srv.artifacts, "crash-staging") and not _files(srv.artifacts, "crash-published")
    record("crash.mid-leg", cell.id, "slow-publish",
           f"killed mid-leg; re-mapped (running) right after restart, then DEAD → re-attached → "
           f"failed ({run['error'][:60]!r}), same run, owner rewritten, pipeline={final['status']}, "
           "stage/publish never fired")


@pytest.mark.parametrize("cell", [p for p in cell_params() if "-dbos-" in p.id])
def test_dbos_resumes_a_template_run_after_a_crash(cell: Cell, server_factory, model_stub) -> None:
    """Kill the server while ask-the-docs' ``assistant`` template leg — a DBOS workflow whose
    id is its run id — waits on its model call (the stub holds the answer); restart on the
    same stores. DBOS recovers the PENDING workflow at boot and runs it again from its last
    checkpoint: the model is called again, the SAME run settles ``done``, the bridge advances
    to the person gate, and the pipeline finishes with one Run per leg. The runtime-level
    proof is tests/core/test_dbos_runtime.py::test_dbos_crash_recovery; this is the whole
    path, through a real server and a pipeline."""
    model_stub.chat_delay = 30.0     # long enough to die inside; reset before the restart
    try:
        srv = server_factory(cell, "dbos-resume")
        api = srv.api()
        before = model_stub.calls["/chat/completions"]
        pid = api.start(ASK)
        rec = api.wait(pid, lambda r: (r["step_state"].get("assistant") or {}).get("status") == "running"
                       and model_stub.calls["/chat/completions"] > before,
                       timeout=120, what="the assistant leg waiting on its model call")
        run_id = rec["step_state"]["assistant"]["run_id"]
        assert run_id and not api.run(run_id).get("owner"), "a DBOS-owned run names no in-process owner"
        wf = dbos_workflow(srv, run_id)
        assert wf is not None and wf[0] == "PENDING", wf

        srv.kill()
        model_stub.chat_delay = 0.0
        srv.start()
        api = srv.api()

        rec = api.wait_step(pid, "review", "awaiting_approval", timeout=180)
        st = rec["step_state"]["assistant"]
        assert st["status"] == "done" and st["run_id"] == run_id, st
        run = api.run(run_id)
        assert run["status"] == "done", (run.get("status"), run.get("error"))
        assert dbos_workflow(srv, run_id) == ("SUCCESS", "cairn.run_template"), dbos_workflow(srv, run_id)
        calls = model_stub.calls["/chat/completions"] - before
        assert calls >= 2, f"the recovered workflow never called the model again ({calls} call(s))"
        api.approve(pid, "review", approver="proof", rationale="after the crash")
        final = api.wait_terminal(pid, timeout=120)
        assert final["status"] == "done", (final["status"], final.get("trigger", {}).get("failure_reason"))
        mine = [r for r in api.runs() if r.get("bundle_pipeline_id") == pid]
        assert sorted(r["step_id"] for r in mine) == ["assistant", "length"], [(r["step_id"], r["status"]) for r in mine]
        record("crash.dbos-resume", cell.id, "ask-the-docs",
               f"killed while assistant run {run_id} (DBOS workflow PENDING) waited on its model call; "
               f"DBOS recovered it at boot, model called again ({calls} calls), same run done, "
               "review approved, pipeline done, one run per leg")
    finally:
        model_stub.chat_delay = 0.0
