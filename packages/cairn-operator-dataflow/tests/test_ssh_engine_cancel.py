"""ADR-0151 D9 — a cancelled SSH leg is cancelled, never retried.

The pipeline's cancel kills the job's process group on the box before the box is
released. The kill ends ``runner.sh`` before it writes ``exit_code``, which the poll
used to read as a recoverable death (#552: ``failed``, ``retriable``), so
``execute_leg`` re-submitted the leg's job on a box that was about to stop. The
cancel now leaves a marker the poll reads first.

Driven through the REAL path: ``execute_leg`` → ``BuiltinSshEngine.poll/cancel`` →
``remote/exec``'s ``_poll``/``_cancel``, against a fake box (one SSH connection that
keeps the job's pid, its liveness and the files the runner contract writes). Only
``submit`` is stubbed (it counts) and the connection lookups are bypassed.
"""
from __future__ import annotations

import asyncio
import dataclasses
import os
import time
import types
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.dataflow import EngineJobHandle
from cairn.core.dataflow.contract import StageRef
from cairn.core.dispatch.leg_executor import execute_leg
from cairn.core.operator.jobs import JobHandle

import operator_remote.operators.exec as remote_exec
from operator_dataflow.engines_ssh import BuiltinSshEngine
from operator_remote.schemas.exec import RemoteExecInput


class _Out:
    def __init__(self, stdout: str = "") -> None:
        self.stdout = stdout


class _Box:
    """One job on one box: alive until its process group is killed."""

    def __init__(self) -> None:
        self.alive = True
        self.files: dict[str, str] = {}
        self.kills = 0

    async def run(self, cmd: str, check: bool = False) -> _Out:
        if cmd.startswith("echo cancelled"):
            self.files["cancelled"] = "cancelled"
            return _Out()
        if "kill -TERM" in cmd:
            self.kills += 1
            self.alive = False            # runner.sh dies with the group: no exit_code
            return _Out()
        if "kill -0" in cmd:
            return _Out("up" if self.alive else "")
        if cmd.startswith("cat "):
            name = cmd.split()[1].rsplit("/", 1)[-1]
            if name == "pid":
                return _Out("4242")
            return _Out(self.files.get(name, ""))
        return _Out()


class _Engine(BuiltinSshEngine):
    """The real SSH engine; only submit is a stub that counts."""

    def __init__(self) -> None:
        super().__init__("gpu")
        self.submits = 0

    def validate(self, spec):
        return []

    async def submit(self, spec, ctx):
        self.submits += 1
        jh = JobHandle(job_id=f"box:/w/{self.submits}", backend="ssh",
                       meta={"host": "box", "user": "u", "port": 22, "workdir": f"/w/{self.submits}",
                             "launched_at": time.time() - 120})
        return EngineJobHandle(
            job_id=jh.job_id, engine=self.name,
            meta={"job_handle": dataclasses.asdict(jh),
                  "exec_input": RemoteExecInput(command="cairn-dataflow-run").model_dump(exclude={"env"})})

    async def _exec_ctx(self, ctx):
        return SimpleNamespace(compute=None, pipeline_id=None, node_name="dataflow")


@pytest.fixture()
def box(monkeypatch):
    b = _Box()

    async def _conn(params):
        return b

    async def _params(handle, connection=""):
        return {"host": "box", "user": "u", "port": 22}

    monkeypatch.setattr(remote_exec, "_get_conn", _conn)
    monkeypatch.setattr(remote_exec, "_conn_params", lambda ctx: {})
    monkeypatch.setattr(remote_exec, "_job_ssh_params", _params)
    monkeypatch.setattr("cairn.core.dataflow.defs.load_flow_defs",
                        lambda: {"demo/train": types.SimpleNamespace(kind="data")})
    monkeypatch.setattr("cairn.core.dataflow.defs.resolve_flow_ref",
                        lambda ref, cfg, kind="data": [StageRef(stage="noop")])
    monkeypatch.setattr("cairn.core.dispatch.leg_executor._POLL_INTERVAL_S", 0.01)
    return b


async def test_a_cancelled_leg_is_not_resubmitted_and_reads_cancelled(box):
    """A data leg (two attempts by default) is cancelled mid-run, as the pipeline
    cancel does through ``cancel_recorded_job`` → ``engine.cancel``."""
    from cairn.core.dispatch.pipeline_bridge import _leg_run_status

    engine = _Engine()
    handles: list = []

    async def _on_submit(handle):
        handles.append(handle)

    leg = asyncio.create_task(execute_leg(
        "flow:demo/train", inputs={}, engine=engine, run_id="r-train",
        on_submit=_on_submit, poll_budget_s=3.0))
    for _ in range(200):
        if handles:
            break
        await asyncio.sleep(0.01)
    await engine.cancel(handles[0])            # what D9's cancel_recorded_job sends
    out = await asyncio.wait_for(leg, timeout=10)

    assert engine.submits == 1, "a cancelled job must never be submitted again"
    assert out.state == "cancelled", (out.state, out.error)
    assert "without an exit code" not in out.error
    assert _leg_run_status(out) == "cancelled", "the leg's run settles cancelled, not failed"
    assert box.kills == 1


async def test_a_job_that_died_on_its_own_is_still_retried(box):
    """#552 unchanged: no cancel marker → a recoverable death → one retry."""
    engine = _Engine()
    box.alive = False
    out = await execute_leg("flow:demo/train", inputs={}, engine=engine, run_id="r-train",
                            poll_budget_s=1.0)
    assert engine.submits == 2, (out.state, out.error)


async def test_a_cancelled_priced_leg_settles_its_box_time_once(box, monkeypatch):
    """With #211's per-attempt pricing (ADR-0151 D5): the cancelled attempt settles
    its wall-clock once, the leg ends `cancelled`, and no second attempt (and no
    second settle) follows."""
    from datetime import datetime, timedelta, timezone

    from cairn.core import budget_reservations as B
    from cairn.core.dispatch import leg_executor as LE
    from cairn.core.dispatch.leg_hooks import flow_leg_record
    from cairn.core.dispatch.pipeline_bridge import _leg_run_status

    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.delenv("CAIRN_STORAGE", raising=False)
    B.reset_reservations()
    t0 = datetime(2026, 10, 24, 8, 0, tzinfo=timezone.utc)
    ticks = [t0, t0 + timedelta(hours=2)]
    monkeypatch.setattr(LE, "_wall_now", lambda: ticks.pop(0) if len(ticks) > 1 else ticks[0])
    settled: list[float] = []
    released: list = []
    real_settle, real_release = LE.settle, LE.release

    async def _settle(res, actual_usd=0.0):
        settled.append(round(actual_usd, 6))
        return await real_settle(res, actual_usd=actual_usd)

    async def _release(res):
        released.append(res)
        return await real_release(res)

    monkeypatch.setattr(LE, "settle", _settle)
    monkeypatch.setattr(LE, "release", _release)

    engine = _Engine()
    handles: list = []

    async def _on_submit(handle):
        handles.append(handle)

    leg = asyncio.create_task(execute_leg(
        "flow:demo/train", inputs={}, engine=engine, run_id="r-train", on_submit=_on_submit,
        series_id="s-gpu", estimated_usd=6.0, spent_usd=0.0, budget_usd=25.0,
        hourly_usd=0.5, poll_budget_s=3.0))
    for _ in range(200):
        if handles:
            break
        await asyncio.sleep(0.01)
    await engine.cancel(handles[0])
    out = await asyncio.wait_for(leg, timeout=10)
    try:
        assert engine.submits == 1
        assert out.state == "cancelled" and _leg_run_status(out) == "cancelled"
        assert settled == [1.0] and released == [], "the box time settles once, never as 'no spend'"
        assert out.gpu_usd == 1.0
        assert flow_leg_record({}, out)["cost"]["gpu_usd"] == 1.0
        assert await B.reserved_total("s-gpu") == 0.0
    finally:
        B.reset_reservations()
