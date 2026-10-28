"""Builtin DataflowEngine (ADR-0081 D3, #1048) — rung 1 of the execution
ladder: ``cairn-dataflow-run`` as a REAL separate OS process.

Local mode spawns the entrypoint via subprocess in an isolated workdir — the
SAME binary and spec-over-env shape the SSH box runs, so conformance in CI
exercises the exact production code path minus the transport. The SSH
transport composes in P1c through the environment backend (ctx.compute →
the proven remote/exec machinery, PR #1044); the Metaflow and SkyPilot
adapters are sibling classes, all selected by
``[profile.backends.dataflow]`` — engines are NEVER separate operators (R7).
"""
from __future__ import annotations
from cairn.core.operator import getenv

import asyncio
import os
import signal
import sys
import tempfile
from pathlib import Path
from typing import Any

from cairn.core.dataflow import (
    DataflowResult,
    DataflowSpec,
    EngineError,
    EngineJobHandle,
    EngineJobStatus,
)
from cairn.core.operator.jobs import live_channel_env_for

from cairn.core.dataflow import results

# live process table — an in-process engine's submit/poll/cancel all happen in
# the same interpreter (the inline-poll path); the SSH mode (P1c) is stateless
# on the control plane and survives restarts, like fix-remote.
_PROCS: dict[str, dict[str, Any]] = {}


class BuiltinLocalEngine:
    """Run the dataflow entrypoint as a local subprocess (own process group)."""

    name = "builtin"
    #: ADR-0118 D7 — kernels run through `run_dataflow`, which checks stage contracts.
    carries_stage_contracts = True

    def validate(self, spec: DataflowSpec) -> list[str]:
        return results.validate(spec)

    async def submit(self, spec: DataflowSpec, ctx: Any) -> EngineJobHandle:
        problems = self.validate(spec)
        if problems:
            raise EngineError("; ".join(problems))
        workdir = Path(tempfile.mkdtemp(prefix="cairn-dataflow-"))
        env = dict(os.environ)
        env["CAIRN_DATAFLOW_SPEC"] = spec.model_dump_json()
        # the subprocess cwd is its WORKDIR — relative paths in stage config
        # (e.g. ingest path) must resolve against the SUBMITTER's cwd, not the
        # temp dir. Remote engines don't set this: on a box, relative = on-box.
        env["CAIRN_SUBMIT_CWD"] = os.getcwd()
        # #2614 — this process owns the job: the runner kills itself (and its
        # group) if we die, so a restarted control plane never finds a leg failed
        # "process table lost" while the orphaned job is still writing.
        env["CAIRN_DATAFLOW_OWNER_PID"] = str(os.getpid())
        if spec.parent_run:
            env["CAIRN_PARENT_RUN"] = spec.parent_run
            # #2025 — the live channel: a kernel's LiveReporter reads these
            env.update(live_channel_env_for(spec.parent_run, "flow"))
        # own session/process group so cancel kills the WHOLE tree (#603) and
        # the runner never inherits our terminal signals.
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "operator_dataflow.engine.entry",
            cwd=workdir, env=env, start_new_session=True,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        job_id = f"builtin-{proc.pid}"
        _PROCS[job_id] = {
            "proc": proc, "workdir": workdir, "cancelled": False,
            # budget is enforced ENGINE-side too: a stage owns its loop, so only
            # the engine can abort MID-stage (the runner's per-stage check is the
            # second layer). Timed from submit — wall clock is what money buys.
            "deadline": (asyncio.get_event_loop().time() + spec.budget.max_seconds)
            if spec.budget.max_seconds else None,
            "budget_hit": False,
        }
        return EngineJobHandle(job_id=job_id, engine=self.name,
                               meta={"workdir": str(workdir), "pid": proc.pid})

    def _job(self, handle: EngineJobHandle) -> dict[str, Any]:
        try:
            return _PROCS[handle.job_id]
        except KeyError as e:
            raise EngineError(f"unknown builtin job {handle.job_id!r} (process table lost)") from e

    async def poll(self, handle: EngineJobHandle) -> EngineJobStatus:
        job = self._job(handle)
        proc: asyncio.subprocess.Process = job["proc"]
        rc = proc.returncode
        metrics = self._read_metrics(job["workdir"])
        if rc is None:
            if job["deadline"] is not None and asyncio.get_event_loop().time() > job["deadline"]:
                job["budget_hit"] = True
                await self.cancel(handle)
                job["cancelled"] = False  # a budget kill is a FAILURE, not a user cancel
                return EngineJobStatus(
                    state="failed", metrics=metrics,
                    error="budget.max_seconds exceeded — dataflow aborted by engine",
                )
            return EngineJobStatus(state="running", metrics=metrics)
        if job["budget_hit"]:
            return EngineJobStatus(state="failed", metrics=metrics,
                                   error="budget.max_seconds exceeded — dataflow aborted by engine")
        if job["cancelled"]:
            return EngineJobStatus(state="cancelled", metrics=metrics, error="cancelled by control")
        if rc == 0:
            return EngineJobStatus(state="succeeded", metrics=metrics)
        error = self._read_error(job["workdir"]) or f"cairn-dataflow-run exit {rc}"
        return EngineJobStatus(state="failed", error=error, metrics=metrics)

    async def cancel(self, handle: EngineJobHandle) -> None:
        job = self._job(handle)
        proc: asyncio.subprocess.Process = job["proc"]
        if proc.returncode is None:
            job["cancelled"] = True
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
            await proc.wait()

    async def result(self, handle: EngineJobHandle) -> DataflowResult:
        job = self._job(handle)
        payload = self._read_result(job["workdir"])
        if payload is None or "error" in payload:
            raise EngineError(
                f"no successful result for {handle.job_id}: "
                f"{(payload or {}).get('error', 'result.json missing')}"
            )
        payload.pop("lineage_parent_run", None)  # stamp, not a contract field
        return DataflowResult.model_validate(payload)

    # The workdir file contract is core's (#2583, ADR-0129 D1); these three are
    # kept one release for callers that spelled them on this class.
    _read_metrics = staticmethod(results.read_metrics)
    _read_result = staticmethod(results.read_result)
    _read_error = staticmethod(results.read_error)


#: Fallback key → env name, for a data-plane worker with no control-plane core
#: installed. Mirrors ``cairn.core.profile_backends.BACKENDS``; kept tiny and
#: local on purpose — if core is unreachable, so is its schema.
_SELECTION_ENV = {
    "dataflow_engine": "CAIRN_DATAFLOW_ENGINE",
    "dataflow_target": "CAIRN_DATAFLOW_TARGET",
    "ml_engine": "CAIRN_ML_ENGINE",
    "ml_target": "CAIRN_ML_TARGET",
    # ADR-0104 D1 (#2071) — the image is the unit of remote work.
    "remote_exec_mode": "CAIRN_REMOTE_EXEC_MODE",
    "remote_image": "CAIRN_TRAIN_IMAGE",
    "remote_env": "CAIRN_REMOTE_ENV",
}


def _selected(key: str, default: str) -> str:
    """One backend-selection plane (#1093): profile record → env var → default.

    Reads through ``cairn.core.operator.selected_backend`` (the operator contract's
    re-export of the one selection plane, ADR-0134 D3b) so a profile-configured engine
    or target is HONOURED — before #1093 this read ``os.environ`` directly, so
    ``[profile.backends] ml_engine = "metaflow"`` was silently ignored and every
    flow ran on the builtin engine. When core isn't importable (a worker on a box
    receives the selection as env, never as a profile) the env var IS the contract.
    """
    try:
        from cairn.core.operator import selected_backend as selected  # noqa: PLC0415 - absent on a data-plane box
    except Exception:  # noqa: BLE001 — no control-plane core here: env is the contract
        name = _SELECTION_ENV.get(key)
        return (getenv(name, default) if name else default) or default
    return selected(key, default=default) or default


def builtin_engine(*, kind: str = "data", target: str = "") -> BuiltinLocalEngine:
    """The in-process engine — or the SSH one when the environment names a box.

    Registered as the ``builtin`` entry in the ``cairn.dataflow_engines`` group
    (#1512): core selects the NAME, this factory builds the engine.
    """
    if target:
        # SSH dispatch (#1050 slice 3): same binary, launched on the
        # environment's compute.ssh box via the proven remote/exec machinery.
        from operator_dataflow.engines_ssh import BuiltinSshEngine  # noqa: PLC0415 - [ssh] extra path
        return BuiltinSshEngine(target)
    return BuiltinLocalEngine()


def resolve_engine(ctx: Any, kind: str = "data") -> BuiltinLocalEngine:
    """The engine-selection seam (ADR-0081 D4, #1050; kind axis ADR-0082 D7,
    #1074), kept here as the operators' door.

    **The selection itself now lives in core** — :func:`
    cairn.core.dataflow.engine_registry.resolve_engine` — because core's dispatch
    path needs it too and used to reach into this pack to get it (#1512, an
    ADR-0084 D1 violation). This delegates rather than reimplementing so there is
    exactly ONE place that decides which engine an environment gets; the factories
    above are what core calls back into.

    Behaviour is unchanged: the environment backend for the flow KIND
    (``dataflow_engine``/``dataflow_target`` for ``kind: data``,
    ``ml_engine``/``ml_target`` for ``kind: ml``) resolves through the ONE config
    plane (#1093) — profile record → ``CAIRN_*`` env override → schema default —
    and an UNINSTALLED selection fails loudly at submit (#568), never silently
    falling back.
    """
    from cairn.core.dataflow.engine_registry import resolve_engine as _resolve  # noqa: PLC0415 - core is the control plane, absent on a data-plane box

    return _resolve(ctx, kind=kind)
