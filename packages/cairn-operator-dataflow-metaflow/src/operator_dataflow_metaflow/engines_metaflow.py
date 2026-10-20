"""Metaflow adapter for the DataflowEngine protocol (#1078, ADR-0082 P2).

Metaflow serves **both** flow kinds through this one adapter (ADR-0083): the
processing power lives in the kernels (DuckDB/Polars), while Metaflow supplies
the substrate cairn does not want to write — step-level checkpointing, `resume`,
`@retry`, foreach fan-out, an S3 datastore.

**Scope corrections vs the issue body**, which predates the ADR-0083 ruling and
assumed an Argo/SFN deployment:

* *"submit = ``DeployedFlow.trigger()``; Runner is DEV-ONLY"* — inverted. With no
  Argo/SFN there is no ``DeployedFlow``; the **subprocess/Runner path is the
  production path** for our rungs (local box → SSH job box). Verified on this
  machine: a flow runs, a FRESH process re-attaches by pathspec and reads
  artifacts, and ``resume`` skips completed steps and reuses their data.
* *"straight line — SFN forbids conditionals"* — that constraint came only from
  the SFN assumption. The bridge is straight-line for a different and better
  reason: the stage list is DYNAMIC and Metaflow's graph is STATIC, so per-stage
  steps would mean code-generating a flow per pipeline, which D1 forbids.
* *"backend fields ``train_engine``/``train_target``"* — ``ml_engine``/
  ``ml_target`` already exist and already route by kind, so nothing is added.

The engine is stateless across control-plane restarts: the handle carries the
Metaflow **pathspec**, and every poll re-attaches from it.
"""
from __future__ import annotations
from cairn.core.operator import getenv

import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional

from cairn.core.dataflow import (
    DataflowResult,
    DataflowSpec,
    EngineError,
    EngineJobHandle,
    EngineJobStatus,
)
from cairn.core.dataflow.resources import aggregate_resources, essential_env, gang_workers
from cairn.core.operator.jobs import live_channel_env_for

# live process table — mirrors the builtin engine; poll/cancel of a job this
# process started need the pid, while a re-attached job polls purely from the
# pathspec + workdir files.
_PROCS: dict[str, dict[str, Any]] = {}

_FLOW_NAME = "CairnFlow"
# #1366 — the gang-scheduled distributed-train bridge; the SECOND (and last)
# static bridge class. Selected when the spec's first stage asks for
# resources.workers >= 2.
_TRAIN_FLOW_NAME = "CairnTrainFlow"
# Runs are submitted as this Metaflow user, so they live in the
# `user:cairn` NAMESPACE. A reader that does not select it gets
# MetaflowNamespaceMismatch even though the run resolves — found live, and a
# confusing failure to debug from the message alone. Pinned (not `setdefault`)
# so the namespace never varies with whichever shell user started the process.
_MF_USER = "cairn"


class MetaflowEngine:
    """Submit a cairn flow to Metaflow; poll/cancel/result via pathspec."""

    name = "metaflow"
    #: ADR-0118 D7 — kernels run through `run_dataflow`, which checks stage contracts.
    carries_stage_contracts = True

    def __init__(self, *, python: Optional[str] = None,
                 datastore: Optional[str] = None,
                 metadata_root: Optional[str] = None,
                 compute: Optional[dict] = None,
                 retry_times: int = 0,
                 secrets: Optional[list] = None,
                 env_isolation: str = "auto",
                 datastore_root: Optional[str] = None,
                 metadata_service: Optional[str] = None) -> None:
        # ADR-0089 (#1345/#1346) — the substrate spec: WHERE the execute step
        # runs (`kind: local|kubernetes|batch` + image/queue) and how often
        # Metaflow itself retries it. Attached at run time via `--with`, so the
        # bridge flow stays the one static CairnFlow (D1). `retry_times=-1` is
        # the sentinel for an unparseable profile value — validate() rejects it
        # at submit instead of the selection seam crashing dispatch.
        self._compute = dict(compute or {"kind": "local"})
        self._retry_times = retry_times
        # #1344 — connections whose credentials the flow may use, and how much
        # of the control plane's environment a task inherits.
        self._secrets = list(secrets or [])
        self._env_isolation = (env_isolation or "auto").lower()
        # #1349 — the two things that make a run readable from ANOTHER box: a
        # shared artifact datastore, and metadata that is not CWD-scoped.
        self._datastore_root = datastore_root or getenv("CAIRN_METAFLOW_DATASTORE_ROOT", "")
        self._metadata_service = metadata_service or getenv("CAIRN_METAFLOW_SERVICE_URL", "")
        # `sys.executable` keeps the child on the SAME interpreter, so the
        # kernel entry points the parent resolved are the ones the child sees.
        self._python = python or sys.executable
        self._datastore = datastore or getenv("CAIRN_METAFLOW_DATASTORE", "")
        # A STABLE metadata root — found live: Metaflow's local metadata lives
        # under the RUNNING process's directory, so a job submitted from a
        # per-job temp workdir was invisible to every other process and the
        # pathspec re-attach failed with MetaflowNotFound. Pinning one root
        # makes the stateless-re-attach property actually true on one box.
        # Across BOXES it needs a shared datastore (S3) or a metadata service —
        # `[profile.backends.dataflow.metaflow]` carries both.
        self._metadata_root = str(
            metadata_root
            or getenv("CAIRN_METAFLOW_ROOT")
            or Path.home() / ".cairn" / "metaflow"
        )

    # ── validate ────────────────────────────────────────────────────────────
    def validate(self, spec: DataflowSpec) -> list[str]:
        """Report what THIS engine cannot run (capability discovery, D3).

        Kernel resolution is the same registry the builtin uses, so an unknown
        stage is caught here rather than after a job is already running.
        """
        try:
            from cairn.core.dataflow.stage_loader import load_stages
        except Exception as e:  # noqa: BLE001 — a broken registry is a validation result
            return [f"stage registry unavailable: {type(e).__name__}: {e}"]

        registry = load_stages()
        problems = [
            f"unknown stage {ref.stage!r} (registry has {sorted(registry)})"
            for ref in spec.stages if ref.stage not in registry
        ]
        if not _metaflow_available():
            problems.append(
                "metaflow is not installed — `pip install 'cairndev[metaflow]'` "
                "(the adapter is an optional extra; the builtin engine needs none)"
            )
        # ADR-0089 (#1346) — substrate config is validated HERE (the engine's
        # own capability-discovery hook), so a bad profile fails at submit with
        # a named problem, not as a Metaflow scheduling error mid-run.
        kind = str(self._compute.get("kind") or "local").lower()
        if kind not in ("local", "kubernetes", "batch"):
            problems.append(
                f"metaflow_compute {kind!r} is not a compute kind — "
                "local | kubernetes | batch"
            )
        # A remote backend needs its CLIENT library on the submitting side.
        # Without it Metaflow cannot reach the API server and the submission
        # HANGS rather than failing — observed live: a kubernetes submit sat
        # for 10 minutes having created no pod. Capability discovery is exactly
        # where that belongs.
        _client = {"kubernetes": "kubernetes", "batch": "boto3"}.get(kind)
        if _client and not _lib_available(_client):
            problems.append(
                f"metaflow_compute={kind} needs the {_client!r} package on the "
                f"CONTROL PLANE (the submitter talks to the backend API) — "
                f"`pip install {_client}`; without it the submit hangs instead "
                "of failing"
            )
        # #1366 — the distributed-train bridge runs the spec as ONE gang; a
        # spec that mixes the gang with sequential post-stages would either
        # run eval/package N times (external writes, duplicated) or reopen
        # the #1358 cross-pod working-set problem. Pipelines already chain
        # legs, so the post-train stages have a first-class home.
        if gang_workers(spec) > 1 and len(spec.stages) > 1:
            problems.append(
                f"a distributed flow (resources.workers={gang_workers(spec)}) must "
                f"contain ONLY the distributed stage, got {len(spec.stages)} — run "
                "eval/package as their own flow leg and chain them in a pipeline"
            )
        if any((ref.resources.workers or 0) > 1
               for ref in spec.stages[1:] if ref.resources):
            problems.append(
                "resources.workers is only honored on the FIRST stage — the gang "
                "bridge (#1366) runs one distributed stage per flow"
            )
        if kind in ("kubernetes", "batch") and not self._compute.get("image"):
            problems.append(
                f"metaflow_compute={kind} needs metaflow_compute_image — remote "
                "tasks run in a container that must ship cairndev[dataflow-engine] "
                "+ the flow's kernels (the SSH-box deployment contract)"
            )
        if self._retry_times < 0:
            problems.append(
                "metaflow_retry_times must be a non-negative integer "
                "(the profile value did not parse)"
            )
        # #1344 — FAIL CLOSED rather than silently under-deliver. Env built
        # here reaches the local Runner subprocess; a @kubernetes/@batch TASK
        # gets its environment from the container + Metaflow's own @secrets
        # sources, so declared connections would resolve control-side and then
        # never arrive. The kernel would fail deep in the run with a confusing
        # "credential missing", after the cluster had already been paid for.
        # #1349 — a datastore misconfiguration is invisible until artifacts are
        # unreadable, which is after the compute has been paid for.
        if self._datastore == "s3" and not self._datastore_root:
            problems.append(
                "metaflow_datastore=s3 needs metaflow_datastore_root "
                "(e.g. s3://bucket/prefix) — without a sysroot the datastore "
                "has nowhere shared to write, so nothing is readable off-box"
            )
        if kind in ("kubernetes", "batch") and self._datastore != "s3":
            problems.append(
                f"compute={kind} needs metaflow_datastore=s3: a remote task cannot "
                "reach the control plane's local filesystem, so a local datastore "
                "loses every artifact the flow produces"
            )
        if kind in ("kubernetes", "batch") and self._secrets:
            problems.append(
                f"metaflow_secrets is set but compute={kind}: this engine can only "
                "deliver declared connections to a LOCAL task today. Remote tasks "
                "need a Metaflow @secrets source (follow-up on #1344) — until then, "
                "bake credentials into the task role/image or run compute=local"
            )
        return problems

    # ── submit ──────────────────────────────────────────────────────────────
    async def submit(self, spec: DataflowSpec, ctx: Any) -> EngineJobHandle:
        problems = self.validate(spec)
        if problems:
            raise EngineError(f"metaflow cannot run this spec: {problems}")

        workdir = Path(tempfile.mkdtemp(prefix="cairn-mf-"))
        run_id_file = workdir / "run_id.txt"
        # the FULL contract shape, not just stage/config — a hand-picked subset
        # silently dropped budget/input_artifacts/parent_run before they ever
        # reached the bridge (#1243: the same class of drop the bridge's own
        # hand-rolled loop caused, one layer up).
        spec_json = spec.model_dump_json()

        env = await self._build_env()
        # Metaflow requires a username for local metadata; a run submitted by
        # the control plane has no shell user, and the failure is otherwise a
        # confusing metadata error rather than an obvious missing-config one.
        env["USERNAME"] = _MF_USER
        env["METAFLOW_USER"] = _MF_USER
        env.update(self.durability_env())
        if spec.parent_run:
            env.update(live_channel_env_for(spec.parent_run, "flow"))  # #2025 — the live channel
        Path(self._metadata_root).mkdir(parents=True, exist_ok=True)
        # The submitter's cwd is where a RELATIVE dataset path resolves (the
        # engine runs kernels in a temp workdir) — the bug the SSH leg found live.
        env.setdefault("CAIRN_SUBMIT_CWD", os.getcwd())

        # #1366 — a stage asking for a coordinated gang routes to the SECOND
        # static bridge (start → N-rank @parallel → join → end); everything
        # else stays on the one-step interpreter. Both are fixed classes, D1.
        gang = gang_workers(spec)
        module = ("operator_dataflow_metaflow.metaflow_train_bridge" if gang > 1
                  else "operator_dataflow_metaflow.metaflow_bridge")
        flow_name = _TRAIN_FLOW_NAME if gang > 1 else _FLOW_NAME
        if gang > 1:
            # Metaflow's local @parallel mock spawns secondary ranks with the
            # bare command `python` — absent from PATH in a uv venv (found
            # live: FileNotFoundError('python') on rank 1). The interpreter's
            # own bin dir always has it.
            env["PATH"] = f"{Path(self._python).parent}{os.pathsep}{env.get('PATH', '')}"

        cmd = [
            self._python, "-m", module, "run",
            "--spec_json", spec_json,
            "--workdir", str(workdir),
            "--run-id-file", str(run_id_file),
            *(["--workers", str(gang)] if gang > 1 else []),
            # ADR-0089 (#1346): the substrate decorators — @resources from the
            # aggregated stage hints, @kubernetes/@batch from the environment's
            # compute block, @retry — attached at run time via `--with`.
            *build_with_args(spec, self._compute, self._retry_times),
        ]
        proc = await asyncio.create_subprocess_exec(
            # cwd is the STABLE metadata root, not the job workdir: Metaflow
            # writes `.metaflow` into the CWD, so running from a per-job temp
            # dir made each run invisible to every other process (found live —
            # MetaflowNotFound on a run that had just succeeded). Kernels are
            # unaffected: they receive the workdir explicitly.
            *cmd, cwd=self._metadata_root, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            start_new_session=True,          # own process group → cancel kills kernels too
        )

        job_id = f"mf-{proc.pid}"
        _PROCS[job_id] = {"proc": proc, "workdir": workdir,
                          "run_id_file": run_id_file, "pathspec": None}
        # `meta` is the engine's own scratch space — the control plane persists
        # it but never interprets it (the builtin engine uses the same shape).
        # #1348: the pgid rides here too. `start_new_session=True` makes the
        # child its own group leader (pgid == pid), and that group is the ONLY
        # way to reach the per-step children Metaflow spawns after a
        # control-plane restart has emptied `_PROCS`.
        return EngineJobHandle(
            engine=self.name, job_id=job_id,
            meta={"workdir": str(workdir), "pid": proc.pid, "pgid": proc.pid,
                  "run_id_file": str(run_id_file),
                  # #1366 — pathspec reconstruction needs the FLOW name, and a
                  # gang run is CairnTrainFlow, not CairnFlow.
                  "flow_name": flow_name},
        )

    # ── poll ────────────────────────────────────────────────────────────────
    async def poll(self, handle: EngineJobHandle) -> EngineJobStatus:
        entry = _PROCS.get(handle.job_id)
        workdir = Path((entry or {}).get("workdir") or handle.meta.get("workdir") or ".")
        progress = _read_json(workdir / "progress.json") or {}
        proc = (entry or {}).get("proc")

        state = str(progress.get("state") or "running")
        metrics = {
            k: v for k, v in progress.items()
            if k in ("stages_done", "stages_total") and isinstance(v, (int, float))
        }
        cost_usd = progress.get("cost_usd")
        if (isinstance(cost_usd, (int, float)) and not isinstance(cost_usd, bool)
                and 0 < cost_usd < float("inf")):
            metrics["cost_usd"] = round(float(cost_usd), 6)
        stage = progress.get("stage")

        if proc is not None and proc.returncode is None:
            # still running — the process is authoritative over the file
            return _status("running", stage, metrics)

        if proc is not None:
            rc = proc.returncode
            if rc == 0:
                return _status("succeeded", None, metrics)
            # Signal deaths split on INTENT (#1322): cancel() marks the workdir
            # before killing, so a signal death WITHOUT the marker is a kill
            # nobody requested — on a loaded box that is the kernel OOM-killer.
            # The old blanket `cancelled` hid a 45-min OOM as a clean cancel.
            if rc is not None and (rc < 0 or rc in (137, 143)):
                if state == "cancelled":
                    return _status("cancelled", stage, metrics)
                sig = -rc if rc < 0 else rc - 128
                return _status(
                    "failed", stage, metrics,
                    error=f"metaflow killed by signal {sig} with no cancel "
                          f"request — likely OOM (free RAM now: {_free_gib()}); "
                          f"no result.json")
            return _status("failed", stage, metrics,
                           error=f"metaflow exited with code {rc}")

        # Re-attached after a control-plane restart: the files are the truth.
        if state in ("succeeded", "failed", "cancelled"):
            return _status(state, None, metrics)
        if not progress and self.is_cross_box:
            # #1364 — a FOREIGN process (another box, a fresh container, a
            # detached scheduler) has no _PROCS entry AND no workdir, so the
            # file-based re-attach above defaults to "running" forever — a
            # finished run that never finishes. With both durability axes
            # configured (#1349) the metadata service is the truth instead.
            st = await asyncio.to_thread(self._status_from_client, handle)
            if st is not None:
                return st
        # #1429 — re-attached to a LOCAL job whose bridge process is gone (the
        # worker restart that dropped `_PROCS` also killed the child, or it
        # died unobserved) and whose files never reached a terminal state:
        # this used to report `running` until the poll budget expired — a
        # 7-day hang for a dead job. The persisted pgid is verified by command
        # line (#1348: pid reuse is the hazard) — a live, matching process is
        # still `running`; a gone one is a FAILED attempt, retriable, so the
        # caller's H2 retry resubmits seeded from the newest checkpoint (H6c).
        pgid = handle.meta.get("pgid") or handle.meta.get("pid")
        if pgid and (await _bridge_process_state(int(pgid), workdir)) is False:
            return EngineJobStatus(
                state="failed", metrics=metrics, retriable=True,
                error=(f"metaflow job {handle.job_id} re-attached but its process "
                       f"(pgid {pgid}) is gone with no terminal result — the worker "
                       f"restart killed local compute; retry resumes from the last "
                       f"durable checkpoint"),
            )
        return _status("running", stage, metrics)

    # ── cancel ──────────────────────────────────────────────────────────────
    async def cancel(self, handle: EngineJobHandle) -> None:
        entry = _PROCS.get(handle.job_id)
        if not entry:
            # #1348 — re-attached after a control-plane restart: `_PROCS` is
            # gone but the detached child SURVIVED and is still computing.
            # Returning here (the old behavior) made cancel a silent no-op that
            # reported success while killing nothing — a run marked cancelled
            # whose GPU kept burning. The persisted pgid is the reattach path.
            await self._cancel_detached(handle)
            return
        proc = entry.get("proc")
        if proc is None or proc.returncode is not None:
            return
        try:
            # kill the GROUP: Metaflow spawns one child process per step, so
            # signalling only the parent orphans the running kernel.
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            return
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        _mark(Path(entry["workdir"]), "cancelled")

    # ── cross-box durability (#1349) ────────────────────────────────────────
    def durability_env(self) -> dict:
        """The METAFLOW_* vars that decide whether a run outlives ONE box.

        Two independent axes, and both must be configured for true cross-box
        operation — this is the gap #1349 names:

        * **datastore** — where step artifacts live. `local` writes files under
          the metadata root, so another box simply cannot read them; `s3` +
          a sysroot makes artifacts (and therefore `resume`) portable.
        * **metadata** — where run/step RECORDS live. Metaflow's local provider
          is CWD-scoped (`<cwd>/.metaflow`), which is why this engine pins one
          stable root; a metadata SERVICE removes the box affinity entirely, so
          a pathspec resolves from anywhere.

        Returned as a dict (not written directly) so the exact contract is
        assertable without spawning Metaflow.
        """
        env: dict = {}
        if self._datastore:
            env["METAFLOW_DEFAULT_DATASTORE"] = self._datastore
        if self._datastore_root:
            # Metaflow keys the sysroot by datastore kind; only s3 is wired
            # here because it is the only shared backend this engine claims.
            env["METAFLOW_DATASTORE_SYSROOT_S3"] = self._datastore_root
        if self._metadata_service:
            env["METAFLOW_DEFAULT_METADATA"] = "service"
            env["METAFLOW_SERVICE_URL"] = self._metadata_service
        return env

    @property
    def is_cross_box(self) -> bool:
        """Can a run submitted here be resolved + resumed from another box?"""
        return bool(self._datastore == "s3" and self._datastore_root and self._metadata_service)

    # ── environment (#1344) ─────────────────────────────────────────────────
    def _strict(self) -> bool:
        """Does this submission get ONLY the declared environment?

        ``auto`` (the default) inherits for **local** compute — the pre-ADR-0089
        behavior, and flipping it would break every deployment that relies on
        ambient AWS/HF/MLflow vars — and is strict for **kubernetes/batch**,
        which is new in #1346 and therefore has no back-compat to preserve.
        Shipping the whole control plane's secrets into a remote container as a
        default is not a defensible posture for a governance product.
        """
        if self._env_isolation in ("strict", "inherit"):
            return self._env_isolation == "strict"
        return str(self._compute.get("kind") or "local").lower() != "local"

    async def _build_env(self) -> dict:
        """The environment a flow task receives.

        Strict mode passes an essential allowlist (interpreter/toolchain vars a
        subprocess cannot run without) plus the resolved values of the DECLARED
        connections — never the ambient control-plane env. Resolution happens
        control-side; ADR-0082 D3 still holds because what crosses is the
        connection's own credentials, not registry/database access.
        """
        if not self._strict():
            env = dict(os.environ)
        else:
            env = {k: v for k, v in os.environ.items() if k in _ESSENTIAL_ENV}
        for name in self._secrets:
            env.update(await _connection_env(name))
        return env

    async def _cancel_detached(self, handle: EngineJobHandle) -> None:
        """Kill a job this process did not start (#1348, ADR-0089).

        **PID reuse is the hazard**, not an edge case: after a restart the
        recorded pid may belong to something else entirely, and signalling it
        blind would kill an innocent process — worse than the no-op this
        replaces. So identity is VERIFIED first (the pgid leader's command line
        must still be this job's bridge invocation) and only a confirmed match
        is signalled. An unverifiable pid is left alone.
        """
        meta = handle.meta or {}
        pgid = meta.get("pgid") or meta.get("pid")
        workdir = Path(meta.get("workdir") or ".")
        if not pgid:
            return  # pre-#1348 handle: nothing durable to reattach to
        pgid = int(pgid)
        if not await _is_our_bridge(pgid, workdir):
            return

        try:
            os.killpg(pgid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            return
        # No Process object to await — poll the group until it's gone, then
        # escalate. Same TERM→KILL ladder as the live path, file-truth marked
        # so a subsequent poll reports `cancelled` rather than `running`.
        for _ in range(20):
            await asyncio.sleep(0.5)
            try:
                os.killpg(pgid, 0)
            except (ProcessLookupError, PermissionError):
                break
        else:
            try:
                os.killpg(pgid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        _mark(workdir, "cancelled")

    # ── result ──────────────────────────────────────────────────────────────
    async def result(self, handle: EngineJobHandle) -> DataflowResult:
        entry = _PROCS.get(handle.job_id)
        workdir = Path((entry or {}).get("workdir") or handle.meta.get("workdir") or ".")
        payload = _read_json(workdir / "result.json")
        if payload is None and (self._strict() or self.is_cross_box):
            # #1358 — a REMOTE task wrote result.json inside its own pod, which
            # the control plane cannot see. Metaflow's datastore is the shared
            # channel that does cross the boundary (and #1349 already requires
            # s3 for remote), so read the payload the bridge published as an
            # ARTIFACT. Same story for a FOREIGN reader of a cross-box run
            # (#1364): the submitter's workdir is another machine's filesystem.
            # Single-box local runs never reach here: their file is right there.
            payload = await asyncio.to_thread(self._result_from_datastore, handle)
        if payload is None or "error" in payload:
            raise EngineError(
                f"no successful result for metaflow job {handle.job_id}: "
                f"{(payload or {}).get('error', 'result.json missing')}"
            )
        payload.pop("lineage_parent_run", None)  # stamp, not a contract field
        return DataflowResult.model_validate(payload)

    def _result_from_datastore(self, handle: EngineJobHandle) -> Optional[dict]:
        """The `result_payload` artifact for a REMOTE run (#1358).

        Blocking (Metaflow's client is sync) — called via ``to_thread``. Reads
        ``execute`` rather than ``end``: ``execute`` is the step that PRODUCES
        the payload, so a run whose bookkeeping tail failed still yields the
        real result instead of nothing.
        """
        spec = self.pathspec(handle)
        if not spec:
            return None
        # The client resolves the datastore from env; this engine holds those
        # values, so apply them rather than relying on an ambient metaflow
        # config that a control plane may not have.
        os.environ.update({k: str(v) for k, v in self.durability_env().items()})
        os.environ.setdefault("METAFLOW_USER", _MF_USER)
        try:
            from metaflow import Run, namespace  # noqa: PLC0415 - optional [metaflow] extra
            namespace(self.metaflow_namespace)   # runs live under user:cairn
            payload = Run(spec)["execute"].task.data.result_payload
        except Exception as e:  # noqa: BLE001 — an unreadable datastore is a result problem
            return {"error": f"could not read result from the metaflow datastore "
                             f"({spec}): {type(e).__name__}: {e}"}
        return dict(payload) if payload else None

    def _status_from_client(self, handle: EngineJobHandle) -> Optional[EngineJobStatus]:
        """Run status via the Metaflow client — the poll source for a process
        that neither spawned the run nor shares its filesystem (#1364).

        Blocking (the client is sync) — called via ``to_thread``. Deliberately
        COARSE: state only, no stage/metrics — mid-run stage progress lives in
        the submitter's progress.json, and pretending the client can see it
        would be a lie. Cancelled is not distinguishable here either (a killed
        run reads as failed); the submitting box's file/process view keeps that
        nuance. ``None`` means "the client cannot say" (no pathspec yet, or
        the run isn't registered) — the caller keeps its own verdict.
        """
        spec = self.pathspec(handle)
        if not spec:
            return None
        os.environ.update({k: str(v) for k, v in self.durability_env().items()})
        os.environ.setdefault("METAFLOW_USER", _MF_USER)
        try:
            from metaflow import Run, namespace  # noqa: PLC0415 - optional [metaflow] extra
            namespace(self.metaflow_namespace)
            run = Run(spec)
            if not run.finished:
                return _status("running", None, {})
            if run.successful:
                return _status("succeeded", None, {})
            metrics: dict[str, Any] = {}
            try:
                cost_usd = run["execute"].task.data.partial_cost_usd
                if (isinstance(cost_usd, (int, float)) and not isinstance(cost_usd, bool)
                        and 0 < cost_usd < float("inf")):
                    metrics["cost_usd"] = round(float(cost_usd), 6)
            except Exception:  # noqa: BLE001 — older runs may have no persisted partial cost
                pass
            return _status("failed", None, metrics,
                           error=f"metaflow run {spec} failed (client status; "
                                 "see the metadata service for step detail)")
        except Exception:  # noqa: BLE001 — an unreadable service is "cannot say", not "failed"
            return None

    # ── metaflow-native extras (not protocol, used by tests + operators) ─────
    @property
    def metaflow_namespace(self) -> str:
        """Namespace runs land in — a reader MUST select it (or ``None``).

        Metaflow scopes runs per user; the control plane submits as ``cairn``,
        so a reader running as a different shell user sees
        ``MetaflowNamespaceMismatch`` on a run that exists and succeeded.
        """
        return f"user:{_MF_USER}"

    @property
    def metadata_root(self) -> str:
        """Directory a READER must run from to resolve this engine's pathspecs.

        Metaflow's local metadata provider is **cwd-based** (``<cwd>/.metaflow``),
        not env-configured — verified by experiment, after
        ``METAFLOW_DATASTORE_SYSROOT_LOCAL`` turned out to move only the
        artifact datastore and produced a nested ``.metaflow/.metaflow``. So the
        contract is: submit and re-attach from the SAME directory. Across boxes
        this becomes a shared S3 datastore + metadata service, carried by
        ``[profile.backends.dataflow.metaflow]``.
        """
        return self._metadata_root

    def pathspec(self, handle: EngineJobHandle) -> Optional[str]:
        """``CairnFlow/<run_id>`` once Metaflow has assigned it.

        This is what makes the engine stateless: any process can re-attach with
        ``metaflow.Run(pathspec)`` — verified live on this machine.
        """
        entry = _PROCS.get(handle.job_id) or {}
        if entry.get("pathspec"):
            return entry["pathspec"]
        if handle.meta.get("pathspec"):
            # #1364 — the ONLY channel a foreign process has: _PROCS is this
            # process's memory and run_id.txt is the submitter's filesystem,
            # but a persisted handle (#1239) travels.
            return str(handle.meta["pathspec"])
        rid_file = Path(entry.get("run_id_file")
                        or handle.meta.get("run_id_file")
                        or Path(handle.meta.get("workdir") or ".") / "run_id.txt")
        try:
            run_id = rid_file.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if not run_id:
            return None
        # #1366 — a gang run's pathspec is CairnTrainFlow/<id>; the handle
        # carries which bridge served it.
        spec = f"{handle.meta.get('flow_name') or _FLOW_NAME}/{run_id}"
        if entry:
            entry["pathspec"] = spec
        # Stamp the handle too: handles outlive _PROCS (control-plane restart,
        # #1239 persistence), and meta is how the pathspec reaches a foreign
        # box's poll/result (#1364).
        handle.meta["pathspec"] = spec
        return spec


# ── helpers ─────────────────────────────────────────────────────────────────


def build_with_args(spec: DataflowSpec, compute: dict, retry_times: int) -> list[str]:
    """The ``--with`` decospecs for one submission — the ADR-0089 mechanism.

    ``run --with <deco>:<k>=<v>,…`` attaches decorators AT RUN TIME, which is
    what lets the substrate vary per environment while the bridge flow stays
    the one static ``CairnFlow`` (D1). Pure + deterministic so the exact argv
    is unit-testable without metaflow installed.
    """
    specs: list[str] = []
    res = aggregate_resources(spec)
    if res:
        kv = ",".join(f"{k}={res[k]}" for k in ("cpu", "gpu", "memory") if k in res)
        specs.append(f"resources:{kv}")

    kind = str(compute.get("kind") or "local").lower()
    if kind == "kubernetes":
        parts = []
        if compute.get("image"):
            parts.append(f"image={compute['image']}")
        # @kubernetes reads @resources hints for sizing; only identity here.
        specs.append("kubernetes" + (":" + ",".join(parts) if parts else ""))
    elif kind == "batch":
        parts = []
        if compute.get("image"):
            parts.append(f"image={compute['image']}")
        if compute.get("queue"):
            parts.append(f"queue={compute['queue']}")
        specs.append("batch" + (":" + ",".join(parts) if parts else ""))

    if retry_times > 0:
        specs.append(f"retry:times={int(retry_times)}")

    out: list[str] = []
    for s in specs:
        out.extend(["--with", s])
    return out


#: The environment allowlist a Metaflow task gets: core's engine-neutral set (the spec
#: contract, the live channel, ``CAIRN_CONN_``) plus Metaflow's own configuration
#: (``METAFLOW_``; ``CAIRN_METAFLOW_``; ``OBS_METAFLOW_``, the former spelling, forwarded until
#: 1.0, ADR-0122). A task cannot reach its datastore or metadata service without them.
#: The bridge module a spawned job runs, as it appears in the process cmdline — how cancel
#: and re-attach recognise OUR bridge. The engine moved out of `cairn-operator-dataflow` into
#: its own wheel (ADR-0134 D2, #2678); a job launched before that upgrade still runs the old
#: module path, so for one release both are recognised. Drop the second entry in the release
#: after.
_BRIDGE_MODULES = ("operator_dataflow_metaflow.metaflow_bridge", "operator_dataflow.metaflow_bridge")

_ESSENTIAL_ENV = essential_env(extra_prefixes=("METAFLOW_", "CAIRN_METAFLOW_", "OBS_METAFLOW_"))


async def _connection_env(name: str) -> dict:
    """Resolve ONE declared connection to env vars (#1344).

    Uses the same ``resolve_connection`` seam the SSH engine already resolves
    its compute target through, so secret refs stay refs in the profile and
    values only ever transit process env here. A connection that cannot be
    resolved contributes nothing rather than raising: a missing credential
    surfaces as the kernel's own "not configured" error, which names the
    actual problem, instead of a submit-time stack trace about the catalog.
    """
    try:
        from cairn.core.operator import resolve_connection  # noqa: PLC0415 - lazy: core is absent on a data-plane box
        resolved = await resolve_connection(name)
    except Exception:  # noqa: BLE001 — an unavailable catalog is not a submit failure
        return {}
    if resolved is None:
        return {}
    out: dict = {}
    for key, value in (resolved.config or {}).items():
        if isinstance(value, (str, int, float, bool)):
            out[f"CAIRN_CONN_{name.upper().replace('-', '_')}_{key.upper()}"] = str(value)
    for key, value in (resolved.secrets or {}).items():
        # Secrets keep their LOGICAL key as the env name (AWS_ACCESS_KEY_ID,
        # HF_TOKEN, …) — that is what the kernels/libraries actually read.
        out[str(key)] = str(value)
    return out


async def _is_our_bridge(pgid: int, workdir: Path) -> bool:
    """Is ``pgid`` still THIS job's bridge process? (#1348 safety gate.)

    A pid recorded before a restart may since have been recycled. Killing it on
    faith would take down an unrelated process — a worse failure than the
    silent no-op being fixed. So the command line must still name the bridge
    module AND this job's workdir (the workdir is per-job and appears verbatim
    in the argv, which makes the check job-specific rather than merely
    "some cairn flow"). Anything unverifiable → ``False``: leave it alone.
    """
    return (await _bridge_process_state(pgid, workdir)) is True


async def _bridge_process_state(pgid: int, workdir: Path) -> Optional[bool]:
    """Tri-state liveness of a job's bridge process: ``True`` = alive and
    verified ours, ``False`` = confirmed gone (or a recycled pid running
    something else), ``None`` = cannot tell.

    Reads ``/proc/<pid>/cmdline`` directly where it exists — the prod image is
    ``python:3.13-slim`` with NO ``ps``, so the old ``ps``-only check answered
    "unverifiable" for every job on the box (which made #1348's detached cancel
    a silent no-op there, and would make #1429's re-attach liveness call every
    live job dead). ``ps`` is the fallback for hosts without procfs (macOS).
    """
    proc_dir = Path("/proc") / str(pgid)
    if Path("/proc").is_dir():
        if not proc_dir.exists():
            return False
        try:
            cmdline = proc_dir.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
        except OSError:
            return None
        return any(m in cmdline for m in _BRIDGE_MODULES) and str(workdir) in cmdline
    try:
        proc = await asyncio.create_subprocess_exec(
            "ps", "-o", "command=", "-p", str(pgid),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
    except (OSError, asyncio.TimeoutError):
        return None
    if proc.returncode not in (0, None) and not out.strip():
        return False          # ps found no such process
    cmdline = out.decode(errors="replace")
    return any(m in cmdline for m in _BRIDGE_MODULES) and str(workdir) in cmdline


def _metaflow_available() -> bool:
    return _lib_available("metaflow")


def _lib_available(module: str) -> bool:
    """Is an optional backend client importable on THIS (submitting) side?"""
    import importlib.util

    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):  # a broken/partial install
        return False


def _read_json(path: Path) -> Optional[dict]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _free_gib() -> str:
    """Best-effort free-RAM string for the likely-OOM post-mortem (#1322)."""
    try:
        import psutil  # noqa: PLC0415 - optional dependency: present via some extras only
        return f"{psutil.virtual_memory().available / 2**30:.1f} GiB"
    except Exception:  # noqa: BLE001 — a post-mortem hint must never raise
        try:
            free = os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
            return f"{free / 2**30:.1f} GiB"
        except (ValueError, OSError, AttributeError):
            return "unknown"


def _status(state: str, stage, metrics: dict, error: str = "") -> EngineJobStatus:
    # `stage` rides inside `metrics` — EngineJobStatus has no top-level `stage`
    # field (only state/metrics/error/retriable), same as every other engine
    # (BuiltinLocalEngine reads it out of metrics.json's `stage` key; the
    # conformance suite's StubEngine puts it in `metrics` too). Passing it as
    # a top-level kwarg raised `extra_forbidden` on ANY running poll that
    # observed a real stage name — a pre-existing bug, not introduced by #1243.
    payload_metrics = dict(metrics)
    if stage:
        payload_metrics["stage"] = str(stage)
    payload: dict[str, Any] = {"state": state, "metrics": payload_metrics}
    if error:
        payload["error"] = error
    return EngineJobStatus(**payload)


def _mark(workdir: Path, state: str) -> None:
    progress = _read_json(workdir / "progress.json") or {}
    progress["state"] = state
    try:
        (workdir / "progress.json").write_text(json.dumps(progress), encoding="utf-8")
    except OSError:
        pass


__all__ = ["MetaflowEngine"]
