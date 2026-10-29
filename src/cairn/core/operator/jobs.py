"""Job-executor lifecycle + driver seam (an internal issue).

The shared lifecycle behind the locked container/remote executors — ``docker``,
``kubernetes_job``, ``remote_job`` — which all run an operator's work on
isolated/remote compute rather than in-process. They differ ONLY in the driver:

  - :class:`RemoteJobDriver` — delegates to an operator implementing
    :class:`RemoteJobOperator` (submit/poll via a provider SDK: Modal, Kaggle, …).
  - :class:`DockerJobDriver` — generic: the engine runs the operator's declared
    image (``docker run``) and marshals I/O via a mounted dir. Works for ANY
    image-declaring operator — the clean realization of "operator owns its
    image, engine runs it isolated".
  - kubernetes_job — same shape, a Job resource (later).

:func:`run_job_lifecycle` is driver- and tier-agnostic: dispatch → (persist
handle) → poll until terminal → return output, with **resume** (re-poll an
existing handle instead of re-dispatching) and **cancel**. It is fully
unit-testable with a fake driver — no daemon or provider account needed.

Durability tiers (the persist/resume *wiring*, not the lifecycle):
  - In-process (this slice): polls in a loop; the resume seam is exercised by
    tests but not yet wired to a durable store (Slice 3).
  - Temporal (Slice 2, needs a cluster): the poll is a heartbeating activity —
    on worker crash Temporal reschedules and resumes from the heartbeat.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import inspect
import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional, Protocol, runtime_checkable

from cairn.core.operator.protocol import I, O, Operation


class JobFailed(RuntimeError):
    """The remote/container job reached a terminal failed state."""


class JobCancelled(JobFailed):
    """The job was stopped by a cancel (``JobStatus.state == "cancelled"``). A
    :class:`JobFailed`, so a caller that handles failure still ends; distinct, so
    one that cares can tell a cancel from a crash (ADR-0151 D9)."""


class JobTimeout(RuntimeError):
    """The job did not reach a terminal state within ``max_polls``."""


@dataclass
class JobHandle:
    """A durable reference to a dispatched job — enough to re-poll after a
    restart without re-dispatching."""
    job_id: str
    backend: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class JobStatus:
    """One poll result. ``output`` carries the operator's output object (or a
    dict) once ``state == 'succeeded'``. ``metrics`` carries mid-train signals
    while running (e.g. ``{"eval_losses": [...]}``) for the learning gate (#144).
    ``retriable`` marks a ``failed`` that is a RECOVERABLE death (box/instance
    gone, OOM-killed — no clean exit) rather than a genuine training error, so the
    control loop may relaunch it from a durable checkpoint (#552/ADR-0064).

    ``cancelled`` (ADR-0151 D9) means the job was stopped by a cancel. It is
    terminal and never retriable: the control loop closes the record cancelled
    and resumes the run as cancelled, and :func:`run_job_lifecycle` ends with
    :class:`JobCancelled`."""
    state: str  # "running" | "succeeded" | "failed" | "cancelled"
    output: Any = None
    error: str = ""
    metrics: dict[str, Any] = field(default_factory=dict)
    retriable: bool = False
    #: Recent stdout tail (last ~KB) so the control plane isn't blind on a running
    #: remote job — persisted onto the record each tick for the console (#553).
    log_tail: str = ""


@runtime_checkable
class JobDriver(Protocol):
    """How a job is dispatched/polled/cancelled. The lifecycle drives it."""
    backend: str

    async def dispatch(self) -> JobHandle: ...
    async def poll(self, handle: JobHandle) -> JobStatus: ...
    async def cancel(self, handle: JobHandle) -> None: ...


@runtime_checkable
class RemoteJobOperator(Protocol):
    """An operator that opts into the ``remote_job`` executor by knowing how to
    submit/poll/cancel its provider job. ``poll`` returns the operator's output
    object once terminal (fetch is folded into poll)."""
    async def submit(self, inp: Any, ctx: Any) -> JobHandle: ...
    async def poll(self, handle: JobHandle, inp: Any, ctx: Any) -> JobStatus: ...
    async def cancel(self, handle: JobHandle) -> None: ...


# ─── the live channel a remote worker reports on (#2025, ADR-0102 D4) ────


def live_channel_env(ctx: Any) -> dict[str, str]:
    """The env a remote worker needs to stream its run events back and read
    control — see :func:`live_channel_env_for`. ``run_id == pipeline_id`` at
    the operator boundary (runtime.py)."""
    return live_channel_env_for(str(getattr(ctx, "pipeline_id", "") or ""),
                                str(getattr(ctx, "node_name", "") or ""))


def live_channel_env_for(run_id: str, node: str = "") -> dict[str, str]:
    """The run + node identity and ONE of the two transports —
    ``CAIRN_JOB_CHANNEL=nats://…`` (the declared service) or the control plane's
    own API (``CAIRN_ENGINE_URL`` + a short-lived bearer minted for this run when
    auth is on). Used by every remote submit (operators) AND by the flow engines
    for a ``flow:`` leg's subprocess/box/pod, so a kernel builds its
    ``LiveReporter`` from the same variables. Nothing here is a secret the box
    did not already need; the token is scoped to ingesting this run's events.
    Empty when there is no run identity."""
    if not run_id or run_id in ("p?", "None"):
        return {}
    env = {"CAIRN_RUN_ID": run_id, "CAIRN_NODE": node}
    # #222 — the W3C trace context of the leg submitting this job, so the job's own
    # spans can join the pipeline trace. Absent (not empty) when tracing is off.
    from cairn.core.obs.telemetry import current_traceparent  # noqa: PLC0415 — keeps the telemetry import off this module's load path
    if (traceparent := current_traceparent()):
        env["TRACEPARENT"] = traceparent
    chan = getenv("CAIRN_JOB_CHANNEL", "").strip()
    if chan.startswith("nats://"):
        # The box wire carries both names (#2429): live.py reads CAIRN_ first; the OBS_
        # twins are for the pinned train-gpu images and go at 1.0 with the rebuilt image.
        env["CAIRN_JOB_CHANNEL"] = env["OBS_JOB_CHANNEL"] = chan
        stream = getenv("CAIRN_JOB_CHANNEL_STREAM", "").strip()
        if stream:
            env["CAIRN_JOB_CHANNEL_STREAM"] = env["OBS_JOB_CHANNEL_STREAM"] = stream
        return env
    url = (getenv("CAIRN_ENGINE_URL") or "").strip().rstrip("/")
    if url:
        env["CAIRN_ENGINE_URL"] = url
        token = _worker_token(run_id)
        if token:
            env["CAIRN_ENGINE_TOKEN"] = token
    return env


def _worker_token(run_id: str) -> str:
    """A bearer for the worker, minted only when the API has a signing secret
    (``CAIRN_JWT_SECRET`` set = auth on; unset = the control plane is open and a
    token would be noise)."""
    if not getenv("CAIRN_JWT_SECRET", "").strip():
        return ""
    try:
        from cairn.core.auth_tokens import create_access_token  # noqa: PLC0415 — the auth stack is optional at the operator layer
        from datetime import timedelta  # noqa: PLC0415
        return create_access_token({"sub": f"worker:{run_id}", "scope": "run-ingest", "run_id": run_id},
                                   expires_delta=timedelta(days=7))
    except Exception:  # noqa: BLE001 — no auth stack, no token; the worker runs blind rather than not at all
        return ""


# ─── HTTP provider base (ADR-0092 principle 3, #1544) ────────────────
#
# :class:`RemoteJobOperator` above is a Protocol — a shape check, not a base —
# so every HTTP GPU provider (train/runpod, train/vast, …) re-implemented the
# same eight things: the httpx client + Bearer header + the ``_transport`` test
# seam, the api-key-from-``ctx.compute`` lookup and its RuntimeError, the
# per-connection image override, the container env assembly, the experiment-
# tracking env bridge, the adapter URI, the success JobStatus and the
# DELETE-shaped cancel. Adding a THIRD provider meant copying all of it.
# This class owns that shape; a provider subclass declares its endpoints and
# writes only the calls that are genuinely provider-specific.
#
# It deliberately declares NO governance attrs (risk / executors /
# long_running / requires_gpu / supports_*): #1539 made the operator CLASS the
# source of truth for its ``[operator.*]`` table, so each subclass keeps
# declaring its own and ``cairn pack sync`` parity is unaffected.


class HttpRemoteJobOperation(Operation[I, O]):
    """Concrete base for a ``RemoteJobOperator`` backed by a REST API.

    A subclass declares the provider facts as class attrs and implements
    ``submit`` (+ any provider-shaped poll parsing); everything below is
    inherited.

    The input/output objects are duck-typed, not typed to a training schema:
    ``_env``/``_adapter_uri``/``_success`` read ``output_name`` /
    ``artifact_target`` / ``hf_repo`` off the input and construct
    ``output_schema`` by keyword. That is the contract the GPU providers
    already share; core stays out of the training pack while still owning the
    ONE copy of the shape.
    """

    #: REST root, e.g. ``"https://rest.runpod.io/v1"``.
    api_base: str = ""
    #: Backend id — ``JobHandle.backend`` and the fallback adapter URI scheme.
    provider: str = ""
    #: Human provider name used in the "no connection" error ("RunPod", "Vast.ai").
    provider_label: str = ""
    #: ``cairn connect <slug>``; defaults to :attr:`provider`.
    connect_slug: str = ""
    #: Env var consulted by :meth:`cancel`, which has no ``ctx`` by protocol.
    api_key_env: str = ""
    #: Key in the compute connection's resolved secrets holding the API key.
    api_key_secret: str = "API_KEY"
    #: httpx timeout for every provider call.
    request_timeout: float = 30.0
    #: Env var the in-image runner reads the serialized input spec from.
    spec_env_var: str = "CAIRN_TRAIN_SPEC"
    #: Experiment tracking (#136): for each tracker the input asks for, the
    #: ``tracking.*`` connection to resolve and how to map its secrets onto the
    #: env vars the in-container runner reads.
    tracker_env: dict[str, tuple[str, dict[str, str]]] = {  # noqa: RUF012 — declarative
        "mlflow": ("mlflow", {"TRACKING_URI": "MLFLOW_TRACKING_URI"}),
        "wandb": ("wandb", {"API_KEY": "WANDB_API_KEY"}),
    }

    #: Tests inject an ``httpx.MockTransport`` here to exercise the provider
    #: REST API hermetically (the same seam as AuthlibBroker). ``None`` → a
    #: real network client.
    _transport: Any = None

    # ── client + connection plumbing ──────────────────────────────────

    def _client(self, api_key: str) -> Any:
        """An ``httpx.AsyncClient`` bound to :attr:`api_base` with Bearer auth."""
        # deliberate deferral: httpx is a base dep but a HEAVY import, and
        # jobs.py is on the import path of every job-executor operator
        # (remote/exec, docker) — most of which never make an HTTP call.
        import httpx  # noqa: PLC0415

        kw: dict[str, Any] = dict(
            base_url=self.api_base,
            timeout=self.request_timeout,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        if self._transport is not None:
            kw["transport"] = self._transport
        return httpx.AsyncClient(**kw)

    @staticmethod
    def _compute(ctx: Any) -> Any:
        """The resolved ``compute.*`` connection bound at dispatch (#121), or None."""
        return getattr(ctx, "compute", None)

    def _config(self, ctx: Any) -> dict[str, Any]:
        """The compute connection's non-secret config (empty when unbound)."""
        compute = self._compute(ctx)
        return dict(getattr(compute, "config", None) or {}) if compute else {}

    def _api_key(self, ctx: Any) -> str:
        """The provider API key from the resolved compute connection (#121).

        ``cairn connect <provider>`` produces a ``compute.<provider>``
        connection whose :attr:`api_key_secret` is handed in via ``ctx.compute``
        at dispatch. Missing ⇒ fail LOUD with the action that fixes it.
        """
        compute = self._compute(ctx)
        key = (compute.secrets.get(self.api_key_secret) if compute else None) or ""
        if not key:
            raise RuntimeError(
                f"{getattr(self, 'name', type(self).__name__)} needs a "
                f"{self.provider_label or self.provider} compute connection — run "
                f"`cairn connect {self.connect_slug or self.provider}` and bind it "
                f"via execution.compute_target"
            )
        return key

    def _image(self, ctx: Any) -> str:
        """The backend image: the connection's ``image`` override, else the
        operator-owned default declared on the class."""
        return self._config(ctx).get("image") or (self.image or "")

    # ── job payload helpers ───────────────────────────────────────────

    def _env(self, inp: Any, ctx: Any) -> dict[str, str]:
        """Container env: the whole input spec as JSON (the in-image runner
        reads :attr:`spec_env_var`) plus any connection-supplied passthrough
        env (HF_TOKEN, AWS creds, MLFLOW_TRACKING_URI, …)."""
        extra = self._config(ctx).get("env", {}) or {}
        return {
            self.spec_env_var: inp.model_dump_json(),
            **{k: str(v) for k, v in dict(extra).items()},
        }

    async def _tracking_env(self, report_to: list[str] | None) -> dict[str, str]:
        """Resolve the ``tracking.*`` connection for each requested tracker and
        map its secrets (+ W&B project) into the container env (#136). A tracker
        whose connection isn't set up is skipped — the job just won't log there.
        """
        # lazy: keep the connections/secret stack off the operator import path
        from cairn.core.connections import resolve_connection  # noqa: PLC0415

        env: dict[str, str] = {}
        for tracker in report_to or []:
            spec = self.tracker_env.get(tracker)
            if not spec:
                continue
            name, mapping = spec
            conn = await resolve_connection(name)
            if conn is None:
                continue
            for secret_key, env_var in mapping.items():
                if conn.secrets.get(secret_key):
                    env[env_var] = conn.secrets[secret_key]
            if tracker == "wandb" and conn.config.get("project"):
                env.setdefault("WANDB_PROJECT", str(conn.config["project"]))
        return env

    async def _job_env(self, inp: Any, ctx: Any) -> dict[str, str]:
        """:meth:`_env` with the #136 tracking creds and the #2025 live-channel
        identity folded in — what a submit actually hands the provider."""
        env = self._env(inp, ctx)
        env.update(await self._tracking_env(getattr(inp, "report_to", None)))
        env.update(live_channel_env(ctx))
        return env

    def _adapter_uri(self, inp: Any) -> str:
        """Where the trained adapter lands — mirrors the in-image saver so the
        downstream promote stage gets a realistic URI."""
        target = getattr(inp, "artifact_target", "")
        name = getattr(inp, "output_name", "")
        if target == "hf":
            return f"hf://{getattr(inp, 'hf_repo', '') or name}"
        if target == "s3":
            return f"s3://cairn-adapters/{name}"
        return f"{self.provider}://adapters/{name}"

    def run_url(self, handle: JobHandle) -> str:
        """The provider console URL for this job (empty when the provider has none)."""
        return ""

    # ── terminal states + cancel + mock ───────────────────────────────

    def _success(self, inp: Any, handle: JobHandle) -> JobStatus:
        """The succeeded ``JobStatus`` every provider returns once its job is
        terminal-OK. ``output`` is a dict (``model_dump``) — the shape the
        durable job store and the output binding want."""
        result = self.output_schema(
            status="succeeded",
            backend=self.provider,
            adapter_uri=self._adapter_uri(inp),
            job_id=handle.job_id,
            run_url=self.run_url(handle),
            metrics={},
        )
        return JobStatus(state="succeeded", output=result.model_dump())

    async def _delete(self, path: str, *, ok: tuple[int, ...] = (200, 204, 404)) -> None:
        """DELETE one provider resource with the ambient (env) API key.

        ``RemoteJobOperator.cancel`` carries no ``ctx``, so the key is read from
        :attr:`api_key_env` as a last resort — the control worker calls cancel
        rarely (explicit abort); submit/poll carry ``ctx.compute``.
        """
        async with self._client(os.environ.get(self.api_key_env or "", "")) as client:
            resp = await client.delete(path)
            if resp.status_code not in ok:
                resp.raise_for_status()

    def mock(self, inp: Any, ctx: Any) -> Any:
        """Offline / ``--mock``: the adapter the real job WOULD have produced."""
        return self.output_schema(
            status="succeeded",
            backend=self.provider,
            adapter_uri=self._adapter_uri(inp),
            metrics={"mock": True},
        )


def is_remote_job_operator(operator_cls: type) -> bool:
    """True if the operator class implements the submit+poll protocol (so the
    remote_job executor can drive it; otherwise it degrades to in-process)."""
    return callable(getattr(operator_cls, "submit", None)) and callable(
        getattr(operator_cls, "poll", None)
    )


async def run_job_lifecycle(
    driver: JobDriver,
    *,
    load_handle: Optional[Callable[[], Optional[JobHandle]]] = None,
    persist: Optional[Callable[[JobHandle], None]] = None,
    poll_interval: float = 2.0,
    max_polls: int = 0,
    on_running: Optional[Callable[[JobHandle, "JobStatus"], Any]] = None,
) -> Any:
    """Dispatch → poll → return output. Resume-safe + cancel-safe.

    If ``load_handle`` returns an existing handle, SKIP dispatch and re-poll it
    (idempotent resume — the whole point: a restarted worker re-attaches by
    job_id, never double-dispatches). ``persist(handle)`` records the handle
    right after dispatch so a durable tier can resume. ``max_polls=0`` = no cap.
    ``on_running`` (ADR-0082 D8a, #1076) observes every non-terminal poll —
    the inline path's seam for bridging stage progress into run events;
    best-effort: an observer failure never affects the job.
    """
    handle = load_handle() if load_handle else None
    if handle is None:
        handle = await driver.dispatch()
        if persist:
            persist(handle)

    polls = 0
    while True:
        status = await driver.poll(handle)
        if status.state == "succeeded":
            return status.output
        if status.state == "failed":
            raise JobFailed(f"{driver.backend} job {handle.job_id!r} failed: {status.error}")
        if status.state == "cancelled":  # ADR-0151 D9 — terminal, never re-polled
            raise JobCancelled(f"{driver.backend} job {handle.job_id!r} cancelled: {status.error}")
        if on_running is not None:
            try:
                out = on_running(handle, status)
                if inspect.isawaitable(out):
                    await out
            except Exception:  # noqa: BLE001 — observer must never affect the job
                pass
        polls += 1
        if max_polls and polls >= max_polls:
            raise JobTimeout(
                f"{driver.backend} job {handle.job_id!r} not terminal after {polls} polls"
            )
        if poll_interval:
            await asyncio.sleep(poll_interval)


# ─── remote_job driver — thin adapter over a RemoteJobOperator ───────


class RemoteJobDriver:
    """Adapts an operator implementing :class:`RemoteJobOperator` to a JobDriver."""

    backend = "remote_job"

    def __init__(self, op: Any, inp: Any, ctx: Any) -> None:
        self._op = op
        self._inp = inp
        self._ctx = ctx

    async def dispatch(self) -> JobHandle:
        return await self._op.submit(self._inp, self._ctx)

    async def poll(self, handle: JobHandle) -> JobStatus:
        return await self._op.poll(handle, self._inp, self._ctx)

    async def cancel(self, handle: JobHandle) -> None:
        await self._op.cancel(handle)


# ─── docker driver — engine runs the operator's declared image ───────
#
# Contract: the operator declares ``image`` (+ optional ``container_entrypoint``).
# The engine writes the operator input to ``<io>/in.json``, runs the image with
# the io dir mounted at /io, and reads the result from ``<io>/out.json``.
# Subprocess calls go through ``_docker`` so the command-building + parsing are
# unit-testable without a daemon (which isn't validated here — reference impl).


def build_docker_run_argv(image: str, io_dir: str, entrypoint: list[str]) -> list[str]:
    """The ``docker run`` argv for one detached job. Pure — unit-tested."""
    return [
        "docker", "run", "--detach", "--rm=false",
        "--volume", f"{io_dir}:/io",
        image, *entrypoint, "/io/in.json", "/io/out.json",
    ]


async def _docker(*args: str) -> tuple[int, str]:  # pragma: no cover - needs a daemon
    """Run a docker CLI command; return (returncode, stdout). Seam for tests."""
    proc = await asyncio.create_subprocess_exec(
        *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    return proc.returncode or 0, out.decode(errors="replace").strip()


class DockerJobDriver:
    """Generic driver: ``docker run`` the operator's declared image; poll via
    ``docker inspect``; read ``out.json`` on success. NOTE: the actual daemon
    calls are reference impls — validate on a host with Docker.

    The entrypoint is the operator's declared ``container_entrypoint``; without
    one the image's own ENTRYPOINT receives the io paths. Core names no operator
    pack's module as a default (ADR-0120 D5, #2415)."""

    backend = "docker"

    def __init__(self, image: str, io_dir: str, entrypoint: Optional[list[str]] = None) -> None:
        self.image = image
        self.io_dir = io_dir
        self.entrypoint = list(entrypoint or [])

    async def dispatch(self) -> JobHandle:  # pragma: no cover - needs a daemon
        rc, out = await _docker(*build_docker_run_argv(self.image, self.io_dir, self.entrypoint))
        if rc != 0:
            raise JobFailed(f"docker run failed: {out}")
        return JobHandle(job_id=out.strip(), backend="docker", meta={"image": self.image})

    async def poll(self, handle: JobHandle) -> JobStatus:  # pragma: no cover - needs a daemon
        rc, state = await _docker(
            "docker", "inspect", "--format", "{{.State.Status}}:{{.State.ExitCode}}", handle.job_id
        )
        if rc != 0:
            return JobStatus(state="failed", error=f"inspect failed: {state}")
        status, _, code = state.partition(":")
        if status != "exited":
            return JobStatus(state="running")
        if code.strip() != "0":
            return JobStatus(state="failed", error=f"container exit {code}")
        out_path = Path(self.io_dir) / "out.json"
        output = json.loads(out_path.read_text()) if out_path.exists() else {}
        return JobStatus(state="succeeded", output=output)

    async def cancel(self, handle: JobHandle) -> None:  # pragma: no cover - needs a daemon
        await _docker("docker", "rm", "--force", handle.job_id)


__all__ = [
    "JobCancelled", "JobFailed", "JobTimeout", "JobHandle", "JobStatus", "JobDriver",
    "RemoteJobOperator", "HttpRemoteJobOperation", "is_remote_job_operator",
    "run_job_lifecycle",
    "RemoteJobDriver", "DockerJobDriver", "build_docker_run_argv",
]
