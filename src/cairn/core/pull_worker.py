"""The pull worker (#565, ADR-0104 D2) — a machine joins by dialling out.

One command on any machine with a container runtime and a network route to the
control plane::

    cairn worker --pull --engine-url https://app.example.com --token <worker-pool token> \\
                 --capabilities gpu=T4

With sign-in on, the token is a worker credential (an internal issue): a
worker-pool token from ``cairn remote worker-token``, or the deployment admin's.

The loop is four moves, and every one of them already existed in cairn except
the first:

1. **claim** — ``POST /jobs/claim`` hands back at most one queued job with a
   lease: its input, the image to run, and the live-channel env
   (``live_channel_env_for``) the container reports on.
2. **run** — ``docker run --rm --gpus all -e <live-channel env> <image>``. The
   ``spawn`` seam is injectable so the command shape is provable without a
   daemon; ``--exec-local`` swaps the whole runner for an in-process call, which
   is how a stub job is exercised in tests and on a machine without Docker.
3. **heartbeat** — ``POST /jobs/{id}/heartbeat`` while the job runs, extending
   the lease. Stop heartbeating (crash, network, power) and the lease expires,
   which puts the job back on the queue for the next worker.
4. **report** — the container streams its curve over the job channel on its own
   (``operator_training.live``, #2025); the worker only relays for runners that
   have no transport of their own, and sends the terminal ``JobStatus`` through
   the last heartbeat. The control plane's ``drive_once`` reads that heartbeat
   and resumes the paused run exactly as it does for a pushed provider job.

There is deliberately **no inbound port and no central key**: the worker holds a
token, the control plane holds nothing about the worker.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence

from cairn.core.operator.job_store import DEFAULT_LEASE_S
from cairn.core.operator.jobs import JobStatus

logger = logging.getLogger("cairn.pull_worker")

#: How often the worker extends its lease while a job runs. Well inside
#: ``DEFAULT_LEASE_S`` so a slow tick is not a lost job.
DEFAULT_HEARTBEAT_S = 30.0
#: How long the worker waits before asking for work again on an empty queue.
DEFAULT_POLL_S = 5.0
#: Tail of container output kept on a failure — enough to see the traceback,
#: not enough to post a log file through the heartbeat.
_ERROR_TAIL = 4000


def iso_in(seconds: float = 0.0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def default_worker_id() -> str:
    return f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


def parse_capabilities(pairs: Optional[Sequence[str]]) -> dict[str, str]:
    """``["gpu=T4", "zone=eu"]`` → ``{"gpu": "T4", "zone": "eu"}``. A bare word
    is a flag (``"gpu"`` → ``{"gpu": "true"}``)."""
    out: dict[str, str] = {}
    for raw in pairs or []:
        for part in str(raw).split(","):
            part = part.strip()
            if not part:
                continue
            key, _, value = part.partition("=")
            out[key.strip()] = value.strip() or "true"
    return out


@dataclass
class ClaimedJob:
    """One job a worker holds a lease on — what ``POST /jobs/claim`` handed back."""

    id: str
    run_id: str
    node_name: str
    provider: str
    input: dict[str, Any] = field(default_factory=dict)
    image: str = ""
    env: dict[str, str] = field(default_factory=dict)
    lease_until: str = ""

    @classmethod
    def from_payload(cls, body: Mapping[str, Any]) -> "ClaimedJob":
        return cls(
            id=str(body.get("id") or ""), run_id=str(body.get("run_id") or ""),
            node_name=str(body.get("node_name") or ""), provider=str(body.get("provider") or ""),
            input=dict(body.get("input") or {}), image=str(body.get("image") or ""),
            env={str(k): str(v) for k, v in (body.get("env") or {}).items()},
            lease_until=str(body.get("lease_until") or ""),
        )


#: run(job) → the terminal :class:`JobStatus` the worker heartbeats back.
Runner = Callable[[ClaimedJob], Awaitable[JobStatus]]
#: spawn(argv) → (exit code, output tail). The Docker seam.
SpawnFn = Callable[[Sequence[str]], Awaitable[tuple[int, str]]]


# ─── running the job: docker, or in-process ──────────────────────────────


def docker_run_argv(job: ClaimedJob, *, image: Optional[str] = None, gpus: bool = True,
                    extra: Sequence[str] = ()) -> list[str]:
    """The command a worker runs for ``job``. ``--rm`` because the record, not
    the container, is the account of what happened; the live-channel env is the
    ONLY thing passed in, and it is scoped to this run."""
    argv = ["docker", "run", "--rm"]
    if gpus:
        argv += ["--gpus", "all"]
    for key, value in sorted(job.env.items()):
        argv += ["-e", f"{key}={value}"]
    argv += list(extra)
    argv.append(image or job.image)
    return argv


async def _spawn(argv: Sequence[str]) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
    out, _ = await proc.communicate()
    return int(proc.returncode or 0), (out or b"").decode("utf-8", "replace")


def docker_runner(*, gpus: bool = True, spawn: Optional[SpawnFn] = None,
                  image: Optional[str] = None, extra: Sequence[str] = ()) -> Runner:
    """Run the job's image. The container reports its own curve over the job
    channel (it has the live-channel env); the exit code is the verdict."""

    async def run(job: ClaimedJob) -> JobStatus:
        target = image or job.image
        if not target:
            return JobStatus(state="failed", error=(
                "this job names no container image — register it with an `image`, or run the "
                "worker with --image to pin one"))
        argv = docker_run_argv(job, image=target, gpus=gpus, extra=extra)
        logger.info("running %s for %s", target, job.id)
        code, out = await (spawn or _spawn)(argv)
        tail = out[-_ERROR_TAIL:]
        if code == 0:
            return JobStatus(state="succeeded", output={"image": target}, log_tail=tail)
        return JobStatus(state="failed", error=f"{target} exited {code}: {tail.strip()}",
                         retriable=code in (125, 137), log_tail=tail)

    return run


def local_exec_runner(registry: Any = None) -> Runner:
    """``--exec-local``: run the job's operator IN THIS PROCESS instead of a
    container. The escape hatch for a machine with no Docker and the path a stub
    job takes in tests — the live-channel env is exported around the call, so an
    operator that builds its reporter from the environment (the training pack's
    ``live.py``) streams its curve exactly as it would inside the image."""

    async def run(job: ClaimedJob) -> JobStatus:
        from cairn.core.operator.protocol import OperationContext  # noqa: PLC0415 — cycle: the operator stack imports this module's CLI
        from cairn.core.operator.registry import default_registry  # noqa: PLC0415 — same

        reg = registry or default_registry
        op_cls = reg.get(job.provider)
        op = op_cls() if isinstance(op_cls, type) else op_cls
        inp = op_cls.input_schema.model_validate(job.input)
        ctx = OperationContext(pipeline_id=job.run_id, node_name=job.node_name)
        prior = {k: os.environ.get(k) for k in job.env}
        os.environ.update(job.env)
        try:
            out = await op.run(inp, ctx)
        finally:
            for key, was in prior.items():
                if was is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = was
        if hasattr(out, "model_dump"):
            out = out.model_dump()
        return JobStatus(state="succeeded", output=out if isinstance(out, dict) else {"result": out})

    return run


# ─── the worker ──────────────────────────────────────────────────────────


class PullWorker:
    """Claim → run → heartbeat → report, against one control plane.

    ``client`` is an injected ``httpx.AsyncClient`` (tests drive the real app
    over ASGI); left out, the worker builds one against ``engine_url``.
    """

    def __init__(self, engine_url: str, token: str = "", *, worker_id: Optional[str] = None,
                 capabilities: Optional[Mapping[str, str]] = None, runner: Optional[Runner] = None,
                 client: Any = None, lease_s: float = DEFAULT_LEASE_S,
                 heartbeat_s: float = DEFAULT_HEARTBEAT_S, poll_s: float = DEFAULT_POLL_S,
                 image: Optional[str] = None) -> None:
        self.engine_url = engine_url.rstrip("/")
        self.token = token
        self.worker_id = worker_id or default_worker_id()
        self.capabilities = dict(capabilities or {})
        self.runner = runner or docker_runner(image=image)
        self.lease_s = lease_s
        self.heartbeat_s = heartbeat_s
        self.poll_s = poll_s
        self._client = client
        self._owns_client = client is None

    # -- transport --

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"} if self.token else {}

    async def _http(self) -> Any:
        if self._client is None:
            import httpx  # noqa: PLC0415 — optional at import time; the worker is the only caller

            self._client = httpx.AsyncClient(base_url=self.engine_url, timeout=60.0)
        return self._client

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "PullWorker":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    # -- the four moves --

    async def claim(self) -> Optional[ClaimedJob]:
        """Ask for one job. ``None`` when the queue has nothing for this worker."""
        c = await self._http()
        r = await c.post("/jobs/claim", headers=self._headers(), json={
            "worker_id": self.worker_id, "capabilities": self.capabilities,
            "lease_seconds": int(self.lease_s)})
        if r.status_code == 204:
            return None
        r.raise_for_status()
        return ClaimedJob.from_payload(r.json())

    async def heartbeat(self, job_id: str, status: JobStatus, *,
                        lease_s: Optional[float] = None) -> dict[str, Any]:
        """Extend the lease and record the status the reconciler reads."""
        c = await self._http()
        out = status.output if isinstance(status.output, dict) else None
        r = await c.post(f"/jobs/{job_id}/heartbeat", headers=self._headers(), json={
            "worker_id": self.worker_id, "status": status.state, "error": status.error or "",
            "metrics": dict(status.metrics or {}), "output": out,
            "lease_seconds": int(lease_s if lease_s is not None else self.lease_s)})
        r.raise_for_status()
        return dict(r.json())

    async def report(self, run_id: str, events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        """Relay report events for a runner with no transport of its own. A real
        container posts these itself; this is the same route (#2025)."""
        c = await self._http()
        r = await c.post(f"/runs/{run_id}/events/ingest", headers=self._headers(),
                         json={"events": [dict(e) for e in events]})
        r.raise_for_status()
        return dict(r.json())

    async def _beat_until_done(self, job_id: str, stop: asyncio.Event) -> None:
        while not stop.is_set():
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self.heartbeat_s)
            if stop.is_set():
                return
            try:
                await self.heartbeat(job_id, JobStatus(state="running"))
            except Exception:  # noqa: BLE001 — a missed beat is not a dead job; the lease covers it
                logger.debug("heartbeat for %s failed", job_id, exc_info=True)

    async def run_once(self) -> Optional[ClaimedJob]:
        """Claim one job, run it to a terminal status, report it. ``None`` when
        there was nothing to claim."""
        job = await self.claim()
        if job is None:
            return None
        await self.heartbeat(job.id, JobStatus(state="running"))
        stop = asyncio.Event()
        beat = asyncio.create_task(self._beat_until_done(job.id, stop))
        try:
            status = await self.runner(job)
        except Exception as e:  # noqa: BLE001 — a runner blowing up is a FAILED job, not a dead worker
            logger.exception("job %s failed in the runner", job.id)
            status = JobStatus(state="failed", error=f"{type(e).__name__}: {e}")
        finally:
            stop.set()
            with contextlib.suppress(asyncio.CancelledError):
                await beat
        await self.heartbeat(job.id, status)
        return job

    async def run_forever(self, *, max_jobs: Optional[int] = None,
                          stop: Optional[asyncio.Event] = None) -> int:
        """Claim and run until ``max_jobs`` are done or ``stop`` is set. Sleeps
        ``poll_s`` on an empty queue; a transport error is a slept-through blip,
        never an exit — a worker that dies on a network hiccup is not a worker."""
        done = 0
        while (max_jobs is None or done < max_jobs) and not (stop and stop.is_set()):
            try:
                job = await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — the control plane blinked; try again
                logger.warning("claim/report failed; retrying in %.0fs", self.poll_s, exc_info=True)
                job = None
            if job is not None:
                done += 1
                continue
            if max_jobs is not None and done >= max_jobs:
                break
            with contextlib.suppress(asyncio.TimeoutError):
                if stop is not None:
                    await asyncio.wait_for(stop.wait(), timeout=self.poll_s)
                else:
                    await asyncio.sleep(self.poll_s)
        return done


__all__ = [
    "ClaimedJob", "DEFAULT_HEARTBEAT_S", "DEFAULT_POLL_S", "PullWorker", "Runner", "SpawnFn",
    "default_worker_id", "docker_run_argv", "docker_runner", "iso_in", "local_exec_runner",
    "parse_capabilities",
]
