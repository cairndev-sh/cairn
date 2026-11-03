"""Remote-job control worker (ADR-0038) — the generic "small worker".

Claims active :class:`RemoteJobRecord`s from the store, polls each remote job,
and on a terminal state records the result + **resumes the paused run**. It is
**not** a graph node — it's standalone execution machinery (a lifespan task, a
standalone ``cairn-job-controller``, or a Temporal worker) that drives *any*
remote job across all runs/providers.

Decoupled from any operator/provider via injected ``poll``/``resume`` callables —
the same shape as the trigger inbox's drain loop (``drain_once``). Default wiring
(resolve the operator by provider to poll; ``RunDispatcher.resume_run`` to resume)
is a follow-up slice; the loop here is fully unit-testable with fakes.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

from cairn.core.operator.job_store import RemoteJobRecord, RemoteJobStore, heartbeat_status
from cairn.core.operator.jobs import JobHandle, JobStatus
from cairn.core.operator.learning import LearningConfig, learning_verdict
from cairn.core.operator.stage_events import emit_stage_event

# ADR-0082 D8a (#1076): per-record stage-progress watermark for `stage` run
# events. In-memory on purpose — a control-loop restart re-emits at most one
# duplicate event, which the event fold tolerates (no store round-trip per poll).
_LAST_STAGE_DONE: dict[str, int | None] = {}

logger = logging.getLogger("cairn.jobs.control")

# poll(record) → current JobStatus (running / succeeded / failed).
PollFn = Callable[[RemoteJobRecord], Awaitable[JobStatus]]
# resume(record, status) → hand the paused run its result (or failure). Default
# wiring calls RunDispatcher.resume_run; injected for testability.
ResumeFn = Callable[[RemoteJobRecord, JobStatus], Awaitable[None]]
# cancel(record) → abort the remote job (the learning gate's L2 kill). Optional;
# default wiring resolves the operator and calls op.cancel.
CancelFn = Callable[[RemoteJobRecord], Awaitable[None]]
# relaunch(record) → RE-SUBMIT a resumably-dead job (box gone / OOM) so it resumes
# from its durable checkpoint. Returns the new JobHandle. Optional (#552/ADR-0064).
RelaunchFn = Callable[[RemoteJobRecord], Awaitable[JobHandle]]
# drain(record) → move the worker's live report events off the job channel into
# the run store (#2025). Optional; a `http` channel needs none (the API ingests).
DrainFn = Callable[[RemoteJobRecord], Awaitable[int]]


def _result_dict(output: Any) -> dict[str, Any]:
    if isinstance(output, dict):
        return output
    if hasattr(output, "model_dump"):
        return output.model_dump()
    return {}


async def _safe_resume(resume: ResumeFn, rec: RemoteJobRecord, status: JobStatus) -> None:
    try:
        await resume(rec, status)
    except Exception:  # noqa: BLE001 — a resume failure must not kill the loop
        logger.exception("resume failed for run %s (job %s)", rec.run_id, rec.id)


async def _safe_cancel(cancel: CancelFn, rec: RemoteJobRecord) -> bool:
    """``True`` when the cancel reached the job; a failure is logged, never raised."""
    try:
        await cancel(rec)
    except Exception:  # noqa: BLE001 — a cancel failure must not kill the loop
        logger.exception("cancel failed for run %s (job %s)", rec.run_id, rec.id)
        return False
    return True


async def _try_relaunch(
    relaunch: RelaunchFn, store: RemoteJobStore, rec: RemoteJobRecord, max_attempts: int,
) -> bool:
    """Relaunch a resumably-dead job from its durable checkpoint, IF one exists and
    the attempt budget isn't spent (#552). Returns True if relaunched (job stays
    active, run stays paused); False → caller should fail the run as usual."""
    if not rec.checkpoint_uri or rec.attempts >= max_attempts:
        return False
    try:
        handle = await relaunch(rec)
    except Exception:  # noqa: BLE001 — a relaunch error must not kill the loop; fail the run
        logger.exception("relaunch failed for run %s (job %s)", rec.run_id, rec.id)
        return False
    await store.mark_relaunch(rec.id, handle)
    logger.info(
        "relaunched run %s (job %s) attempt %d/%d from %s",
        rec.run_id, rec.id, rec.attempts + 1, max_attempts, rec.checkpoint_uri,
    )
    return True


async def drive_once(
    store: RemoteJobStore,
    poll: PollFn,
    resume: ResumeFn,
    *,
    cancel: Optional[CancelFn] = None,
    learning: Optional[LearningConfig] = None,
    relaunch: Optional[RelaunchFn] = None,
    drain: Optional[DrainFn] = None,
    max_attempts: int = 2,
    limit: int = 50,
) -> dict[str, int]:
    """Poll every active job once; on terminal, record result + resume the run.
    A poll that raises fails the job (and resumes with the failure) — never
    crashes the loop.

    When ``cancel`` + ``learning`` are given (ADR-0039 Gate 2), a still-running
    job whose poll reports an eval-loss trajectory (``status.metrics['eval_losses']``)
    is judged: if it isn't learning, the job is **aborted** (cancel + mark_failed +
    resume-failed) so the GPU is released mid-run.

    When ``relaunch`` is given (#552/ADR-0064), a ``failed`` poll that is
    ``retriable`` (box/instance gone, OOM — not a clean training error) AND has a
    recorded ``checkpoint_uri`` AND is under ``max_attempts`` is RE-SUBMITTED to
    resume from that checkpoint instead of failing the run. Returns a
    ``{done, failed, cancelled, aborted, relaunched, running, claimed}`` tally."""
    # ADR-0104 D2 (#565): a pull worker that died stops extending its lease. Put
    # its job back on the queue BEFORE this tick reads the active set, so a lost
    # worker costs one lease, not the run. Guarded because ``store`` is a
    # Protocol and the loop's test fakes predate the method.
    requeue = getattr(store, "requeue_expired", None)
    if requeue is not None:
        try:
            n = await requeue()
            if n:
                logger.info("re-queued %d job(s) whose worker lease expired", n)
        except Exception:  # noqa: BLE001 — a failed sweep must not kill the loop
            logger.debug("worker-lease sweep failed", exc_info=True)
    active = await store.claim_active(limit=limit)
    done = failed = cancelled = aborted = relaunched = running = 0
    for rec in active:
        # #2025: live report events first, so a job that finishes this tick
        # has its whole curve in the store before the run resumes.
        if drain is not None:
            try:
                await drain(rec)
            except Exception:  # noqa: BLE001 — a drain failure must not kill the loop
                logger.debug("drain failed for run %s", rec.run_id, exc_info=True)
        try:
            status = await poll(rec)
        except Exception as e:  # noqa: BLE001 — failed poll → fail the job
            err = f"{type(e).__name__}: {e}"
            await store.mark_failed(rec.id, err)
            await _safe_resume(resume, rec, JobStatus(state="failed", error=err))
            failed += 1
            continue
        # Persist a newly-reported durable checkpoint location so a later (or this)
        # resumable death can relaunch from it (#552). Update the local rec too.
        ck = (status.metrics or {}).get("checkpoint_uri")
        if ck and ck != rec.checkpoint_uri:
            await store.set_checkpoint_uri(rec.id, ck)
            rec = rec.model_copy(update={"checkpoint_uri": ck})
        # Persist the latest stdout tail so the console isn't blind on a live job (#553).
        if status.log_tail and status.log_tail != rec.last_log_tail:
            await store.set_log_tail(rec.id, status.log_tail)
            rec = rec.model_copy(update={"last_log_tail": status.log_tail})
        # ADR-0082 D8a (#1076): stage transitions in the poll's metrics become
        # `stage` RUN EVENTS — the run timeline sees inside the flow. Watermark
        # is in-memory per record (a loop restart re-emits at most one event,
        # which the fold tolerates); cleared on terminal.
        if status.state in ("queued", "running"):
            _LAST_STAGE_DONE[rec.id] = await emit_stage_event(
                rec.run_id, node_name=rec.node_name, job_id=rec.job_id,
                metrics=status.metrics or {}, last_done=_LAST_STAGE_DONE.get(rec.id),
            )
        else:
            _LAST_STAGE_DONE.pop(rec.id, None)
        if status.state == "succeeded":
            result = _result_dict(status.output)
            await store.mark_done(rec.id, result)
            await _safe_resume(resume, rec, status)
            done += 1
        elif status.state == "failed":
            if status.retriable and relaunch is not None and await _try_relaunch(
                relaunch, store, rec, max_attempts
            ):
                relaunched += 1
                continue  # run stays paused; the relaunched job will resume it
            await store.mark_failed(rec.id, status.error)
            await _safe_resume(resume, rec, status)
            failed += 1
        elif status.state == "cancelled":
            # ADR-0151 D9 — stopped by a cancel: terminal, never relaunched, and not
            # a trajectory the learning gate may judge. Closed and resumed as such.
            await store.mark_cancelled(rec.id)
            await _safe_resume(resume, rec, status)
            cancelled += 1
        elif cancel is not None and learning is not None and (status.metrics or {}).get("eval_losses"):
            # Mid-train quality gate: is it learning? If not, abort to stop spend.
            v = learning_verdict(status.metrics["eval_losses"], learning)
            if not v.learning:
                await _safe_cancel(cancel, rec)
                reason = f"early-abort: {v.verdict} — {v.reason}"
                await store.mark_failed(rec.id, reason)
                await _safe_resume(resume, rec, JobStatus(state="failed", error=reason))
                aborted += 1
            else:
                running += 1
        else:
            running += 1  # still running — leave it active for the next tick
    return {"done": done, "failed": failed, "cancelled": cancelled, "aborted": aborted, "relaunched": relaunched,
            "running": running, "claimed": len(active)}


async def run_control_loop(
    store: RemoteJobStore,
    poll: PollFn,
    resume: ResumeFn,
    *,
    cancel: Optional[CancelFn] = None,
    learning: Optional[LearningConfig] = None,
    relaunch: Optional[RelaunchFn] = None,
    max_attempts: int = 2,
    interval: float = 5.0,
) -> None:
    """Background loop: drive active jobs every ``interval`` seconds. Cancel the
    task to stop (server lifespan / standalone worker). On crash + restart the
    store still holds the active jobs, so the loop re-attaches by JobHandle.
    ``cancel`` + ``learning`` enable the mid-train early-abort (ADR-0039 Gate 2);
    ``relaunch`` + ``max_attempts`` enable resume-from-checkpoint on a resumable
    death (#552)."""
    while True:
        try:
            await drive_once(
                store, poll, resume, cancel=cancel, learning=learning,
                relaunch=relaunch, max_attempts=max_attempts,
            )
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — a bad tick must not kill the loop
            logger.exception("remote-job control tick failed")
        await asyncio.sleep(interval)


# ─── default wiring (resolve operator by provider; resume via dispatcher) ──


async def _record_compute(rec: RemoteJobRecord) -> Any:
    """The job's compute connection, resolved in the account that owns the job's
    workspace. The control worker runs outside any request, so the record — not
    the ambient context — names the account; a tenant's job never resolves the
    deployment's connection or another account's by the same name."""
    from cairn.core.connections import resolve_connection  # noqa: PLC0415 — lazy: off the import path
    from cairn.core.workspace import workspace_account  # noqa: PLC0415 — lazy, as above

    # called inside workspace_scope(rec.workspace_id): a record naming no workspace has
    # already been refused with auth on, and means the implicit default with auth off
    account = await workspace_account(rec.workspace_id or None)
    return await resolve_connection(rec.compute_target, account_id=account)


async def _record_scope(rec: RemoteJobRecord, op: Any) -> Any:
    """``workspace_scope(rec.workspace_id)`` for a job driven outside any request.
    A record that names no workspace is refused with auth on; the job it names is
    then cancelled (``op.cancel``, best-effort) before the refusal propagates, as
    nothing will poll or resume it any more and it would keep running."""
    from cairn.core.workspace import WorkspaceScopeRequired, workspace_scope  # noqa: PLC0415 — lazy: off the import path

    try:
        return workspace_scope(rec.workspace_id)
    except WorkspaceScopeRequired:
        try:
            await op.cancel(rec.handle())
        except Exception:  # noqa: BLE001 — best-effort: the refusal itself must still land
            logger.exception("could not cancel refused job %s (run %s)", rec.id, rec.run_id)
        raise


def build_default_poll(registry: Any = None) -> PollFn:
    """A ``poll`` that resolves the operator by ``record.provider`` (operator
    name) from the registry, reconstructs its input, rebuilds a context (with
    ``ctx.compute`` from ``compute_target``, #121), and calls ``op.poll``.

    ADR-0104 D2 (#565): a **pull** job has no provider to poll — the machine
    dialled out and reports through ``heartbeat``. So its last heartbeat IS the
    poll result, and the rest of the loop (terminal → mark + resume, the
    learning gate, the stage events) is untouched."""

    async def poll(rec: RemoteJobRecord) -> JobStatus:
        from cairn.core.operator.protocol import OperationContext
        from cairn.core.operator.registry import default_registry

        pulled = heartbeat_status(rec)
        if pulled is not None:
            return pulled

        reg = registry or default_registry
        op_cls = reg.get(rec.provider)
        op = op_cls() if isinstance(op_cls, type) else op_cls
        inp = op_cls.input_schema.model_validate(rec.input)
        # ADR-0118 D4 — the flow door's admitted stage contracts, persisted in its handle meta
        ctx = OperationContext(pipeline_id=rec.run_id, node_name=rec.node_name,
                               dataflow_contracts=(rec.job_meta or {}).get("dataflow_contracts"))
        with await _record_scope(rec, op):  # the job's own workspace, not the worker's
            if rec.compute_target:
                ctx.compute = await _record_compute(rec)
            return await op.poll(rec.handle(), inp, ctx)

    return poll


def build_default_relaunch(registry: Any = None) -> RelaunchFn:
    """A ``relaunch`` that resolves the operator by ``record.provider`` and calls
    ``op.submit`` again with the record's original input + compute — re-arming a
    resumably-dead job so it resumes from its durable checkpoint (#552). Mirrors
    ``build_default_poll``: the box's runner pulls the checkpoint before training."""

    async def relaunch(rec: RemoteJobRecord) -> JobHandle:
        from cairn.core.operator.protocol import OperationContext
        from cairn.core.operator.registry import default_registry

        reg = registry or default_registry
        op_cls = reg.get(rec.provider)
        op = op_cls() if isinstance(op_cls, type) else op_cls
        inp = op_cls.input_schema.model_validate(rec.input)
        # ADR-0118 D4 — the flow door's admitted stage contracts, persisted in its handle meta
        ctx = OperationContext(pipeline_id=rec.run_id, node_name=rec.node_name,
                               dataflow_contracts=(rec.job_meta or {}).get("dataflow_contracts"))
        with await _record_scope(rec, op):  # the job's own workspace, not the worker's
            if rec.compute_target:
                ctx.compute = await _record_compute(rec)
            return await op.submit(inp, ctx)

    return relaunch


#: Per-record drain cursor (the channel's opaque position). In-memory like
#: ``_LAST_STAGE_DONE``: a loop restart replays from the channel's start and the
#: store's idempotent ingest drops what it already has.
_DRAIN_CURSOR: dict[str, Any] = {}


def build_default_drain(channel: Any = None, store: Any = None) -> DrainFn:
    """A ``drain`` over the process job channel into the run store (#2025)."""
    from cairn.core.operator.job_channel import drain_channel_events, get_job_channel  # noqa: PLC0415 — cycle: job_channel imports this module's types

    chan = channel or get_job_channel()

    async def drain(rec: RemoteJobRecord) -> int:
        nonlocal store
        if store is None:
            from cairn.core.storage import get_storage  # noqa: PLC0415 — keep the storage stack off the operator import path
            store = get_storage().runs
        n, _DRAIN_CURSOR[rec.id] = await drain_channel_events(chan, store, rec.run_id, _DRAIN_CURSOR.get(rec.id))
        return n

    return drain


def build_default_cancel(registry: Any = None) -> CancelFn:
    """A ``cancel`` that resolves the operator by ``record.provider`` and calls
    its ``op.cancel(handle)`` — the L2 abort the learning gate triggers."""

    async def cancel(rec: RemoteJobRecord) -> None:
        from cairn.core.operator.registry import default_registry

        reg = registry or default_registry
        op_cls = reg.get(rec.provider)
        op = op_cls() if isinstance(op_cls, type) else op_cls
        await op.cancel(rec.handle())

    return cancel


async def cancel_remote_jobs_for_run(run_id: str, *, registry: Any = None) -> int:
    """Cancel every ACTIVE remote job belonging to ``run_id`` (op.cancel — e.g.
    remote/exec SSH-kills the pid) and mark its record cancelled.

    Called when a run is cancelled so a cancelled run does NOT orphan the remote
    (GPU) job — a leaked marketplace pod or an SSH training process keeps billing
    (#601). Best-effort per job (a cancel failure is logged, never raised); every
    record is closed, and the count is of the jobs the cancel actually reached."""
    from cairn.core.operator.job_store import get_remote_job_store  # noqa: PLC0415

    store = get_remote_job_store()
    cancel = build_default_cancel(registry)
    n = 0
    for rec in await active_remote_jobs_for_run(run_id, store=store):
        reached = await _safe_cancel(cancel, rec)
        await store.mark_cancelled(rec.id)
        n += int(reached)
    return n


async def active_remote_jobs_for_run(run_id: str, *, store: Any = None) -> list[RemoteJobRecord]:
    """The ACTIVE (submitted/running) remote-job records of ``run_id`` — what a
    run cancel kills, and what tells a restarted bridge (#2555) that a run the
    dispatcher keeps ``running`` is paused on a job the control loop will resume."""
    from cairn.core.operator.job_store import _ACTIVE, get_remote_job_store  # noqa: PLC0415

    store = store or get_remote_job_store()
    # By status, so an active job is not crowded out by newer finished ones.
    return [rec for status in _ACTIVE for rec in await store.list(status=status, limit=500)
            if rec.run_id == run_id]


def build_default_resume(dispatcher: Any) -> ResumeFn:
    """A ``resume`` that wakes the paused run via ``RunDispatcher.resume_run`` —
    the same path HITL uses. The re-run reads the terminal record from the store,
    so the decision payload just carries the outcome."""

    async def resume(rec: RemoteJobRecord, status: JobStatus) -> None:
        if status.state == "succeeded":
            decision = {"status": "succeeded", "result": _result_dict(status.output)}
        elif status.state == "cancelled":  # ADR-0151 D9 — a cancel, not a crash
            decision = {"status": "cancelled", "error": status.error or "remote job cancelled"}
        else:
            decision = {"status": "failed", "error": status.error or "remote job failed"}
        await dispatcher.resume_run(rec.run_id, decision)

    return resume


__all__ = [
    "PollFn", "ResumeFn", "CancelFn", "RelaunchFn", "drive_once", "run_control_loop",
    "build_default_poll", "build_default_resume", "build_default_cancel", "build_default_drain",
    "build_default_relaunch",
]
