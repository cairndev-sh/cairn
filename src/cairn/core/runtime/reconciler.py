"""Run reconciler (#976) — heal "zombie" runs stuck at a non-terminal status.

Under ``CAIRN_RUNTIME=temporal`` a run's terminal event is written from *inside*
the graph execution path. When a workflow reaches a terminal state OUTSIDE that
path — the langgraph-plugin activity dies, retries exhaust, the workflow fails
or is terminated, or a worker restart drops the in-flight completion — cairn's
run store never gets the ``done``/``failed`` event. The run then shows
``running`` in the console forever (a zombie), and cancel can't help (#958)
because there is nothing live to interrupt.

This is the write-side complement to #977 (which made the ``status`` COLUMN
authoritative on read): a periodic sweep asks Temporal for the *actual* status
of every run cairn still thinks is active and, when Temporal says the workflow
is terminal, writes the matching terminal event + status. It mirrors
:class:`cairn.core.approval.reaper.ApprovalReaper` (start/shutdown/_loop) and is
started from the trigger server's lifespan on EVERY runtime, as a leader-elected
singleton (#1533). Beside the runtime verdict it has two liveness verdicts for
runs no runtime can see: the flow-leg heartbeat (ADR-0086 H1) and the owner
verdict for in-process legs (ADR-0125 D3, :func:`owner_verdict`), which runs
first.

The Temporal query is injected as ``status_fn`` so this module never imports
``temporalio`` (an optional extra): the server wires it to
:meth:`TemporalRuntime.workflow_terminal_status`.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from cairn.core.liveness import PROBE_TIMEOUT_SECONDS  # ADR-0125 D3 — one bound on a probe
from cairn.core.storage.protocols import ACTIVE_STATUSES, RunStore
from cairn.core.storage.transitions import PIPELINE_TERMINAL

logger = logging.getLogger("cairn.runtime.reconciler")

# run_id → cairn terminal status, or None if the workflow is still active /
# unknown / not found (in which case the run is left untouched).
StatusFn = Callable[[str], Awaitable[Optional[str]]]

#: (run_id, event_type, data) → publish the terminal event the way the
#: dispatcher would (``run.<type>`` on the bus). Without it a reconciled leg
#: run terminalizes in the STORE but its pipeline never hears — the bridge
#: chains on bus events, so the pipeline would hang exactly as before, one
#: layer up (ADR-0086 H1).
EmitFn = Callable[[str, str, dict], Awaitable[None]]

#: (run) → True when a DURABLE OWNER is still alive for this run and will
#: retry/re-adopt it itself (#1429/#1412 G1: a Temporal-owned pipeline leg whose
#: workflow is still RUNNING). The heartbeat verdict must DEFER to it: failing
#: the leg after 300s of worker outage races the owner's own activity retry —
#: found live 2026-08-18: worker down 10 min → reconciler failed the leg at 5
#: min → the retried activity found a failed record and honored it → pipeline
#: failed instead of re-adopting. The runtime verdict (status_fn) still wins.
OwnerAliveFn = Callable[[Any], Awaitable[bool]]
#: run_id → the finished workflow's return value, or None (#1869). Consulted
#: only for a ``done`` verdict: a run whose submitter died (DBOS recovery after a
#: restart) finishes with nobody awaiting the result, and reconciling from the
#: STATUS alone left ``result_payloads`` empty — a "done" run with nothing to show.
ResultFn = Callable[[str], Awaitable[Optional[Any]]]

DEFAULT_SWEEP_SECONDS = 60.0
# Don't reconcile a run younger than this: a just-submitted run's workflow may
# not be queryable yet, and its own terminal write may still be in flight. The
# grace window keeps the sweep from racing a healthy run to a false terminal.
DEFAULT_GRACE_SECONDS = 120.0
# ADR-0086 H1 (#1239): a `running` run whose heartbeat is older than this is an
# ORPHAN — its poll loop is dead (worker crash, leg task lost in a restart).
# The leg stamps every ≥10s and polls at most every 20s (leg_executor), so 300s
# is ~15 missed beats: far past slow, safely into dead. 0 disables the verdict.
DEFAULT_HEARTBEAT_TTL_SECONDS = 300.0


def sweep_seconds_from_env() -> float:
    try:
        return float(os.environ.get("CAIRN_RECONCILE_SWEEP_SECONDS", "") or DEFAULT_SWEEP_SECONDS)
    except ValueError:
        return DEFAULT_SWEEP_SECONDS


def grace_seconds_from_env() -> float:
    try:
        return float(os.environ.get("CAIRN_RECONCILE_GRACE_SECONDS", "") or DEFAULT_GRACE_SECONDS)
    except ValueError:
        return DEFAULT_GRACE_SECONDS


def heartbeat_ttl_from_env() -> float:
    """``CAIRN_RECONCILE_HEARTBEAT_TTL_SECONDS`` — 0 disables the heartbeat
    verdict (the Temporal status_fn keeps working either way)."""
    try:
        return float(os.environ.get("CAIRN_RECONCILE_HEARTBEAT_TTL_SECONDS", "")
                     or DEFAULT_HEARTBEAT_TTL_SECONDS)
    except ValueError:
        return DEFAULT_HEARTBEAT_TTL_SECONDS


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _heartbeat_verdict(run: Any, now: datetime, ttl_seconds: float) -> Optional[str]:
    """ADR-0086 H1 (#1239): ``failed`` when a *running* run's own liveness stamp
    has expired — its poll loop is dead, so nothing will ever terminalize it.

    Deliberately narrow: only ``running`` (queued runs haven't started a loop;
    ``awaiting_approval`` waits don't beat by design — the approval reaper owns
    that lifecycle), and only runs that HAVE a stamp — a legacy row or a
    template run is never judged by a silence it never promised to break."""
    if ttl_seconds <= 0 or str(getattr(run, "status", "")) != "running":
        return None
    beat = _parse_iso(getattr(run, "last_heartbeat_at", None))
    if beat is None:
        return None
    return "failed" if (now - beat).total_seconds() > ttl_seconds else None


# ── ADR-0125 D3: the owner verdict for in-process legs ───────────────────

#: owner id → ``True`` when that owner's lock is free, ``False`` when it is held,
#: ``None`` when the probe cannot tell. Implemented in ``core/liveness.py``
#: (``lock_free``) and injected, the way ``owner_alive_fn`` is.
LockFreeFn = Callable[[str], Awaitable[Optional[bool]]]
#: (run) → hand a DEAD leg of a running/paused pipeline to the pipeline bridge,
#: which applies its adopt rules to it (ADR-0125 D4). True when it acted.
DeadLegFn = Callable[[Any], Awaitable[bool]]
#: pipeline id → its status, or None when there is no such pipeline.
PipelineStatusFn = Callable[[str], Awaitable[Optional[str]]]
#: run id → the dispatcher's cancel (closes the run's remote jobs and budget holds).
CancelFn = Callable[[str], Awaitable[Any]]

ALIVE = "alive"
UNDETERMINED = "undetermined"
DEAD = "dead"
LEGACY = "legacy"

#: The run statuses the owner verdict judges. ``awaiting_approval`` waits on a
#: person, not a process.
OWNER_JUDGED_STATUSES = frozenset({"queued", "running"})


async def owner_verdict(run: Any, probe: Optional[LockFreeFn], *, self_owner: str,
                        first_free_seen: bool = False,
                        cache: Optional[dict[str, Optional[bool]]] = None) -> str:
    """ADR-0125 D3 — is the process that owns this in-process leg alive?

    The first row that matches wins:

    1. ``owner`` is this process's owner id → ALIVE.
    2. the probe raises or times out → UNDETERMINED.
    3. the probe finds the owner's lock held → ALIVE.
    4. the lock is free, and ``first_free_seen`` (an earlier probe more than one
       re-acquire interval ago found it free too) → DEAD.
    5. the lock is free for the first time → UNDETERMINED.
    6. ``owner`` is empty (written before ADR-0125, or by a durable runtime) →
       LEGACY: the #2540 rules and the heartbeat verdict apply.

    Only the reconciler can pass ``first_free_seen``; adopt is one-shot and never
    sees DEAD. ``cache`` (owner → probe result) makes one probe per distinct
    owner per pass. UNDETERMINED never justifies a destructive action."""
    owner = str(getattr(run, "owner", "") or "")
    if not owner:
        return LEGACY
    if self_owner and owner == self_owner:
        return ALIVE
    if cache is not None and owner in cache:
        free = cache[owner]
    else:
        free = None
        if probe is not None:
            try:
                free = await asyncio.wait_for(probe(owner), timeout=PROBE_TIMEOUT_SECONDS)
            except Exception as exc:  # noqa: BLE001 — row 2: unknown, never dead
                logger.debug("owner probe for %s failed: %s", owner, exc)
                free = None
        if cache is not None:
            cache[owner] = free
    if free is None:
        return UNDETERMINED
    if not free:
        return ALIVE
    return DEAD if first_free_seen else UNDETERMINED


class OwnerWatch:
    """The reconciler's memory of first-free observations, per owner (D3 row 4).

    An owner is DEAD only when its lock was found free on two probes more than
    one re-acquire interval apart, so a Postgres restart, which drops every owner
    lock at once, never kills live legs: their processes re-acquire in one
    interval. Reset when leadership changes (a new leader starts afresh)."""

    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = interval_seconds
        self._first_free: dict[str, datetime] = {}

    def first_free_seen(self, owner: str, now: datetime) -> bool:
        seen = self._first_free.get(owner)
        return seen is not None and (now - seen).total_seconds() > self.interval_seconds

    def observe(self, results: dict[str, Optional[bool]], now: datetime) -> None:
        for owner, free in results.items():
            if free is True:
                self._first_free.setdefault(owner, now)
            elif free is False:
                self._first_free.pop(owner, None)

    def reset(self) -> None:
        self._first_free.clear()


async def reconcile_stale_runs(
    store: RunStore,
    status_fn: StatusFn,
    *,
    grace_seconds: float = DEFAULT_GRACE_SECONDS,
    now: Optional[datetime] = None,
    workspace_id: Optional[str] = None,
    limit: int = 500,
    heartbeat_ttl_seconds: float = 0.0,
    emit_fn: Optional[EmitFn] = None,
    owner_alive_fn: Optional[OwnerAliveFn] = None,
    result_fn: Optional[ResultFn] = None,
    owner_probe: Optional[LockFreeFn] = None,
    self_owner: str = "",
    owner_watch: Optional[OwnerWatch] = None,
    dead_leg_fn: Optional[DeadLegFn] = None,
    pipeline_status_fn: Optional[PipelineStatusFn] = None,
    cancel_fn: Optional[CancelFn] = None,
) -> list[str]:
    """One sweep: reconcile every run cairn still thinks is active whose Temporal
    workflow has actually reached a terminal state. Returns the reconciled run ids.

    For each active run older than ``grace_seconds``, ``status_fn(run_id)`` returns
    the cairn terminal status (``done``/``failed``/``cancelled``) if the workflow
    is terminal, or ``None`` if it is still running / paused / unknown — in which
    case the run is left untouched. A terminal verdict writes both the ``status``
    (via :meth:`RunStore.update`) and an audit event tagged ``reconciled_by`` so a
    reconciled ``done`` is honest that its result/report may be incomplete (the
    workflow finished but cairn never captured the output).

    ADR-0125 D3/D4 — a ``queued``/``running`` run that names an ``owner`` is
    judged by :func:`owner_verdict` before the heartbeat verdict (``owner_probe``
    given): ALIVE and UNDETERMINED leave it alone, LEGACY falls through to the
    heartbeat verdict, and DEAD (two free probes, tracked in ``owner_watch``)
    either settles it ``cancelled`` when its pipeline is terminal or gone — through
    ``cancel_fn`` first, which closes its remote jobs and budget holds — or hands
    it to the pipeline bridge (``dead_leg_fn``), whose adopt rules decide. A run
    this process owns (``self_owner``) is never judged.

    A per-run failure is logged and skipped — one bad run must not abort the sweep.
    """
    now = now or datetime.now(timezone.utc)
    reconciled: list[str] = []
    probed: dict[str, Optional[bool]] = {}  # D3 — one probe per distinct owner per sweep
    # ACTIVE_STATUSES = queued / running / awaiting_approval. An awaiting_approval
    # run under temporal is a workflow paused INTERNALLY (status RUNNING), so
    # status_fn returns None and we skip it — the approval reaper (#896) owns that
    # lifecycle. We only ever act when Temporal itself reports terminal.
    for status in sorted(ACTIVE_STATUSES):
        try:
            runs = await store.list(status=status, workspace_id=workspace_id, limit=limit)  # type: ignore[arg-type]
        except Exception as exc:  # noqa: BLE001
            logger.warning("reconciler: list(status=%s) failed: %s", status, exc)
            continue
        for run in runs:
            started = _parse_iso(getattr(run, "created_at", None))
            if started and (now - started).total_seconds() < grace_seconds:
                continue  # inside the grace window — too young to judge
            cause = "runtime"
            try:
                terminal = await status_fn(run.id)
            except Exception as exc:  # noqa: BLE001 — a bad query must not abort the sweep
                logger.debug("reconciler: status_fn(%s) failed: %s", run.id, exc)
                continue
            if (not terminal and owner_probe is not None and status in OWNER_JUDGED_STATUSES
                    and getattr(run, "bundle_pipeline_id", "")):
                # ADR-0125 D3 — an in-process LEG names its owner process; ask
                # whether that process still holds its lock BEFORE judging by
                # silence (a task or template leg never beats, and a live leg
                # whose loop is blocked stops beating but keeps its lock). A run
                # that belongs to no pipeline is not a leg: it is never judged
                # here, whatever its ``owner`` says — a standalone template run
                # paused on a remote job is resumed by the remote-job control
                # loop, and settling it would cancel that job.
                verdict = await owner_verdict(
                    run, owner_probe, self_owner=self_owner, cache=probed,
                    first_free_seen=bool(owner_watch and owner_watch.first_free_seen(
                        str(getattr(run, "owner", "") or ""), now)))
                if verdict == DEAD:
                    if await _settle_dead_leg(store, run, now, emit_fn=emit_fn,
                                              dead_leg_fn=dead_leg_fn,
                                              pipeline_status_fn=pipeline_status_fn,
                                              cancel_fn=cancel_fn):
                        reconciled.append(run.id)
                    continue
                if verdict != LEGACY:
                    continue  # ALIVE / UNDETERMINED — never a destructive action
            if not terminal:
                # H1 (#1239): no runtime verdict — judge liveness instead. The
                # runtime answer wins when both exist (it is authoritative);
                # the heartbeat only speaks for runs nothing else can see.
                terminal = _heartbeat_verdict(run, now, heartbeat_ttl_seconds)
                cause = "heartbeat"
                if terminal and owner_alive_fn is not None:
                    # A live durable owner CAN see it and will re-adopt (#1429);
                    # a silence-based failure here would pre-empt that.
                    try:
                        if await owner_alive_fn(run):
                            logger.info("reconciler: %s silent past TTL but its durable owner "
                                        "is alive — deferring to its retry/re-adopt", run.id)
                            continue
                    except Exception as exc:  # noqa: BLE001 — unknown owner ⇒ heartbeat rules
                        logger.debug("reconciler: owner_alive_fn(%s) failed: %s", run.id, exc)
            if not terminal or terminal == run.status:
                continue
            note = (
                "terminal state recovered from the runtime; "
                "the run's result/report may be incomplete"
                if cause == "runtime" else
                f"orphaned: no heartbeat for over {int(heartbeat_ttl_seconds)}s — "
                "the leg's poll loop is gone (worker crash or lost restart); "
                "any engine-side work was abandoned unobserved"
            )
            extra: dict[str, Any] = {}
            if terminal == "done" and result_fn is not None:
                # #1869 — fetch what the workflow returned and persist it the way
                # a live completion would, so a recovered run is not a hollow "done".
                try:
                    output = await result_fn(run.id)
                except Exception as exc:  # noqa: BLE001 — a missing result never blocks the verdict
                    logger.debug("reconciler: result_fn(%s) failed: %s", run.id, exc)
                    output = None
                if isinstance(output, dict) and output:
                    extra = _completion_fields(run, output, await _events_or_empty(store, run.id))
                    note = "terminal state AND result recovered from the runtime"
            try:
                # The list and the probes above took time, and a workflow that has just ended is
                # the moment its own task records why. That record is better than this one, so
                # it isn't overwritten and no second terminal is published. The store has no
                # conditional update, so a write landing between these two lines still can be.
                current = await store.get(run.id)
                if current is None or current.status not in ACTIVE_STATUSES:
                    logger.debug("reconciler: %s ended while it was being judged; leaving it", run.id)
                    continue
                await store.update(
                    run.id,
                    status=terminal,
                    ended_at=now.isoformat(),
                    error=(None if terminal == "done" else f"reconciled: {note}"),
                    **extra,
                )
                await store.append_event(
                    run.id,
                    terminal,
                    {
                        "reconciled_by": "reconciler",
                        "cause": cause,
                        "prior_status": run.status,
                        # honest signal: a reconciled `done` means the workflow
                        # finished but cairn never captured its output/report.
                        "note": note,
                        "result_recovered": bool(extra),
                    },
                )
                if emit_fn is not None:
                    # Publish through the dispatcher's path so `run.<type>` hits
                    # the bus and the PIPELINE advances/fails — a store-only
                    # terminal write leaves the pipeline hanging one tier up.
                    try:
                        await emit_fn(run.id, terminal, {
                            "reconciled_by": "reconciler", "cause": cause,
                            "prior_status": run.status, "error": note,
                        })
                    except Exception:  # noqa: BLE001 — best-effort like every emit
                        logger.debug("reconciler: emit for %s failed", run.id, exc_info=True)
                reconciled.append(run.id)
                logger.info("reconciler: %s %s -> %s (%s)", run.id, run.status, terminal, cause)
            except Exception as exc:  # noqa: BLE001
                logger.warning("reconciler: failed to reconcile %s: %s", run.id, exc)
    if owner_watch is not None:
        owner_watch.observe(probed, now)
    return reconciled


async def _settle_dead_leg(store: RunStore, run: Any, now: datetime, *,
                           emit_fn: Optional[EmitFn], dead_leg_fn: Optional[DeadLegFn],
                           pipeline_status_fn: Optional[PipelineStatusFn],
                           cancel_fn: Optional[CancelFn]) -> bool:
    """ADR-0125 D4 — act on a LEG whose owner process is DEAD. Returns True when
    the reconciler settled the run itself. A run with no ``bundle_pipeline_id``
    is not a leg and is never settled here (D3 applies to leg Runs).

    - Its pipeline is terminal, or its pipeline record is gone: nothing will
      ever finish the leg — cancel-then-crash. The dispatcher's cancel closes
      its remote jobs and budget holds, then the run is settled ``cancelled``
      with the reason.
    - Its pipeline is running or paused: the bridge applies its adopt rules (a
      task leg → #1289, a template or handle-less flow leg → failed and parked,
      a flow leg with a handle → re-attached, a finalizer → reset), so the
      restart risk rules decide, not a blanket failure."""
    pid = str(getattr(run, "bundle_pipeline_id", "") or "")
    if not pid:
        return False  # not a leg: out of scope, whatever its owner says
    pstatus: Optional[str] = None
    if pipeline_status_fn is None:
        return False  # nobody told us the pipeline's state: never guess
    try:
        pstatus = await pipeline_status_fn(pid)
    except Exception as exc:  # noqa: BLE001 — unknown pipeline state: act next sweep
        logger.debug("reconciler: pipeline_status_fn(%s) failed: %s", pid, exc)
        return False
    if pstatus is not None and pstatus not in PIPELINE_TERMINAL:
        if dead_leg_fn is not None:
            try:
                if await dead_leg_fn(run):
                    logger.info("reconciler: %s's owner %s is dead — handed to the "
                                "pipeline bridge", run.id, getattr(run, "owner", ""))
            except Exception as exc:  # noqa: BLE001 — the next sweep retries
                logger.warning("reconciler: dead-leg handoff for %s failed: %s", run.id, exc)
        return False
    note = (f"orphaned: its owner process {getattr(run, 'owner', '')} is gone and its "
            f"pipeline is {pstatus or 'gone'}; nothing can finish this leg")
    try:
        if cancel_fn is not None:
            await cancel_fn(run.id)
        cur = await store.get(run.id)
        if cur is None or str(getattr(cur, "status", "")) not in OWNER_JUDGED_STATUSES:
            return False  # settled meanwhile — announced by whoever did it
        updated = await store.update(run.id, status="cancelled", ended_at=now.isoformat(),
                                     error=f"reconciled: {note}")
        if str(getattr(updated, "status", "")) != "cancelled":
            return False
        await store.append_event(run.id, "cancelled", {
            "reconciled_by": "reconciler", "cause": "owner", "prior_status": run.status,
            "owner": getattr(run, "owner", ""), "note": note})
        if emit_fn is not None:
            try:
                await emit_fn(run.id, "cancelled", {
                    "reconciled_by": "reconciler", "cause": "owner",
                    "prior_status": run.status, "error": note})
            except Exception:  # noqa: BLE001 — best-effort like every emit
                logger.debug("reconciler: emit for %s failed", run.id, exc_info=True)
        logger.info("reconciler: %s %s -> cancelled (owner dead, pipeline %s)",
                    run.id, run.status, pstatus or "gone")
        return True
    except Exception as exc:  # noqa: BLE001
        logger.warning("reconciler: failed to settle dead leg %s: %s", run.id, exc)
        return False


async def _events_or_empty(store: RunStore, run_id: str) -> list:
    try:
        return list(await store.get_events(run_id))
    except Exception:  # noqa: BLE001
        return []


def _completion_fields(run: Any, output: dict, events: list) -> dict[str, Any]:
    """The ``result_payloads`` + ``cost`` a LIVE completion writes (#1869), built
    with the dispatcher's own helpers so the two records are byte-identical."""
    # deliberate heavy-dep deferral: the dispatcher is the largest module in
    # core and the reconciler is imported at server boot on every deploy.
    from cairn.core.dispatch.dispatcher import (  # noqa: PLC0415
        _aggregate_cost, _json_safe, _llm_cost_from_node_events,
    )
    payloads = [{
        "template_name": getattr(run, "template_ref", "") or "",
        "output": _json_safe(output),
        "llm_cost_usd": _llm_cost_from_node_events(events),
    }]
    return {"result_payloads": payloads, "cost": _aggregate_cost(payloads)}


class RunReconciler:
    """Periodic sweep that heals zombie runs (a plain asyncio task).

    Mirrors :class:`ApprovalReaper`'s start/shutdown lifecycle; started from the
    trigger server's lifespan on every runtime, by the elected leader (#1533). A
    failing sweep is logged, never fatal — a bad sweep must not kill the loop or
    the server. ``start``/``shutdown`` run on promotion/demotion, so they reset
    the first-free owner observations (ADR-0125 D3): a new leader starts afresh.
    """

    def __init__(
        self,
        store: RunStore,
        status_fn: StatusFn,
        *,
        sweep_seconds: Optional[float] = None,
        grace_seconds: Optional[float] = None,
        heartbeat_ttl_seconds: Optional[float] = None,
        emit_fn: Optional[EmitFn] = None,
        enabled: bool = True,
        owner_alive_fn: Optional[OwnerAliveFn] = None,
        result_fn: Optional[ResultFn] = None,
        owner_probe: Optional[LockFreeFn] = None,
        self_owner_fn: Optional[Callable[[], str]] = None,
        owner_reacquire_seconds: float = 0.0,
        dead_leg_fn: Optional[DeadLegFn] = None,
        pipeline_status_fn: Optional[PipelineStatusFn] = None,
        cancel_fn: Optional[CancelFn] = None,
    ) -> None:
        self.store = store
        self.owner_probe = owner_probe
        self.self_owner_fn = self_owner_fn
        self.owner_watch = OwnerWatch(owner_reacquire_seconds)
        self.dead_leg_fn = dead_leg_fn
        self.pipeline_status_fn = pipeline_status_fn
        self.cancel_fn = cancel_fn
        self.status_fn = status_fn
        self.owner_alive_fn = owner_alive_fn
        self.result_fn = result_fn
        self.sweep_seconds = sweep_seconds if sweep_seconds is not None else sweep_seconds_from_env()
        self.grace_seconds = grace_seconds if grace_seconds is not None else grace_seconds_from_env()
        self.heartbeat_ttl_seconds = (
            heartbeat_ttl_seconds if heartbeat_ttl_seconds is not None
            else heartbeat_ttl_from_env()
        )
        self.emit_fn = emit_fn
        self.enabled = enabled
        self._task: Optional[asyncio.Task[Any]] = None

    def start(self) -> None:
        """Start the sweep loop. Idempotent; a no-op when disabled."""
        if not self.enabled or (self._task and not self._task.done()):
            return
        self.owner_watch.reset()
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "run reconciler started (sweep=%gs, grace=%gs)",
            self.sweep_seconds, self.grace_seconds,
        )

    def shutdown(self) -> None:
        self.owner_watch.reset()
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("run reconciler stopped")

    def owner_kwargs(self) -> dict[str, Any]:
        """The ADR-0125 owner-verdict arguments of one sweep."""
        return dict(
            owner_probe=self.owner_probe,
            self_owner=self.self_owner_fn() if self.self_owner_fn is not None else "",
            owner_watch=self.owner_watch, dead_leg_fn=self.dead_leg_fn,
            pipeline_status_fn=self.pipeline_status_fn, cancel_fn=self.cancel_fn,
        )

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.sweep_seconds)
                await reconcile_stale_runs(
                    self.store, self.status_fn, grace_seconds=self.grace_seconds,
                    heartbeat_ttl_seconds=self.heartbeat_ttl_seconds,
                    emit_fn=self.emit_fn, owner_alive_fn=self.owner_alive_fn, result_fn=self.result_fn,
                    **self.owner_kwargs(),
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 — a bad sweep must not kill the loop
                logger.warning("run reconciler sweep failed: %s", exc)


__all__ = [
    "ALIVE",
    "DEAD",
    "LEGACY",
    "UNDETERMINED",
    "OwnerWatch",
    "RunReconciler",
    "owner_verdict",
    "reconcile_stale_runs",
    "StatusFn",
    "EmitFn",
    "DEFAULT_SWEEP_SECONDS",
    "DEFAULT_GRACE_SECONDS",
    "DEFAULT_HEARTBEAT_TTL_SECONDS",
    "sweep_seconds_from_env",
    "grace_seconds_from_env",
    "heartbeat_ttl_from_env",
]
