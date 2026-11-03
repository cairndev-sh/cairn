"""#896 — HITL approval-gate lifecycle: TTL auto-expire + bulk-dismiss.

A governance product must not leak governance state. Abandoned ``awaiting_approval``
runs pile up with no expiry, and a queue full of zombie gates erodes the
"waiting on you" signal that is the whole point of a HITL gate. This reaps them.

The reaper is a periodic sweep over the run STORE (never the durable workflow):
each ``awaiting_approval`` run whose last activity predates the TTL transitions to
the terminal ``expired`` status, with an audited event on the append-only log —
exactly the ``store.update`` + ``append_event`` the reject path already uses. It is
purely additive governance-state hygiene: it never touches the run engine's
execution path.

Excluded: **sandbox** (Playground) runs (#877) — ephemeral, never real governance
state. TTL is deployment/governance-owned (ADR-0070) via env, so a run/workflow
can't weaken it; the sweep is a no-op when ``CAIRN_APPROVAL_TTL_HOURS <= 0``.

The "last activity" basis is the run's most recent event timestamp — a paused run
emits nothing until it's resolved, so that IS the pause moment — falling back to
``started_at`` → ``created_at`` for legacy runs with no event log.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Optional

from cairn.core.storage.protocols import Run, RunStore

logger = logging.getLogger(__name__)

#: 7 days — long enough not to surprise a legitimately-slow approver, short enough
#: to clear the zombies. 0 (or negative) disables the sweep entirely.
DEFAULT_TTL_HOURS = 168.0
DEFAULT_SWEEP_MINUTES = 60.0
_EXPIRE_EVENT = "expired"


def ttl_hours_from_env() -> float:
    """Governance-owned TTL (ADR-0070): env-set, never weakenable by a run/workflow."""
    try:
        return float(os.environ.get("CAIRN_APPROVAL_TTL_HOURS", DEFAULT_TTL_HOURS))
    except (TypeError, ValueError):
        return DEFAULT_TTL_HOURS


def sweep_minutes_from_env() -> float:
    try:
        return max(1.0, float(os.environ.get("CAIRN_APPROVAL_SWEEP_MINUTES", DEFAULT_SWEEP_MINUTES)))
    except (TypeError, ValueError):
        return DEFAULT_SWEEP_MINUTES


def _parse_iso(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


async def _last_activity(store: RunStore, run: Run) -> Optional[datetime]:
    """When did this paused run last do anything? The last event's ts (a paused run
    emits nothing until resolved, so that IS the pause moment), falling back to
    ``started_at`` → ``created_at`` for legacy runs without an event log."""
    try:
        evs = await store.get_events(run.id)
    except Exception:  # noqa: BLE001 — a store hiccup on one run must not abort the sweep
        evs = []
    if evs:
        ts = _parse_iso(evs[-1].ts)
        if ts:
            return ts
    return _parse_iso(run.started_at) or _parse_iso(run.created_at)


async def expire_run(store: RunStore, run: Run, *, now: datetime, reason: str,
                     actor: Optional[str] = None) -> bool:
    """Transition ONE ``awaiting_approval`` run → ``expired`` + append an audit event.
    Returns True iff it actually expired (False when the run wasn't awaiting approval
    — the fail-closed guard that keeps a bulk-dismiss from touching a resolved run).

    #233 — the outcome also reaches the hash-chained audit sink: ``approval.dismissed``
    when a person (``actor``) dismissed it, ``approval.expired`` when the TTL sweep did."""
    if run.status != "awaiting_approval":
        return False
    await store.update(run.id, status="expired", ended_at=now.isoformat(), error=reason)
    # #917 — under the temporal runtime the run's workflow is parked on its
    # approval wait_condition; expiring only the store row left it OPEN forever
    # (zombies accumulated one per expired approval). Best-effort: the status
    # transition above is the contract, a Temporal hiccup must not break it.
    from cairn.core import settings  # noqa: PLC0415 — cycle: settings → storage

    if settings.runtime() == "temporal":
        try:
            from cairn.core.runtime.temporal.runtime import TemporalRuntime  # noqa: PLC0415 — optional [runtime-temporal] extra

            await TemporalRuntime().terminate(run.id, reason=f"approval expired: {reason}")
        except Exception:  # noqa: BLE001 — best-effort; the zombie reconciler (#976) is the backstop
            logger.warning("approval reaper: workflow terminate failed for %s", run.id)
    try:
        await store.append_event(
            run.id, _EXPIRE_EVENT, {"reason": reason, "prior_status": "awaiting_approval"}
        )
    except Exception:  # noqa: BLE001 — the status transition is the contract; the audit event is best-effort
        logger.warning("approval reaper: audit event failed for %s", run.id)
    from cairn.core.obs.audit import audit_emit  # noqa: PLC0415 — keep obs off the import path

    await audit_emit(
        "approval.dismissed" if actor else "approval.expired", actor=actor or None,
        target=run.id, run_id=run.id, pipeline_id=getattr(run, "bundle_pipeline_id", None) or None,
        step_id=getattr(run, "step_id", None) or None, template_ref=getattr(run, "template_ref", "") or None,
        reason=reason[:300])
    return True


async def expire_stale_approvals(
    store: RunStore,
    *,
    ttl_hours: float,
    now: Optional[datetime] = None,
    workspace_id: Optional[str] = None,
    actor: Optional[str] = None,
) -> list[str]:
    """Sweep: expire every ``awaiting_approval`` (non-sandbox) run whose last activity
    is older than ``ttl_hours``. Returns the expired run ids. No-op when
    ``ttl_hours <= 0`` (the sweep is disabled)."""
    if ttl_hours <= 0:
        return []
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=ttl_hours)
    reason = f"approval expired: no decision within {ttl_hours:g}h TTL (#896)"
    runs = await store.list(status="awaiting_approval", workspace_id=workspace_id, limit=1000)
    expired: list[str] = []
    for run in runs:
        if getattr(run, "sandbox", False):
            continue  # #877 — Playground runs are ephemeral, never real governance state
        last = await _last_activity(store, run)
        if last is not None and last > cutoff:
            continue  # still within its TTL
        if await expire_run(store, run, now=now, reason=reason, actor=actor):
            expired.append(run.id)
    if expired:
        logger.info("approval reaper expired %d stale gate(s): %s", len(expired), expired)
    return expired


async def dismiss_runs(
    store: RunStore,
    run_ids: Iterable[str],
    *,
    now: Optional[datetime] = None,
    actor: str = "console",
    workspace_id: Optional[str] = None,
) -> dict[str, list[str]]:
    """Bulk-dismiss (the console action): expire the given ``awaiting_approval`` runs.
    Skips ids that aren't awaiting approval (already resolved) or live in another
    workspace — a dismiss can never touch a run the caller can't see or a decided
    gate. Returns ``{expired: [...], skipped: [...]}``."""
    now = now or datetime.now(timezone.utc)
    reason = f"approval dismissed by {actor} (#896)"
    expired: list[str] = []
    skipped: list[str] = []
    for rid in run_ids:
        run = await store.get(rid, workspace_id=workspace_id)
        if run is not None and await expire_run(store, run, now=now, reason=reason, actor=actor):
            expired.append(rid)
        else:
            skipped.append(rid)
    return {"expired": expired, "skipped": skipped}


class ApprovalReaper:
    """Periodic TTL sweep over ``awaiting_approval`` runs (a plain asyncio task).

    Mirrors ``SkillScheduler``'s start/shutdown lifecycle; started from the trigger
    server's lifespan when the TTL is > 0. A failing sweep is logged, never fatal —
    a bad sweep must not kill the loop or the server."""

    def __init__(
        self,
        store: RunStore,
        *,
        ttl_hours: Optional[float] = None,
        sweep_seconds: Optional[float] = None,
    ) -> None:
        self.store = store
        self.ttl_hours = ttl_hours if ttl_hours is not None else ttl_hours_from_env()
        self.sweep_seconds = (
            sweep_seconds if sweep_seconds is not None else sweep_minutes_from_env() * 60.0
        )
        self._task: Optional[asyncio.Task[Any]] = None

    @property
    def enabled(self) -> bool:
        return self.ttl_hours > 0

    def start(self) -> None:
        """Start the sweep loop. Idempotent; a no-op when the TTL disables reaping."""
        if not self.enabled or (self._task and not self._task.done()):
            return
        self._task = asyncio.create_task(self._loop())
        logger.info(
            "approval reaper started (ttl=%gh, sweep=%gs)", self.ttl_hours, self.sweep_seconds
        )

    def shutdown(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            logger.info("approval reaper stopped")

    async def _loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.sweep_seconds)
                await expire_stale_approvals(self.store, ttl_hours=self.ttl_hours)
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 — a bad sweep must not kill the loop
                logger.warning("approval reaper sweep failed: %s", exc)


__all__ = [
    "ApprovalReaper",
    "DEFAULT_TTL_HOURS",
    "DEFAULT_SWEEP_MINUTES",
    "dismiss_runs",
    "expire_run",
    "expire_stale_approvals",
    "sweep_minutes_from_env",
    "ttl_hours_from_env",
]
