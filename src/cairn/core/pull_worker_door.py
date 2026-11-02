"""The pull-worker door (#565, ADR-0104 D2 — ADR-0099 §1: derive, refuse, never start).

D2 lets a machine join by dialling out: it claims a job off the remote-job
store, runs the image, and reports over the job channel. Which means a run
whose ML target is a **worker** target has nowhere to execute unless some
worker is actually up — and the failure without this door is the worst kind:
the run is admitted, the job record is written, and it sits in ``submitted``
forever because nothing is polling.

So this module answers one question — *does this launch need a pull worker, and
has one checked in?* — for the preflight (dry run) and the dispatcher (the real
door), from the same code, exactly like :mod:`cairn.core.job_channel_door`.

Two ADR-0099 §1 properties it keeps:

* **Derived, never declared.** The requirement comes from the environment's
  ``ml_target`` naming a ``worker:`` target (or the explicit
  ``CAIRN_REMOTE_WORKERS=required``), not from a field on a manifest. The
  *answer* comes from the job store's own claim/heartbeat stamps, not from a
  registry a worker would have to remember to write to.
* **Refuse, don't start.** The control plane never spawns a worker; it names
  the command a human runs.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Iterable, Optional

from cairn.core.job_channel_door import ServiceVerdict, remote_job_operators

ML_TARGET_ENV = "CAIRN_ML_TARGET"
REMOTE_WORKERS_ENV = "CAIRN_REMOTE_WORKERS"
#: An ML target of the form ``worker:<pool>`` means "whatever machine claims it",
#: as opposed to ``<connection>`` (a box the control plane pushes into).
WORKER_TARGET_PREFIX = "worker:"
SERVICE = "pull-worker"
#: A worker that has not claimed or heartbeated in this long is not up.
DEFAULT_WITHIN_MINUTES = 10

#: ``last_checkin()`` → the ISO timestamp of the most recent worker check-in, or
#: ``None``. Injectable so the door is provable with no store and no worker.
CheckinFn = Callable[[], Awaitable[Optional[str]]]


def ml_target() -> str:
    return getenv(ML_TARGET_ENV, "").strip()


def target_names_worker(target: Optional[str] = None) -> bool:
    return (target if target is not None else ml_target()).startswith(WORKER_TARGET_PREFIX)


def workers_required(target: Optional[str] = None) -> bool:
    """Whether this environment expects work to be PULLED. Either the ML target
    names a ``worker:`` pool, or the deployment says so outright."""
    if getenv(REMOTE_WORKERS_ENV, "").strip().lower() in {"required", "1", "true", "yes", "on"}:
        return True
    return target_names_worker(target)


async def last_worker_checkin(*, store: Any = None, limit: int = 200) -> Optional[str]:
    """The most recent moment a worker touched the job store — a heartbeat if
    there is one, otherwise the claim that stamped a lease. Derived from the
    records themselves, so a worker cannot be "up" without having done work."""
    if store is None:
        from cairn.core.operator.job_store import get_remote_job_store  # noqa: PLC0415 — the door touches storage; keep it off the import path

        store = get_remote_job_store()
    try:
        records = await store.list(limit=limit)
    except Exception:  # noqa: BLE001 — an unreachable store is not a worker check-in
        return None
    stamps = [s for s in (r.last_heartbeat_at or (r.updated_at if r.lease_until else None)
                          for r in records) if s]
    return max(stamps) if stamps else None


def _is_recent(stamp: Optional[str], within_minutes: float) -> bool:
    if not stamp:
        return False
    try:
        seen = datetime.fromisoformat(stamp)
    except ValueError:
        return False
    if seen.tzinfo is None:
        seen = seen.replace(tzinfo=timezone.utc)
    return seen >= datetime.now(timezone.utc) - timedelta(minutes=within_minutes)


async def check_pull_worker(operator_names: Iterable[str], *, target: Optional[str] = None,
                            last_checkin: Optional[CheckinFn] = None,
                            within_minutes: float = DEFAULT_WITHIN_MINUTES) -> ServiceVerdict:
    """The verdict for a launch invoking ``operator_names``.

    Requires a worker only when the environment pulls **and** some leg hands
    work to another machine (an operator implementing ``submit``); then refuses
    unless one has checked in inside ``within_minutes``, naming the command."""
    if not workers_required(target):
        return ServiceVerdict()
    remote = remote_job_operators(operator_names)
    if not remote:
        return ServiceVerdict()
    v = ServiceVerdict(required=[SERVICE])
    checkin = last_checkin or last_worker_checkin
    seen = await checkin()
    if _is_recent(seen, within_minutes):
        return v
    v.ok = False
    where = target if target is not None else ml_target()
    v.reasons.append(
        f"{', '.join(remote)} runs on a machine that PULLS its work"
        + (f" ({ML_TARGET_ENV}={where})" if where else f" ({REMOTE_WORKERS_ENV}=required)")
        + ", and no pull worker has checked in"
        + (f" (last seen {seen})" if seen else " at all")
        + f" in the last {int(within_minutes)} minutes — start "
          "`cairn admin worker --pull --engine-url <control-plane-url> --token <token>` on the machine "
          "that should run it, or point the environment's ML target at a connection instead"
    )
    return v


__all__ = [
    "CheckinFn", "DEFAULT_WITHIN_MINUTES", "ML_TARGET_ENV", "REMOTE_WORKERS_ENV", "SERVICE",
    "ServiceVerdict", "WORKER_TARGET_PREFIX", "check_pull_worker", "last_worker_checkin",
    "ml_target", "target_names_worker", "workers_required",
]
