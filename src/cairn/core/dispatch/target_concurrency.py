"""Per-target concurrency cap (ADR-0086 H5, #1247).

Concurrency control used to be **global-lane only** (`dispatcher._lane_concurrency`:
realtime/background). Nothing limited in-flight legs *per compute target*, so five
pipelines launched together SSH-dispatched five train legs onto the same GPU box —
the submission-side cousin of #1180 bug 11 (per-second SSH polling that exhausted
the worker's fd budget; the poll side got backoff, the submit side got nothing).

Prefect models this as work-pool concurrency slots backed by a `concurrency_limit`
table plus a lease protocol. The cairn-sized version needs neither, because the
run store already holds every fact:

* the **cap** is a field on the compute connection (``max_in_flight``); unset =
  unlimited, which is why installing this slice changes nothing by default;
* the **count** is "runs stamped with this target that are still holding it"
  (:data:`IN_FLIGHT_STATUSES`) — derived, never stored, so it needs no lease
  bookkeeping, survives a control-plane restart for free, and self-heals against
  zombies: a dead leg keeps its slot only until H1's orphan reconciler (#1239)
  terminalizes its run, at which point the count drops on its own.

What this module is: the exact, fleet-wide COUNT (#1531 — a store-side
aggregate, no row cap). What it still is not, by itself, is a mutex: the
count is only as strong as the critical section it is read inside. Since
#1532p2 the bridge reads it inside a per-target ``CriticalSections`` hold —
process-local by default, a Postgres advisory lock fleet-wide when
``CAIRN_STORAGE`` is Postgres — so under a shared-Postgres deployment "two
processes racing the same last slot can both take it" is no longer true: the
count-then-claim is serialized across replicas. On non-Postgres deployments
the pre-#1532 honesty stands (the cap makes N+1 rare, not impossible), which
is the correct floor for a single-process dev setup.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from cairn.core.storage.protocols import SCAN_COUNT_LIMIT, scan_in_flight_on_target

logger = logging.getLogger("cairn.core.dispatch.target_concurrency")

#: The authoring key — a top-level :class:`~cairn.core.connections.Connection`
#: field, and (same name) an accepted ``config`` key so ``cairn connect …
#: --set max_in_flight=2`` needs no CLI change.
MAX_IN_FLIGHT_KEY = "max_in_flight"

#: Statuses that HOLD a slot on a target. Deliberately NOT "every non-terminal
#: status": a ``queued`` leg is one that is *waiting for* a slot, so counting it
#: would make the cap self-blocking (every waiter would keep the count at the cap
#: forever). ``awaiting_approval`` is in because such a run's engine job, when it
#: has one, is still parked on the box.
IN_FLIGHT_STATUSES: tuple[str, ...] = ("running", "awaiting_approval")

#: One-shot latch for the legacy-store warning below — a fallback that logs on
#: every admission would be noise, and the condition is a property of the store
#: object, not of the call.
_warned_legacy_count = False


async def target_cap(target: str, *, store: Any = None) -> Optional[int]:
    """The ``max_in_flight`` cap for the compute connection named ``target``.

    ``None`` means **unlimited** — no target, no such connection, no cap set, a
    non-numeric or non-positive value, or a store that could not be read. Every
    one of those degrades to today's behavior on purpose: a concurrency knob must
    never be able to fail a leg.
    """
    name = str(target or "").strip()
    if not name:
        return None
    try:
        from cairn.core.connections import (  # noqa: PLC0415 - cycle: connections → secrets stack
            lookup_account, default_connection_store, run_account,
        )

        account = lookup_account(await run_account())  # the leg's account resolves the target
        owned = store is None
        store = store or default_connection_store()
        try:
            conn = await store.get_by_name(name, account_id=account)
        finally:
            close = getattr(store, "close", None)
            if owned and callable(close):
                close()
    except Exception:  # noqa: BLE001 — an unreadable connection store means "no cap"
        logger.debug("target %r: cap lookup failed — treating as unlimited", name, exc_info=True)
        return None
    if conn is None:
        return None
    raw = getattr(conn, MAX_IN_FLIGHT_KEY, None)
    if raw is None:
        raw = (getattr(conn, "config", None) or {}).get(MAX_IN_FLIGHT_KEY)
    try:
        cap = int(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        if raw is not None:
            logger.warning("connection %r has a non-numeric %s=%r — ignored (unlimited)",
                           name, MAX_IN_FLIGHT_KEY, raw)
        return None
    return cap if cap > 0 else None


async def in_flight_on_target(run_store: Any, target: str, *,
                              exclude_run_id: str = "") -> int:
    """How many runs currently hold a slot on ``target`` (see :data:`IN_FLIGHT_STATUSES`).

    ``exclude_run_id`` drops the asking leg's own run from the count — a leg that
    is waiting for a slot must not be blocked by itself.

    ADR-0091 tier-2 (#1531): the count is a STORE-SIDE aggregate
    (:meth:`~cairn.core.storage.protocols.RunStore.count_in_flight_on_target`),
    not a list-and-filter in this process. The old shape read the 500 newest runs
    per slot-holding status and filtered them here, which made the cap silently
    approximate above 500 concurrent runs — and approximate in the admitting
    direction, since runs that fell off the end of the list were runs the count
    never saw. Two replicas sharing one Postgres now ask the database the same
    exact question, so a cap of N is a cap of N fleet-wide.

    The signature is unchanged, and so is the failure posture: a store that
    predates the method (a duck-typed test stub, an out-of-tree implementation
    that is not a ``RunStore`` subclass) falls back to the OLD scan rather than
    raising — a concurrency knob must never be able to fail a leg.
    """
    global _warned_legacy_count
    name = str(target or "").strip()
    if not name:
        return 0
    counter = getattr(run_store, "count_in_flight_on_target", None)
    if counter is not None:
        try:
            return int(await counter(name, IN_FLIGHT_STATUSES,
                                     exclude_run_id=exclude_run_id))
        except TypeError:
            # A store that has the NAME but not the signature — same class of
            # "older store" as not having it at all, so it takes the same path.
            logger.debug("count_in_flight_on_target rejected the call on %r",
                         type(run_store).__name__, exc_info=True)
    if not _warned_legacy_count:
        _warned_legacy_count = True
        logger.warning(
            "run store %s has no count_in_flight_on_target — falling back to the "
            "pre-#1531 in-Python scan, which is capped at %d runs per status and "
            "can UNDER-count (i.e. over-admit) above that. Implement the method.",
            type(run_store).__name__, SCAN_COUNT_LIMIT,
        )
    return await scan_in_flight_on_target(
        run_store, name, IN_FLIGHT_STATUSES, exclude_run_id=exclude_run_id)


__all__ = ["IN_FLIGHT_STATUSES", "MAX_IN_FLIGHT_KEY", "in_flight_on_target",
           "target_cap"]
