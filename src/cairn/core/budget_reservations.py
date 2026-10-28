"""Atomic budget reserve-at-submit (#1094, ADR-0083 Q16) — **fleet-shared**
since #1530 (ADR-0091 tier 2, slice 1/4).

Budget enforcement was **check-then-submit**, which loses two ways:

1. **The race** — two doors submitting concurrently when the remaining budget
   covers only one BOTH pass the check. The failure mode is the worst kind:
   *paid for what policy said we couldn't*.
2. **The estimate was never supplied** — both call sites
   (``dispatcher.py``/``run.py``) invoked ``series_budget_exceeded(series_id)``
   with no ``est_usd``, while that function's docstring promises
   "already-spent + this run's estimate > budget". The gate therefore degraded
   to ``spent > budget``: purely REACTIVE, blocking only once a series had
   ALREADY overspent, so a $300 series happily admitted a $500 run.

Both close with one primitive: **reserve the estimate atomically at admission**,
then ``settle`` any attempt with known positive spend (including failure or
cancellation), and ``release`` only when no positive spend was reported. The
actual spend becomes real in the ledger, so keeping the reservation would
double-count it.

**#1530 — where the reservations LIVE.** The original implementation held them
in a module-global dict behind one ``asyncio.Lock``, and said so honestly: the
lock is per-process, which closes the race for ONE control plane and nothing
more. Two replicas each kept their own private ledger of holds, so a series
whose budget covered one run admitted one run *per replica*; and a restart
silently dropped every hold, so an in-flight run's estimate stopped counting
against the cap it was admitted under.

This module now has a **backend seam**:

- :class:`_MemoryBackend` — the original semantics, unchanged, and still the
  default. Single-process dev keeps working with **zero configuration**.
- :class:`_PostgresBackend` — one ``series_reservations`` table in the shared
  Postgres, selected exactly the way every other store selects Postgres:
  ``CAIRN_STORAGE=postgres://…`` (#427). One env var keeps meaning "shared state
  lives in PG".

The Postgres reserve is one transaction: take a per-series advisory lock, sum
the live holds, insert or refuse. Settle/release are **state transitions**
(``held`` → ``settled``/``released``), never deletes, so the table doubles as
an audit trail of every claim the control plane ever admitted; only ``held``
rows count against a budget.

**Restart safety, and its limit.** Held rows survive a process death — that is
the point, and it is what makes the cap true across a restart. It also means a
process that dies mid-run leaves a hold nothing will ever settle. Reaping those
is deliberately NOT in this slice: see :func:`held_for_run` and the note on
``_STALE_HOLD_REAPER`` below for where a reaper attaches.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import os
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

from cairn.core.pg_schema_init import PgSchemaInit

#: WHERE A STALE-HOLD REAPER ATTACHES (follow-up, deliberately not built here).
#:
#: A control plane that dies between ``reserve`` and its terminal leaves a
#: ``held`` row for a run that is no longer in flight. The existing zombie
#: reconciler (#976, ``dispatch/dispatcher.py`` — the loop that terminalizes
#: runs whose owning process vanished) is the right owner: for every run it
#: declares dead it should call :func:`held_for_run` and :func:`release` the
#: rows, in the same pass and for the same reason. Nothing here sweeps by age —
#: a hold with no run to check against is indistinguishable from a legitimately
#: long one, and expiring it by clock would free budget a live run is spending.
_STALE_HOLD_REAPER = "see #976 reconciler; scope noted in #1530, not built"


class BudgetExceeded(RuntimeError):
    """A reservation would push the series past its cap. Raised at admission,
    before any spend — the caller maps it onto the existing ``on_breach``
    behaviour (gate or fail), never a silent overrun."""


@dataclass(frozen=True)
class Reservation:
    """A held claim on a series budget. Opaque to callers except ``amount_usd``.

    ``run_id`` is the run the claim was admitted for — the key
    :func:`held_for_run` looks holds up by, which is what lets a terminal
    handler in a *different* process (or a later one, after a restart) close a
    reservation it never took.
    """

    id: str
    series_id: str
    amount_usd: float = field(default=0.0)
    run_id: str = ""


def _new_reservation_id() -> str:
    """Globally unique — a monotonic per-process counter would collide across
    replicas writing the same table."""
    return f"res-{uuid.uuid4().hex[:16]}"


def _refusal(series_id: str, amount: float, spent_usd: float,
             budget_usd: float, held: float) -> str:
    """The one refusal message, shared by both backends so the operator sees
    identical text whichever one is live (a CLI test asserts on it)."""
    return (
        f"series {series_id!r}: this run's estimate ${amount:.2f} would "
        f"exceed the ${float(budget_usd):.2f} cap "
        f"(${float(spent_usd or 0.0):.2f} spent, "
        f"${held:.2f} reserved by in-flight runs). "
        f"Raise the budget or start a new series."
    )


# ─── Memory backend (default; single-process semantics, unchanged) ───────


class _MemoryBackend:
    """Process-local reservations behind one ``asyncio.Lock``.

    One lock for the whole table: reservations are sub-millisecond dict edits,
    and per-series locks would need their own creation lock to avoid the same
    race this module exists to close.
    """

    def __init__(self) -> None:
        # series_id → reservation_id → Reservation. Held ONLY while in flight.
        self._held: dict[str, dict[str, Reservation]] = {}
        # run_id → the actual_usd its settled claims recorded (ADR-0152), kept ONLY
        # while the run still holds a claim: the total is read only to close a held
        # row (an adopted attempt), so it is dropped with the run's last hold and the
        # dict stays bounded by the runs in flight. After a restart this backend has
        # neither holds nor totals; the Postgres rows keep both.
        self._settled: dict[str, float] = {}
        self._lock = asyncio.Lock()

    def _total(self, series_id: str) -> float:
        return float(sum(r.amount_usd for r in (self._held.get(series_id) or {}).values()))

    async def reserve(self, series_id: str, *, amount: float, spent_usd: float,
                      budget_usd: float, run_id: str) -> Reservation:
        async with self._lock:
            if budget_usd:
                held = self._total(series_id)
                if float(spent_usd or 0.0) + held + amount > float(budget_usd):
                    raise BudgetExceeded(
                        _refusal(series_id, amount, spent_usd, budget_usd, held))
            r = Reservation(id=_new_reservation_id(), series_id=series_id,
                            amount_usd=amount, run_id=run_id)
            self._held.setdefault(series_id, {})[r.id] = r
            return r

    async def close(self, reservation: Reservation, *, state: str,
                    actual_usd: float = 0.0) -> None:
        """Drop the hold. ``state`` is recorded only by the Postgres backend —
        in memory there is no audit trail to write it to, and both terminals
        free the same budget."""
        async with self._lock:
            held = self._held.get(reservation.series_id, {}).pop(reservation.id, None)
            if held is None or not held.run_id:
                return
            if not any(r.run_id == held.run_id for holds in self._held.values() for r in holds.values()):
                self._settled.pop(held.run_id, None)    # the run's last hold: nothing left to read it
            elif state == "settled":
                self._settled[held.run_id] = self._settled.get(held.run_id, 0.0) + float(actual_usd or 0.0)

    async def reserved_total(self, series_id: str) -> float:
        return self._total(series_id)

    async def held_for_run(self, run_id: str) -> list[Reservation]:
        if not run_id:
            return []
        return [r for holds in self._held.values() for r in holds.values()
                if r.run_id == run_id]

    async def settled_usd_for_run(self, run_id: str) -> float:
        return float(self._settled.get(run_id, 0.0)) if run_id else 0.0

    def reset(self) -> None:
        self._held.clear()
        self._settled.clear()


# ─── Postgres backend (shared across replicas) ───────────────────────────


_CREATE_RESERVATIONS_SQL = """
    CREATE TABLE IF NOT EXISTS series_reservations (
        reservation_id TEXT PRIMARY KEY,
        series_id      TEXT NOT NULL,
        run_id         TEXT NOT NULL DEFAULT '',
        amount_usd     NUMERIC NOT NULL DEFAULT 0,
        actual_usd     NUMERIC,
        state          TEXT NOT NULL DEFAULT 'held',
        created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    )
"""
#: The ONE index the hot path needs: the reserve transaction's SUM is scoped
#: ``(series_id, state='held')``, and it runs while holding the series lock —
#: so it is the statement every concurrent submitter on that series waits on.
_CREATE_RESERVATIONS_SERIES_IDX = (
    "CREATE INDEX IF NOT EXISTS idx_series_reservations_held "
    "ON series_reservations(series_id) WHERE state = 'held'"
)
_CREATE_RESERVATIONS_RUN_IDX = (
    "CREATE INDEX IF NOT EXISTS idx_series_reservations_run "
    "ON series_reservations(run_id) WHERE state = 'held'"
)

#: **Why an advisory lock and not ``SELECT … FOR UPDATE``.**
#:
#: The check is over an AGGREGATE (`SUM` of the series' held rows). `FOR UPDATE`
#: cannot lock that — Postgres rejects it with aggregates, and even where it
#: parses, row locks only cover rows that ALREADY EXIST: the row this
#: transaction is about to INSERT is precisely the one no other transaction can
#: see or lock, which is the whole phantom-read shape of this race. The usual
#: fix is to lock an anchor ROW for the series and serialize on that — but the
#: series itself lives in the pilot ledger, which may be a *different* store
#: backend entirely (SQLite), so there is no guaranteed row in THIS database to
#: lock.
#:
#: ``pg_advisory_xact_lock`` needs no anchor row: it serializes every reserver
#: of the same series id for the duration of the transaction and is released
#: automatically on COMMIT **or ROLLBACK** (so a refusal, which rolls back,
#: never strands the lock). ``hashtext`` collisions cost only spurious
#: serialization between two unrelated series — never a wrong answer.
_LOCK_SERIES_SQL = "SELECT pg_advisory_xact_lock(hashtext($1))"

_SUM_HELD_SQL = (
    "SELECT COALESCE(SUM(amount_usd), 0)::float8 FROM series_reservations "
    "WHERE series_id = $1 AND state = 'held'"
)
#: ``$4::float8::numeric`` — the column is NUMERIC (money), but asyncpg refuses
#: to bind a Python float to a numeric parameter; the cast lets the call site
#: stay float-typed like the rest of the cost plumbing.
_INSERT_RESERVATION_SQL = """
    INSERT INTO series_reservations (reservation_id, series_id, run_id, amount_usd)
    VALUES ($1, $2, $3, $4::float8::numeric)
"""
#: ``AND state = 'held'`` IS the idempotency: closing an already-terminal
#: reservation updates zero rows, which is a no-op and not an error.
_CLOSE_RESERVATION_SQL = """
    UPDATE series_reservations
       SET state = $2, actual_usd = $3::float8::numeric, updated_at = now()
     WHERE reservation_id = $1 AND state = 'held'
"""
_SUM_SETTLED_FOR_RUN_SQL = (
    "SELECT COALESCE(SUM(actual_usd), 0)::float8 FROM series_reservations "
    "WHERE run_id = $1 AND state = 'settled'"
)
_SELECT_HELD_FOR_RUN_SQL = (
    "SELECT reservation_id, series_id, run_id, amount_usd::float8 AS amount_usd "
    "FROM series_reservations WHERE run_id = $1 AND state = 'held' "
    "ORDER BY created_at ASC"
)


class _PostgresBackend(PgSchemaInit):
    """Reservations in the shared Postgres — the fleet-safe backend (#1530).

    Mirrors :class:`~cairn.core.storage.postgres.runs.PostgresRunStore` and
    :class:`~cairn_platform.pilot.PostgresPilotScoreStore`: lazy asyncpg pool from
    the process-shared :func:`shared_pool` (#614), schema created lazily on
    first use (the repo's current convention — #1471 Alembic is queued, not
    landed).

    ``pool`` is a test seam: a second, independent pool stands in for a second
    control-plane replica within one test process.
    """

    def __init__(self, dsn: Optional[str] = None, *, pool: Any = None) -> None:
        self._dsn = dsn
        # The injected pool is held UNPUBLISHED (#1904): an injected pool still
        # gets the schema ensured once — CREATE … IF NOT EXISTS is idempotent, so
        # a second "replica" pool costs one no-op DDL — and `self._pool` is set
        # by :class:`PgSchemaInit` only after that DDL has run.
        self._given_pool = pool
        self._pool: Any = None
        self._schema_ready = False

    async def _open_pool(self):
        if self._given_pool is not None:
            return self._given_pool
        try:
            import asyncpg  # noqa: F401,PLC0415 — optional dep (storage-postgres)
        except ImportError as e:
            raise RuntimeError(
                "Postgres budget reservations require the 'storage-postgres' "
                "optional dep group. Install with: pip install 'cairndev[storage-postgres]'"
            ) from e
        if not self._dsn:
            raise RuntimeError(
                "Postgres budget reservations need a DSN. Pass dsn= or set "
                "CAIRN_STORAGE=postgres://…"
            )
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415

        # #614: one pool per DSN, shared across every Postgres store here.
        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool: Any) -> None:
        async with pool.acquire() as c:
            await c.execute(_CREATE_RESERVATIONS_SQL)
            await c.execute(_CREATE_RESERVATIONS_SERIES_IDX)
            await c.execute(_CREATE_RESERVATIONS_RUN_IDX)

    async def reserve(self, series_id: str, *, amount: float, spent_usd: float,
                      budget_usd: float, run_id: str) -> Reservation:
        """Check-and-claim in ONE transaction, serialized per series.

        A refusal raises inside the transaction, which rolls back: no row is
        written and the advisory lock is dropped by the rollback itself.
        """
        pool = await self._get_pool()
        rid = _new_reservation_id()
        async with pool.acquire() as c:
            async with c.transaction():
                await c.execute(_LOCK_SERIES_SQL, series_id)
                held = float(await c.fetchval(_SUM_HELD_SQL, series_id) or 0.0)
                if budget_usd and float(spent_usd or 0.0) + held + amount > float(budget_usd):
                    raise BudgetExceeded(
                        _refusal(series_id, amount, spent_usd, budget_usd, held))
                await c.execute(_INSERT_RESERVATION_SQL, rid, series_id, run_id, amount)
        return Reservation(id=rid, series_id=series_id, amount_usd=amount,
                           run_id=run_id)

    async def close(self, reservation: Reservation, *, state: str,
                    actual_usd: float = 0.0) -> None:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            await c.execute(_CLOSE_RESERVATION_SQL, reservation.id, state,
                            float(actual_usd or 0.0))

    async def reserved_total(self, series_id: str) -> float:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            return float(await c.fetchval(_SUM_HELD_SQL, series_id) or 0.0)

    async def held_for_run(self, run_id: str) -> list[Reservation]:
        if not run_id:
            return []
        pool = await self._get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(_SELECT_HELD_FOR_RUN_SQL, run_id)
        return [
            Reservation(id=r["reservation_id"], series_id=r["series_id"],
                        amount_usd=float(r["amount_usd"] or 0.0),
                        run_id=r["run_id"])
            for r in rows
        ]

    async def settled_usd_for_run(self, run_id: str) -> float:
        if not run_id:
            return 0.0
        pool = await self._get_pool()
        async with pool.acquire() as c:
            return float(await c.fetchval(_SUM_SETTLED_FOR_RUN_SQL, run_id) or 0.0)

    def reset(self) -> None:
        """No-op: durable holds are the POINT of this backend. A live-PG test
        scopes itself with a unique series id rather than truncating a table
        another replica may be reserving against."""


# ─── Backend selection ───────────────────────────────────────────────────


_backend_instance: Any = None


def _backend() -> Any:
    """The process's reservation backend. **``CAIRN_STORAGE`` is the selector**
    (#427, same decision as every other store): ``postgres://…`` → shared
    Postgres, anything else → in-memory. Zero config keeps single-process dev
    working exactly as before."""
    global _backend_instance
    if _backend_instance is None:
        url = getenv("CAIRN_STORAGE", "").strip()
        if url.startswith(("postgres://", "postgresql://")):
            _backend_instance = _PostgresBackend(dsn=url)
        else:
            _backend_instance = _MemoryBackend()
    return _backend_instance


def reset_reservations() -> None:
    """Drop in-memory reservations and re-resolve the backend from the env.

    Tests only. It is NO LONGER "also correct after a control-plane restart":
    under the Postgres backend a restart must NOT forget its holds — a run
    admitted against a cap keeps counting against it until something settles or
    releases it (#1530). Postgres rows are deliberately untouched here.
    """
    global _backend_instance
    if _backend_instance is not None:
        _backend_instance.reset()
    _backend_instance = None


# ─── Public API ──────────────────────────────────────────────────────────


async def reserved_total(series_id: str) -> float:
    """Sum currently held for ``series_id``."""
    return await _backend().reserved_total(series_id)


async def remaining_usd(series_id: str, *, spent_usd: float,
                        budget_usd: float) -> Optional[float]:
    """Budget left after settled spend AND live reservations.

    ``None`` for an uncapped series. Never negative — an overspent series reads
    as zero remaining, not as a negative number a UI would render oddly.
    """
    if not budget_usd:
        return None
    left = float(budget_usd) - float(spent_usd) - await reserved_total(series_id)
    return max(0.0, left)


async def reserve(series_id: str, *, est_usd: float, spent_usd: float,
                  budget_usd: float, run_id: str = "") -> Reservation:
    """Atomically claim ``est_usd`` against the series budget.

    The check and the claim happen under one lock (an ``asyncio.Lock`` in
    memory, a per-series advisory lock inside one Postgres transaction), which
    is the whole point: between a bare check and a submit, another door — in
    this process or another replica — can consume the same budget.

    ``run_id`` records which run the claim was admitted for, so
    :func:`held_for_run` can close it from a terminal handler that never saw
    the :class:`Reservation` object (a resumed HITL run; a later process).

    Raises :class:`BudgetExceeded` when ``spent + reserved + est`` would exceed
    the cap. An uncapped series (``budget_usd`` falsy) always succeeds — today's
    semantics, preserved.
    """
    amount = max(0.0, float(est_usd or 0.0))
    return await _backend().reserve(
        series_id, amount=amount, spent_usd=spent_usd, budget_usd=budget_usd,
        run_id=run_id,
    )


async def release(reservation: Optional[Reservation]) -> None:
    """Free a reservation when the attempt reported no positive spend.

    A flow leg on a priced box always bills its wall-clock, so it settles instead
    on every outcome (ADR-0151 D5).

    Idempotent: terminal handling can fire more than once (the completion path
    and the reconciler both see the same run), and a double release must not
    free budget that was never held. Callers with a reported positive spend
    must use :func:`settle` even when the attempt failed or was cancelled.
    """
    if reservation is None:
        return
    await _backend().close(reservation, state="released")


async def settle(reservation: Optional[Reservation],
                 actual_usd: float = 0.0) -> None:
    """Close a reservation after an attempt completes with known spend, including
    a priced-box attempt that ended in any state with box time to record
    (ADR-0151 D5).

    This applies to successful, failed, or cancelled attempts. The hold stops
    counting and the actual cost is recorded as spend, so holding the estimate
    as well would double-count it. Under the Postgres backend the row is
    transitioned to ``settled`` with ``actual_usd`` recorded, so the table is
    a full audit trail of what each admission actually cost.
    """
    if reservation is None:
        return
    await _backend().close(reservation, state="settled", actual_usd=actual_usd)


async def held_for_run(run_id: str) -> list[Reservation]:
    """Every still-held reservation admitted for ``run_id``.

    The lookup that makes reserve→terminal survive a hand-off: the HITL resume
    path settles a claim taken before the pause (possibly by a process that has
    since restarted), and a cancel closes one whose owning task is already gone.
    Also the query a stale-hold reaper would drive (see ``_STALE_HOLD_REAPER``).
    """
    return await _backend().held_for_run(run_id)


async def settled_usd_for_run(run_id: str) -> float:
    """What the claims admitted for ``run_id`` have already SETTLED (the sum of their
    ``actual_usd``). Read-only. An adopted leg attempt subtracts it when it closes the
    run's held row, because its window runs from the leg's start, so the run's earlier
    attempts are not settled twice (ADR-0152). The Postgres backend sums the settled
    rows; the in-memory backend keeps the total only while the run still holds a claim
    (the only time a held row can be closed against it)."""
    return await _backend().settled_usd_for_run(run_id)


__all__ = [
    "BudgetExceeded", "Reservation", "held_for_run", "release", "remaining_usd",
    "reserve", "reserved_total", "reset_reservations", "settle", "settled_usd_for_run",
]
