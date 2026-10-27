"""Per-key critical sections for the pipeline bridge (#1532 part 2, ADR-0091).

The bridge guards two read-modify-write sections with process-local
``asyncio.Lock``s: the per-pipeline record mutation (``_lock(pipeline_id)``)
and the per-target count→claim slot admission (``_admit_lock``). Both were
honest about their scope — the admission comment literally said "in THIS
process … a brake, not a distributed mutex". With N control-plane replicas
that scope is the bug: two replicas can advance one pipeline concurrently, or
both claim the same last slot under a target cap.

This module makes the section fleet-wide on Postgres when that is the selected
storage backend. The pipeline section is used by both the in-process bridge and
Temporal worker activities; coordination stays on Postgres while Temporal remains
the optional durability tier for agentic bodies. Requiring Temporal for
control-plane scale would tax every future engine (Ray/Spark/Dask) that brings
its own orchestration. ``CAIRN_STORAGE`` is the selector, exactly as it is for
the run store and the #1530 budget backend: ``postgres://…`` → fleet-wide
advisory locks, anything else → the same per-key ``asyncio.Lock`` behaviour as
before (single-process dev and the entire hermetic test suite are unchanged).

**Why session-scoped ``pg_advisory_lock``, unlike #1530's xact-scoped one.**
The budget reserve is one transaction on one connection, so the
transaction-scoped lock fits and self-releases on COMMIT/ROLLBACK. These
sections are different: they span many awaits and many statements on OTHER
connections (store get → advance logic → store put), so there is no single
transaction to scope to. A session lock held on a dedicated connection covers
exactly that shape — and its crash story is the one we want: the process
dies, the session dies, Postgres frees the lock. No lease timestamps, no
reaper.

**Why a dedicated (tiny) pool, obtained through the shared helper.** The
section body does store I/O through the SHARED asyncpg pool. If the lock
connection came from that same pool, every holder would need a second
connection from a pool it is already occupying — classic self-exhaustion
deadlock under load. So lock connections come from their own pool — but via
``shared_pool(dsn, purpose="advisory-locks")``, not a private
``asyncpg.create_pool``: #614/#666 (a live ``TooManyConnectionsError``, 103
of 100) made pool creation the helper's job so sizing stays central and
``reset_pools()`` still closes everything. A source-level test enforces
that, and it caught this module doing it the wrong way first.

**One pool PER NAMESPACE, and why that is not optional.** The bridge nests
these sections: ``_advance`` holds the per-pipeline section and, inside it,
the leg-submit path takes the per-target admit section. A task therefore
holds two lock connections at once. If both namespaces shared one pool,
N concurrent pipelines would each pin the pipeline connection and then all
wait for an admit connection that only another holder can release — a
textbook nested-lock pool deadlock, and one that appears only under
concurrent load in production. Partitioning by namespace caps each task at
ONE connection per pool, so the nesting cannot exhaust either.

**Local lock first, then Postgres.** Two tasks in one process contending for
the same key would otherwise each pin a lock connection while Postgres
serializes them — the local lock makes in-process contention free and caps
the number of pinned connections at one per key. Ordering is uniform
(local → PG) so it cannot deadlock against itself.

``hashtext(namespace:key)`` collisions (int32) cost a spurious serialization
between unrelated keys — never a wrong answer. Same trade #1530 documented.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

logger = logging.getLogger("cairn.dispatch.coordination")

def _lock_pool_max() -> int:
    """Ceiling for EACH namespace's advisory-lock pool. Small on purpose —
    sections are short and #666 exists to protect Postgres headroom — but
    tunable, because it bounds how many pipelines one replica can hold
    sections for concurrently. ``CAIRN_PG_LOCK_POOL_MAX`` overrides."""
    try:
        return max(1, int(getenv("CAIRN_PG_LOCK_POOL_MAX", "8")))
    except ValueError:
        return 8


_LOCK_SQL = "SELECT pg_advisory_lock(hashtext($1))"
_UNLOCK_SQL = "SELECT pg_advisory_unlock(hashtext($1))"


def _postgres_dsn() -> str:
    """The fleet DSN iff shared state lives in Postgres — same selector as the
    run store and the #1530 budget backend (#427: one env var, one meaning)."""
    url = getenv("CAIRN_STORAGE", "").strip()
    return url if url.startswith(("postgres://", "postgresql://")) else ""


class CriticalSections:
    """Named, per-key async critical sections; fleet-wide under Postgres.

    Usage::

        self._sections = CriticalSections(namespace="pipeline")
        ...
        async with self._sections.hold(pipeline_id):
            <read-modify-write>

    One instance per *concern* (pipeline record vs target admission), so the
    namespaces cannot collide and each concern's contention is visible on its
    own instance.
    """

    def __init__(self, *, namespace: str) -> None:
        self.namespace = namespace
        self._local: dict[str, asyncio.Lock] = {}
        self._pool: Any = None
        self._pool_guard = asyncio.Lock()

    # ── local layer ──────────────────────────────────────────────────

    def _local_lock(self, key: str) -> asyncio.Lock:
        lock = self._local.get(key)
        if lock is None:
            lock = self._local[key] = asyncio.Lock()
        return lock

    # ── postgres layer ───────────────────────────────────────────────

    def _demote(self) -> None:
        """Fall back to PROCESS-LOCAL sections, warning exactly once. The
        coordination upgrade must never be able to turn into an outage — the
        pre-#1532 behaviour is the floor, not a failure. ``aclose()`` resets,
        so a restarted bridge retries Postgres."""
        if self._pool is not False:
            self._pool = False  # sentinel: tried and failed
            logger.warning(
                "coordination[%s]: advisory-lock pool unavailable — falling "
                "back to PROCESS-LOCAL sections (the pre-#1532 behaviour). "
                "Fleet-wide mutual exclusion is OFF for this process until it "
                "can reach Postgres again.",
                self.namespace, exc_info=True,
            )

    async def _get_pool(self) -> Any:
        """The DEDICATED lock pool (see module docstring), or ``None`` when the
        deployment is not Postgres-backed or the pool is demoted. NOTE:
        ``min_size=0`` means construction succeeds without connecting — real
        connect failures surface at ``acquire()``, which is why ``hold()``
        guards that path too."""
        dsn = _postgres_dsn()
        if not dsn:
            return None
        if self._pool is None:
            async with self._pool_guard:
                if self._pool is None:
                    try:
                        from cairn.core.storage.postgres.pool import (  # noqa: PLC0415 — optional dep (storage-postgres extra)
                            shared_pool,
                        )

                        # min_size=0 → no connections held while idle. The pool
                        # is PER NAMESPACE (see the module docstring): sharing one
                        # across the nested pipeline→admit sections deadlocks.
                        self._pool = await shared_pool(
                            dsn, purpose=f"advisory-locks:{self.namespace}",
                            min_size=0, max_size=_lock_pool_max(),
                        )
                    except Exception:  # noqa: BLE001 — degrade, once, loudly
                        self._demote()
        return self._pool or None

    @asynccontextmanager
    async def hold(self, key: str) -> AsyncIterator[None]:
        """Hold the section for ``key``: local lock always, Postgres advisory
        lock additionally when the deployment is Postgres-backed."""
        name = f"{self.namespace}:{key}"
        async with self._local_lock(key):
            pool = await self._get_pool()
            if pool is None:
                yield
                return
            try:
                conn = await pool.acquire()
            except Exception:  # noqa: BLE001 — unreachable PG = degrade, not outage
                self._demote()
                yield
                return
            acquired = False
            try:
                await conn.execute(_LOCK_SQL, name)
                acquired = True
                yield
            finally:
                if acquired:
                    try:
                        await conn.execute(_UNLOCK_SQL, name)
                    except Exception:  # noqa: BLE001 — a broken conn still frees the lock on close
                        logger.warning(
                            "coordination[%s]: unlock failed for %r — closing "
                            "the connection so the SESSION releases the lock",
                            self.namespace, name, exc_info=True,
                        )
                        conn = await self._terminate(pool, conn)
                if conn is not None:
                    await pool.release(conn)

    @staticmethod
    async def _terminate(pool: Any, conn: Any) -> Optional[Any]:
        """Hard-close a connection whose unlock failed. Session death IS the
        release path Postgres guarantees; returning ``None`` tells the caller
        there is nothing left to hand back to the pool."""
        try:
            await pool.release(conn, timeout=1)
        except Exception:  # noqa: BLE001 — last resort below
            pass
        try:
            conn.terminate()
        except Exception:  # noqa: BLE001 — already dead is fine
            pass
        return None

    async def aclose(self) -> None:
        """Drop this instance's references (bridge ``stop()``).

        The pool itself is NOT closed here: it is the process-shared
        ``purpose="advisory-locks"`` pool, and another CriticalSections
        instance (there are two — pipeline and target-admit) may still hold
        it. ``reset_pools()`` owns its lifecycle, exactly as for store pools.
        Clearing the reference resets the demotion sentinel too, so a bridge
        restarted after a Postgres outage retries instead of staying local
        forever. Temporal activities use their own module-level instance, and
        both instances coordinate across processes through the same Postgres
        advisory-lock key."""
        self._pool = None
        # Keep lock objects alive while holders unwind. Clearing this map while
        # another owner still has a key held would let a new holder create a
        # second asyncio.Lock for the same key and bypass local exclusion.


__all__ = ["CriticalSections"]
