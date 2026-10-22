"""Durable trigger inbox — at-least-once inbound delivery with retry + replay.

A verified, matched trigger (``template`` + mapped ``input``) is **persisted here
before dispatch**, so an inbound webhook survives a crash and is retried with
exponential backoff on failure (dead-lettered after ``max_attempts``). On
restart, anything still ``pending`` is drained again.

This is a **work queue**, deliberately separate from the notification ``Bus``
(#59: the bus is fire-and-forget, *no control flow*). The substrate is chosen by
``CAIRN_TRIGGER_INBOX`` behind the :class:`TriggerInbox` Protocol:

* ``""`` / ``"sqlite"`` → :class:`SqliteTriggerInbox` (durable default,
  ``~/.cairn/trigger_inbox.db``) — lean, zero new infra.
* ``":memory:"`` / ``"memory"`` → :class:`InMemoryTriggerInbox` (tests/dev).
* ``"nats://…"`` → NATS JetStream (deferred #50) — the multi-node swap, slots in
  behind this same Protocol. Not implemented yet (raises with guidance).

Replay (``replay_since``) re-enqueues terminal records from a point in time —
the inbound analogue of the Bus's ``replay``.

Moved here from ``cairn.core.triggers.inbox`` (#2416, ADR-0120 D6): the inbox
is a store, so it lives below the HTTP layer; the old path is a deprecated
alias for one release.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso

logger = logging.getLogger("cairn.triggers.inbox")

PENDING, DONE, DEAD = "pending", "done", "dead"


def _backoff_at(now_iso: str, attempts: int, base_delay: float) -> str:
    """ISO timestamp for the next attempt — exponential backoff base·2^(n-1)."""
    base = datetime.fromisoformat(now_iso)
    return (base + timedelta(seconds=base_delay * (2 ** max(0, attempts - 1)))).isoformat()


class InboxRecord(BaseModel):
    """One durable inbound trigger awaiting (or past) dispatch."""

    id: str = Field(default_factory=lambda: uuid.uuid4().hex[:12])
    source: str
    template: str
    input: dict[str, Any] = Field(default_factory=dict)
    #: #212 — the workspace that declared the receiver, where the queued run starts.
    #: ``None`` (a pack/overlay receiver, or a row an older server queued) binds
    #: nothing: the run starts in the process default, as an inline delivery does.
    workspace: Optional[str] = None
    status: str = PENDING  # pending | done | dead
    attempts: int = 0
    max_attempts: int = 5
    next_retry_at: Optional[str] = None  # ISO; None ⇒ due immediately
    last_error: Optional[str] = None
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: Optional[str] = None


# Handler dispatches a claimed record (e.g. start the template run). Raising
# triggers a retry; returning normally acks the record as done.
Handler = Callable[[InboxRecord], Awaitable[Any]]


@runtime_checkable
class TriggerInbox(Protocol):
    """Durable at-least-once inbox for inbound triggers."""

    async def enqueue(self, source: str, template: str, input: dict[str, Any], *, max_attempts: int = 5, workspace: Optional[str] = None) -> InboxRecord: ...
    async def claim_due(self, *, now: str, limit: int = 50) -> list[InboxRecord]: ...
    async def mark_done(self, record_id: str) -> None: ...
    async def mark_retry(self, record_id: str, *, attempts: int, next_retry_at: str, error: str) -> None: ...
    async def mark_dead(self, record_id: str, *, error: str) -> None: ...
    async def list(self, *, status: Optional[str] = None, since: Optional[str] = None, limit: int = 200) -> list[InboxRecord]: ...
    def close(self) -> None: ...


# ─── in-memory (tests / dev) ────────────────────────────────────────────


class InMemoryTriggerInbox:
    """Volatile inbox — durability only within the process (tests/dev)."""

    def __init__(self) -> None:
        self._by_id: dict[str, InboxRecord] = {}

    async def enqueue(self, source: str, template: str, input: dict[str, Any], *, max_attempts: int = 5, workspace: Optional[str] = None) -> InboxRecord:
        rec = InboxRecord(source=source, template=template, input=dict(input), max_attempts=max_attempts,
                          workspace=workspace)
        self._by_id[rec.id] = rec
        return rec

    async def claim_due(self, *, now: str, limit: int = 50) -> list[InboxRecord]:
        due = [
            r for r in self._by_id.values()
            if r.status == PENDING and (r.next_retry_at is None or r.next_retry_at <= now)
        ]
        due.sort(key=lambda r: r.created_at)
        return due[:limit]

    def _update(self, record_id: str, **fields: Any) -> None:
        cur = self._by_id.get(record_id)
        if cur is not None:
            self._by_id[record_id] = cur.model_copy(update={**fields, "updated_at": utc_now_iso()})

    async def mark_done(self, record_id: str) -> None:
        self._update(record_id, status=DONE)

    async def mark_retry(self, record_id: str, *, attempts: int, next_retry_at: str, error: str) -> None:
        self._update(record_id, status=PENDING, attempts=attempts, next_retry_at=next_retry_at, last_error=error)

    async def mark_dead(self, record_id: str, *, error: str) -> None:
        self._update(record_id, status=DEAD, last_error=error)

    async def list(self, *, status: Optional[str] = None, since: Optional[str] = None, limit: int = 200) -> list[InboxRecord]:
        rows = [
            r for r in self._by_id.values()
            if (status is None or r.status == status) and (since is None or r.created_at >= since)
        ]
        rows.sort(key=lambda r: r.created_at, reverse=True)
        return rows[:limit]

    def close(self) -> None:
        pass


# ─── sqlite (durable default — mirrors SqliteConnectionStore) ───────────


class SqliteTriggerInbox:
    """Embedded-SQLite inbox — restart-safe, single file. Full record in a JSON
    ``data`` blob; ``status``/``next_retry_at`` mirrored to columns so the
    claim query is indexable. Sqlite work runs in a thread under a lock."""

    def __init__(self, path: str | None = None) -> None:
        import sqlite3

        if path is None:
            path = getenv("CAIRN_TRIGGER_INBOX_DB", "").strip() or os.path.expanduser(
                "~/.cairn/trigger_inbox.db"
            )
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS inbox (
                id            TEXT PRIMARY KEY,
                source        TEXT,
                status        TEXT,
                next_retry_at TEXT,
                created_at    TEXT,
                data          TEXT NOT NULL
            )
            """
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS inbox_due ON inbox(status, next_retry_at)")
        self._conn.commit()
        self._lock = asyncio.Lock()

    def _put_sync(self, rec: InboxRecord) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO inbox (id, source, status, next_retry_at, created_at, data) VALUES (?,?,?,?,?,?)",
            (rec.id, rec.source, rec.status, rec.next_retry_at, rec.created_at, rec.model_dump_json()),
        )
        self._conn.commit()

    async def enqueue(self, source: str, template: str, input: dict[str, Any], *, max_attempts: int = 5, workspace: Optional[str] = None) -> InboxRecord:
        rec = InboxRecord(source=source, template=template, input=dict(input), max_attempts=max_attempts,
                          workspace=workspace)
        async with self._lock:
            await asyncio.to_thread(self._put_sync, rec)
        return rec

    def _claim_sync(self, now: str, limit: int) -> list[InboxRecord]:
        rows = self._conn.execute(
            "SELECT data FROM inbox WHERE status=? AND (next_retry_at IS NULL OR next_retry_at<=?) "
            "ORDER BY created_at ASC LIMIT ?",
            (PENDING, now, limit),
        ).fetchall()
        return [InboxRecord.model_validate_json(r[0]) for r in rows]

    async def claim_due(self, *, now: str, limit: int = 50) -> list[InboxRecord]:
        async with self._lock:
            return await asyncio.to_thread(self._claim_sync, now, limit)

    def _update_sync(self, record_id: str, fields: dict[str, Any]) -> None:
        row = self._conn.execute("SELECT data FROM inbox WHERE id=?", (record_id,)).fetchone()
        if row is None:
            return
        rec = InboxRecord.model_validate_json(row[0]).model_copy(update={**fields, "updated_at": utc_now_iso()})
        self._conn.execute(
            "UPDATE inbox SET status=?, next_retry_at=?, data=? WHERE id=?",
            (rec.status, rec.next_retry_at, rec.model_dump_json(), record_id),
        )
        self._conn.commit()

    async def mark_done(self, record_id: str) -> None:
        async with self._lock:
            await asyncio.to_thread(self._update_sync, record_id, {"status": DONE})

    async def mark_retry(self, record_id: str, *, attempts: int, next_retry_at: str, error: str) -> None:
        async with self._lock:
            await asyncio.to_thread(
                self._update_sync, record_id,
                {"status": PENDING, "attempts": attempts, "next_retry_at": next_retry_at, "last_error": error},
            )

    async def mark_dead(self, record_id: str, *, error: str) -> None:
        async with self._lock:
            await asyncio.to_thread(
                self._update_sync, record_id, {"status": DEAD, "next_retry_at": None, "last_error": error}
            )

    def _list_sync(self, status: Optional[str], since: Optional[str], limit: int) -> list[InboxRecord]:
        sql, params = "SELECT data FROM inbox", []
        clauses = []
        if status is not None:
            clauses.append("status=?"); params.append(status)
        if since is not None:
            clauses.append("created_at>=?"); params.append(since)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at DESC LIMIT ?"; params.append(limit)
        rows = self._conn.execute(sql, tuple(params)).fetchall()
        return [InboxRecord.model_validate_json(r[0]) for r in rows]

    async def list(self, *, status: Optional[str] = None, since: Optional[str] = None, limit: int = 200) -> list[InboxRecord]:
        async with self._lock:
            return await asyncio.to_thread(self._list_sync, status, since, limit)

    def close(self) -> None:
        self._conn.close()


# ─── NATS JetStream (multi-node swap, #50/#115) ─────────────────────────


class NatsJetStreamTriggerInbox:
    """JetStream-backed inbox — durable streams + redelivery + dead-letter, the
    multi-node swap (#115). Maps the :class:`TriggerInbox` Protocol onto a durable
    **pull consumer**: ``claim_due`` fetches a batch (holding each ``Msg`` for
    ack), ``mark_done`` acks, ``mark_retry`` naks with a backoff delay (JetStream
    redelivers), ``mark_dead`` terms + republishes to a DLQ subject. Reuses the
    same :func:`drain_once`/:func:`run_drain_loop` as the DB store, and — unlike
    the single-process DB drain — supports **parallel consumers** across nodes.

    ``nats`` is an optional dep (``cairn[nats]``), imported lazily. ``list``/
    ``replay`` are limited (JetStream isn't a queryable store; the sqlite/postgres
    store stays the queryable tier) — see #115."""

    STREAM = "CAIRN_TRIGGER_INBOX"
    SUBJECT = "trigger.inbox"
    DLQ_SUBJECT = "trigger.inbox.dlq"
    DURABLE = "cairn-inbox-worker"

    def __init__(
        self,
        url: str,
        *,
        stream: Optional[str] = None,
        subject: Optional[str] = None,
        durable: Optional[str] = None,
        ack_wait: float = 60.0,
    ) -> None:
        self._url = url
        self._stream = stream or getenv("CAIRN_TRIGGER_INBOX_STREAM", self.STREAM)
        self._subject = subject or getenv("CAIRN_TRIGGER_INBOX_SUBJECT", self.SUBJECT)
        self._dlq = f"{self._subject}.dlq"
        self._durable = durable or getenv("CAIRN_TRIGGER_INBOX_CONSUMER", self.DURABLE)
        self._ack_wait = float(getenv("CAIRN_TRIGGER_INBOX_ACK_WAIT", ack_wait))
        self._nc = None
        self._js = None
        self._sub = None
        self._inflight: dict[str, Any] = {}  # record_id -> JetStream Msg (claim→mark)

    async def _ensure(self) -> None:
        if self._js is not None:
            return
        # lazy: nats-py is the optional `[nats]` extra — keep it off the base import path
        import nats
        from nats.js.api import AckPolicy, ConsumerConfig, RetentionPolicy, StreamConfig

        self._nc = await nats.connect(self._url)
        self._js = self._nc.jetstream()
        try:
            await self._js.add_stream(
                StreamConfig(name=self._stream, subjects=[self._subject, self._dlq], retention=RetentionPolicy.LIMITS)
            )
        except Exception:  # noqa: BLE001 — stream already exists
            pass
        self._sub = await self._js.pull_subscribe(
            self._subject,
            durable=self._durable,
            config=ConsumerConfig(ack_wait=self._ack_wait, ack_policy=AckPolicy.EXPLICIT),
        )

    async def enqueue(self, source: str, template: str, input: dict[str, Any], *, max_attempts: int = 5, workspace: Optional[str] = None) -> InboxRecord:
        await self._ensure()
        rec = InboxRecord(source=source, template=template, input=dict(input), max_attempts=max_attempts,
                          workspace=workspace)
        await self._js.publish(self._subject, rec.model_dump_json().encode(), headers={"Cairn-Id": rec.id})
        return rec

    async def claim_due(self, *, now: str, limit: int = 50) -> list[InboxRecord]:
        await self._ensure()
        import nats.errors

        try:
            msgs = await self._sub.fetch(batch=limit, timeout=1.0)
        except nats.errors.TimeoutError:
            return []
        out: list[InboxRecord] = []
        for m in msgs:
            try:
                rec = InboxRecord.model_validate_json(m.data)
            except Exception:  # noqa: BLE001 — undecodable → drop off the stream
                await m.term()
                continue
            # JetStream tracks delivery count; reflect it so drain's max-attempts gate works
            rec.attempts = max(0, (m.metadata.num_delivered or 1) - 1)
            self._inflight[rec.id] = m
            out.append(rec)
        return out

    async def mark_done(self, record_id: str) -> None:
        m = self._inflight.pop(record_id, None)
        if m is not None:
            await m.ack()

    async def mark_retry(self, record_id: str, *, attempts: int, next_retry_at: str, error: str) -> None:
        m = self._inflight.pop(record_id, None)
        if m is None:
            return
        delay = max(0.0, (datetime.fromisoformat(next_retry_at) - datetime.now(timezone.utc)).total_seconds())
        await m.nak(delay=delay)  # JetStream redelivers after `delay`

    async def mark_dead(self, record_id: str, *, error: str) -> None:
        m = self._inflight.pop(record_id, None)
        if m is None:
            return
        try:
            rec = InboxRecord.model_validate_json(m.data).model_copy(update={"status": DEAD, "last_error": error})
            await self._js.publish(self._dlq, rec.model_dump_json().encode())
        finally:
            await m.term()  # stop redelivery

    async def list(self, *, status: Optional[str] = None, since: Optional[str] = None, limit: int = 200) -> list[InboxRecord]:
        # JetStream isn't a queryable store — the sqlite/postgres store is the
        # queryable/replay tier. Returns empty here (documented limit, #115).
        logger.debug("NatsJetStreamTriggerInbox.list is not supported (use the DB store for status/replay)")
        return []

    async def aclose(self) -> None:
        if self._nc is not None and not self._nc.is_closed:
            await self._nc.close()
        self._nc = self._js = self._sub = None

    def close(self) -> None:
        # best-effort: schedule an async close without blocking the sync Protocol
        nc = self._nc
        if nc is not None and not nc.is_closed:
            try:
                asyncio.get_event_loop().create_task(nc.close())
            except Exception:  # noqa: BLE001 — no running loop / already closing
                pass


# ─── drain + replay (substrate-agnostic) ────────────────────────────────


async def drain_once(
    inbox: TriggerInbox,
    handler: Handler,
    *,
    now: Optional[str] = None,
    base_delay: float = 2.0,
    limit: int = 50,
) -> dict[str, int]:
    """Process all due records once. ``handler`` raising → retry with backoff;
    returning → ack as done; ``max_attempts`` reached → dead-letter. Returns a
    ``{dispatched, retried, dead, claimed}`` tally."""
    now = now or utc_now_iso()
    due = await inbox.claim_due(now=now, limit=limit)
    dispatched = retried = dead = 0
    for rec in due:
        try:
            await handler(rec)
        except Exception as e:  # noqa: BLE001 — failure → retry/dead, never crash the drain
            attempts = rec.attempts + 1
            err = f"{type(e).__name__}: {e}"
            if attempts >= rec.max_attempts:
                await inbox.mark_dead(rec.id, error=err)
                dead += 1
                logger.warning("trigger inbox %s dead-lettered after %d attempts: %s", rec.id, attempts, err)
            else:
                await inbox.mark_retry(
                    rec.id, attempts=attempts, next_retry_at=_backoff_at(now, attempts, base_delay), error=err
                )
                retried += 1
        else:
            await inbox.mark_done(rec.id)
            dispatched += 1
    return {"dispatched": dispatched, "retried": retried, "dead": dead, "claimed": len(due)}


def record_workspace(rec: InboxRecord) -> str:
    """The workspace a record belongs to: the one it carries, else ``default``
    (a pack/overlay receiver's delivery, or one an older server queued) — #218."""
    from cairn.core.workspace import DEFAULT_WORKSPACE  # noqa: PLC0415 — as the other storage modules

    return rec.workspace or DEFAULT_WORKSPACE


async def replay_since(inbox: TriggerInbox, *, since: str, limit: int = 500,
                       workspace: Optional[str] = None) -> int:
    """Re-enqueue terminal (done/dead) records created at/after ``since`` as fresh
    pending records. ``workspace`` set → only that workspace's records
    (:func:`record_workspace`); ``None`` → every workspace's. Returns how many were
    re-enqueued."""
    count = 0
    for rec in await inbox.list(since=since, limit=limit):
        if workspace is not None and record_workspace(rec) != workspace:
            continue
        if rec.status in (DONE, DEAD):
            await inbox.enqueue(rec.source, rec.template, rec.input, max_attempts=rec.max_attempts,
                                workspace=rec.workspace)
            count += 1
    return count


async def run_drain_loop(
    inbox: TriggerInbox, handler: Handler, *, interval: float = 5.0, base_delay: float = 2.0
) -> None:
    """Background loop: drain due records every ``interval`` seconds. Cancel the
    task to stop (used by the server lifespan). Single loop ⇒ no overlap/double
    dispatch (multi-node parallelism is the NATS-JetStream swap's job)."""
    while True:
        try:
            await drain_once(inbox, handler, base_delay=base_delay)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — a bad tick must not kill the loop
            logger.exception("trigger inbox drain tick failed")
        await asyncio.sleep(interval)


# ─── factory ────────────────────────────────────────────────────────────


def get_trigger_inbox(spec: Optional[str] = None) -> TriggerInbox:
    """Pick an inbox from ``CAIRN_TRIGGER_INBOX`` (or ``spec``). Durable sqlite by
    default; ``memory``/``:memory:`` for tests; ``nats://…`` is the deferred
    multi-node swap (#50) — same Protocol, not implemented yet."""
    spec = (spec if spec is not None else getenv("CAIRN_TRIGGER_INBOX", "")).strip().lower()
    if spec in ("memory", ":memory:"):
        return InMemoryTriggerInbox()
    if spec.startswith("nats://"):
        # multi-node swap (#115) — needs the `[nats]` extra (nats-py)
        return NatsJetStreamTriggerInbox(spec)
    return SqliteTriggerInbox()


__all__ = [
    "DEAD",
    "DONE",
    "PENDING",
    "Handler",
    "InMemoryTriggerInbox",
    "InboxRecord",
    "NatsJetStreamTriggerInbox",
    "SqliteTriggerInbox",
    "TriggerInbox",
    "drain_once",
    "get_trigger_inbox",
    "record_workspace",
    "replay_since",
    "run_drain_loop",
]
