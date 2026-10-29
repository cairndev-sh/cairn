"""The `pipelines` store (ADR-0069, #746 Phase 1).

A :class:`PipelineRecord` is the durable owner of one bundle-pipeline execution —
the grouping the child runs FK back to (a step *is* a normal governed run; the
pipeline is the record at the *pipeline* grain, the same way pilots/experiments
group `run_id`s). It holds the resolved :class:`~cairn.core.engine.bundle_pipeline.PipelinePlan`
snapshot, per-step state, and a cursor, so the core ``PipelineBridge`` (Phase 2) can
resume from ``step_state`` on restart (adopt-by-``run_id``).

Mirrors :class:`~cairn.core.workflow_config_store` (JSON-blob record, in-memory /
sqlite / postgres, ``CAIRN_STORAGE`` selects postgres). Kept deliberately thin — the
*enforcement* lives in the core PipelineBridge (ADR-0069 §2), not here; this is
just durable state.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import os
import uuid
from typing import Any, Optional

from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso
from cairn.core.engine.bundle_pipeline import PipelinePlan
from cairn.core.storage.transitions import guard_pipeline_put
from cairn.core.pg_schema_init import PgSchemaInit

# status values a pipeline moves through (ADR-0069 §4). `paused` covers both a
# `manual` step awaiting an explicit advance and an `approval` step awaiting HITL.
PIPELINE_STATUSES = ("queued", "running", "paused", "done", "failed", "cancelled")


#: ``trigger.source``, read out of the record's JSON blob — the provenance the
#: #2128 C backfill stamps and the reversal finds it by. One expression per
#: tier because that is the only difference between the two queries.
_TRIGGER_SOURCE_EXPR_SQLITE = "json_extract(data, '$.trigger.source')"
_TRIGGER_SOURCE_EXPR_POSTGRES = "data->'trigger'->>'source'"


def new_pipeline_id() -> str:
    """A fresh pipeline id (``pl_<hex>``) — the grouping id child runs point back at."""
    return f"pl_{uuid.uuid4().hex[:16]}"


class StepState(BaseModel):
    """Live state of one step within a running pipeline."""

    run_id: Optional[str] = None  # the governed run this step became (adopt-by-id on resume)
    #: The engine job handle, persisted at submit (ADR-0086 H1, #1239) — the
    #: re-attach token #1130's plan_resume was designed around but never had:
    #: without it every mid-flight leg classified as orphaned after a restart.
    #: Shape: {job_id, engine, meta} — the engine's own durable-by-design dict.
    handle: Optional[dict] = None
    # pending | awaiting_advance (manual) | awaiting_approval (HITL) | running | done | failed | skipped
    status: str = "pending"
    verdict: Optional[str] = None  # the step's gate/exit verdict, if any (pass|warn|fail|…)
    approver: Optional[str] = None  # who approved an approval-gated step (ADR-0069 §2, #746)
    started_at: Optional[str] = None
    # when the step entered an awaiting_* state — the replay cursor for on:<event> catch-up
    # on a durable bus (#763): an event published since here still fires it after a restart.
    awaiting_since: Optional[str] = None
    #: #1791 — the dedupe marker for the approval notification, sitting next to
    #: the key it is derived from (``pipeline:step:awaiting_since``). One pause =
    #: one notification, and because the marker is part of the durable record it
    #: survives a bridge restart: the resumed bridge re-reads the same pause and
    #: recognises it as already announced. A NEW pause of the same step stamps a
    #: new ``awaiting_since``, so the key changes and the operator is told again
    #: — which is the behaviour that makes this a dedupe and not a mute.
    #: Stored inside the record's JSON blob ⇒ no migration (same as
    #: ``PipelineRecord.governance``): older rows deserialize to None.
    notified_key: Optional[str] = None
    #: ADR-0125 D5 (#2555 item 7) — the write-ahead marker of a ``schedule:`` /
    #: ``on:`` fire: ``"<trigger>@<fire time, UTC, second precision>"``, written
    #: and put by the bridge inside the pipeline's critical section BEFORE the
    #: leg is submitted. It records only that a fire began. A fire whose key
    #: equals the stored one is refused; a step found waiting with a key on
    #: adopt is parked for a person (or takes the run the fire created) instead
    #: of being re-armed. It belongs to the current wait: ``_mark_awaiting``
    #: clears it. In the JSON blob ⇒ no migration; older rows read None.
    fired_key: Optional[str] = None
    ended_at: Optional[str] = None


class PipelineRecord(BaseModel):
    """One bundle-pipeline execution — the durable owner row (ADR-0069 §4)."""

    pipeline_id: str = Field(default_factory=new_pipeline_id)
    bundle_ref: str = ""  # bundle name@version the plan came from
    pipeline_name: str = ""  # [[bundle.pipeline]].name
    status: str = "queued"
    plan: Optional[PipelinePlan] = None  # resolved PipelinePlan snapshot
    step_state: dict[str, StepState] = Field(default_factory=dict)  # step_id → StepState
    cursor: list[str] = Field(default_factory=list)  # active step id(s): scalar-as-1 for linear, set for DAG/joins
    inputs: dict[str, Any] = Field(default_factory=dict)  # pipeline-level (bundle) input
    environment: Optional[str] = None  # profile/env the pipeline runs under
    trigger: dict[str, Any] = Field(default_factory=dict)  # what started it (source + payload)
    workspace_id: str = "default"  # tenancy scope (#223)
    #: #1688 — the terms this pipeline was admitted under (`series`, `budget`,
    #: `approvers`, `timeout`), snapshotted from the plan at start. Read by
    #: `_execute_and_settle_flow_leg` (series + budget → the reservation) and by
    #: `gate_context_for` (approvers + timeout → the approver's queue row). Both
    #: readers predate this field and were already spelled
    #: `getattr(rec, "governance", None) or {}` — a defaulting getattr against a
    #: field that never existed, which is why the gap read as green for so long.
    #: Stored inside the record's JSON blob, so no migration: rows written
    #: before this field deserialize to `{}` and behave exactly as they did.
    governance: dict[str, Any] = Field(default_factory=dict)
    #: #2190 (T05, C3) — the AdmissionDecision made ONCE at the launch door for
    #: this request: verdict, refusing/degrading reasons, each version's facts
    #: with the policy's `required` stamped on its checks, holdout, licence,
    #: policy identity. Facts live on the versions; this is the decision.
    #: Inside the JSON blob ⇒ no migration; older rows deserialize to {}.
    admission: dict[str, Any] = Field(default_factory=dict)
    #: #1155 — loop name → the iteration currently running. Persisted so a
    #: resumed pipeline reattaches mid-loop instead of restarting it (#1130).
    loop_iterations: dict[str, int] = Field(default_factory=dict)
    #: #1787 (ADR-0097) — the terminal state this pipeline is HEADED for, while
    #: its ``finalizer:`` steps still owe work. ``""`` for every pipeline without
    #: one, which is why nothing else changes: the two-move terminal (decide the
    #: intent → run the finalizers → apply the status) only exists on plans that
    #: declare a finalizer.
    #:
    #: It is the field that makes restart safety FREE rather than a second sweep:
    #: while a finalizer is owed the record stays ``running``, so the existing
    #: ``_adopt_in_flight`` (which lists running + paused) already finds it. A
    #: design that wrote ``done`` first and remembered the debt afterwards would
    #: have needed a full scan of every terminal pipeline at every boot.
    #: Stored inside the record's JSON blob ⇒ no migration; older rows read "".
    terminal_intent: str = ""
    #: #222 — the W3C trace context of the short ``cairn.pipeline`` root span opened
    #: at launch (``{"traceparent": "00-<trace>-<span>-<flags>"}``). Every leg, gate
    #: and flow-stage span starts as its child, in whichever request or process
    #: starts it (a gate approved hours later, a restarted server, a Temporal
    #: worker), so one pipeline is one trace. ``{}`` when tracing is off.
    #: Stored inside the record's JSON blob like ``Run.trace_id`` ⇒ no migration;
    #: older rows deserialize to ``{}`` and their legs trace as before.
    trace_context: dict[str, str] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: Optional[str] = None
    ended_at: Optional[str] = None


class InMemoryPipelineStore:
    def __init__(self) -> None:
        self._by_id: dict[str, PipelineRecord] = {}

    async def get(self, pipeline_id: str) -> Optional[PipelineRecord]:
        return self._by_id.get(pipeline_id)

    async def put(self, rec: PipelineRecord) -> PipelineRecord:
        prev = self._by_id.get(rec.pipeline_id)
        if prev is not None and not guard_pipeline_put(
            prev.status, rec.status, pipeline_id=rec.pipeline_id
        ):
            return prev  # illegal transition — whole put refused (#1245)
        rec = rec.model_copy(update={"updated_at": utc_now_iso()})
        self._by_id[rec.pipeline_id] = rec
        return rec

    async def list(
        self, *, workspace_id: Optional[str] = None, status: Optional[str] = None
    ) -> list[PipelineRecord]:
        out = [
            r for r in self._by_id.values()
            if (workspace_id is None or r.workspace_id == workspace_id)
            and (status is None or r.status == status)
        ]
        return sorted(out, key=lambda r: r.created_at, reverse=True)

    async def list_by_trigger_source(
        self, source: str, *, after_id: str = "", limit: int = 200
    ) -> list[PipelineRecord]:
        out = [
            r for r in self._by_id.values()
            if str((r.trigger or {}).get("source") or "") == source
            and r.pipeline_id > after_id
        ]
        out.sort(key=lambda r: r.pipeline_id)
        return out[:limit]

    async def delete(self, pipeline_id: str) -> bool:
        return self._by_id.pop(pipeline_id, None) is not None

    def close(self) -> None:
        pass


class SqlitePipelineStore:
    """JSON-blob sqlite store, ``to_thread``'d (mirrors SqliteWorkflowConfigStore)."""

    def __init__(self, path: str | None = None) -> None:
        import sqlite3  # noqa: PLC0415 — stdlib, lazy like the sibling stores

        if path is None:
            from cairn.core import settings  # noqa: PLC0415

            path = getenv("CAIRN_PIPELINE_DB", "").strip() or settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS pipelines "
            "(pipeline_id TEXT PRIMARY KEY, workspace_id TEXT, bundle_ref TEXT, "
            " status TEXT, created_at TEXT, data TEXT NOT NULL)"
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS ix_pipelines_ws ON pipelines(workspace_id)")
        self._conn.commit()
        self._lock = asyncio.Lock()

    def _put_sync(self, rec: PipelineRecord) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO pipelines "
            "(pipeline_id, workspace_id, bundle_ref, status, created_at, data) VALUES (?,?,?,?,?,?)",
            (rec.pipeline_id, rec.workspace_id, rec.bundle_ref, rec.status, rec.created_at, rec.model_dump_json()),
        )
        self._conn.commit()

    async def put(self, rec: PipelineRecord) -> PipelineRecord:
        async with self._lock:
            prev = await asyncio.to_thread(self._get_sync, rec.pipeline_id)
            if prev is not None and not guard_pipeline_put(
                prev.status, rec.status, pipeline_id=rec.pipeline_id
            ):
                return prev  # illegal transition — whole put refused (#1245)
            rec = rec.model_copy(update={"updated_at": utc_now_iso()})
            await asyncio.to_thread(self._put_sync, rec)
        return rec

    def _get_sync(self, pipeline_id: str) -> Optional[PipelineRecord]:
        row = self._conn.execute(
            "SELECT data FROM pipelines WHERE pipeline_id=?", (pipeline_id,)
        ).fetchone()
        return PipelineRecord.model_validate_json(row[0]) if row else None

    async def get(self, pipeline_id: str) -> Optional[PipelineRecord]:
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, pipeline_id)

    def _list_sync(self, workspace_id: Optional[str], status: Optional[str]) -> list[PipelineRecord]:
        q = "SELECT data FROM pipelines"
        clauses, args = [], []
        if workspace_id is not None:
            clauses.append("workspace_id=?"); args.append(workspace_id)
        if status is not None:
            clauses.append("status=?"); args.append(status)
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY created_at DESC"
        rows = self._conn.execute(q, tuple(args)).fetchall()
        return [PipelineRecord.model_validate_json(r[0]) for r in rows]

    async def list(
        self, *, workspace_id: Optional[str] = None, status: Optional[str] = None
    ) -> list[PipelineRecord]:
        async with self._lock:
            return await asyncio.to_thread(self._list_sync, workspace_id, status)

    def _by_source_sync(self, source: str, after_id: str, limit: int) -> list[PipelineRecord]:
        rows = self._conn.execute(
            f"SELECT data FROM pipelines WHERE {_TRIGGER_SOURCE_EXPR_SQLITE} = ? "
            "AND pipeline_id > ? ORDER BY pipeline_id ASC LIMIT ?",
            (source, after_id, limit),
        ).fetchall()
        return [PipelineRecord.model_validate_json(r[0]) for r in rows]

    async def list_by_trigger_source(
        self, source: str, *, after_id: str = "", limit: int = 200
    ) -> list[PipelineRecord]:
        """Keyset page of records whose ``trigger.source`` is ``source``.

        The reversal side of the #2128 C backfill: ``source="backfill"`` finds
        exactly the records that backfill wrote, and nothing else — the live
        sources are ``start``/``launch``/``dispatch``/``schedule``/``webhook``/
        ``cli``. ``trigger`` lives in the record's JSON blob, so this is a
        ``json_extract``; see
        :data:`cairn.core.storage.backfill_pipelines.REVERSAL_SQL_SQLITE` for
        the same predicate spelled for an operator at a sqlite/psql prompt.
        """
        async with self._lock:
            return await asyncio.to_thread(self._by_source_sync, source, after_id, limit)

    def _delete_sync(self, pipeline_id: str) -> bool:
        cur = self._conn.execute("DELETE FROM pipelines WHERE pipeline_id=?", (pipeline_id,))
        self._conn.commit()
        return cur.rowcount > 0

    async def delete(self, pipeline_id: str) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._delete_sync, pipeline_id)

    def close(self) -> None:
        self._conn.close()


class PostgresPipelineStore(PgSchemaInit):
    """Postgres-backed store — mirrors :class:`SqlitePipelineStore` (full record in a
    JSONB ``data`` column) and :class:`PostgresWorkflowConfigStore` (lazy asyncpg pool).
    Selected when ``CAIRN_STORAGE=postgres://…`` so pipeline state lives in the one prod
    Postgres alongside the runs store (#427)."""

    _CREATE = (
        "CREATE TABLE IF NOT EXISTS pipelines ("
        " pipeline_id TEXT PRIMARY KEY, workspace_id TEXT, bundle_ref TEXT, status TEXT,"
        " created_at TEXT, data JSONB NOT NULL)"
    )
    _CREATE_IX = "CREATE INDEX IF NOT EXISTS ix_pipelines_ws ON pipelines(workspace_id)"
    _UPSERT = (
        "INSERT INTO pipelines (pipeline_id, workspace_id, bundle_ref, status, created_at, data)"
        " VALUES ($1,$2,$3,$4,$5,$6::jsonb)"
        " ON CONFLICT (pipeline_id) DO UPDATE SET workspace_id=EXCLUDED.workspace_id,"
        " bundle_ref=EXCLUDED.bundle_ref, status=EXCLUDED.status, data=EXCLUDED.data"
    )
    _SELECT = "SELECT data FROM pipelines WHERE pipeline_id=$1"
    _DELETE = "DELETE FROM pipelines WHERE pipeline_id=$1"
    #: #2128 C — the reversal predicate, keyset-paged by the primary key.
    _BY_SOURCE = (
        f"SELECT data::text AS data FROM pipelines WHERE {_TRIGGER_SOURCE_EXPR_POSTGRES} = $1"
        " AND pipeline_id > $2 ORDER BY pipeline_id ASC LIMIT $3"
    )

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn
        self._pool = None
        self._schema_ready = False

    async def _open_pool(self):
        try:
            import asyncpg  # noqa: F401, PLC0415 — optional dep (storage-postgres)
        except ImportError as e:
            raise RuntimeError(
                "Postgres pipeline store requires the 'storage-postgres' optional dep."
            ) from e
        if not self._dsn:
            raise RuntimeError("PostgresPipelineStore needs a DSN (CAIRN_STORAGE=postgres://…).")
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415

        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool) -> None:
        async with pool.acquire() as c:
            await c.execute(self._CREATE)
            await c.execute(self._CREATE_IX)

    async def get(self, pipeline_id: str) -> Optional[PipelineRecord]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            row = await c.fetchrow(self._SELECT, pipeline_id)
        return PipelineRecord.model_validate_json(row["data"]) if row else None

    async def put(self, rec: PipelineRecord) -> PipelineRecord:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            async with c.transaction():
                prev_status = await c.fetchval(
                    "SELECT status FROM pipelines WHERE pipeline_id=$1 FOR UPDATE",
                    rec.pipeline_id,
                )
                if prev_status is not None and not guard_pipeline_put(
                    prev_status, rec.status, pipeline_id=rec.pipeline_id
                ):
                    row = await c.fetchval(
                        "SELECT data FROM pipelines WHERE pipeline_id=$1", rec.pipeline_id
                    )
                    return PipelineRecord.model_validate_json(row)  # refused (#1245)
                rec = rec.model_copy(update={"updated_at": utc_now_iso()})
                await c.execute(
                    self._UPSERT, rec.pipeline_id, rec.workspace_id, rec.bundle_ref,
                    rec.status, rec.created_at, rec.model_dump_json(),
                )
        return rec

    async def list(
        self, *, workspace_id: Optional[str] = None, status: Optional[str] = None
    ) -> list[PipelineRecord]:
        q = "SELECT data FROM pipelines"
        clauses, args = [], []
        if workspace_id is not None:
            args.append(workspace_id); clauses.append(f"workspace_id=${len(args)}")
        if status is not None:
            args.append(status); clauses.append(f"status=${len(args)}")
        if clauses:
            q += " WHERE " + " AND ".join(clauses)
        q += " ORDER BY created_at DESC"
        pool = await self._get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(q, *args)
        return [PipelineRecord.model_validate_json(r["data"]) for r in rows]

    async def list_by_trigger_source(
        self, source: str, *, after_id: str = "", limit: int = 200
    ) -> list[PipelineRecord]:
        """Keyset page of records whose ``trigger.source`` is ``source`` — the
        Postgres twin of :meth:`SqlitePipelineStore.list_by_trigger_source`, and
        the query prod's reversal runs (``REVERSAL_SQL_POSTGRES``)."""
        pool = await self._get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch(self._BY_SOURCE, source, after_id, limit)
        return [PipelineRecord.model_validate_json(r["data"]) for r in rows]

    async def delete(self, pipeline_id: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            status = await c.execute(self._DELETE, pipeline_id)
        return status.split()[-1] != "0"  # asyncpg returns e.g. "DELETE 1"

    def close(self) -> None:
        pass  # pool lives for the process (parity with the sibling stores)


_default_store: Any = None


def default_pipeline_store() -> Any:
    """Process-wide store singleton — mirrors the other console stores. **`CAIRN_STORAGE`
    is the selector** (#427): ``postgres://…`` → Postgres; else the shared cairn.db
    sqlite. Swappable for the in-memory store in tests."""
    global _default_store
    if _default_store is None:
        url = getenv("CAIRN_STORAGE", "").strip()
        if url.startswith(("postgres://", "postgresql://")):
            _default_store = PostgresPipelineStore(dsn=url)
        else:
            _default_store = SqlitePipelineStore()
    return _default_store
