"""DB-backed store for **console/user-authored** trigger specs.

Triggers resolve as a merge of three sources, last-wins (see the router):

  ① pack-bundled YAML   ``<pack>/triggers/*.yaml``   read-only, distributable defaults
  ② user-overlay dir    ``~/.cairn/triggers/*.yaml``  hand / git authored
  ③ this store          ``~/.cairn/triggers.db``      what the *console* writes  ← writable

The console writes layer ③ (not a file) because it targets a possibly **remote**
engine: a file on the operator's laptop never reaches the engine box, but a DB
row — SQLite locally, the same Postgres mirror in prod — *is* the engine's
source of truth and survives multiple replicas. This mirrors
:mod:`cairn.core.connections` exactly (``SqliteConnectionStore``).

A row stores the **validated YAML text** of the spec plus indexed ``id``/
``name``/``source`` columns; the merge step parses that text back through the
same strict loader the file path uses. Moving between ② and ③ is an explicit
export/import — never a sync-on-every-write (that's a two-writer consistency
trap).

Moved here from ``cairn.core.triggers.store`` (#2416, ADR-0120 D6): a store is
persistence, so it lives below the HTTP layer; the old path is a deprecated
alias for one release.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import functools
import os
import threading
import re
from typing import Optional

from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso
from cairn.core.workspace import DEFAULT_WORKSPACE
from cairn.core.pg_schema_init import PgSchemaInit


def _slug(name: str) -> str:
    """Stable, URL-safe id from a trigger name (pk + lookup key)."""
    s = re.sub(r"[^a-zA-Z0-9._-]+", "-", name.strip().lower()).strip("-")
    return s or "trigger"


class StoredTrigger(BaseModel):
    """One console-authored trigger: its validated YAML text + metadata.

    ``yaml`` is the canonical spec text (already validated via
    ``load_trigger_spec_text`` before it ever reaches the store). ``source`` is
    mirrored from the spec for the indexed column / quick listing."""

    id: str = ""
    #: Tenancy scope (ADR-0048 rev 2, #238). ``"default"`` for single-node/OSS.
    #: Stamped from ``current_workspace()`` on create; the console lists/gets/
    #: deletes within its workspace. (Per-workspace trigger-router *matching*
    #: partitioning is the SaaS event-routing concern, #224 H.)
    workspace_id: str = DEFAULT_WORKSPACE
    name: str
    source: str = ""
    yaml: str
    enabled: bool = True
    description: str = ""
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)
    #: ADR-0101 D1 (#1973) — set to the workflow ref when this row is the
    #: MATERIALISATION of that flow's ``triggers:`` front matter, empty when a
    #: user authored it in Settings › Triggers. The declared/hand-made line: a
    #: re-enable converges rows it owns and never touches rows it doesn't.
    #: Rides the JSON ``data`` blob free (no column, no migration).
    declared_by: str = ""

    def model_post_init(self, _ctx: object) -> None:  # pydantic v2 hook
        if not self.id:
            object.__setattr__(self, "id", _slug(self.name))


class TriggerOwnedByAnotherWorkspace(ValueError):
    """A write would take over another workspace's trigger row (#213).

    Ids and names are unique across the whole store, not per workspace, and every
    backend's ``create`` is an upsert: a row written for workspace B with an id or
    name that A's row already holds used to REPLACE A's row and move it to B, so B
    captured A's receiver. ``create`` refuses that instead; a caller answering an
    HTTP request maps this to 409."""

    def __init__(self, trigger_id: str, name: str, owner: str, workspace_id: str) -> None:
        self.trigger_id, self.name, self.owner, self.workspace_id = trigger_id, name, owner, workspace_id
        super().__init__(
            f"trigger {name!r} (id {trigger_id!r}) already exists in workspace {owner!r}; "
            f"workspace {workspace_id!r} cannot create or replace it — give it another name"
        )


def _claim_conflict(rows: "list[tuple[str, str, str]]", t: "StoredTrigger") -> None:
    """Raise when any existing ``(id, name, workspace_id)`` row with ``t``'s id or
    name belongs to another workspace (#213)."""
    for rid, name, ws in rows:
        if (rid == t.id or name == t.name) and ws != t.workspace_id:
            raise TriggerOwnedByAnotherWorkspace(t.id, t.name, ws, t.workspace_id)


class InMemoryTriggerStore:
    """Volatile store (tests / no-config default)."""

    def __init__(self) -> None:
        self._by_id: dict[str, StoredTrigger] = {}

    async def create(self, t: StoredTrigger) -> StoredTrigger:
        _claim_conflict([(r.id, r.name, r.workspace_id) for r in self._by_id.values()], t)
        self._by_id[t.id] = t
        return t

    async def get(self, trigger_id: str, workspace_id: Optional[str] = None) -> Optional[StoredTrigger]:
        t = self._by_id.get(trigger_id)
        # workspace isolation (#238): refuse a cross-workspace lookup by id.
        if t is not None and workspace_id is not None and t.workspace_id != workspace_id:
            return None
        return t

    async def get_by_name(self, name: str) -> Optional[StoredTrigger]:
        return next((t for t in self._by_id.values() if t.name == name), None)

    async def list(self, workspace_id: Optional[str] = None) -> list[StoredTrigger]:
        return self.list_sync(workspace_id)

    def list_sync(self, workspace_id: Optional[str] = None) -> list[StoredTrigger]:
        # workspace_id=None → all (the global trigger router); a value → scoped
        # (the console). Default workspace is just another value here.
        recs = self._by_id.values()
        if workspace_id is not None:
            recs = [t for t in recs if t.workspace_id == workspace_id]
        return sorted(recs, key=lambda t: t.name)

    async def delete(self, trigger_id: str, workspace_id: Optional[str] = None) -> bool:
        t = self._by_id.get(trigger_id)
        if t is None or (workspace_id is not None and t.workspace_id != workspace_id):
            return False
        del self._by_id[trigger_id]
        return True

    def close(self) -> None:  # parity with the sqlite store
        pass


def _conn_locked(fn):
    """#163 — one ``sqlite3`` connection, shared across threads
    (``check_same_thread=False``): every use holds the store's thread lock. The
    asyncio lock serializes callers on one event loop only: a cached instance
    (``StorageBackend.triggers``) used from two event loops, as test clients'
    portal threads do, a cancelled ``to_thread`` whose worker keeps running, a
    direct ``list_sync`` call, or ``close`` racing an in-flight read could each
    drive the connection from two threads at once, which crashes CPython's
    sqlite3 module (a segfault, not an exception). An RLock: re-entrant on
    purpose (a locked caller may read again)."""
    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        with self._conn_lock:
            return fn(self, *args, **kwargs)
    return wrapper


class SqliteTriggerStore:
    """Embedded-SQLite store — restart-safe, single file. Mirrors
    :class:`~cairn.core.connections.SqliteConnectionStore`: the full
    :class:`StoredTrigger` serializes to a JSON ``data`` blob, async ops run via
    ``asyncio.to_thread`` so they never block the event loop. The load/merge path
    (router build, synchronous) uses :meth:`list_sync`. WAL does NOT make one
    shared connection safe across threads: every use holds ``_conn_lock`` (#163)."""

    def __init__(self, path: str | None = None) -> None:
        import sqlite3  # stdlib, but the connection is an instance handle

        if path is None:
            from cairn.core import settings

            # single shared cairn.db (#427/ADR-0058); CAIRN_*_DB overrides
            path = getenv("CAIRN_TRIGGERS_DB", "").strip() or settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn_lock = threading.RLock()  # #163 — see _conn_locked
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS triggers (
                id           TEXT PRIMARY KEY,
                name         TEXT UNIQUE,
                source       TEXT,
                workspace_id TEXT NOT NULL DEFAULT 'default',
                data         TEXT NOT NULL
            )
            """
        )
        # workspace_id is a real INDEXED column (ADR-0051): migrate pre-existing
        # DBs (add column + back-fill from the JSON blob once) + index the predicate.
        from cairn.core.storage import migrations
        if "workspace_id" in migrations.add_missing_columns(
            self._conn, "triggers", {"workspace_id": "TEXT NOT NULL DEFAULT 'default'"}
        ):
            self._conn.execute(
                "UPDATE triggers SET workspace_id=COALESCE(json_extract(data,'$.workspace_id'),'default')"
            )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_trig_ws_name ON triggers(workspace_id, name)"
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    @_conn_locked
    def _create_sync(self, t: StoredTrigger) -> None:
        # #213 — INSERT OR REPLACE drops any row holding this id OR name, whatever
        # its workspace; refuse when that row is another workspace's. The check and the
        # write share one IMMEDIATE transaction: another store instance on the same file
        # (default_trigger_store() builds one per call) cannot write in between.
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            _claim_conflict(self._conn.execute(
                "SELECT id, name, workspace_id FROM triggers WHERE id=? OR name=?", (t.id, t.name),
            ).fetchall(), t)
            self._conn.execute(
                "INSERT OR REPLACE INTO triggers (id, name, source, workspace_id, data) VALUES (?,?,?,?,?)",
                (t.id, t.name, t.source, t.workspace_id, t.model_dump_json()),
            )
        except BaseException:
            self._conn.rollback()
            raise
        self._conn.commit()

    async def create(self, t: StoredTrigger) -> StoredTrigger:
        async with self._lock:
            await asyncio.to_thread(self._create_sync, t)
        return t

    @_conn_locked
    def _get_sync(self, sql: str, params: tuple) -> Optional[StoredTrigger]:
        row = self._conn.execute(sql, params).fetchone()
        return StoredTrigger.model_validate_json(row[0]) if row else None

    async def get(self, trigger_id: str, workspace_id: Optional[str] = None) -> Optional[StoredTrigger]:
        # query-enforced scoping (ADR-0051 §4): a cross-workspace id is not-found.
        sql, params = (
            ("SELECT data FROM triggers WHERE id=? AND workspace_id=?", (trigger_id, workspace_id))
            if workspace_id is not None
            else ("SELECT data FROM triggers WHERE id=?", (trigger_id,))
        )
        async with self._lock:
            return await asyncio.to_thread(self._get_sync, sql, params)

    async def get_by_name(self, name: str) -> Optional[StoredTrigger]:
        async with self._lock:
            return await asyncio.to_thread(
                self._get_sync, "SELECT data FROM triggers WHERE name=?", (name,)
            )

    @_conn_locked
    def list_sync(self, workspace_id: Optional[str] = None) -> list[StoredTrigger]:
        """Synchronous read for the load/merge path (router build is sync).
        ``workspace_id=None`` → all (the global router); a value → scoped to that
        workspace (the console). Filtered on the real indexed ``workspace_id``
        column (ADR-0051)."""
        if workspace_id is not None:
            rows = self._conn.execute(
                "SELECT data FROM triggers WHERE workspace_id=? ORDER BY name ASC",
                (workspace_id,),
            ).fetchall()
        else:
            rows = self._conn.execute("SELECT data FROM triggers ORDER BY name ASC").fetchall()
        return [StoredTrigger.model_validate_json(r[0]) for r in rows]

    async def list(self, workspace_id: Optional[str] = None) -> list[StoredTrigger]:
        async with self._lock:
            return await asyncio.to_thread(self.list_sync, workspace_id)

    @_conn_locked
    def _delete_sync(self, trigger_id: str, workspace_id: Optional[str]) -> bool:
        if workspace_id is not None:
            cur = self._conn.execute(
                "DELETE FROM triggers WHERE id=? AND workspace_id=?",
                (trigger_id, workspace_id),
            )
        else:
            cur = self._conn.execute("DELETE FROM triggers WHERE id=?", (trigger_id,))
        self._conn.commit()
        return cur.rowcount > 0

    async def delete(self, trigger_id: str, workspace_id: Optional[str] = None) -> bool:
        async with self._lock:
            return await asyncio.to_thread(self._delete_sync, trigger_id, workspace_id)

    @_conn_locked
    def close(self) -> None:
        self._conn.close()


# ── Postgres backend (mirrors storage/postgres/runs.py + connections.py) — #427 ──
_CREATE_TRIGGERS_PG_SQL = """
    CREATE TABLE IF NOT EXISTS triggers (
        id           TEXT PRIMARY KEY,
        name         TEXT UNIQUE,
        source       TEXT,
        workspace_id TEXT NOT NULL DEFAULT 'default',
        data         JSONB NOT NULL
    )
"""
_CREATE_TRIGGERS_PG_IDX_SQL = (
    "CREATE INDEX IF NOT EXISTS idx_trig_ws_name ON triggers(workspace_id, name)"
)
_INSERT_TRIGGER_PG_SQL = """
    INSERT INTO triggers (id, name, source, workspace_id, data)
    VALUES ($1, $2, $3, $4, $5::jsonb)
    ON CONFLICT (id) DO UPDATE
    SET name = EXCLUDED.name, source = EXCLUDED.source,
        workspace_id = EXCLUDED.workspace_id, data = EXCLUDED.data
    WHERE triggers.workspace_id = EXCLUDED.workspace_id
"""
_SELECT_TRIGGER_PG_CLAIMS = "SELECT id, name, workspace_id FROM triggers WHERE id = $1 OR name = $2"
_SELECT_TRIGGER_PG_BY_ID = "SELECT data::text FROM triggers WHERE id = $1"
_SELECT_TRIGGER_PG_BY_ID_WS = "SELECT data::text FROM triggers WHERE id = $1 AND workspace_id = $2"
_SELECT_TRIGGER_PG_BY_NAME = "SELECT data::text FROM triggers WHERE name = $1"
_LIST_TRIGGERS_PG_SQL = "SELECT data::text FROM triggers ORDER BY name ASC"
_LIST_TRIGGERS_PG_BY_WS_SQL = (
    "SELECT data::text FROM triggers WHERE workspace_id = $1 ORDER BY name ASC"
)
_DELETE_TRIGGER_PG_SQL = "DELETE FROM triggers WHERE id = $1"
_DELETE_TRIGGER_PG_WS_SQL = "DELETE FROM triggers WHERE id = $1 AND workspace_id = $2"


class PostgresTriggerStore(PgSchemaInit):
    """Postgres-backed trigger store — mirrors :class:`SqliteTriggerStore`
    (full :class:`StoredTrigger` in a JSONB ``data`` column, indexed
    ``workspace_id``/``name``) and :class:`~cairn.core.connections.PostgresConnectionStore`
    (lazy asyncpg pool, created on first call). Selected by
    :func:`default_trigger_store` when ``CAIRN_STORAGE=postgres://…`` so
    console-authored triggers live in the one prod Postgres instead of a
    per-container SQLite file — the storage-unification move for this store
    (#427)."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn
        self._pool = None
        self._schema_ready = False

    async def _open_pool(self):
        try:
            import asyncpg  # noqa: PLC0415 — optional dep (storage-postgres)
        except ImportError as e:
            raise RuntimeError(
                "Postgres trigger store requires the 'storage-postgres' optional "
                "dep group. Install with: pip install 'cairndev[storage-postgres]'"
            ) from e
        if not self._dsn:
            raise RuntimeError(
                "PostgresTriggerStore needs a DSN. Pass dsn= or set CAIRN_STORAGE=postgres://…"
            )
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415

        # #614: one pool per DSN, shared across every Postgres store in this process.
        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool) -> None:
        async with pool.acquire() as c:
            await c.execute(_CREATE_TRIGGERS_PG_SQL)
            await c.execute(_CREATE_TRIGGERS_PG_IDX_SQL)

    async def create(self, t: StoredTrigger) -> StoredTrigger:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            # #213 — never take over another workspace's row; the WHERE on the
            # upsert keeps that true even if a row lands between the two statements.
            rows = await c.fetch(_SELECT_TRIGGER_PG_CLAIMS, t.id, t.name)
            _claim_conflict([(r["id"], r["name"], r["workspace_id"]) for r in rows], t)
            status = await c.execute(
                _INSERT_TRIGGER_PG_SQL, t.id, t.name, t.source, t.workspace_id, t.model_dump_json()
            )
            if status.split()[-1] == "0":  # the upsert's WHERE refused the update
                raise TriggerOwnedByAnotherWorkspace(t.id, t.name, "another workspace", t.workspace_id)
        return t

    async def get(self, trigger_id: str, workspace_id: Optional[str] = None) -> Optional[StoredTrigger]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            if workspace_id is not None:
                row = await c.fetchrow(_SELECT_TRIGGER_PG_BY_ID_WS, trigger_id, workspace_id)
            else:
                row = await c.fetchrow(_SELECT_TRIGGER_PG_BY_ID, trigger_id)
        return StoredTrigger.model_validate_json(row["data"]) if row else None

    async def get_by_name(self, name: str) -> Optional[StoredTrigger]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            row = await c.fetchrow(_SELECT_TRIGGER_PG_BY_NAME, name)
        return StoredTrigger.model_validate_json(row["data"]) if row else None

    def list_sync(self, workspace_id: Optional[str] = None) -> list[StoredTrigger]:
        """Synchronous read for the load/merge path (router build is sync).
        Not supported here: the pool is asyncio-only. Callers on the Postgres
        backend must use :meth:`list` (async); mirrors that this backend is
        selected only in server/engine contexts that already run a loop."""
        raise RuntimeError(
            "PostgresTriggerStore.list_sync() is not supported — use the async "
            "list() instead (the asyncpg pool has no sync path)."
        )

    async def list(self, workspace_id: Optional[str] = None) -> list[StoredTrigger]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            if workspace_id is not None:
                rows = await c.fetch(_LIST_TRIGGERS_PG_BY_WS_SQL, workspace_id)
            else:
                rows = await c.fetch(_LIST_TRIGGERS_PG_SQL)
        return [StoredTrigger.model_validate_json(r["data"]) for r in rows]

    async def delete(self, trigger_id: str, workspace_id: Optional[str] = None) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            if workspace_id is not None:
                status = await c.execute(_DELETE_TRIGGER_PG_WS_SQL, trigger_id, workspace_id)
            else:
                status = await c.execute(_DELETE_TRIGGER_PG_SQL, trigger_id)
        return status.split()[-1] != "0"  # asyncpg returns e.g. "DELETE 1"

    def close(self) -> None:
        # The pool lives for the process (parity with SqliteTriggerStore.close,
        # which closes its single conn); pool teardown happens with the event loop.
        pass


def default_trigger_store():
    """Pick a trigger store from the env. **`CAIRN_STORAGE` is the selector**
    (#427): ``postgres://…`` / ``postgresql://…`` → :class:`PostgresTriggerStore`
    (the one prod Postgres), mirroring the registry-family stores
    (:func:`~cairn.core.connections.default_connection_store`). Otherwise
    durable-by-default sqlite at ``~/.cairn/triggers.db`` (like the run +
    connection stores) so triggers added via the console persist and are
    visible to the trigger router at runtime. ``CAIRN_TRIGGERS_DB`` overrides the
    sqlite path; ``CAIRN_TRIGGERS_DB=:memory:`` opts into the volatile in-memory
    store (tests otherwise construct stores explicitly)."""
    url = getenv("CAIRN_STORAGE", "").strip()
    if url.startswith(("postgres://", "postgresql://")):
        return PostgresTriggerStore(dsn=url)
    if getenv("CAIRN_TRIGGERS_DB", "").strip() == ":memory:":
        return InMemoryTriggerStore()
    return SqliteTriggerStore()
