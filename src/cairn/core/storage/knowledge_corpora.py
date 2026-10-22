"""Workspace (BYO) knowledge corpora — drop-in knowledge without a pip pack (#516).

A pack ships knowledge as a published pip distribution (kind="knowledge"). That's
right for CURATED/registry corpora (MITRE, CVE), but too heavy for a customer's
OWN data. This store is the lighter path: an ACCOUNT + workspace-scoped record —
name + source ref + role + governance metadata — that gets indexed and composed
into a knowledge role exactly like a pack corpus, with zero packaging.

**Ingress (the source is NEVER a raw server path — it's a governed artifact
reference so a tenant's data can't leak):**
  - ``s3://bucket/prefix`` — cairn PULLS from the customer's S3 using that
    account's stored S3 connection (connection catalog, account-scoped creds).
  - ``artifact://…`` — files UPLOADED to cairn are written to cairn's artifact
    storage, namespaced by ``account_id``/``workspace_id`` (the same isolation
    boundary #237/ADR-0054 enforce for runs/memory), then referenced here.
  - ``file://…`` — local dev only.

At index time the ref is resolved through the artifact layer to a local dir,
parsed (the RAG ingest readers), embedded, and upserted into ``rag_<name>`` scoped by
workspace — so account A never reads account B's corpus. Same governance surface
as a pack corpus (license/source/citation) + the same role-composition (Slice 2).
Registry packs = the on-ramp; these = the retention moat.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import os
import sqlite3
import uuid
from typing import Optional, Protocol, runtime_checkable

from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso
from cairn.core.pg_schema_init import PgSchemaInit


def _new_id() -> str:
    return f"wkb-{uuid.uuid4().hex[:12]}"


#: Longest corpus name a store accepts on ``add``. The name is part of the corpus's
#: vector-collection name (``rag_<name>__<embedder>``, after the workspace prefix),
#: so an unbounded name makes an unbounded table name.
MAX_CORPUS_NAME_LEN = 48


def corpus_name_problem(name: str) -> Optional[str]:
    """Why ``name`` cannot name a new corpus, or ``None``. Callers that take a name
    from a user check this first (a store's ``add`` refuses the same names)."""
    if len(name or "") > MAX_CORPUS_NAME_LEN:
        return f"a corpus name is at most {MAX_CORPUS_NAME_LEN} characters"
    return None


def _check_new(corpus: "WorkspaceCorpus") -> None:
    problem = corpus_name_problem(corpus.name)
    if problem:
        raise ValueError(problem)


class WorkspaceCorpus(BaseModel):
    """A drop-in knowledge corpus registered against a workspace."""

    corpus_id: str = Field(default_factory=_new_id)
    account_id: str = "default"    # tenant — the isolation boundary (ADR-0054)
    workspace_id: str = "default"
    name: str = ""                 # the corpus name → collection rag_<name>
    #: Governed source ref, resolved through the artifact layer at index time.
    #: ``s3://bucket/prefix`` (pulled via the account's S3 connection) |
    #: ``artifact://…`` (uploaded to cairn's account-scoped artifact storage) |
    #: ``file://…`` (local dev). NEVER a raw server path — that would let a
    #: tenant read outside its boundary.
    source_uri: str = ""
    role: Optional[str] = None     # the global knowledge role it provides into
    license: Optional[str] = None
    source: Optional[str] = None
    citation_policy: Optional[str] = None
    attribution: Optional[str] = None
    created_at: str = Field(default_factory=utc_now_iso)


@runtime_checkable
class WorkspaceCorpusStore(Protocol):
    async def add(self, corpus: WorkspaceCorpus) -> WorkspaceCorpus: ...
    async def list(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]: ...
    #: Synchronous read for the role-composition hot path (list_roles is sync).
    def list_sync(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]: ...
    async def get(self, corpus_id: str) -> Optional[WorkspaceCorpus]: ...
    async def delete(self, corpus_id: str) -> bool: ...


class InMemoryWorkspaceCorpusStore:
    def __init__(self) -> None:
        self._rows: dict[str, WorkspaceCorpus] = {}
        self._lock = asyncio.Lock()

    async def add(self, corpus: WorkspaceCorpus) -> WorkspaceCorpus:
        _check_new(corpus)
        async with self._lock:
            self._rows[corpus.corpus_id] = corpus.model_copy(deep=True)
        return corpus

    async def list(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]:
        return self.list_sync(workspace_id)

    def list_sync(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]:
        rows = [c.model_copy(deep=True) for c in self._rows.values()
                if workspace_id is None or c.workspace_id == workspace_id]
        rows.sort(key=lambda c: c.created_at)
        return rows

    async def get(self, corpus_id: str) -> Optional[WorkspaceCorpus]:
        async with self._lock:
            c = self._rows.get(corpus_id)
            return c.model_copy(deep=True) if c else None

    async def delete(self, corpus_id: str) -> bool:
        async with self._lock:
            return self._rows.pop(corpus_id, None) is not None


class SqliteWorkspaceCorpusStore:
    def __init__(self, path: str | None = None) -> None:
        if path is None:
            from cairn.core import settings
            path = getenv("CAIRN_EVAL_LEDGER_DB", "").strip() or settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS workspace_corpora (
                corpus_id    TEXT PRIMARY KEY,
                workspace_id TEXT NOT NULL,
                created_at   TEXT NOT NULL,
                data         TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_wkb_ws ON workspace_corpora(workspace_id)"
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    def _add_sync(self, c: WorkspaceCorpus) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO workspace_corpora (corpus_id, workspace_id, created_at, data) "
            "VALUES (?, ?, ?, ?)",
            (c.corpus_id, c.workspace_id, c.created_at, c.model_dump_json()),
        )
        self._conn.commit()

    async def add(self, corpus: WorkspaceCorpus) -> WorkspaceCorpus:
        _check_new(corpus)
        async with self._lock:
            await asyncio.to_thread(self._add_sync, corpus)
        return corpus

    def _list_sync(self, workspace_id: Optional[str]) -> list[WorkspaceCorpus]:
        if workspace_id is None:
            rows = self._conn.execute(
                "SELECT data FROM workspace_corpora ORDER BY created_at"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT data FROM workspace_corpora WHERE workspace_id = ? ORDER BY created_at",
                (workspace_id,),
            ).fetchall()
        return [WorkspaceCorpus.model_validate_json(r[0]) for r in rows]

    async def list(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]:
        async with self._lock:
            return await asyncio.to_thread(self._list_sync, workspace_id)

    def list_sync(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]:
        # WAL + check_same_thread=False makes a direct read safe (no lock needed).
        return self._list_sync(workspace_id)

    async def get(self, corpus_id: str) -> Optional[WorkspaceCorpus]:
        async with self._lock:
            row = self._conn.execute(
                "SELECT data FROM workspace_corpora WHERE corpus_id = ?", (corpus_id,)
            ).fetchone()
        return WorkspaceCorpus.model_validate_json(row[0]) if row else None

    async def delete(self, corpus_id: str) -> bool:
        async with self._lock:
            cur = self._conn.execute(
                "DELETE FROM workspace_corpora WHERE corpus_id = ?", (corpus_id,)
            )
            self._conn.commit()
            return cur.rowcount > 0


_CREATE_PG_SQL = """
    CREATE TABLE IF NOT EXISTS workspace_corpora (
        corpus_id    TEXT PRIMARY KEY,
        workspace_id TEXT NOT NULL,
        created_at   TEXT NOT NULL,
        data         JSONB NOT NULL
    )
"""
_CREATE_PG_INDEX_SQL = "CREATE INDEX IF NOT EXISTS idx_wkb_ws ON workspace_corpora(workspace_id)"


class PostgresWorkspaceCorpusStore(PgSchemaInit):
    """Postgres-backed BYO corpus store — the multi-node prod tier (mirrors
    :class:`PostgresEvalLedger`: lazy asyncpg pool, JSONB ``data`` column).
    Selected when ``CAIRN_STORAGE=postgres://…``.

    ``list_sync`` (the sync role-composition hot path) is served from an
    in-process cache warmed by ``add``/``list`` — asyncpg is async-only, so a
    freshly-started node composes BYO corpora after its first async read; adds on
    OTHER nodes propagate on the next refresh (eventual consistency for this
    low-volume config, acceptable; the async path is always exact)."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn
        self._pool = None
        self._schema_ready = False
        self._cache: dict[str, WorkspaceCorpus] = {}

    async def _open_pool(self):
        try:
            import asyncpg  # noqa: PLC0415,F401 — dep guard; shared_pool opens the pool
        except ImportError as e:
            raise RuntimeError(
                "Postgres BYO corpus store requires the 'storage-postgres' extra: "
                "pip install 'cairndev[storage-postgres]'"
            ) from e
        if not self._dsn:
            raise RuntimeError("PostgresWorkspaceCorpusStore needs a DSN (CAIRN_STORAGE=postgres://…)")
        # #614: share one pool per DSN across every Postgres store in this process.
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415
        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool) -> None:
        async with pool.acquire() as c:
            await c.execute(_CREATE_PG_SQL)
            await c.execute(_CREATE_PG_INDEX_SQL)

    async def add(self, corpus: WorkspaceCorpus) -> WorkspaceCorpus:
        _check_new(corpus)
        pool = await self._get_pool()
        async with pool.acquire() as c:
            await c.execute(
                "INSERT INTO workspace_corpora (corpus_id, workspace_id, created_at, data) "
                "VALUES ($1, $2, $3, $4::jsonb) ON CONFLICT (corpus_id) DO UPDATE SET "
                "workspace_id = EXCLUDED.workspace_id, data = EXCLUDED.data",
                corpus.corpus_id, corpus.workspace_id, corpus.created_at, corpus.model_dump_json(),
            )
        self._cache[corpus.corpus_id] = corpus  # warm the sync-read cache
        return corpus

    async def list(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            rows = await c.fetch("SELECT data FROM workspace_corpora ORDER BY created_at")
        all_rows = [WorkspaceCorpus.model_validate_json(r["data"]) for r in rows]
        self._cache = {c.corpus_id: c for c in all_rows}  # refresh cache from truth
        return [c for c in all_rows if workspace_id is None or c.workspace_id == workspace_id]

    def list_sync(self, workspace_id: Optional[str] = None) -> list[WorkspaceCorpus]:
        rows = [c for c in self._cache.values()
                if workspace_id is None or c.workspace_id == workspace_id]
        rows.sort(key=lambda c: c.created_at)
        return rows

    async def get(self, corpus_id: str) -> Optional[WorkspaceCorpus]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            row = await c.fetchrow("SELECT data FROM workspace_corpora WHERE corpus_id = $1", corpus_id)
        return WorkspaceCorpus.model_validate_json(row["data"]) if row else None

    async def delete(self, corpus_id: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            res = await c.execute("DELETE FROM workspace_corpora WHERE corpus_id = $1", corpus_id)
        self._cache.pop(corpus_id, None)
        return res.endswith("1")


_STORE: Optional[WorkspaceCorpusStore] = None


def get_workspace_corpus_store() -> WorkspaceCorpusStore:
    """Process singleton, routed by ``CAIRN_STORAGE`` (mirrors the run store / eval
    ledger): ``postgres://…`` → Postgres (multi-node); else SQLite (single-node);
    else in-memory (no writable path)."""
    global _STORE
    if _STORE is None:
        dsn = getenv("CAIRN_STORAGE", "").strip()
        if dsn.startswith(("postgres://", "postgresql://")):
            _STORE = PostgresWorkspaceCorpusStore(dsn=dsn)
        else:
            try:
                _STORE = SqliteWorkspaceCorpusStore()
            except Exception:  # noqa: BLE001 — no writable DB path → volatile tier
                _STORE = InMemoryWorkspaceCorpusStore()
    return _STORE


__all__ = [
    "MAX_CORPUS_NAME_LEN", "corpus_name_problem",
    "WorkspaceCorpus", "WorkspaceCorpusStore",
    "InMemoryWorkspaceCorpusStore", "SqliteWorkspaceCorpusStore",
    "get_workspace_corpus_store",
]
