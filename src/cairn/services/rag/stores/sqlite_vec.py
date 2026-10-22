"""sqlite-vec vector store — dense-only embedded backend.

The §24.4 Tier-1 promise: vectors live in the same SQLite database as
runs/events, so dev needs **zero extra processes** to do RAG.

By default, this driver writes to the **same file** as
:class:`~cairn.core.storage.SqliteRunStore` (``CAIRN_RUN_DB`` if set,
else ``~/.cairn/runs.db``). Set ``CAIRN_SQLITE_VEC_DB`` to override and
keep vectors in a separate file. One file is the recommended dev
configuration — single backup target, single connection.

Layout: **two tables per collection**.

  - ``<collection>_vec`` — vec0 virtual table holding ``(rowid, embedding)``.
    Virtual tables only support INTEGER rowids; can't store TEXT ids
    natively.
  - ``<collection>`` — companion table holding ``(id TEXT PK, source,
    payload JSON, rowid INTEGER UNIQUE)``. The rowid column maps external
    string ids → internal int rowids so we can JOIN back into the vec0
    table.

vec0 does NOT support ``ON CONFLICT``; upserts are a transactional
DELETE-then-INSERT against both tables.

Sparse vectors are accepted but ignored — sqlite-vec is dense-only
today (sparse support is in flight upstream).

Pinned via the ``rag-sqlite-vec`` optional dep group:

.. code-block:: bash

    pip install 'cairndev[rag-sqlite-vec]'
    export RAG_VECTOR_STORE=sqlite_vec

Lives alongside qdrant + pgvector + pinecone in
:mod:`cairn.services.rag.stores` — the canonical home for vector
backends per §24.4.4b.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import json
import logging
import os
import sqlite3
import struct
from typing import Any, Optional

from cairn.services.rag.stores.base import VectorStore

logger = logging.getLogger(__name__)


def _require_sqlite_vec():
    """Lazy import — keep the dep optional. Clean error if not installed."""
    try:
        import sqlite_vec  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "sqlite-vec backend requires the 'rag-sqlite-vec' optional dep "
            "group. Install with: pip install 'cairndev[rag-sqlite-vec]'"
        ) from e


def _serialize_float32(vec: list[float]) -> bytes:
    """Pack a float vector into the sqlite-vec wire format.

    sqlite-vec stores vectors as packed little-endian float32. The
    ``sqlite_vec.serialize_float32`` helper does the same thing; we inline
    it via ``struct`` so the import stays lazy and tests can exercise the
    SQL shape without the optional dep installed.
    """
    return struct.pack(f"<{len(vec)}f", *vec)


def _default_db_path() -> str:
    """Resolve the SQLite file for this driver.

    Priority:
      1. ``CAIRN_SQLITE_VEC_DB`` env (explicit override).
      2. ``CAIRN_RUN_DB`` env (shared file with :class:`SqliteRunStore` —
         the §24.4 Tier-1 "one file" promise).
      3. ``~/.cairn/runs.db`` (matches SqliteRunStore's default).
    """
    override = getenv("CAIRN_SQLITE_VEC_DB")
    if override:
        return override
    from cairn.core import settings as _settings
    return _settings.run_db_path() or os.path.expanduser("~/.cairn/runs.db")


# ─── SQL templates ──────────────────────────────────────────────────────
#
# Collection name is interpolated into table names; we validate it
# strictly via _safe_collection. Everything else uses parameterized queries.


def _create_vec_table_sql(table: str, dim: int) -> str:
    # vec0 virtual table — vectors keyed by rowid (INTEGER). Use FLOAT[N]
    # form; sqlite-vec parses this into a fixed-dim float32 vector column.
    return f"CREATE VIRTUAL TABLE IF NOT EXISTS {table}_vec USING vec0(embedding FLOAT[{dim}])"


def _create_companion_table_sql(table: str) -> str:
    # Companion table: external string id ↔ internal integer rowid.
    # rowid is UNIQUE so JOIN back to the vec0 table is single-row.
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id      TEXT PRIMARY KEY,
            source  TEXT,
            payload TEXT NOT NULL DEFAULT '{{}}',
            rowid   INTEGER UNIQUE
        )
    """


def _create_source_index_sql(table: str) -> str:
    return f"CREATE INDEX IF NOT EXISTS {table}_source_idx ON {table}(source)"


def _select_rowid_sql(table: str) -> str:
    return f"SELECT rowid FROM {table} WHERE id = ?"


def _next_rowid_sql(table: str) -> str:
    # Next free integer rowid. COALESCE handles the empty-table case.
    return f"SELECT COALESCE(MAX(rowid), 0) + 1 FROM {table}"


def _delete_companion_sql(table: str) -> str:
    return f"DELETE FROM {table} WHERE id = ?"


def _delete_vec_sql(table: str) -> str:
    return f"DELETE FROM {table}_vec WHERE rowid = ?"


def _insert_companion_sql(table: str) -> str:
    return f"INSERT INTO {table} (id, source, payload, rowid) VALUES (?, ?, ?, ?)"


def _insert_vec_sql(table: str) -> str:
    return f"INSERT INTO {table}_vec (rowid, embedding) VALUES (?, ?)"


def _search_sql(table: str, filter_sql: str = "") -> str:
    # Join vec0 KNN result with the companion table. The `k = ?` in the
    # WHERE clause tells sqlite-vec how many neighbors to return.
    where_extra = f" AND {filter_sql}" if filter_sql else ""
    return f"""
        SELECT c.id, c.source, c.payload, v.distance
        FROM {table}_vec AS v
        JOIN {table}    AS c ON c.rowid = v.rowid
        WHERE v.embedding MATCH ? AND v.k = ?{where_extra}
        ORDER BY v.distance
    """


def _delete_by_source_select_sql(table: str) -> str:
    return f"SELECT rowid FROM {table} WHERE source = ?"


def _delete_by_source_companion_sql(table: str) -> str:
    return f"DELETE FROM {table} WHERE source = ?"


def _delete_by_source_vec_sql(table: str, placeholders: str) -> str:
    return f"DELETE FROM {table}_vec WHERE rowid IN ({placeholders})"


def _has_source_sql(table: str) -> str:
    return f"SELECT 1 FROM {table} WHERE source = ? LIMIT 1"


def _list_sources_sql(table: str) -> str:
    return f"""
        SELECT source,
               COALESCE(json_extract(payload, '$.source_type'), 'unknown') AS source_type,
               COUNT(*) AS chunk_count
        FROM {table}
        WHERE source IS NOT NULL
        GROUP BY source, source_type
        LIMIT ?
    """


# ─── Driver ─────────────────────────────────────────────────────────────


class SqliteVecStore(VectorStore):
    """Dense-only sqlite-vec driver.

    A single :class:`sqlite3.Connection` is opened lazily on first use,
    with the sqlite-vec extension loaded. WAL mode is enabled (mirroring
    :class:`SqliteRunStore`) so a crash mid-write can't corrupt the file.
    Concurrency: an ``asyncio.Lock`` serializes access and the blocking sqlite3
    work runs on a worker thread via ``asyncio.to_thread`` (#86) so it never
    stalls the event loop — same pattern as :class:`SqliteRunStore`. The lock
    guarantees one thread touches the ``check_same_thread=False`` connection at
    a time.
    """

    def __init__(self, path: Optional[str] = None) -> None:
        self._path: Optional[str] = path
        self._conn: Optional[sqlite3.Connection] = None
        self._lock = asyncio.Lock()
        self._collection: Optional[str] = None
        #: collections this store already created, by dim: callers select the
        #: collection before every store call, so the DDL runs once per process
        self._ready: dict[str, int] = {}

    @staticmethod
    def _safe_collection(collection: str) -> str:
        if not collection.replace("_", "").isalnum():
            raise ValueError(
                f"invalid collection name: {collection!r} "
                "(must be alphanumeric + underscores)"
            )
        return collection

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        _require_sqlite_vec()
        import sqlite_vec  # noqa: PLC0415

        path = self._path or _default_db_path()
        if path != ":memory:":
            parent = os.path.dirname(os.path.abspath(path))
            os.makedirs(parent, exist_ok=True)
        self._path = path
        conn = sqlite3.connect(path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        self._conn = conn
        return conn

    @property
    def _coll(self) -> str:
        if self._collection:
            return self._collection
        from cairn.core.provider_settings import get_settings
        return self._safe_collection(get_settings().rag_collection_name)

    # ── Contract ───────────────────────────────────────────────────────

    # The sqlite3 work in every method runs via ``asyncio.to_thread`` under the
    # lock (#86): the connection is ``check_same_thread=False`` and the lock
    # serializes, so exactly one worker thread touches it at a time, and the
    # blocking vec0 queries never stall the event loop.

    def _initialize_sync(self, table: str, dim: int) -> None:
        conn = self._get_conn()
        conn.execute(_create_vec_table_sql(table, dim))
        conn.execute(_create_companion_table_sql(table))
        conn.execute(_create_source_index_sql(table))
        conn.commit()

    async def initialize(self, collection: str, dim: int) -> None:
        table = self._safe_collection(collection)
        if self._ready.get(table) == dim:
            self._collection = table
            return
        async with self._lock:
            await asyncio.to_thread(self._initialize_sync, table, dim)
        self._collection = table
        self._ready[table] = dim
        logger.info("sqlite-vec collection ready: %s (dim=%d)", table, dim)

    async def upsert(
        self,
        ids: list[str],
        dense: list[list[float]],
        sparse: list[dict] | None,
        payloads: list[dict],
    ) -> int:
        if sparse is not None:
            logger.debug(
                "sqlite-vec backend ignores sparse vectors (dense-only); "
                "sparse support is upstream-in-flight"
            )
        if not ids:
            return 0
        table = self._coll
        async with self._lock:
            await asyncio.to_thread(self._upsert_sync, table, ids, dense, payloads)
        return len(ids)

    def _upsert_sync(
        self, table: str, ids: list[str], dense: list[list[float]],
        payloads: list[dict],
    ) -> None:
        conn = self._get_conn()
        with conn:  # transactional
            for pid, dvec, payload in zip(ids, dense, payloads):
                # vec0 has no ON CONFLICT — for upsert semantics, look up
                # the existing rowid (if any), DELETE both tables, then
                # INSERT fresh. Companion rowid stays stable across the
                # update so external references survive.
                row = conn.execute(_select_rowid_sql(table), (pid,)).fetchone()
                if row is not None:
                    rowid = row[0]
                    conn.execute(_delete_vec_sql(table), (rowid,))
                    conn.execute(_delete_companion_sql(table), (pid,))
                else:
                    rowid = conn.execute(_next_rowid_sql(table)).fetchone()[0]
                conn.execute(
                    _insert_companion_sql(table),
                    (pid, payload.get("source"), json.dumps(payload), rowid),
                )
                conn.execute(
                    _insert_vec_sql(table),
                    (rowid, _serialize_float32(dvec)),
                )

    async def search(
        self,
        dense: list[float],
        sparse: dict | None = None,
        top_k: int = 5,
        filters: dict | None = None,
    ) -> list[dict]:
        if sparse is not None:
            logger.debug("sqlite-vec search ignores sparse vector (dense-only)")
        table = self._coll

        # JSON containment-ish filters via json_extract on the companion
        # payload. Match semantics: any-of within a key; AND across keys.
        filter_sql = ""
        extra_params: list[Any] = []
        if filters:
            clauses = []
            for key, vals in filters.items():
                if not isinstance(vals, list):
                    vals = [vals]
                ors = []
                for v in vals:
                    ors.append("json_extract(c.payload, ?) = ?")
                    extra_params.append(f"$.{key}")
                    extra_params.append(v)
                clauses.append("(" + " OR ".join(ors) + ")")
            filter_sql = " AND ".join(clauses)

        sql = _search_sql(table, filter_sql)
        params = (_serialize_float32(dense), top_k, *extra_params)
        async with self._lock:
            rows = await asyncio.to_thread(self._fetchall_sync, sql, params)

        out: list[dict] = []
        for r in rows:
            id_, source, payload_json, distance = r
            payload = json.loads(payload_json)
            # Convert L2 distance → cosine-similarity-shape score in [0, 1]
            # roughly; sqlite-vec default distance is L2. For true cosine,
            # we'd configure the vec0 table with cosine ops in initialize().
            # For parity with pgvector and Qdrant, use 1/(1+distance) as a
            # monotonic similarity proxy until we wire cosine ops explicitly.
            score = 1.0 / (1.0 + float(distance))
            out.append({"score": round(score, 4), **payload})
        return out

    def _fetchall_sync(self, sql: str, params) -> list:
        return self._get_conn().execute(sql, params).fetchall()

    def _delete_by_source_sync(self, table: str, source: str) -> int:
        conn = self._get_conn()
        rowids = [
            r[0] for r in conn.execute(
                _delete_by_source_select_sql(table), (source,)
            ).fetchall()
        ]
        if not rowids:
            return 0
        placeholders = ",".join("?" for _ in rowids)
        with conn:
            conn.execute(_delete_by_source_vec_sql(table, placeholders), rowids)
            cur = conn.execute(_delete_by_source_companion_sql(table), (source,))
            count = cur.rowcount
        return int(count)

    async def delete_by_source(self, source: str) -> int:
        table = self._coll
        async with self._lock:
            return await asyncio.to_thread(self._delete_by_source_sync, table, source)

    async def list_sources(self, limit: int = 50) -> list[dict]:
        table = self._coll
        async with self._lock:
            rows = await asyncio.to_thread(
                self._fetchall_sync, _list_sources_sql(table), (limit,)
            )
        return [
            {"source": r[0], "source_type": r[1], "chunk_count": int(r[2])}
            for r in rows
        ]

    async def has_source(self, source: str) -> bool:
        table = self._coll
        async with self._lock:
            rows = await asyncio.to_thread(
                self._fetchall_sync, _has_source_sql(table), (source,)
            )
        return len(rows) > 0

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
        self._ready.clear()  # a reopened (e.g. in-memory) database may not have them
