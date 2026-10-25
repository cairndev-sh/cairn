"""PGVector vector store — dense-only Postgres backend.

Implements the :class:`VectorStore` contract using ``pgvector`` for dense
cosine-distance ANN. **Sparse vectors are accepted but ignored** — pgvector
has no native sparse support; hybrid retrieval needs a separate FTS index
(``tsvector`` + GIN) or a SPLADE-style sparse index, both of which are
out of scope for the dense-only first cut.

Layout: one table per collection (cleaner mental model, matches Qdrant's
collection isolation, makes ``DROP TABLE`` a clean teardown). Each row is
``(id, embedding, source, payload)``; ``source`` is denormalized out of
the JSONB payload because we filter on it heavily (``delete_by_source``,
``has_source``, ``list_sources``).

Pinned via the ``rag-pgvector`` optional dep group:

.. code-block:: bash

    pip install 'cairndev[rag-pgvector]'
    export RAG_VECTOR_STORE=pgvector
    export PGVECTOR_DSN='postgresql://user:pass@host:5432/cairn'

Lives in :mod:`cairn.services.rag.stores` (alongside qdrant + pinecone)
as the canonical home for vector-store drivers. The higher-level
:class:`~cairn.core.storage.HybridRagStore` automatically picks it up
through the factory in :mod:`cairn.services.rag.stores.__init__`.

P5 of §24.4 will add a Postgres-backed ``RunStore`` for runs/events; that
plus this driver means a single Postgres instance can host the whole
storage tier (runs + events + memory + RAG vectors).
"""
from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Optional

from cairn.services.rag.stores.base import VectorStore

logger = logging.getLogger(__name__)


def _require_asyncpg():
    """Lazy import — keep the dep optional. Clean error if not installed."""
    try:
        import asyncpg  # noqa: F401
        import pgvector  # noqa: F401
        from pgvector.asyncpg import register_vector  # noqa: F401
    except ImportError as e:
        raise RuntimeError(
            "pgvector backend requires the 'rag-pgvector' optional dep group. "
            "Install with: pip install 'cairndev[rag-pgvector]'"
        ) from e


# ─── SQL templates ──────────────────────────────────────────────────────
#
# Collection name is interpolated (not a parameter — table names can't be
# bound). We validate it strictly in :meth:`_safe_collection` to prevent
# SQL injection. Everything else uses parameterized queries.


#: Postgres keeps only the first 63 bytes of an identifier and silently drops the rest.
_PG_IDENTIFIER_MAX = 63


def pg_table_name(collection: str) -> str:
    """The table a collection is stored in. A name that fits in 63 bytes is used
    as is (unchanged from before). A longer one — which Postgres would cut, so
    that two long names sharing their first 63 bytes, or differing only in the
    embedder fingerprint at the end, would land in one table — becomes a readable
    40-byte head plus a 16-hex SHA-256 of the WHOLE name (57 bytes at most)."""
    raw = collection.encode("utf-8")
    if len(raw) <= _PG_IDENTIFIER_MAX:
        return collection
    head = raw[:40].decode("utf-8", "ignore").rstrip("_")
    return f"{head}_{hashlib.sha256(raw).hexdigest()[:16]}"


def _index_name(table: str, suffix: str) -> str:
    """The name of one of ``table``'s indexes.

    For a table name of at most 61 bytes this is ``<table>_<suffix>`` cut to 63
    bytes — the name Postgres has always given these indexes, so existing
    indexes keep their names. The cut keeps at least ``_e`` / ``_s`` after the
    table name, so the two indexes of a table differ, and differ from the
    table. A 62- or 63-byte table name would leave both cut names equal (or
    equal to the table name), so ``CREATE INDEX IF NOT EXISTS`` skipped an
    index; such a table gets hashed index names instead (:func:`pg_table_name`
    of the full ``<table>_<suffix>``)."""
    full = f"{table}_{suffix}"
    if len(table.encode("utf-8")) <= _PG_IDENTIFIER_MAX - 2:
        return full.encode("utf-8")[:_PG_IDENTIFIER_MAX].decode("utf-8", "ignore")
    return pg_table_name(full)


def _create_table_sql(table: str, dim: int) -> str:
    return f"""
        CREATE TABLE IF NOT EXISTS {table} (
            id        TEXT PRIMARY KEY,
            embedding vector({dim}) NOT NULL,
            source    TEXT,
            payload   JSONB NOT NULL DEFAULT '{{}}'::jsonb
        )
    """


def _create_index_sql(table: str) -> str:
    # HNSW > IVFFlat for typical recall/QPS at small-to-mid scale. Use
    # cosine ops to match the embedder (we normalize on the embedder side).
    return f"""
        CREATE INDEX IF NOT EXISTS {_index_name(table, "embedding_idx")}
        ON {table}
        USING hnsw (embedding vector_cosine_ops)
    """


def _create_source_index_sql(table: str) -> str:
    # Backs delete_by_source / has_source / list_sources cheaply.
    return f"CREATE INDEX IF NOT EXISTS {_index_name(table, 'source_idx')} ON {table}(source)"


def _upsert_sql(table: str) -> str:
    return f"""
        INSERT INTO {table} (id, embedding, source, payload)
        VALUES ($1, $2, $3, $4::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET embedding = EXCLUDED.embedding,
            source    = EXCLUDED.source,
            payload   = EXCLUDED.payload
    """


def _search_sql(table: str, filter_sql: str = "") -> str:
    # ``<=>`` is cosine distance (smaller is closer). Score = 1 - distance
    # gives a cosine-similarity-shaped value in [0, 1] for parity with Qdrant.
    where = f"WHERE {filter_sql}" if filter_sql else ""
    return f"""
        SELECT id, source, payload, 1 - (embedding <=> $1) AS score
        FROM {table}
        {where}
        ORDER BY embedding <=> $1
        LIMIT $2
    """


def _delete_by_source_sql(table: str) -> str:
    return f"DELETE FROM {table} WHERE source = $1"


def _has_source_sql(table: str) -> str:
    return f"SELECT 1 FROM {table} WHERE source = $1 LIMIT 1"


def _list_sources_sql(table: str) -> str:
    return f"""
        SELECT source,
               COALESCE(payload->>'source_type', 'unknown') AS source_type,
               COUNT(*) AS chunk_count
        FROM {table}
        WHERE source IS NOT NULL
        GROUP BY source, source_type
        LIMIT $1
    """


# ─── Driver ─────────────────────────────────────────────────────────────


class PGVectorStore(VectorStore):
    """Dense-only pgvector driver.

    A lazy asyncpg connection pool is created on first use. Each connection
    has the pgvector type codecs registered so ``vector`` columns marshal
    transparently to/from Python lists of floats.

    All abstract methods raise :class:`RuntimeError` with a clear message
    if the ``rag-pgvector`` optional dep group isn't installed.
    """

    def __init__(self) -> None:
        self._pool = None
        self._collection: Optional[str] = None
        self._initialized_for_dim: dict[str, int] = {}
        self._extension_ready = False

    # ── Lazy pool ──────────────────────────────────────────────────────

    @staticmethod
    def _safe_collection(collection: str) -> str:
        """Reject names that aren't valid pg identifiers.

        Collection name becomes a table name — interpolated, not parameterized
        (pg won't bind identifiers). This validator is the only thing
        standing between user input and a SQL-injection footgun.
        """
        if not collection.replace("_", "").isalnum():
            raise ValueError(
                f"invalid collection name: {collection!r} "
                "(must be alphanumeric + underscores)"
            )
        return collection

    async def _get_pool(self):
        if self._pool is not None:
            return self._pool
        _require_asyncpg()
        import asyncpg  # noqa: PLC0415
        from pgvector.asyncpg import register_vector  # noqa: PLC0415

        from cairn.core.provider_settings import get_settings
        dsn = get_settings().pgvector_dsn
        if not dsn:
            raise RuntimeError(
                "PGVECTOR_DSN is not set. "
                "Set it to a postgres:// URL pointing at a database with "
                "the pgvector extension enabled."
            )

        from cairn.core.pg_schema_init import schema_init_lock  # noqa: PLC0415 — core ↔ services stay function-local

        async def _init_conn(conn):
            # Once per store, on the connection being initialised and under the
            # fleet-wide schema lock: two replicas on a fresh database otherwise race
            # CREATE EXTENSION (#2551). ``register_vector`` needs the type to exist.
            if not self._extension_ready:
                async with schema_init_lock(conn):
                    await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
                self._extension_ready = True
            await register_vector(conn)

        self._pool = await asyncpg.create_pool(
            dsn,
            min_size=1,
            max_size=8,
            init=_init_conn,
        )
        return self._pool

    @property
    def _coll(self) -> str:
        if self._collection:
            return self._collection
        from cairn.core.provider_settings import get_settings
        return pg_table_name(self._safe_collection(get_settings().rag_collection_name))

    # ── Contract ───────────────────────────────────────────────────────

    async def initialize(self, collection: str, dim: int) -> None:
        table = pg_table_name(self._safe_collection(collection))
        if self._initialized_for_dim.get(table) == dim:
            # already created by this store: callers run initialize() per query,
            # save and ingest, so the DDL and the fleet-wide lock run once (#2551)
            self._collection = table
            return
        pool = await self._get_pool()
        from cairn.core.pg_schema_init import schema_init_lock  # noqa: PLC0415 — core ↔ services stay function-local

        # the collection's DDL runs on the lock-holding connection: replicas
        # serialise it instead of racing CREATE TABLE IF NOT EXISTS (#2551)
        async with pool.acquire() as conn, schema_init_lock(conn):
            await conn.execute(_create_table_sql(table, dim))
            await conn.execute(_create_index_sql(table))
            await conn.execute(_create_source_index_sql(table))
        self._collection = table
        self._initialized_for_dim[table] = dim
        logger.info("pgvector collection ready: %s (dim=%d)", table, dim)

    async def upsert(
        self,
        ids: list[str],
        dense: list[list[float]],
        sparse: list[dict] | None,
        payloads: list[dict],
    ) -> int:
        # Sparse is accepted to match the contract; pgvector dense-only
        # ignores it. Hybrid will land as a v2 (tsvector FTS).
        if sparse is not None:
            logger.debug(
                "pgvector backend ignores sparse vectors (dense-only); "
                "hybrid requires tsvector FTS — out of scope for the v1 driver"
            )
        if not ids:
            return 0
        pool = await self._get_pool()
        sql = _upsert_sql(self._coll)
        rows = [
            (pid, dvec, payload.get("source"), json.dumps(payload))
            for pid, dvec, payload in zip(ids, dense, payloads)
        ]
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(sql, rows)
        return len(rows)

    async def search(
        self,
        dense: list[float],
        sparse: dict | None = None,
        top_k: int = 5,
        filters: dict | None = None,
    ) -> list[dict]:
        if sparse is not None:
            logger.debug("pgvector search ignores sparse vector (dense-only)")
        pool = await self._get_pool()

        # Build filters into the SQL. We use JSONB containment (`@>`) which
        # matches Qdrant's "field equals any-of" semantics for simple cases.
        # Filter values are still parameterized — only column reference is
        # interpolated, and we restrict that to source for now.
        filter_sql = ""
        extra_params: list[Any] = []
        if filters:
            clauses = []
            for key, vals in filters.items():
                if not isinstance(vals, list):
                    vals = [vals]
                # JSONB containment: payload @> '{key: val}' for each val
                placeholders = []
                for v in vals:
                    extra_params.append(json.dumps({key: v}))
                    placeholders.append(f"payload @> ${len(extra_params) + 2}::jsonb")
                clauses.append("(" + " OR ".join(placeholders) + ")")
            filter_sql = " AND ".join(clauses)

        sql = _search_sql(self._coll, filter_sql)
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, dense, top_k, *extra_params)

        out: list[dict] = []
        for r in rows:
            payload = r["payload"]
            # asyncpg returns JSONB as dict already when type codec is set,
            # but be defensive — some configs return strings.
            if isinstance(payload, str):
                payload = json.loads(payload)
            out.append({"score": round(float(r["score"]), 4), **payload})
        return out

    async def delete_by_source(self, source: str) -> int:
        pool = await self._get_pool()
        sql = _delete_by_source_sql(self._coll)
        async with pool.acquire() as conn:
            result = await conn.execute(sql, source)
        # asyncpg returns a status string like "DELETE 12"; parse the count.
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0

    async def list_sources(self, limit: int = 50) -> list[dict]:
        pool = await self._get_pool()
        sql = _list_sources_sql(self._coll)
        async with pool.acquire() as conn:
            rows = await conn.fetch(sql, limit)
        return [
            {
                "source": r["source"],
                "source_type": r["source_type"],
                "chunk_count": int(r["chunk_count"]),
            }
            for r in rows
        ]

    async def has_source(self, source: str) -> bool:
        pool = await self._get_pool()
        sql = _has_source_sql(self._coll)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(sql, source)
        return row is not None

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
