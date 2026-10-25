"""Pluggable vector store — select backend via RAG_VECTOR_STORE env var.

In-built core backends:
  sqlite_vec — DEFAULT. Dense-only embedded backend via the
               ``rag-sqlite-vec`` optional dep group. Delivers the §24.4
               Tier-1 "one SQLite file for everything in dev" promise —
               defaults to the same file as ``SqliteRunStore``. Sparse
               accepted but ignored (upstream-in-flight).
  pgvector   — prod. Dense-only Postgres backend via the ``rag-pgvector``
               optional dep group. Sparse accepted but ignored; hybrid
               requires a tsvector FTS extension (out of scope for v1).

External PROVIDERS — resolved from the ``cairn.vector_stores`` entry-point
group (core imports none); live in ``cairn/providers/vectorstores/`` (#76):
  qdrant     — hybrid dense + sparse search via Qdrant (``rag-qdrant``)
  pinecone   — stub (community contribution welcome)

To add a new backend: drop a driver file alongside qdrant.py /
pgvector.py / sqlite_vec.py, subclass :class:`VectorStore`, and add an
``if name == "..."`` branch below. The higher-level
:class:`HybridRagStore` in :mod:`cairn.core.storage.rag`
automatically picks it up.
"""
from __future__ import annotations

import logging
from functools import lru_cache

from cairn.services.rag.stores.base import VectorStore
from cairn.services.rag.stores.locking import LOCK_WAIT_S, StoreLock, VectorStoreBusy, on_collection

logger = logging.getLogger(__name__)

#: Guards the process-wide store of :func:`get_vector_store`, which every
#: workspace's collections share: callers select their collection and run the
#: store call under it (:func:`on_collection`).
SHARED_STORE_LOCK = StoreLock()

__all__ = [
    "LOCK_WAIT_S", "SHARED_STORE_LOCK", "StoreLock", "VectorStoreBusy",
    "build_vector_store", "get_vector_store", "on_collection", "reset_vector_store",
]


@lru_cache(maxsize=1)
def get_vector_store() -> VectorStore:
    """Process-wide RAG vector store (cached). Backend via ``RAG_VECTOR_STORE``
    (default: ``sqlite_vec``)."""
    from cairn.core.provider_settings import get_settings
    return build_vector_store(get_settings().rag_vector_store or "sqlite_vec")


def build_vector_store(name: str) -> VectorStore:
    """Construct a fresh vector store by backend name.

    In-built (core): ``sqlite_vec`` (DEFAULT — dev/local, `rag-sqlite-vec`) and
    ``pgvector`` (prod, `rag-pgvector`). Any other backend (``qdrant``,
    ``pinecone``, community) is an external PROVIDER resolved from the
    ``cairn.vector_stores`` entry-point group — core imports none of them.
    Unknown / not-installed → fall back to sqlite_vec.
    """
    name = (name or "sqlite_vec").lower()

    if name in ("sqlite_vec", "sqlite-vec", "sqlitevec"):
        from cairn.services.rag.stores.sqlite_vec import SqliteVecStore
        logger.info("RAG vector store: sqlite-vec (dense-only, embedded)")
        return SqliteVecStore()

    if name == "pgvector":
        from cairn.services.rag.stores.pgvector import PGVectorStore
        logger.info("RAG vector store: pgvector (dense-only)")
        return PGVectorStore()

    # ── external providers (qdrant, pinecone, community) ─────────────────
    # Resolved from the `cairn.vector_stores` entry-point group — core imports
    # NONE of them (#76). Providers live in cairn/providers/vectorstores/ with
    # their deps behind the rag-<name> extras.
    factory = _load_provider(name)
    if factory is not None:
        logger.info("RAG vector store: %s (provider)", name)
        return factory()

    logger.warning(
        "RAG_VECTOR_STORE=%r is not in-built and no `cairn.vector_stores` "
        "provider is installed — falling back to sqlite_vec (install the "
        "provider extra, e.g. rag-%s).", name, name,
    )
    from cairn.services.rag.stores.sqlite_vec import SqliteVecStore
    return SqliteVecStore()


def _load_provider(name: str):
    """Resolve an external vector-store provider from the ``cairn.vector_stores``
    entry-point group. Returns the VectorStore subclass (a zero-arg factory) or None.
    Core never imports the providers directly — they self-register (#76)."""
    from importlib.metadata import entry_points

    for ep in entry_points(group="cairn.vector_stores"):
        if ep.name.lower() == name:
            return ep.load()
    return None


def reset_vector_store() -> None:
    """Drop the cached vector store (tests, or after RAG_VECTOR_STORE changes)."""
    get_vector_store.cache_clear()
