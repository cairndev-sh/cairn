"""Incremental corpus indexer — orchestration over the existing pipeline (ADR-0023).

This is the lifecycle layer the ADR identified as the genuine gap: it does NOT
re-implement parsing/chunking/embedding/upsert (all in ``ingest.py``). It scans
a corpus directory, asks ``plan()`` what changed, and drives the existing
``ingest_document`` for the changed set — recording outcomes in the catalog so
the next run skips unchanged files.

Fixes the standing correctness bug: ``ingest_document`` skips any source it has
already embedded (``has_source``), so a *changed* file is never re-indexed on
its own. Here we ``delete_by_source`` before re-ingesting a changed file, so the
new content actually lands.

Idempotency-first (§24.23.2): a crashed run re-runs cheaply — completed
documents are ``indexed`` in the catalog with matching hashes and get skipped.
No workflow engine. Failures are isolated per-document into a structured partial
result; transient embedding/store errors are the pipeline's concern.
"""
from __future__ import annotations

import logging
import os
import re

from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso
from cairn.core.backend_connections import apply_backend_connection
from cairn.core.storage.rag_catalog import (
    CatalogEntry,
    RagCatalogStore,
    default_rag_catalog,
)
from cairn.core.workspace import workspace_collection
from cairn.services.rag.idempotency import content_hash, document_id
from cairn.services.rag.ingest import ingest_document
from cairn.services.rag.plan import RagIndexPlan, plan, resolved_build_fingerprint

logger = logging.getLogger(__name__)

_STORED_RE = re.compile(r"Stored (\d+) chunks")


class IndexResult(BaseModel):
    """Normalized job outcome (§24.23.3 status model + structured partial)."""

    corpus: str
    status: str  # completed | partially_completed | failed
    documents_indexed: int = 0
    documents_skipped: int = 0
    documents_deleted: int = 0
    chunks_indexed: int = 0
    failed_documents: list[dict] = Field(default_factory=list)


def _collection_for(corpus: str, override: str | None) -> str:
    # Corpus paths (e.g. "rag/runbooks") → a flat, backend-safe collection name.
    base = override or (re.sub(r"[^A-Za-z0-9_]+", "_", corpus).strip("_") or "default")
    # Namespace the vectors by workspace (#237) — default workspace unchanged.
    return workspace_collection(base)


def scan_corpus(root: str) -> dict[str, str]:
    """Map every file under ``root`` → its content hash. Skips dotfiles/dotdirs."""
    live: dict[str, str] = {}
    if not os.path.isdir(root):
        logger.warning("[index] corpus root not found: %s", root)
        return live
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for name in filenames:
            if name.startswith("."):
                continue
            path = os.path.join(dirpath, name)
            try:
                live[path] = content_hash(path)
            except OSError as e:
                logger.warning("[index] unreadable file skipped: %s (%s)", path, e)
    return live


async def index_corpus(
    corpus: str,
    root: str,
    *,
    catalog: RagCatalogStore | None = None,
    collection: str | None = None,
) -> IndexResult:
    """Bring ``corpus`` (files under ``root``) up to date in the vector store.

    File-granularity incremental (ADR-0023 Fork C): changed → re-embed whole
    file; removed → tombstone; unchanged → skip.
    """
    catalog = catalog or default_rag_catalog()
    coll = _collection_for(corpus, collection)
    # Workspace-scoped catalog/document-id key (#237): the catalog tracks
    # incremental-index state keyed by (corpus, source) → document_id, so two
    # workspaces indexing the same path must not collide on the PK or skip each
    # other's docs. The user-facing IndexResult.corpus stays the original name.
    cat_corpus = workspace_collection(corpus)
    build_fp = resolved_build_fingerprint()

    live = scan_corpus(root)
    entries = await catalog.list_by_corpus(cat_corpus)
    p: RagIndexPlan = plan(cat_corpus, live, entries, build_fp)
    logger.info(
        "[index] %s: %d to index, %d skip, %d delete",
        corpus, len(p.to_index), len(p.to_skip), len(p.to_delete),
    )

    result = IndexResult(
        corpus=corpus, status="completed", documents_skipped=len(p.to_skip),
    )

    # Vector store handle for tombstoning + forced re-embed. Lazy so a no-op
    # plan needs no store/embedder init.
    # The store is process-wide and shared by every collection: each store call
    # selects `coll` under its lock (`on_collection`), one document at a time;
    # parsing, embedding and catalog writes run outside it.
    store = dim = None
    if p.to_index or p.to_delete:
        from cairn.services.rag.embeddings import get_embedder
        from cairn.services.rag.stores import SHARED_STORE_LOCK, get_vector_store, on_collection

        await apply_backend_connection("rag_vector_store")  # #336: bound-connection creds → env
        store = get_vector_store()
        dim = get_embedder().dim
        async with on_collection(store, coll, dim, SHARED_STORE_LOCK):
            pass  # the collection exists before anything is removed or indexed

    # Tombstone removed sources.
    for source in p.to_delete:
        try:
            async with on_collection(store, coll, dim, SHARED_STORE_LOCK):
                await store.delete_by_source(source)
            await catalog.tombstone(document_id(cat_corpus, source))
            result.documents_deleted += 1
        except Exception as e:  # noqa: BLE001
            result.failed_documents.append({"source": source, "op": "delete", "error": str(e)})

    # (Re)index changed/new sources. delete_by_source first so the pipeline's
    # has_source skip doesn't block a re-embed.
    for source in p.to_index:
        did = document_id(cat_corpus, source)
        try:
            async with on_collection(store, coll, dim, SHARED_STORE_LOCK):
                await store.delete_by_source(source)
            outcome = await ingest_document(source, coll)  # takes the lock per store call
            m = _STORED_RE.search(outcome or "")
            chunks = int(m.group(1)) if m else 0
            result.chunks_indexed += chunks
            result.documents_indexed += 1
            await catalog.upsert(CatalogEntry(
                document_id=did, corpus=cat_corpus, source=source,
                content_hash=live[source], build_fingerprint=build_fp,
                chunk_count=chunks, status="indexed", indexed_at=utc_now_iso(),
            ))
        except Exception as e:  # noqa: BLE001 — isolate per-document failures
            logger.warning("[index] failed: %s (%s)", source, e)
            result.failed_documents.append({"source": source, "op": "index", "error": str(e)})
            await catalog.upsert(CatalogEntry(
                document_id=did, corpus=cat_corpus, source=source,
                content_hash=live.get(source, ""), build_fingerprint=build_fp,
                status="failed", indexed_at=utc_now_iso(), error=str(e),
            ))

    if result.failed_documents:
        result.status = "partially_completed" if result.documents_indexed else "failed"
    return result
