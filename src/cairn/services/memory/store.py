"""Generic episodic memory store.

Vendor-agnostic engine service: a vector-store-backed store (default sqlite-vec;
RAG_VECTOR_STORE selects the backend) for ``type=episodic_memory`` records with
cosine-similarity dedup. Packs supply the *schema* of what goes in a record;
this module owns the read/write/dedup machinery.

Public API:
  - ``save_episodic(payload, embed_text, dedup_id=None)`` — upsert with dedup
  - ``recall_episodic(query, top_k=5, filters=None)`` — semantic search
  - ``list_episodic(limit=20)`` — enumerate stored memories

Payload is an arbitrary dict; the store stamps ``type=episodic_memory`` and
``created_at`` / ``updated_at`` if absent. Dedup at ``>= DEDUP_THRESHOLD`` cosine
similarity merges payloads (string fields: longer wins; list fields: union).
"""
from __future__ import annotations

import hashlib
import logging
from functools import lru_cache
from typing import Any

from cairn.core._time import utc_now_iso
from cairn.core.backend_connections import apply_backend_connection
from cairn.core.provider_settings import get_settings
from cairn.core.workspace import workspace_collection
from cairn.services.rag.embeddings import get_embedder
from cairn.services.rag.stores import StoreLock, build_vector_store, on_collection

logger = logging.getLogger(__name__)

DEDUP_THRESHOLD = 0.85
_RECORD_TYPE = "episodic_memory"

#: Guards the process-wide memory store (:func:`_get_store`), which every
#: workspace's memory collection shares: each store call selects its collection
#: under this lock (:func:`on_collection`); embedding and merging run outside it.
_STORE_LOCK = StoreLock()


@lru_cache(maxsize=1)
def _get_store():
    """Separate store INSTANCE — its own collection (``memory_collection_name``,
    set in `_ensure_collection`) so episodic memory doesn't collide with RAG
    corpora. Backend follows RAG_VECTOR_STORE (default sqlite_vec); no longer
    qdrant-only (#76)."""
    return build_vector_store(get_settings().rag_vector_store or "sqlite_vec")


def _stable_id(seed: str) -> str:
    return hashlib.sha256(seed.lower().encode()).hexdigest()[:16]


def _generic_merge(existing: dict, new: dict) -> dict:
    """Merge two payloads: longer string wins, list fields unioned, new wins otherwise."""
    merged = dict(existing)
    for k, new_val in new.items():
        old_val = existing.get(k)
        if isinstance(new_val, str) and isinstance(old_val, str):
            if len(new_val) > len(old_val):
                merged[k] = new_val
        elif isinstance(new_val, list) and isinstance(old_val, list):
            merged[k] = sorted({str(x) for x in (old_val + new_val) if x})
        else:
            merged[k] = new_val
    merged["updated_at"] = utc_now_iso()
    merged["recall_count"] = existing.get("recall_count", 0)
    return merged


async def _ensure_collection() -> str:
    # #336: resolve profile-bound store connections (memory's own, and the RAG
    # store it inherits from by default) into the env BEFORE the cached store
    # is built; unbound → env fallback unchanged.
    await apply_backend_connection("rag_vector_store")
    await apply_backend_connection("memory_vector_store")
    s = get_settings()
    store = _get_store()
    embedder = get_embedder()
    # Namespace episodic memory by workspace (#237) so one tenant never recalls
    # another's memories. Default unchanged. The store is process-wide:
    # save/recall/list select the returned collection again for each store
    # call, under `_STORE_LOCK`.
    coll = workspace_collection(s.memory_collection_name)
    async with on_collection(store, coll, embedder.dim, _STORE_LOCK):
        pass
    return coll


async def save_episodic(
    payload: dict[str, Any],
    *,
    embed_text: str,
    dedup_id: str | None = None,
) -> dict[str, Any]:
    """Save an episodic record with cosine-similarity dedup.

    ``payload`` is stored verbatim plus ``type``, ``created_at``, ``updated_at``,
    ``recall_count``, ``source``, ``source_type`` if absent.
    ``embed_text`` is what gets embedded for similarity search and dedup.
    ``dedup_id`` overrides the auto-derived stable id (default: hash of embed_text).

    Returns ``{action: "created"|"merged", id: str, detail: str}``.
    """
    coll = await _ensure_collection()
    store = _get_store()
    embedder = get_embedder()

    dense = await embedder.embed_one(embed_text)
    sparse = await embedder.embed_sparse_one(embed_text)

    async with on_collection(store, coll, embedder.dim, _STORE_LOCK):
        existing = await store.search(dense, sparse, top_k=3)

    now = utc_now_iso()
    full_payload = {
        "type": _RECORD_TYPE,
        "created_at": now,
        "updated_at": now,
        "recall_count": 0,
        "source": payload.get("source") or f"memory:{dedup_id or _stable_id(embed_text)}",
        "source_type": _RECORD_TYPE,
        **payload,
    }

    # Dedup: if an existing hit is similar enough, merge instead of insert.
    for hit in existing:
        if hit.get("type") != _RECORD_TYPE:
            continue
        if hit.get("score", 0) < DEDUP_THRESHOLD:
            continue
        merged = _generic_merge(hit, full_payload)
        mid = dedup_id or _stable_id(embed_text)
        m_dense = await embedder.embed_one(embed_text)
        m_sparse = await embedder.embed_sparse_one(embed_text)
        async with on_collection(store, coll, embedder.dim, _STORE_LOCK):
            await store.upsert([mid], [m_dense], [m_sparse] if m_sparse else None, [merged])
        logger.info("Merged episodic memory %s (score=%.3f)", mid, hit.get("score", 0))
        return {"action": "merged", "id": mid, "detail": f"Merged (similarity={hit['score']:.2f})"}

    mid = dedup_id or _stable_id(embed_text)
    async with on_collection(store, coll, embedder.dim, _STORE_LOCK):
        await store.upsert([mid], [dense], [sparse] if sparse else None, [full_payload])
    logger.info("Created episodic memory %s", mid)
    return {"action": "created", "id": mid, "detail": "New memory stored"}


async def recall_episodic(
    query: str,
    *,
    top_k: int = 5,
    filters: dict | None = None,
) -> list[dict[str, Any]]:
    """Semantic search over episodic memory."""
    coll = await _ensure_collection()
    store = _get_store()
    embedder = get_embedder()

    dense = await embedder.embed_one(query)
    sparse = await embedder.embed_sparse_one(query)

    async with on_collection(store, coll, embedder.dim, _STORE_LOCK):
        results = await store.search(dense, sparse, top_k=top_k * 2, filters=filters)
    return [hit for hit in results if hit.get("type") == _RECORD_TYPE][:top_k]


async def list_episodic(limit: int = 20) -> list[dict[str, Any]]:
    """List stored episodic memories (via the backend's ``list_sources``)."""
    coll = await _ensure_collection()
    store = _get_store()
    async with on_collection(store, coll, get_embedder().dim, _STORE_LOCK):
        docs = await store.list_sources(limit=limit * 5)
    memories = [d for d in docs if d.get("source_type") == _RECORD_TYPE]
    return memories[:limit]


def reset_memory_store() -> None:
    """Drop the cached memory store (tests, or after backend env changes)."""
    _get_store.cache_clear()
