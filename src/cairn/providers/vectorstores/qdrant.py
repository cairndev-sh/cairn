"""Qdrant vector store — hybrid dense + sparse with RRF fusion."""
from __future__ import annotations

import inspect
import logging
from typing import Any

from cairn.core.provider_settings import get_settings
from cairn.services.rag.stores.base import VectorStore

logger = logging.getLogger(__name__)

DENSE_VEC = "dense"
SPARSE_VEC = "sparse"


async def _await_maybe(result: Any) -> Any:
    """Await if coroutine (server mode); return directly for file mode."""
    if inspect.isawaitable(result):
        return await result
    return result


class QdrantStore(VectorStore):

    def __init__(self) -> None:
        self._client = None
        self._collection: str | None = None
        #: collections this store already checked or created: callers select the
        #: collection before every store call, so the check runs once per process
        self._ready: set[str] = set()

    def _get_client(self):
        if self._client is None:
            from qdrant_client import AsyncQdrantClient
            s = get_settings()
            self._client = AsyncQdrantClient(url=s.qdrant_url)
        return self._client

    @property
    def _coll(self) -> str:
        if self._collection:
            return self._collection
        return get_settings().rag_collection_name

    async def initialize(self, collection: str, dim: int) -> None:
        from qdrant_client.models import (
            Distance,
            Modifier,
            SparseVectorParams,
            VectorParams,
        )
        if collection in self._ready:
            self._collection = collection
            return
        client = self._get_client()
        collections_response = await _await_maybe(client.get_collections())
        existing = {c.name for c in collections_response.collections}

        self._collection = collection

        if collection not in existing:
            await _await_maybe(client.create_collection(
                collection_name=collection,
                vectors_config={
                    DENSE_VEC: VectorParams(size=dim, distance=Distance.COSINE),
                },
                sparse_vectors_config={
                    SPARSE_VEC: SparseVectorParams(modifier=Modifier.IDF),
                },
            ))
            logger.info("Created hybrid collection: %s", collection)
        else:
            logger.info("Using existing collection: %s", collection)
        self._ready.add(collection)

    async def upsert(
        self,
        ids: list[str],
        dense: list[list[float]],
        sparse: list[dict] | None,
        payloads: list[dict],
    ) -> int:
        from qdrant_client.models import PointStruct, SparseVector
        client = self._get_client()

        points = []
        for i, (pid, d_vec, payload) in enumerate(zip(ids, dense, payloads)):
            vector: dict = {DENSE_VEC: d_vec}
            if sparse and i < len(sparse):
                sv = sparse[i]
                vector[SPARSE_VEC] = SparseVector(
                    indices=sv["indices"], values=sv["values"],
                )
            points.append(PointStruct(id=pid, vector=vector, payload=payload))

        await _await_maybe(client.upsert(
            collection_name=self._coll, points=points,
        ))
        return len(points)

    async def search(
        self,
        dense: list[float],
        sparse: dict | None = None,
        top_k: int = 5,
        filters: dict | None = None,
    ) -> list[dict]:
        from qdrant_client.models import (
            FieldCondition,
            Filter,
            Fusion,
            FusionQuery,
            MatchAny,
            Prefetch,
            SparseVector,
        )
        client = self._get_client()

        must_conditions = []
        if filters:
            for key, vals in filters.items():
                must_conditions.append(
                    FieldCondition(key=key, match=MatchAny(any=vals))
                )
        query_filter = Filter(must=must_conditions) if must_conditions else None

        prefetch = [
            Prefetch(query=dense, using=DENSE_VEC, limit=top_k * 3, filter=query_filter),
        ]
        if sparse:
            prefetch.append(Prefetch(
                query=SparseVector(indices=sparse["indices"], values=sparse["values"]),
                using=SPARSE_VEC,
                limit=top_k * 3,
                filter=query_filter,
            ))

        results = await _await_maybe(client.query_points(
            collection_name=self._coll,
            prefetch=prefetch,
            query=FusionQuery(fusion=Fusion.RRF),
            limit=top_k * 2,
            with_payload=True,
        ))

        return [
            {"score": round(p.score, 4), **p.payload}
            for p in results.points
        ]

    async def delete_by_source(self, source: str) -> int:
        from qdrant_client.models import FieldCondition, Filter, MatchValue
        client = self._get_client()
        await _await_maybe(client.delete(
            collection_name=self._coll,
            points_selector=Filter(
                must=[FieldCondition(key="source", match=MatchValue(value=source))]
            ),
        ))
        return -1  # Qdrant delete doesn't return count

    async def list_sources(self, limit: int = 50) -> list[dict]:
        client = self._get_client()
        results, _ = await _await_maybe(client.scroll(
            collection_name=self._coll,
            limit=min(limit * 10, 500),
            with_payload=["source", "source_type"],
            with_vectors=False,
        ))
        sources: dict[str, dict] = {}
        for point in results:
            src = point.payload.get("source", "unknown")
            if src not in sources:
                sources[src] = {
                    "source": src,
                    "source_type": point.payload.get("source_type", "unknown"),
                    "chunk_count": 0,
                }
            sources[src]["chunk_count"] += 1
        return list(sources.values())[:limit]

    async def has_source(self, source: str) -> bool:
        from qdrant_client.models import FieldCondition, Filter, MatchValue
        client = self._get_client()
        try:
            results, _ = await _await_maybe(client.scroll(
                collection_name=self._coll,
                scroll_filter=Filter(
                    must=[FieldCondition(key="source", match=MatchValue(value=source))]
                ),
                limit=1,
                with_payload=False,
                with_vectors=False,
            ))
            return len(results) > 0
        except Exception:
            return False

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None
        self._ready.clear()
