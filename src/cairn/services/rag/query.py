"""Query workflow (LlamaIndex Workflow — adapted from multimodel-rag).

Stage 1  Embed query     Dense + sparse in parallel via pluggable EmbeddingProvider
Stage 2  Hybrid search   Dense ANN + Sparse BM25 → RRF fusion via pluggable VectorStore
Stage 3  Rerank          top-K in RRF order (no model reranker)
Stage 4  Synthesize      LLM generates grounded answer with citations
"""
from __future__ import annotations

import logging
from contextlib import nullcontext
from pathlib import Path
from typing import Any, List, Optional

from llama_index.core.workflow import Event, StartEvent, StopEvent, Workflow, step

from cairn.core.provider_settings import get_settings
from cairn.core.backend_connections import apply_backend_connection
from cairn.core.workspace import workspace_collection
from cairn.services.rag.embeddings import EmbeddingProvider, get_embedder
from cairn.services.rag.schemas import QueryResponse, SourceNode
from cairn.services.rag.stores import SHARED_STORE_LOCK, get_vector_store, on_collection
from cairn.services.rag.stores.base import VectorStore

logger = logging.getLogger(__name__)

_DEFAULT_SYNTHESIS_PROMPT = """\
You are a precise technical assistant for operational runbooks and documentation.

Instructions:
- Answer using ONLY the provided context. Never hallucinate.
- Cite sources inline as [chunk_type | filename | page].
- If context is insufficient, state exactly what is missing.

=== CONTEXT ===
{context}

=== QUESTION ===
{question}

=== ANSWER ==="""


# ── Events ────────────────────────────────────────────────────────────────────


class EmbeddedQueryEvent(Event):
    query_str: str
    dense_emb: List[float]
    sparse_vec: Optional[dict]
    top_k: int


class RetrievedEvent(Event):
    query_str: str
    candidates: List[dict]
    top_k: int


class RerankEvent(Event):
    query_str: str
    context_str: str
    sources: List[dict]


# ── Workflow ──────────────────────────────────────────────────────────────────


class QueryWorkflow(Workflow):

    def __init__(
        self,
        store: VectorStore,
        embedder: EmbeddingProvider,
        *args,
        synthesis_prompt: str | None = None,
        job_tag: str = "rag",
        collection: str | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.store = store
        self.embedder = embedder
        #: When set, the retrieval selects this collection under the shared store's
        #: lock (:func:`on_collection`); ``None`` = the caller owns the store.
        self.collection = collection
        self.synthesis_prompt = synthesis_prompt or _DEFAULT_SYNTHESIS_PROMPT
        self.job_tag = job_tag

    # ── Stage 1: embed query ──────────────────────────────────────────────────

    @step
    async def embed_query(self, ev: StartEvent) -> EmbeddedQueryEvent:
        import asyncio
        dense_emb, sparse_vec = await asyncio.gather(
            self.embedder.embed_one(ev.query),
            self.embedder.embed_sparse_one(ev.query),
        )
        return EmbeddedQueryEvent(
            query_str=ev.query,
            dense_emb=dense_emb,
            sparse_vec=sparse_vec,
            top_k=getattr(ev, "top_k", 3),
        )

    # ── Stage 2: hybrid search + RRF ─────────────────────────────────────────

    @step
    async def hybrid_retrieve(self, ev: EmbeddedQueryEvent) -> RetrievedEvent:
        # only select + retrieve hold the shared store's lock: the query is embedded
        # before it and the answer synthesised after it
        scope = (nullcontext(self.store) if self.collection is None
                 else on_collection(self.store, self.collection, self.embedder.dim, SHARED_STORE_LOCK))
        async with scope as store:
            candidates = await store.search(
                dense=ev.dense_emb,
                sparse=ev.sparse_vec,
                top_k=ev.top_k * 3,
            )
        logger.info("[retrieve] %d candidates after RRF", len(candidates))
        return RetrievedEvent(
            query_str=ev.query_str,
            candidates=candidates,
            top_k=ev.top_k,
        )

    # ── Stage 3: rerank (top-K in RRF order) ──────────────────────────────────

    @step
    async def rerank(self, ev: RetrievedEvent) -> RerankEvent:
        candidates = ev.candidates
        if not candidates:
            return RerankEvent(query_str=ev.query_str, context_str="", sources=[])

        top = candidates[:ev.top_k]

        context_parts = [
            f"[{c.get('chunk_type', 'TEXT').upper()} | "
            f"{Path(c.get('source', '?')).name} | "
            f"p.{c.get('page_label') or c.get('endpoint') or '?'}]\n{c.get('content', '')}"
            for c in top
        ]

        sources = [
            {
                "score": c.get("score", 0),
                "chunk_type": c.get("chunk_type", "text"),
                "source": c.get("source", ""),
                "page": c.get("page_label", ""),
                "preview": c.get("content", "")[:300],
            }
            for c in top
        ]

        logger.info("[rerank] %d chunks selected for synthesis", len(top))
        return RerankEvent(
            query_str=ev.query_str,
            context_str="\n\n---\n\n".join(context_parts),
            sources=sources,
        )

    # ── Stage 4: synthesize ───────────────────────────────────────────────────

    @step
    async def synthesize(self, ev: RerankEvent) -> StopEvent:
        if not ev.context_str:
            return StopEvent(result={"answer": "No relevant content found.", "sources": []})

        from cairn.core.llm.client import llm_complete
        s = get_settings()
        prompt = self.synthesis_prompt.format(context=ev.context_str, question=ev.query_str)

        response = await llm_complete(
            messages=[{"role": "user", "content": prompt}],
            model=s.llm_planner_model or "gpt-4o-mini",
            temperature=0.1,
            job_tag=self.job_tag,
        )
        answer = response.choices[0].message.content or "Unable to generate answer."

        sources = [
            SourceNode(
                score=s_dict.get("score", 0),
                chunk_type=s_dict.get("chunk_type", "text"),
                source=s_dict.get("source", ""),
                page=s_dict.get("page", ""),
                preview=s_dict.get("preview", ""),
            )
            for s_dict in ev.sources
        ]

        return StopEvent(result=QueryResponse(answer=answer, sources=sources).model_dump())


# ── Public API ────────────────────────────────────────────────────────────────


async def search(
    query: str,
    *,
    collection: str | None = None,
    top_k: int = 3,
    synthesis_prompt: str | None = None,
    job_tag: str = "rag",
) -> dict:
    """Hybrid-search a vector collection and synthesize a grounded answer.

    Domain-neutral RAG entry point. A pack supplies its own ``collection`` (and
    optionally a domain-flavored ``synthesis_prompt`` / ``job_tag``); the engine
    owns the embed → retrieve → rerank → synthesize machinery. ``collection``
    defaults to the configured ``rag_collection_name`` for back-compat.
    """
    s = get_settings()
    # #336: a profile-bound connection supplies the store's creds (env-injected
    # once) before the (cached) store is built; unbound → env fallback unchanged.
    await apply_backend_connection("rag_vector_store")
    store = get_vector_store()
    embedder = get_embedder()
    # Read from the same workspace-namespaced collection the indexer wrote (#237);
    # idempotent + default-workspace unchanged.
    collection = workspace_collection(collection or s.rag_collection_name)

    try:
        async with on_collection(store, collection, embedder.dim, SHARED_STORE_LOCK):
            pass  # the collection exists (or the error surfaces) before the query runs
    except Exception as exc:
        logger.warning("RAG store initialization failed: %s", exc)
        return {"answer": "Knowledge base is not available.", "sources": []}

    wf = QueryWorkflow(
        store, embedder,
        synthesis_prompt=synthesis_prompt,
        job_tag=job_tag,
        collection=collection,  # the store is shared: the retrieval selects it again
        timeout=120,
        verbose=False,
    )
    result = await wf.run(query=query, top_k=top_k)
    return result
