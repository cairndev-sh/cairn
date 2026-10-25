"""Ingestion workflow (LlamaIndex Workflow — adapted from multimodel-rag).

source path / URL
  -> DocumentRouter         (per-type parser: trafilatura / MD / RST / OpenAPI)
  -> LlamaParse             (PDF only, when LLAMA_CLOUD_API_KEY is set; without it, and for
                             Office formats — no parser ships since rag/parse was retired,
                             #2413 — the document fails with UnparseableDocument, #2451)
  -> MarkdownNodeParser     (section-aware chunking for MD / PDF output)
  -> SentenceSplitter       (for HTML / text / RST)
  -> type detection         (text / table / equation / code / figure)
  -> dense embed            (via pluggable EmbeddingProvider)
  -> sparse BM25            (via pluggable EmbeddingProvider)
  -> vector store upsert    (via pluggable VectorStore)
"""
from __future__ import annotations

import asyncio
import logging
import re
import uuid
from contextlib import nullcontext
from typing import Any, List, Optional

from llama_index.core.schema import Document, TextNode
from llama_index.core.workflow import Event, StartEvent, StopEvent, Workflow, step

from cairn.core.provider_settings import get_settings
from cairn.core.backend_connections import apply_backend_connection
from cairn.core.workspace import workspace_collection
from cairn.services.rag.embeddings import EmbeddingProvider, get_embedder
from cairn.services.rag.readers import read_source
from cairn.services.rag.stores import SHARED_STORE_LOCK, get_vector_store, on_collection
from cairn.services.rag.stores.base import VectorStore

logger = logging.getLogger(__name__)


# ── Events ────────────────────────────────────────────────────────────────────


class ParsedEvent(Event):
    docs: List[Any]
    source: str
    source_type: str


class ChunkedEvent(Event):
    nodes: List[Any]
    source: str


class UnparseableDocument(RuntimeError):
    """A source no installed parser reads (#2451). Raised, not skipped, so the indexer records
    the file in ``failed_documents`` with catalog status ``failed`` — visible, and retried on the
    next run — instead of counting it indexed with no chunks."""


# ── Workflow ──────────────────────────────────────────────────────────────────


class IngestWorkflow(Workflow):

    def __init__(
        self,
        store: VectorStore,
        embedder: EmbeddingProvider,
        *args,
        collection: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.store = store
        self.embedder = embedder
        #: When set, each store call selects this collection under the shared
        #: store's lock (:func:`on_collection`); ``None`` = the caller owns the store.
        self.collection = collection
        s = get_settings()
        self.llama_client = None
        if s.llama_cloud_api_key:
            try:
                from llama_cloud import LlamaCloud
                self.llama_client = LlamaCloud(api_key=s.llama_cloud_api_key)
            except ImportError:
                pass

    def _on_collection(self):
        if self.collection is None:
            return nullcontext(self.store)
        return on_collection(self.store, self.collection, self.embedder.dim, SHARED_STORE_LOCK)

    # ── Step 1: Route + parse ─────────────────────────────────────────────────

    #: Office formats: no parser ships for them since the ``rag/parse`` operator and its
    #: ``operator-rag-parse`` wheel were retired (#2413). Read as text they would embed zip
    #: bytes, so they fail with the reason instead (#2451).
    _OFFICE_EXTS = (".docx", ".pptx", ".xlsx")

    @step
    async def parse(self, ev: StartEvent) -> ParsedEvent | StopEvent:
        source: str = ev.file_path

        async with self._on_collection() as store:
            seen = await store.has_source(source)
        if seen:
            logger.info("[parse] skipping (already embedded): %s", source)
            return StopEvent(result=f"Skipped {source} — already embedded")

        logger.info("[parse] %s", source)

        if source.lower().endswith(self._OFFICE_EXTS):
            raise UnparseableDocument(f"{source}: no Office parser ships since rag/parse was retired "
                                      "(#2413); convert it to PDF or Markdown")

        docs, source_type = await read_source(source)

        if source_type == "pdf" and not docs:
            # The built-in readers do not parse PDF: LlamaParse does, when a key is set (#2451).
            if not self.llama_client:
                raise UnparseableDocument(f"{source}: PDF parsing needs LlamaParse — set "
                                          "LLAMA_CLOUD_API_KEY (the cairndev[rag] extra installs llama-cloud)")
            docs = await asyncio.to_thread(_parse_pdf, self.llama_client, source)
            source_type = "pdf"

        if not docs:
            logger.warning("[parse] no content extracted from %s", source)
            return StopEvent(result=f"No content extracted from {source}")

        return ParsedEvent(docs=docs, source=source, source_type=source_type)

    # ── Step 2: Chunk ─────────────────────────────────────────────────────────

    @step
    async def chunk(self, ev: ParsedEvent) -> ChunkedEvent:
        if ev.source_type in ("pdf", "markdown"):
            from llama_index.core.node_parser import MarkdownNodeParser
            parser = MarkdownNodeParser()
        elif ev.source_type in ("openapi", "json", "yaml", "csv", "notebook"):
            parser = None
        else:
            from llama_index.core.node_parser import SentenceSplitter
            parser = SentenceSplitter(chunk_size=512, chunk_overlap=64)

        if parser is not None:
            raw_nodes = parser.get_nodes_from_documents(ev.docs)
        else:
            raw_nodes = [TextNode(text=doc.text, metadata={**doc.metadata}) for doc in ev.docs]

        text_nodes: list = []
        for node in raw_nodes:
            content = node.get_content().strip()
            if not content:
                continue
            chunk_type = node.metadata.get("chunk_type") or _detect_type(content)
            node.metadata.update({
                "chunk_type": chunk_type,
                "source": ev.source,
                "source_type": ev.source_type,
            })
            text_nodes.append(node)

        logger.info("[chunk] %d nodes from %s", len(text_nodes), ev.source)
        return ChunkedEvent(nodes=text_nodes, source=ev.source)

    # ── Step 3: Dual embed + upsert ──────────────────────────────────────────

    @step
    async def embed_and_store(self, ev: ChunkedEvent) -> StopEvent:
        nodes = ev.nodes
        batch_size = 50
        stored = 0

        for i in range(0, len(nodes), batch_size):
            batch = nodes[i: i + batch_size]
            texts = [n.get_content() for n in batch]

            dense_embs, sparse_vecs = await asyncio.gather(
                self.embedder.embed(texts),
                self.embedder.embed_sparse(texts),
            )

            ids = [_stable_uuid(n.node_id) for n in batch]
            payloads = [
                {
                    "content": n.get_content(),
                    "chunk_type": n.metadata.get("chunk_type", "text"),
                    "source": n.metadata.get("source", ""),
                    "source_type": n.metadata.get("source_type", "unknown"),
                    "page_label": str(n.metadata.get("page_label", "")),
                    "header_path": n.metadata.get("header_path", ""),
                    "domain": n.metadata.get("domain", ""),
                    "endpoint": n.metadata.get("endpoint", ""),
                }
                for n in batch
            ]

            async with self._on_collection() as store:  # embedding stays outside the lock
                count = await store.upsert(ids, dense_embs, sparse_vecs, payloads)
            stored += count
            logger.info("[store] %d/%d", stored, len(nodes))

        return StopEvent(result=f"Stored {stored} chunks from {ev.source}")


# ── Public API ────────────────────────────────────────────────────────────────


async def ingest_document(file_path: str, collection: str | None = None) -> str:
    """Ingest a document into a vector collection.

    Domain-neutral. ``collection`` defaults to the configured
    ``rag_collection_name`` for back-compat; a pack passes its own collection.
    """
    s = get_settings()
    await apply_backend_connection("rag_vector_store")  # #336: bound-connection creds → env
    store = get_vector_store()
    embedder = get_embedder()
    # Namespace by workspace (#237); idempotent so an already-scoped collection
    # passed from the indexer is not double-prefixed. Default workspace unchanged.
    coll = workspace_collection(collection or s.rag_collection_name)
    async with on_collection(store, coll, embedder.dim, SHARED_STORE_LOCK):
        pass  # the collection exists before parsing starts, as before
    # The store is process-wide: each of the workflow's store calls selects `coll`
    # under the store's lock; parsing and embedding run outside it.
    wf = IngestWorkflow(store, embedder, collection=coll, timeout=300, verbose=False)
    result = await wf.run(file_path=file_path)
    return str(result)


# ── Helpers ───────────────────────────────────────────────────────────────────


def _parse_pdf(client, source: str) -> List[Document]:
    file = client.files.create(file=open(source, "rb"), purpose="parse")
    result = client.parsing.parse(
        file_id=file.id, tier="agentic", version="latest", expand=["markdown"],
    )
    docs = []
    for i, page in enumerate(result.markdown.pages or []):
        docs.append(Document(
            text=page.markdown or "",
            metadata={"source": source, "page_label": str(i + 1)},
        ))
    return docs


def _detect_type(content: str) -> str:
    if re.search(r"^```[\w]*\n", content, re.MULTILINE):
        return "code"
    if re.search(r"\|.+\|", content) and re.search(r"\|[\s\-:]+\|", content):
        return "table"
    if "$$" in content or r"\begin{equation}" in content:
        return "equation"
    if re.match(r"!\[", content):
        return "figure"
    stripped = content.strip()
    if len(stripped) < 120 and not stripped.endswith(".") and "\n" not in stripped:
        if re.match(r"^#{1,6}\s", stripped):
            return "header"
    return "text"


def _stable_uuid(node_id: str) -> str:
    try:
        uuid.UUID(node_id)
        return node_id
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, node_id))
