"""Workspace-scoping of RAG corpora + episodic memory (#237, ADR-0048 rev 2).

The workspace pass (#223/#225/#228/#229) scoped runs/audit/installed-workflows
but left vector data global. These tests pin the cross-cutting contract added in
#237: every collection-resolution chokepoint namespaces by the active workspace,
so one tenant never reads another's RAG corpora or episodic memories — while the
default workspace keeps the exact pre-#237 collection names (back-compat).

Hermetic: the vector store + embedder are mocked, so no sqlite-vec extension /
network / disk is touched. We assert the *collection name* each chokepoint
resolves, which is the isolation boundary.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

from cairn.core import workspace as ws


# ── indexer: corpus → collection + catalog key ──────────────────────

def test_indexer_collection_for_namespaces_by_workspace():
    from cairn.services.rag.indexer import _collection_for

    # default workspace → the historical flat name (back-compat)
    assert _collection_for("rag/runbooks", None) == "rag_runbooks"
    assert _collection_for("rag/runbooks", "custom_coll") == "custom_coll"

    tok = ws.set_current_workspace("acme")
    try:
        assert _collection_for("rag/runbooks", None) == "ws_acme__rag_runbooks"
        # an explicit override is scoped too (a tenant's named corpus stays isolated)
        assert _collection_for("rag/runbooks", "custom_coll") == "ws_acme__custom_coll"
    finally:
        ws.reset_current_workspace(tok)


# ── episodic memory service: _ensure_collection ─────────────────────

async def test_memory_ensure_collection_namespaces_by_workspace():
    import cairn.services.memory.store as mem

    fake_store = AsyncMock()
    fake_embedder = type("E", (), {"dim": 8})()
    with patch.object(mem, "_get_store", return_value=fake_store), \
         patch.object(mem, "get_embedder", return_value=fake_embedder):
        # default → the configured bare name
        await mem._ensure_collection()
        assert fake_store.initialize.await_args.args[0] == "obs_memories"

        tok = ws.set_current_workspace("beta")
        try:
            await mem._ensure_collection()
            assert fake_store.initialize.await_args.args[0] == "ws_beta__obs_memories"
        finally:
            ws.reset_current_workspace(tok)


# ── operator runtime path: _store_for (the real path packs invoke) ──



