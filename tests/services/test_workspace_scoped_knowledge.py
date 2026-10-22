"""Knowledge corpora and retrieval are scoped to the workspace (ADR-0062 "Isolation").

What these tests pin:

* role composition (``list_roles`` / ``resolve_role``) lists only the CURRENT
  workspace's bring-your-own corpora, beside the pack corpora every workspace
  shares;
* ambient composition (``compose_knowledge``) never retrieves from a corpus of
  another workspace;
* indexing a workspace's corpus writes into that workspace's collection whatever
  context runs it, and a search by the corpus name from another workspace (the
  ``default`` one included) finds nothing; the owner finds its chunks;
* the collection key is unambiguous per workspace, and unchanged for ``default``
  and for every slug workspace id (no migration of existing collections).

The sqlite-vec case runs the real ingest chain, the real ``SqliteVecStore`` and
the real ``rag/search`` operator; only the embedder is a deterministic fake. The
pgvector case runs the same scenario when ``PGVECTOR_DSN`` names a live database.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import types

import pytest

from cairn.core import workspace as ws
from cairn.core.storage.knowledge_corpora import InMemoryWorkspaceCorpusStore, WorkspaceCorpus

A, B = "team-a", "team-b"
ROLE = "handbook"


def _corpus(workspace_id: str, name: str, source_uri: str = "git+https://example.com/x.git") -> WorkspaceCorpus:
    return WorkspaceCorpus(workspace_id=workspace_id, name=name, source_uri=source_uri, role=ROLE)


def _pack(tmp_path, name: str = "shipped", corpus: str = "guide"):
    """A pack that ships one corpus into ``ROLE`` (shape ``discover_corpora`` reads)."""
    (tmp_path / "rag").mkdir(exist_ok=True)
    req = types.SimpleNamespace(corpus="rag", freshness=None, role=ROLE)
    manifest = types.SimpleNamespace(required_rag={corpus: req})
    return types.SimpleNamespace(name=name, manifest=manifest, rag_dir=str(tmp_path / "rag"))


@pytest.fixture
def corpora(monkeypatch):
    """One BYO corpus in each of default, A and B, all providing into ``ROLE``."""
    import cairn.core.storage.knowledge_corpora as kc

    store = InMemoryWorkspaceCorpusStore()
    for c in (_corpus("default", "deploy-notes"), _corpus(A, "a-notes"), _corpus(B, "b-notes")):
        store._rows[c.corpus_id] = c
    monkeypatch.setattr(kc, "_STORE", store)
    return store


def _in(workspace_id):
    return ws.set_current_workspace(workspace_id)


# ── composition: list_roles / resolve_role ─────────────────────────────


@pytest.mark.parametrize(("workspace_id", "own"), [(A, "a-notes"), (B, "b-notes"), ("default", "deploy-notes")])
def test_roles_list_only_the_current_workspace_corpora_beside_pack_corpora(tmp_path, corpora, workspace_id, own):
    from cairn.services.rag.roles import list_roles, resolve_role

    tok = _in(workspace_id)
    try:
        names = sorted(t.name for t in list_roles([_pack(tmp_path)])[ROLE])
        resolved = sorted(t.name for t in resolve_role(ROLE, packs=[_pack(tmp_path)]))
    finally:
        ws.reset_current_workspace(tok)
    assert names == sorted(["guide", own])
    assert resolved == names


def test_roles_of_a_workspace_with_no_corpora_are_the_pack_corpora_only(tmp_path, corpora):
    from cairn.services.rag.roles import list_roles

    tok = _in("team-c")
    try:
        roles = list_roles([_pack(tmp_path)])
    finally:
        ws.reset_current_workspace(tok)
    assert [t.name for t in roles[ROLE]] == ["guide"]


def test_a_byo_target_names_its_workspace_collection_and_a_pack_target_is_unchanged(tmp_path, corpora):
    from cairn.services.rag.embeddings import rag_collection
    from cairn.services.rag.roles import list_roles

    tok = _in(B)
    try:
        by_name = {t.name: t for t in list_roles([_pack(tmp_path)])[ROLE]}
    finally:
        ws.reset_current_workspace(tok)
    assert by_name["b-notes"].collection == "ws_team_b__" + rag_collection("b-notes")
    assert by_name["guide"].collection == rag_collection("guide")  # pack corpus: as before


def test_a_default_workspace_byo_target_keeps_the_bare_collection(corpora):
    from cairn.services.rag.byo import workspace_corpus_targets
    from cairn.services.rag.embeddings import rag_collection

    [t] = workspace_corpus_targets([_corpus("default", "deploy-notes")])
    assert t.collection == rag_collection("deploy-notes")


# ── ambient composition: compose_knowledge ────────────────────────────


@pytest.mark.asyncio
async def test_compose_never_retrieves_from_another_workspace_corpus(tmp_path, corpora):
    from cairn.services.rag.inject import compose_knowledge

    searched: list[str] = []

    async def _search(corpus, query, top_k):
        searched.append(corpus)
        return [(0.9, {"text": f"chunk of {corpus}", "source": f"{corpus}.md"})]

    tok = _in(A)
    try:
        ctx = await compose_knowledge([ROLE], "how do we deploy?", search=_search, packs=[_pack(tmp_path)])
    finally:
        ws.reset_current_workspace(tok)
    assert sorted(searched) == ["a-notes", "guide"]
    assert {c.source_pack for c in ctx.citations} == {"shipped", f"workspace:{A}"}


# ── collection key: unambiguous per workspace ─────────────────────────


def test_collection_key_is_unchanged_for_default_and_slug_workspace_ids():
    assert ws.workspace_collection("rag_x__ab12", "default") == "rag_x__ab12"
    assert ws.workspace_collection("rag_x__ab12", "acme") == "ws_acme__rag_x__ab12"
    assert ws.workspace_collection("rag_x__ab12", "acme-eu-2") == "ws_acme_eu_2__rag_x__ab12"
    assert ws.workspace_collection("ws_acme__rag_x", "acme") == "ws_acme__rag_x"  # idempotent


@pytest.mark.parametrize("raw", ["ws_team_b__rag_x__ab12", "ws__0123456789abcdef__rag_x", "ws_x"])
def test_the_default_workspace_cannot_name_another_workspace_collection(raw):
    with pytest.raises(ValueError, match="reserved"):
        ws.workspace_collection(raw, "default")
    tok = _in("default")
    try:
        with pytest.raises(ValueError, match="reserved"):
            ws.workspace_collection(raw)
    finally:
        ws.reset_current_workspace(tok)
    # inside a workspace a raw name is still its own (prefixed), as before
    assert ws.workspace_collection(raw, A).startswith("ws_team_a__")


@pytest.mark.parametrize(("other", "slug"), [
    ("acmeé", "acme"),             # a non-ASCII letter in a workspace name
    ("~no-workspace", "no-workspace"),
    ("a_b", "a-b"),
    ("Acme", "acme"),              # table names are case-insensitive in SQLite and Postgres
    ("-acme-", "acme"),
])
def test_collection_keys_of_distinct_workspaces_never_coincide(other, slug):
    keys = {ws.workspace_collection("rag_x__ab12", w).lower() for w in (other, slug, "default")}
    assert len(keys) == 3
    k = ws.workspace_collection("rag_x__ab12", other)
    assert k.replace("_", "").isalnum()  # still a valid table name for every backend
    assert ws.workspace_collection(k, other) == k  # idempotent for that workspace too


# ── end to end: index from outside the workspace, search from every workspace ──


class _Embedder:
    """Deterministic: every text embeds to the same unit vector, so a search finds
    every chunk of the collection it reads — the test measures WHICH collection."""

    dim = 4

    async def embed(self, texts):
        return [[0.5] * self.dim for _ in texts]

    async def embed_one(self, text):
        return [0.5] * self.dim

    async def embed_sparse(self, texts):
        return None

    async def embed_sparse_one(self, text):
        return None


def _backends():
    yield pytest.param("sqlite_vec", id="sqlite-vec")
    pg = pytest.mark.skipif(not os.environ.get("PGVECTOR_DSN"), reason="PGVECTOR_DSN not set (no live pgvector)")
    yield pytest.param("pgvector", id="pgvector", marks=pg)


def _make_store(backend, tmp_path):
    if backend == "sqlite_vec":
        pytest.importorskip("sqlite_vec")
        from cairn.services.rag.stores.sqlite_vec import SqliteVecStore

        return SqliteVecStore(path=str(tmp_path / "vec.db"))
    pytest.importorskip("pgvector")
    from cairn.services.rag.stores.pgvector import PGVectorStore

    return PGVectorStore()


@pytest.fixture
def wired(monkeypatch, tmp_path, request):
    """The real ingest chain + vector store + ``rag/search`` operator on one
    backend; a fake embedder; no backend-connection lookups."""
    pytest.importorskip("llama_index.core")
    search_mod = pytest.importorskip("operator_rag_local.operators.search")
    import cairn.services.rag.embeddings as embeddings
    import cairn.services.rag.indexer as indexer
    import cairn.services.rag.ingest as ingest
    import cairn.services.rag.stores as stores

    backend = request.param
    shared = _make_store(backend, tmp_path)  # the process-wide store the index path uses
    built = []

    def _build(_name):  # the operator's per-collection stores, same database
        s = _make_store(backend, tmp_path)
        built.append(s)
        return s

    async def _no_connection(*_a, **_k):
        return None

    emb = _Embedder()
    for module, attr, value in (
        (stores, "get_vector_store", lambda: shared), (ingest, "get_vector_store", lambda: shared),
        (embeddings, "get_embedder", lambda: emb), (ingest, "get_embedder", lambda: emb),
        (search_mod, "get_embedder", lambda: emb), (search_mod, "build_vector_store", _build),
        (ingest, "apply_backend_connection", _no_connection),
        (indexer, "apply_backend_connection", _no_connection),
    ):
        monkeypatch.setattr(module, attr, value)
    monkeypatch.setattr(search_mod, "_STORES", {})
    return search_mod


async def _search(search_mod, corpus: str) -> list[str]:
    from operator_rag_local.schemas.search import SearchInput

    out = await search_mod.SearchOp().run(SearchInput(corpus=corpus, query="what is the code?", top_k=5), None)
    assert out.ok, out.error
    return [str(h.payload.get("content") or h.payload.get("text") or "") for h in out.hits]


@pytest.mark.asyncio
@pytest.mark.parametrize("wired", list(_backends()), indirect=True)
async def test_a_workspace_corpus_is_retrievable_only_in_its_workspace(wired, tmp_path):
    from cairn.core.storage.rag_catalog import InMemoryRagCatalog
    from cairn.services.rag.byo import index_workspace_corpus

    docs = tmp_path / "b-docs"
    docs.mkdir()
    (docs / "notes.md").write_text("# Team B\n\nThe vault code for team B is 4417-blue.\n", encoding="utf-8")
    corpus = _corpus(B, "b-notes", source_uri=f"file://{docs}")

    # indexed from outside the workspace (no request in B: the CLI, a background job)
    result = await index_workspace_corpus(corpus, catalog=InMemoryRagCatalog(), allow_local=True)
    assert result.documents_indexed == 1 and result.chunks_indexed >= 1

    for other in ("default", A):
        tok = _in(other)
        try:
            assert await _search(wired, "b-notes") == [], f"{other} retrieved team B's corpus"
        finally:
            ws.reset_current_workspace(tok)

    tok = _in(B)
    try:
        hits = await _search(wired, "b-notes")
    finally:
        ws.reset_current_workspace(tok)
    assert any("4417-blue" in h for h in hits)


@pytest.mark.asyncio
@pytest.mark.parametrize("wired", list(_backends()), indirect=True)
async def test_a_default_workspace_corpus_still_indexes_and_retrieves_in_default(wired, tmp_path):
    from cairn.core.storage.rag_catalog import InMemoryRagCatalog
    from cairn.services.rag.byo import index_workspace_corpus

    docs = tmp_path / "d-docs"
    docs.mkdir()
    (docs / "notes.md").write_text("# Deploy\n\nThe deploy window is Tuesday.\n", encoding="utf-8")
    corpus = _corpus("default", "deploy-notes", source_uri=f"file://{docs}")

    await index_workspace_corpus(corpus, catalog=InMemoryRagCatalog(), allow_local=True)
    assert any("Tuesday" in h for h in await _search(wired, "deploy-notes"))
    tok = _in(A)
    try:
        assert await _search(wired, "deploy-notes") == []
    finally:
        ws.reset_current_workspace(tok)
