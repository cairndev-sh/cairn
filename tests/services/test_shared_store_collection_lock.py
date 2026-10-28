"""One collection operation at a time on a shared vector store.

The process-wide RAG store (``get_vector_store``) and the episodic-memory store
are each ONE instance used for every workspace's collection, and a store keeps
the collection its next operation acts on as instance state. Callers select
their collection and run the store call under the store's ``StoreLock``
(``on_collection``) — and only the store call: embedding, parsing, catalog
writes and answer synthesis run outside it.

The interleaving tests force the worst case deterministically: the first
caller's store operation pauses until a second caller (another workspace) has
selected its own collection on the same store — or until a short timeout, when
the lock keeps the second caller out — and then records the collection it
actually acted on.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import asyncio
import time
import types

import pytest

from cairn.core import workspace as ws
from cairn.services.rag.stores import StoreLock, VectorStoreBusy, on_collection

A, B = "team-a", "team-b"
_HOLD_S = 0.2  # how long the first operation waits for the other caller to cut in


class _SharedStore:
    """A fake store shared by both callers. The first collection selected is the
    "first caller's"; its first operation pauses until another collection has been
    selected (or ``_HOLD_S`` passes), then records the collection it acted on.
    ``op_delay`` makes every store operation take that long (timing tests)."""

    def __init__(self, *, hold: bool = True, op_delay: float = 0.0):
        self._collection = None
        self.first = None
        self.other_selected = asyncio.Event()
        self.acted: list[tuple[str, str]] = []  # (operation, collection acted on)
        self._held = not hold
        self.op_delay = op_delay

    async def initialize(self, collection, dim):
        self._collection = collection
        if self.first is None:
            self.first = collection
        elif collection != self.first:
            self.other_selected.set()

    async def _act(self, op):
        if self._collection == self.first and not self._held:
            self._held = True
            try:
                await asyncio.wait_for(self.other_selected.wait(), _HOLD_S)
            except TimeoutError:
                pass
        if self.op_delay:
            await asyncio.sleep(self.op_delay)
        self.acted.append((op, self._collection))
        return self._collection

    async def has_source(self, source):
        await self._act(f"has_source:{source}")
        return False

    async def upsert(self, ids, dense, sparse, payloads):
        await self._act(f"upsert:{payloads[0].get('source', '')}")
        return len(ids)

    async def search(self, dense, sparse=None, top_k=5, filters=None):
        coll = await self._act("search")
        return [{"score": 1.0, "content": f"chunk of {coll}", "source": coll, "type": "episodic_memory"}]

    async def list_sources(self, limit=50):
        await self._act("list_sources")
        return []

    async def delete_by_source(self, source):
        await self._act(f"delete:{source}")
        return 0


class _Embedder:
    dim = 4

    async def embed(self, texts):
        return [[0.5] * self.dim for _ in texts]

    async def embed_one(self, text):
        return [0.5] * self.dim

    async def embed_sparse(self, texts):
        return None

    async def embed_sparse_one(self, text):
        return None


async def _no_connection(*_a, **_k):
    return None


async def _llm_answer(**_k):
    msg = types.SimpleNamespace(content="answer")
    return types.SimpleNamespace(choices=[types.SimpleNamespace(message=msg)])


async def _in_workspace(workspace_id, coro_fn):
    tok = ws.set_current_workspace(workspace_id)
    try:
        return await coro_fn()
    finally:
        ws.reset_current_workspace(tok)


async def _first_then_second(first, second):
    """Start ``first``, let it reach its store operation, then start ``second``."""
    t1 = asyncio.create_task(first())
    for _ in range(50):
        await asyncio.sleep(0)
    t2 = asyncio.create_task(second())
    return await asyncio.gather(t1, t2)


@pytest.fixture
def rag(monkeypatch):
    """The RAG index/ingest/query modules wired to one shared fake store."""
    pytest.importorskip("llama_index.core")
    import cairn.core.llm.client as llm_client
    import cairn.services.rag.embeddings as embeddings
    import cairn.services.rag.indexer as indexer
    import cairn.services.rag.ingest as ingest
    import cairn.services.rag.query as query
    import cairn.services.rag.stores as stores

    def wire(store, llm=_llm_answer):
        emb = _Embedder()
        for module, attr, value in (
            (stores, "get_vector_store", lambda: store), (ingest, "get_vector_store", lambda: store),
            (query, "get_vector_store", lambda: store), (embeddings, "get_embedder", lambda: emb),
            (ingest, "get_embedder", lambda: emb), (query, "get_embedder", lambda: emb),
            (indexer, "apply_backend_connection", _no_connection),
            (ingest, "apply_backend_connection", _no_connection),
            (query, "apply_backend_connection", _no_connection),
            (ingest, "get_settings", lambda: types.SimpleNamespace(llama_cloud_api_key=None, rag_collection_name="kb")),
            (llm_client, "llm_complete", llm),
        ):
            monkeypatch.setattr(module, attr, value)
        return types.SimpleNamespace(indexer=indexer, ingest=ingest, query=query)

    return wire


def _docs(tmp_path, name, n=1):
    d = tmp_path / name
    d.mkdir()
    for i in range(n):
        (d / f"doc{i}.md").write_text(f"# {name} {i}\n\nNotes {i}.\n", encoding="utf-8")
    return d


# ── isolation under a forced interleaving ─────────────────────────────


@pytest.mark.asyncio
async def test_concurrent_ingests_of_two_workspaces_write_to_their_own_collections(rag, tmp_path):
    store = _SharedStore()
    m = rag(store)
    doc_a, doc_b = _docs(tmp_path, "a") / "doc0.md", _docs(tmp_path, "b") / "doc0.md"

    await _first_then_second(
        lambda: _in_workspace(A, lambda: m.ingest.ingest_document(str(doc_a), "rag_notes")),
        lambda: _in_workspace(B, lambda: m.ingest.ingest_document(str(doc_b), "rag_notes")),
    )
    expected = {str(doc_a): "ws_team_a__rag_notes", str(doc_b): "ws_team_b__rag_notes"}
    acted = [(op, coll) for op, coll in store.acted if ":" in op and op.split(":", 1)[1] in expected]
    assert len(acted) == 4, store.acted
    for op, coll in acted:
        assert coll == expected[op.split(":", 1)[1]], (op, coll, store.acted)


@pytest.mark.asyncio
async def test_concurrent_queries_of_two_workspaces_read_their_own_collections(rag):
    store = _SharedStore()
    m = rag(store)
    res_a, res_b = await _first_then_second(
        lambda: _in_workspace(A, lambda: m.query.search("q", collection="rag_notes")),
        lambda: _in_workspace(B, lambda: m.query.search("q", collection="rag_notes")),
    )
    assert {s["source"] for s in res_a["sources"]} == {"ws_team_a__rag_notes"}, store.acted
    assert {s["source"] for s in res_b["sources"]} == {"ws_team_b__rag_notes"}, store.acted


@pytest.mark.asyncio
async def test_an_index_run_and_a_query_of_two_workspaces_stay_in_their_collections(rag, tmp_path):
    """index_corpus in A (deletes + nested ingest_document) against query.search in B."""
    from cairn.core.storage.rag_catalog import InMemoryRagCatalog

    store = _SharedStore()
    m = rag(store)
    docs = _docs(tmp_path, "a-docs", n=2)
    result, answer = await _first_then_second(
        lambda: _in_workspace(A, lambda: m.indexer.index_corpus(
            "notes", str(docs), catalog=InMemoryRagCatalog(), collection="rag_notes")),
        lambda: _in_workspace(B, lambda: m.query.search("q", collection="rag_notes")),
    )
    assert result.documents_indexed == 2, result
    a_ops = [c for op, c in store.acted if op != "search"]
    assert len(a_ops) == 6 and set(a_ops) == {"ws_team_a__rag_notes"}, store.acted
    assert {s["source"] for s in answer["sources"]} == {"ws_team_b__rag_notes"}, store.acted


@pytest.mark.asyncio
async def test_concurrent_memory_recall_and_save_of_two_workspaces_stay_in_their_collections(monkeypatch):
    import cairn.services.memory.store as mem

    store, emb = _SharedStore(), _Embedder()
    monkeypatch.setattr(mem, "_get_store", lambda: store)
    monkeypatch.setattr(mem, "get_embedder", lambda: emb)
    monkeypatch.setattr(mem, "apply_backend_connection", _no_connection)

    recalled, _saved = await _first_then_second(
        lambda: _in_workspace(A, lambda: mem.recall_episodic("q")),
        lambda: _in_workspace(B, lambda: mem.save_episodic({"note": "b"}, embed_text="b")),
    )
    assert {h["source"] for h in recalled} == {"ws_team_a__obs_memories"}, store.acted
    assert [c for op, c in store.acted if op.startswith("upsert:")] == ["ws_team_b__obs_memories"], store.acted


# ── narrow spans ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_a_query_does_not_hold_the_store_while_it_synthesises(rag):
    synthesising, finish = asyncio.Event(), asyncio.Event()

    async def _llm(**_k):
        if not synthesising.is_set():  # the first query's synthesis waits
            synthesising.set()
            await finish.wait()
        return await _llm_answer()

    store = _SharedStore(hold=False)
    m = rag(store, llm=_llm)
    first = asyncio.create_task(_in_workspace(A, lambda: m.query.search("q", collection="rag_notes")))
    await asyncio.wait_for(synthesising.wait(), 5)
    second = await asyncio.wait_for(
        _in_workspace(B, lambda: m.query.search("q", collection="rag_notes")), 2)
    assert not first.done()  # A is still synthesising while B searched and answered
    assert {s["source"] for s in second["sources"]} == {"ws_team_b__rag_notes"}
    finish.set()
    assert {s["source"] for s in (await first)["sources"]} == {"ws_team_a__rag_notes"}


@pytest.mark.asyncio
async def test_a_search_waits_for_one_store_call_not_for_a_whole_index_run(rag, tmp_path):
    from cairn.core.storage.rag_catalog import InMemoryRagCatalog

    op = 0.05
    store = _SharedStore(hold=False, op_delay=op)
    m = rag(store)
    docs = _docs(tmp_path, "many", n=8)  # 8 documents x 3 store calls each
    started = time.monotonic()
    index = asyncio.create_task(_in_workspace(A, lambda: m.indexer.index_corpus(
        "notes", str(docs), catalog=InMemoryRagCatalog(), collection="rag_notes")))
    await asyncio.sleep(3 * op)  # the run is under way
    t0 = time.monotonic()
    answer = await _in_workspace(B, lambda: m.query.search("q", collection="rag_notes"))
    search_latency = time.monotonic() - t0
    await index
    run = time.monotonic() - started
    assert {s["source"] for s in answer["sources"]} == {"ws_team_b__rag_notes"}
    assert run > 20 * op, run
    assert search_latency < 6 * op, (search_latency, run)  # about one store call ahead of it, not the run


# ── the lock itself ───────────────────────────────────────────────────


async def _hold_forever(store, lock, entered):
    async with on_collection(store, "c1", 4, lock):
        entered.set()
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_a_holder_cancelled_mid_operation_releases_the_lock():
    lock, store, entered = StoreLock(), _SharedStore(hold=False), asyncio.Event()
    holder = asyncio.create_task(_hold_forever(store, lock, entered))
    await entered.wait()
    holder.cancel()
    with pytest.raises(asyncio.CancelledError):
        await holder
    assert not lock.locked()
    async with on_collection(store, "c2", 4, lock, timeout=0.5):
        assert store._collection == "c2"


@pytest.mark.asyncio
async def test_a_waiter_cancelled_while_waiting_never_holds_the_lock():
    lock, store, entered = StoreLock(), _SharedStore(hold=False), asyncio.Event()
    holder = asyncio.create_task(_hold_forever(store, lock, entered))
    await entered.wait()

    async def _wait():
        async with on_collection(store, "c2", 4, lock):
            pass

    waiter = asyncio.create_task(_wait())
    await asyncio.sleep(0.01)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    holder.cancel()
    with pytest.raises(asyncio.CancelledError):
        await holder
    assert not lock.locked()


@pytest.mark.asyncio
async def test_a_task_that_inherits_the_holders_context_does_not_hold_the_lock():
    lock, store = StoreLock(), _SharedStore(hold=False)
    async with on_collection(store, "c1", 4, lock):
        async def _child():
            async with on_collection(store, "c2", 4, lock, timeout=0.1):
                pass

        child = asyncio.create_task(_child())  # copies this task's context
        with pytest.raises(VectorStoreBusy):
            await child
        assert store._collection == "c1"


@pytest.mark.asyncio
async def test_a_bounded_wait_raises_a_busy_error():
    lock, store, entered = StoreLock(), _SharedStore(hold=False), asyncio.Event()
    holder = asyncio.create_task(_hold_forever(store, lock, entered))
    await entered.wait()
    t0 = time.monotonic()
    with pytest.raises(VectorStoreBusy, match="knowledge store busy, try again"):
        async with on_collection(store, "c2", 4, lock, timeout=0.1):
            pass
    assert time.monotonic() - t0 < 1.0
    holder.cancel()
    with pytest.raises(asyncio.CancelledError):
        await holder
    assert not lock.locked()


@pytest.mark.asyncio
async def test_nested_entry_by_the_holder_restores_the_outer_collection():
    lock, store = StoreLock(), _SharedStore(hold=False)
    async with on_collection(store, "outer", 4, lock):
        async with on_collection(store, "inner", 4, lock, timeout=0.1):
            assert store._collection == "inner"
        assert store._collection == "outer"
    assert not lock.locked()


@pytest.mark.asyncio
async def test_waiters_get_the_lock_in_arrival_order():
    lock, store, order = StoreLock(), _SharedStore(hold=False), []
    gate = asyncio.Event()

    async def _take(name):
        async with on_collection(store, name, 4, lock):
            order.append(name)
            await gate.wait()

    first = asyncio.create_task(_take("w0"))
    await asyncio.sleep(0)
    rest = []
    for i in range(1, 4):
        rest.append(asyncio.create_task(_take(f"w{i}")))
        await asyncio.sleep(0)
    gate.set()
    await asyncio.gather(first, *rest)
    assert order == ["w0", "w1", "w2", "w3"]


@pytest.mark.asyncio
async def test_a_waiter_that_gives_up_while_its_hand_over_is_pending_does_not_hold_the_lock():
    """The waiter is cancelled, then a release picks it before its task resumes:
    on resuming it must not count as the owner, and re-entering must take the
    lock for real (a third task then waits), never a second holder."""
    lock, store, seen = StoreLock(), _SharedStore(hold=False), {}
    await lock.acquire()  # this test task holds the lock

    async def _other():
        try:
            await lock.acquire(timeout=0.05)
        except VectorStoreBusy:
            return "waited"
        lock.release()
        return "second holder"

    async def _waiter():
        try:
            await lock.acquire(timeout=5)
        except asyncio.CancelledError:
            asyncio.current_task().uncancel()
            seen["owned_after_giving_up"] = lock.owned_by_current_task()
            async with on_collection(store, "again", 4, lock, timeout=1):
                seen["owner_is_me"] = lock.owned_by_current_task()
                seen["other"] = await asyncio.create_task(_other())
            return
        seen["acquired"] = True
        lock.release()

    w = asyncio.create_task(_waiter())
    for _ in range(5):
        await asyncio.sleep(0)  # parked on its future
    w.cancel()      # its future is cancelled now; the task resumes on a later tick
    lock.release()  # this release picks it first
    await w
    assert "acquired" not in seen
    assert seen == {"owned_after_giving_up": False, "owner_is_me": True, "other": "waited"}
    assert not lock.locked()


@pytest.mark.asyncio
async def test_acquiring_outside_an_asyncio_task_fails_closed():
    lock = StoreLock()
    loop = asyncio.get_running_loop()
    out = loop.create_future()

    def _callback():  # a plain loop callback: no current task
        coro = lock.acquire(timeout=0.1)
        try:
            coro.send(None)
            out.set_result("suspended")
        except RuntimeError as exc:
            out.set_result(str(exc))
        except StopIteration:
            out.set_result("acquired")
        finally:
            coro.close()

    loop.call_soon(_callback)
    assert "needs a running asyncio task" in await out
    assert not lock.locked()
