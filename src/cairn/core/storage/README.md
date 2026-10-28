# `cairn.core.storage`

The engine's unified storage layer. One `StorageBackend` object exposes
every store the engine needs (`runs`, `memory`, `rag` today; sessions /
checkpoints / approvals / budgets in P6). Constructed once at startup
from env, injected into every stateful consumer.

> Full design + rationale lives in **§24.4** of `docs/Roadmap.md`. This
> README is the implementation summary — what's actually here, how to
> use it, how to extend it.

## What's in here

```
core/storage/
├── README.md                ← this file
├── __init__.py              ← public re-exports + build_storage() + default_*() factories
├── backend.py               ← StorageBackend dataclass (typed bundle of all stores)
├── protocols.py             ← Protocols for every store kind (RunStore, MemoryStore, RagStore)
├── episodic.py              ← EpisodicMemoryStore adapter (wraps services.memory)
├── rag.py                   ← HybridRagStore adapter (wraps services.rag)
├── memory/
│   └── runs.py              ← InMemoryRunStore (Tier 0: volatile)
└── sqlite/
    └── runs.py              ← SqliteRunStore (Tier 1: single-file, restart-safe)
```

The directory names `memory/` and `sqlite/` are **storage tiers** (volatile
in-process vs. embedded persistent), NOT references to the "memory store"
concept. The episodic memory store lives at the root (`episodic.py`) to
avoid the name clash.

## The Protocol model

Every store kind has a Protocol in `protocols.py`. Consumers program
against the Protocol, not the concrete implementation. Today's Protocols:

| Protocol | Methods | Today's impl |
|---|---|---|
| `RunStore` (ABC, coupled with events) | `create / update / get / list / find_active_by_fingerprint / append_event / get_events / subscribe` | `InMemoryRunStore`, `SqliteRunStore` |
| `MemoryStore` (Protocol, runtime_checkable) | `save / recall / list` | `EpisodicMemoryStore` (wraps `services.memory.store`) |
| `RagStore` (Protocol, runtime_checkable) | `search / ingest_document` | `HybridRagStore` (wraps `services.rag.{query,ingest}`) |

`RunStore` is `ABC` (not `Protocol`) because the `subscribe()` method has
shared concrete behavior across implementations. The other two have no
shared concrete code so they're pure Protocols.

### Why runs + events are one Store, not two

Roadmap §24.4.3 sketched separate `RunStore` + `EventStore` Protocols.
That was paper; the actual code couples them. Reasons:

- The `subscribe(run_id, after_seq)` pattern naturally couples them —
  you're always subscribing to a *run's* events.
- Existing code worked this way; the split was complexity for no
  benefit.
- Tier-3 swap-ins (Clickhouse for events) are still possible by
  subclassing `RunStore` and overriding only `append_event` / `get_events`.

## Using the storage layer

### Most common: at engine startup

```python
from cairn.core.storage import build_storage

backend = build_storage()         # reads CAIRN_RUN_DB / CAIRN_MEMORY / CAIRN_RAG
dispatcher = RunDispatcher(runner, backend=backend)
```

### In a test that needs just one store

```python
from cairn.core.storage import InMemoryRunStore, default_memory_store

# Bare RunStore for a test
store = InMemoryRunStore()

# Or a default MemoryStore
mem = default_memory_store()
```

### In a test that needs the whole backend with a fake

```python
from cairn.core.storage import build_storage

backend = build_storage()
backend.memory = FakeMemoryStore()    # swap in a fake; the dataclass is mutable
```

### Inside a consumer (post-4.7 wiring)

```python
class MyConsumer:
    def __init__(self, backend: StorageBackend):
        self.runs = backend.runs       # for run lookup
        self.memory = backend.memory   # for memory recall
        # ...
```

Don't reach back to per-store factories (`default_store()`,
`default_memory_store()`, `default_rag_store()`) from inside the engine
— take a `StorageBackend` instead. The factories are for tests and the
legacy callsites the migration is gradually replacing.

## Env knobs

| Var | Today | P5+ |
|---|---|---|
| `CAIRN_STORAGE` | **Wired (P5 minimal)** — single URL routes the run store. `postgres://…` / `postgresql://…` → `PostgresRunStore` (requires `storage-postgres` dep). `sqlite://:memory:` or `sqlite:///path` → `SqliteRunStore`. Takes precedence over `CAIRN_RUN_DB`. Memory + RAG still on their own env knobs in v1. | Will also dispatch memory + RAG when full P5 lands. |
| `CAIRN_RUN_DB` | Set → `SqliteRunStore` at the path. Unset → `InMemoryRunStore`. (Legacy — subsumed by `CAIRN_STORAGE` when both set.) | Deprecated; remove after `CAIRN_STORAGE` covers all stores. |
| `CAIRN_MEMORY` | Reserved — read but not branched on yet. Always `EpisodicMemoryStore` (Qdrant via `services.memory`). | Picks between sqlite-vec / pgvector / qdrant. |
| `CAIRN_RAG` | Reserved — same. Always `HybridRagStore`. | Picks between sqlite-vec / pgvector / qdrant / chroma. |
| `RAG_VECTOR_STORE` | Wired today. Picks the vector backend driver (`qdrant` / `pgvector` / `sqlite_vec` / `pinecone`). | Will be merged into `CAIRN_STORAGE` resolution. |
| `LANGGRAPH_CHECKPOINT` (future) | Not wired. | When wired: `postgres://…` → `langgraph-checkpoint-postgres` for durable graph state. Distinct from `CAIRN_STORAGE` (LangGraph state vs our orchestration metadata are different concerns). |

### "One Postgres for everything" prod recipe

```bash
pip install 'cairndev[storage-postgres,rag-pgvector]'

export CAIRN_STORAGE=postgres://user:pass@host/cairn          # runs + events
export RAG_VECTOR_STORE=pgvector                                # RAG vectors
export PGVECTOR_DSN=postgres://user:pass@host/cairn          # same Postgres
```

Now runs, events, and RAG vectors all share one Postgres instance.
Memory store still uses the same vector backend (Qdrant or pgvector
depending on `RAG_VECTOR_STORE`). Sessions / approvals / budgets land in
P6 with similar dispatch.

The env shape is intentional — a single `CAIRN_STORAGE` for the common
case; per-store overrides for power users who want a different backend
per concern (e.g. Postgres for runs, Qdrant for memory, Clickhouse for
event analytics).

## Adding a new store kind

Walkthrough for when sessions / checkpoints / approvals / budgets land
(P6) or when an external pack wants its own typed store:

1. **Define a Protocol** in `protocols.py`:
   ```python
   @runtime_checkable
   class SessionStore(Protocol):
       async def get(self, session_id: str) -> Optional[Session]: ...
       async def save(self, session: Session) -> None: ...
       # ...
   ```

2. **Add an impl** in the matching backend subpackage (or at the
   storage root if backend-agnostic):
   ```python
   # core/storage/sqlite/sessions.py
   class SqliteSessionStore: ...
   ```

3. **Add a `default_sessions_store()` factory** next to the impl (or
   in `__init__.py`):
   ```python
   def default_sessions_store() -> SessionStore:
       # env-based selection
       return SqliteSessionStore()
   ```

4. **Add a field to `StorageBackend`** in `backend.py`:
   ```python
   @dataclass
   class StorageBackend:
       runs: RunStore
       memory: MemoryStore
       rag: RagStore
       sessions: SessionStore   # ← new
   ```

5. **Extend `build_storage()`** to populate it:
   ```python
   def build_storage() -> StorageBackend:
       return StorageBackend(
           runs=default_store(),
           memory=default_memory_store(),
           rag=default_rag_store(),
           sessions=default_sessions_store(),    # ← new
       )
   ```

6. **Re-export** from `__init__.py` (`SessionStore`,
   `default_sessions_store`).

That's it. Consumers that already hold a `StorageBackend` get the new
store by reading `backend.sessions` — no plumbing changes.

## What's deliberately deferred

- **sqlite-vec for `MemoryStore` + `RagStore`** (P4.8 in the roadmap):
  Tier 1 vector storage in the same SQLite file as runs/events. Real
  ops simplification (one DB file for everything in dev) but no
  concrete user need today — Qdrant works fine. Lands when someone
  actually wants to ditch Qdrant for local dev.
- **Postgres + pgvector for all stores** (P5): the prod-tier story.
  Same shape as sqlite-vec, different backend.
- **`CAIRN_STORAGE` as the unified knob**: P5 introduces it; today the
  three per-concern knobs work independently.
- **New stores** (sessions / checkpoints / approvals / budgets):
  P6 — needed for real chat + multi-user + cost gating.
- **Tier-3 swap-ins** (Clickhouse for events, Restate for checkpoints,
  Chroma for RAG): P7, optional, per-need.

The architecture supports all of these as additive changes. The
existing storage layout doesn't have to move.

## Provenance

§24.4 of `docs/Roadmap.md`. Built in P4 across six commits:

- 4.1–4.3: relocation of `RunStore` from `dispatch/run.py` to here.
- 4.4: `MemoryStore` Protocol + `EpisodicMemoryStore` adapter.
- 4.5: `RagStore` Protocol + `HybridRagStore` adapter.
- 4.6: `StorageBackend` dataclass + `build_storage()` factory.
- 4.7: `RunDispatcher` + `triggers/server.py` consume the backend.
- 4.9 (this file): docs.

P4.8 (sqlite-vec wiring) is deliberately deferred per the §24.10 / §24.12
"don't pre-build" principle the rest of the roadmap follows.
