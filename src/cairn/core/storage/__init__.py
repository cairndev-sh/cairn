"""Unified storage layer for the engine.

§24.4 of the Roadmap. The eight (later) stores — runs/events, memory, RAG,
sessions, checkpoints, approvals, budgets — sit behind one set of Protocols
in :mod:`cairn.core.storage.protocols`. Concrete backends live in
``sqlite/``, ``postgres/`` (P5), and ``backends/`` (Tier-3 swap-ins).

Phase 1 (this commit) lands runs/events only: the canonical home for the
``RunStore`` Protocol and its in-memory + SQLite implementations, previously
in ``cairn.core.dispatch.run``. No behavior change — pure relocation.
Memory and RAG move under the same umbrella in 4.4/4.5 (separate commit).

Public surface:

  - Protocols + types: :mod:`cairn.core.storage.protocols`
  - In-memory impl: :mod:`cairn.core.storage.memory.runs`
  - SQLite impl: :mod:`cairn.core.storage.sqlite.runs`
  - Factory: :func:`default_store` picks a backend from env.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import logging
from typing import Optional

from cairn.core.storage.backend import StorageBackend
from cairn.core.storage.episodic import EpisodicMemoryStore, default_memory_store
from cairn.core.storage.eval_ledger import (
    EvalCaseResult,
    EvalLedgerStore,
    EvalRun,
    FailureLabel,
    InMemoryEvalLedger,
    PostgresEvalLedger,
    SqliteEvalLedger,
    default_eval_ledger,
)
from cairn.core.storage.memory.runs import InMemoryRunStore
from cairn.core.storage.protocols import (
    ACTIVE_STATUSES,
    TERMINAL_EVENT_TYPES,
    Lane,
    MemoryStore,
    RagStore,
    Run,
    RunEvent,
    RunResultSummary,
    RunStore,
    Status,
    default_priority,
    fingerprint,
    new_run_id,
)
from cairn.core.storage.rag import HybridRagStore, default_rag_store
from cairn.core.storage.rag_catalog import (
    CatalogEntry,
    InMemoryRagCatalog,
    RagCatalogStore,
    SqliteRagCatalog,
    default_rag_catalog,
)
from cairn.core.storage.sqlite.runs import SqliteRunStore

logger = logging.getLogger(__name__)


def _obs_storage_url() -> Optional[str]:
    """Read ``CAIRN_STORAGE`` once. Returns the URL or None if unset."""
    import os
    val = getenv("CAIRN_STORAGE", "").strip()
    return val or None


def default_store() -> RunStore:
    """Pick a run store from the environment.

    Selection precedence:

      1. ``CAIRN_STORAGE=postgres://…`` or ``postgresql://…`` → Postgres
         (Tier 2). Requires the ``storage-postgres`` optional dep group.
      2. ``CAIRN_STORAGE=sqlite:///path`` → :class:`SqliteRunStore` at the
         path. ``CAIRN_STORAGE=sqlite://:memory:`` for an ephemeral store.
      3. ``CAIRN_RUN_DB`` set (legacy knob) → :class:`SqliteRunStore` at
         that path (Tier 1).
      4. Nothing set → volatile :class:`InMemoryRunStore` (Tier 0).

    A SET value that matches none of the above is refused (``ValueError``),
    not silently downgraded to Tier 0 (#1855).

    The historical default (in-memory when nothing is set) is preserved
    so this change is non-breaking. Explicit dependent installations can
    adopt Postgres by setting ``CAIRN_STORAGE`` and installing the
    optional dep group.
    """
    url = _obs_storage_url()
    if url is not None:
        if url.startswith(("postgres://", "postgresql://")):
            from cairn.core.storage.postgres.runs import PostgresRunStore
            return PostgresRunStore(dsn=url)
        if url.startswith("sqlite://"):
            # Strip the `sqlite://` scheme; the remainder IS the path.
            #   sqlite://:memory:        → ":memory:"     (ephemeral)
            #   sqlite:///abs/path.db    → "/abs/path.db" (the third slash is
            #                              the leading "/" of an absolute path)
            #   sqlite://rel/path.db     → "rel/path.db"  (relative to CWD)
            # The earlier leading-slash strip mangled absolute paths (#76):
            # sqlite:///tmp/x.db silently became the *relative* "tmp/x.db", and
            # sqlite:///x.db crashed ("unable to open database file"). The unit
            # tests construct SqliteRunStore(path=...) directly, so they never
            # exercised this URL→path mapping — only the env-driven path did.
            # #1471 — via settings.sqlite_path_from_url, the ONE parser: same
            # mapping as above, plus `sqlite:////abs` (SQLAlchemy's 4-slash
            # absolute spelling) normalized to the same file the other stores
            # get, instead of each store stripping its own slash count.
            from cairn.core import settings  # noqa: PLC0415 - cycle: settings ↔ storage
            return SqliteRunStore(path=settings.sqlite_path_from_url(url) or ":memory:")
        # #1855 — refuse, don't degrade. This branch used to log a WARNING and
        # fall through to CAIRN_RUN_DB / in-memory, so `CAIRN_STORAGE=sqlite` (no
        # URL) silently became InMemoryRunStore: findings evaporated on restart,
        # and under CAIRN_RUNTIME=dbos a second default_store() built a second
        # empty store and a gated run's approval failed with KeyError — a
        # misconfiguration that presented as a bug in the runtime. Three
        # missing characters separated durable from volatile storage, with no
        # feedback. Unset stays in-memory (that is the documented default);
        # SET-but-unrecognized is an error naming the accepted forms. It names at
        # most the value's scheme: a DSN with a typo in its scheme, or none, still
        # carries the user and password, and this message lands in terminals and CI
        # logs (GHSA-gw2v-3922-g7gm).
        from cairn.core import settings  # noqa: PLC0415 - cycle: settings ↔ storage
        scheme = settings.url_scheme(url)
        what = f"a {scheme}:// URL" if scheme else "a value with no <scheme>://"
        raise ValueError(
            f"CAIRN_STORAGE is not a storage URL (it is {what}; the value is not "
            "shown, it may carry a password). Accepted forms: "
            "postgres://user:pass@host:5432/db, postgresql://…, "
            "sqlite:///abs/path.db, sqlite://rel/path.db, sqlite://:memory:. "
            "Unset CAIRN_STORAGE for the in-memory default. (#1855)"
        )

    from cairn.core import settings as _settings
    if _settings.run_db_is_set():
        return SqliteRunStore()
    return InMemoryRunStore()


def build_storage() -> StorageBackend:
    """Construct the single :class:`StorageBackend` consumers share.

    Reads env knobs in priority order:

      - ``CAIRN_STORAGE`` (new) — single URL routes the run store. v1 dispatches
        only the run store; memory/RAG stay on their own env knobs for now.
        Set ``CAIRN_STORAGE=postgres://…`` + ``RAG_VECTOR_STORE=pgvector`` +
        ``PGVECTOR_DSN=…`` for the "one Postgres for everything" prod tier.
      - ``CAIRN_RUN_DB`` (legacy) — picks SQLite vs InMemory for runs/events.
        Subsumed by ``CAIRN_STORAGE`` when both are set.
      - ``CAIRN_MEMORY`` (reserved) — memory store backend.
      - ``CAIRN_RAG`` (reserved) — RAG store backend.

    The per-concern factories (``default_store`` / ``default_memory_store`` /
    ``default_rag_store``) keep working independently for tests + the legacy
    callsites; ``build_storage`` is the single injection point for the
    engine.

    Construct once at engine startup; pass the result into Dispatcher /
    Runner / CLI / chat endpoint. Avoid calling this per-request — the
    underlying stores hold connections + connection pools.
    """
    return StorageBackend(
        runs=default_store(),
        memory=default_memory_store(),
        rag=default_rag_store(),
        rag_catalog=default_rag_catalog(),
    )


# Process-global storage singleton (ADR-0058 / #427). The one DI injection point:
# consumers call `get_storage()` and read the store they need off it (`.runs`,
# `.policy`, `.connections`, …) instead of importing scattered per-store
# factories. Config-family stores build lazily on first access (StorageBackend
# cached_property), so constructing the singleton is cheap — only the 4 eager
# engine-data stores are built up front.
_STORAGE: Optional[StorageBackend] = None


def get_storage() -> StorageBackend:
    """Return the process-wide :class:`StorageBackend`, building it once."""
    global _STORAGE
    if _STORAGE is None:
        _STORAGE = build_storage()
    return _STORAGE


def reset_storage() -> None:
    """Drop the cached singleton — for tests that swap env/backends."""
    global _STORAGE
    _STORAGE = None


__all__ = [
    "ACTIVE_STATUSES",
    "CatalogEntry",
    "EpisodicMemoryStore",
    "EvalCaseResult",
    "EvalLedgerStore",
    "EvalRun",
    "FailureLabel",
    "HybridRagStore",
    "InMemoryEvalLedger",
    "InMemoryRagCatalog",
    "InMemoryRunStore",
    "Lane",
    "MemoryStore",
    "PostgresEvalLedger",
    "RagCatalogStore",
    "RagStore",
    "Run",
    "RunEvent",
    "RunResultSummary",
    "RunStore",
    "SqliteEvalLedger",
    "SqliteRagCatalog",
    "SqliteRunStore",
    "Status",
    "StorageBackend",
    "TERMINAL_EVENT_TYPES",
    "build_storage",
    "default_eval_ledger",
    "default_memory_store",
    "default_priority",
    "default_rag_catalog",
    "default_rag_store",
    "default_store",
    "fingerprint",
    "get_storage",
    "new_run_id",
    "reset_storage",
]
