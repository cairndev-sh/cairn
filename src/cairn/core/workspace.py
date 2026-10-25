"""Workspaces — the first-class tenancy boundary (ADR-0048 rev 2, issue #223).

A **workspace** is the unit of isolation cairn scopes everything by: runs,
installed-workflow records, schemas, and (as a key prefix) blob artifacts. It
is a real entity, not a bare ``tenant_id`` discriminator — that distinction is
the whole point of ADR-0048 rev 2 (a column with no entity behind it is the
Airflow Multi-Team anti-pattern).

Day-one shape (the no-regret foundation):
  - **OSS self-hosted** runs in a single implicit ``default`` workspace — every
    record stamps ``workspace_id="default"`` and :func:`current_workspace`
    returns ``"default"``, so there is zero behaviour change.
  - **Managed SaaS** activates one workspace per tenant later, with no schema
    rewrite — :func:`current_workspace` becomes request-derived (subdomain /
    header / token claim; ADR-0048 open question, issue #224 H).

What is NOT workspace-scoped: operator *code* (the shared capability pool,
ADR-0048 §3). Per-workspace execution isolation is sandboxing (#205) +
dependency isolation (#204), never per-workspace code directories.

This module deliberately mirrors the store pattern used by
``triggers/store.py`` / ``registry_install.py`` / ``workflow_registry.py``:
a pydantic ``BaseModel`` + InMemory and Sqlite stores (a JSON ``data`` blob,
``asyncio.to_thread`` under a lock).
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import contextlib
import hashlib
import os
import re
import sqlite3
from contextvars import ContextVar, Token
from typing import Any, Optional

from pydantic import BaseModel, Field

from cairn.core._time import utc_now_iso
from cairn.core.account import DEFAULT_ACCOUNT
from cairn.core.pg_schema_init import PgSchemaInit

#: The implicit workspace every single-node / OSS deployment runs in.
DEFAULT_WORKSPACE = "default"

#: Request-scoped active workspace (#228). An HTTP middleware sets this from the
#: ``X-Cairn-Workspace`` header per request; ``asyncio.create_task`` copies the
#: context, so the run-create task spawned inside a request inherits it. ``None``
#: means "unset" → fall back to the env/default. A ContextVar (not a global) so
#: concurrent requests for different workspaces never bleed into each other.
_active: ContextVar[Optional[str]] = ContextVar("cairn_workspace", default=None)


def current_workspace() -> str:
    """Resolve the active workspace id for this context.

    Precedence: request-scoped ContextVar (#228) → ``CAIRN_WORKSPACE`` env →
    ``"default"``. Callers that already route every record through this function
    need no change when SaaS adds real per-request tenant resolution.
    """
    return (_active.get() or getenv("CAIRN_WORKSPACE", "").strip() or DEFAULT_WORKSPACE)


def set_current_workspace(workspace_id: Optional[str]) -> Token:
    """Set the request-scoped workspace; returns a token for :func:`reset_current_workspace`."""
    return _active.set((workspace_id or "").strip() or None)


def reset_current_workspace(token: Token) -> None:
    """Restore the prior request-scoped workspace (pairs with :func:`set_current_workspace`)."""
    _active.reset(token)


def workspace_scope(workspace_id: Optional[str]) -> Any:
    """Make ``workspace_id`` the :func:`current_workspace` for the block.

    The dispatch layer enters it from a run's RECORD wherever it executes work
    outside the request that started it (a pipeline leg advanced off a bus event,
    a resumed run, a Temporal activity), so everything resolved inside — the
    run's connections and secrets among it — is resolved for the workspace the
    record names, not whichever context the executing task happened to inherit.

    A record that names no workspace means the implicit ``default`` with auth off
    (today's behaviour). With auth on it raises :class:`WorkspaceScopeRequired`:
    the record cannot say whose work it is, so nothing runs under a guess."""
    if not (workspace_id or "").strip():
        from cairn.core.auth_tokens import auth_enabled  # noqa: PLC0415 — keep the import graph of this module flat

        if auth_enabled():
            raise WorkspaceScopeRequired("the record names no workspace")
    return _scope(workspace_id)


@contextlib.contextmanager
def _scope(workspace_id: Optional[str]):
    token = set_current_workspace(workspace_id)
    try:
        yield
    finally:
        reset_current_workspace(token)


def scoped_workspace() -> str:
    """The workspace in scope (a request's, or a run's scoped from its record),
    for work started on its behalf — a launch from inside a run lands where the
    run is. With nothing in scope: the process default with auth off; with auth
    on, :class:`WorkspaceScopeRequired`."""
    ws = _active.get()
    if ws:
        return ws
    from cairn.core.auth_tokens import auth_enabled  # noqa: PLC0415 — keep the import graph of this module flat

    if auth_enabled():
        raise WorkspaceScopeRequired("no workspace in scope to start work in")
    return current_workspace()


def workspace_blob_path(workspace_id: str, *parts: str) -> str:
    """Blob *namespace* key for a workspace: ``workspace/<id>/<parts…>``.

    A pure string join (no filesystem touch) so it works identically for a
    local artifact dir (self-hosted) and an object-store prefix (S3/GCS on a
    fleet). ``workspace/<id>/`` is the blob namespace, NOT "the workspace".
    """
    safe = (workspace_id or DEFAULT_WORKSPACE).strip() or DEFAULT_WORKSPACE
    tail = "/".join(p.strip("/") for p in parts if p)
    return f"workspace/{safe}/{tail}" if tail else f"workspace/{safe}"


def workspace_collection(name: str, workspace_id: Optional[str] = None) -> str:
    """Namespace a vector-store collection / catalog-corpus key by workspace (#237).

    The default workspace keeps the bare ``name`` (back-compat — existing
    ``obs_runbooks`` / ``obs_memories`` collections and catalogued corpora are
    untouched); any other workspace gets a backend-safe ``ws_<id>__<name>``
    prefix. Mirrors :func:`workspace_blob_path` and ``runs_root`` (default flat,
    others namespaced), so single-workspace OSS sees zero change and SaaS
    isolates a tenant's RAG corpora + episodic memory with no migration.

    **Backend-safe:** vector-store collections double as SQLite table names
    (:class:`SqliteVecStore`), so the workspace id is sanitized to
    ``[A-Za-z0-9_]``. **Idempotent** for the active workspace: re-applying it to
    an already-namespaced name is a no-op, so a collection resolved once and
    passed downstream (indexer → ``ingest_document``) is never double-prefixed.

    In the default workspace a name starting with ``ws_`` (the prefix of every
    other workspace's collections) raises ``ValueError``.

    **Unambiguous:** two workspaces never share a prefix. A slug id (what
    ``POST /workspaces`` makes from an ASCII name: ``[a-z0-9]`` runs joined by
    single dashes, at most :data:`MAX_WORKSPACE_ID_LEN` long) keeps the readable
    ``ws_<id with - as _>__`` it always had; any other id (non-ASCII, uppercase,
    underscores, leading/trailing dashes, longer slugs) maps to
    ``ws__<sha256 prefix>__``, which no slug id can produce, so sanitizing can
    never fold one workspace onto another (SQLite and Postgres table names are
    also case-insensitive, hence the lowercase-only slug rule).

    Concurrency: the vector stores hold the active collection as instance state.
    The process-wide RAG and memory stores are therefore used one collection
    operation at a time (``cairn.services.rag.stores.on_collection``), and the
    ``rag/search`` / ``memory/search`` operators keep one store per collection.
    """
    ws = (workspace_id or current_workspace()).strip() or DEFAULT_WORKSPACE
    if ws == DEFAULT_WORKSPACE:
        # the default workspace's names are bare, so a bare name never carries
        # another workspace's prefix
        if name.startswith("ws_"):
            raise ValueError(f"collection name {name!r} is reserved for workspace-scoped collections")
        return name
    prefix = _collection_prefix(ws)
    return name if name.startswith(prefix) else f"{prefix}{name}"


#: A workspace id in the slug form ``_slug_workspace_id`` produces from an ASCII name.
_SLUG_WORKSPACE_ID = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*")

#: Longest workspace id a create assigns (``POST /workspaces``, an account's first
#: workspace). Collection names start with the workspace prefix, and Postgres keeps
#: only the first 63 bytes of an identifier: with ids this short, the whole prefix
#: always falls inside those bytes, so two workspaces' tables stay distinct.
MAX_WORKSPACE_ID_LEN = 40


def cap_workspace_id(slug: str, suffix: str = "") -> str:
    """``slug`` followed by ``suffix`` (a ``-2`` dedupe), cut so the result is at
    most :data:`MAX_WORKSPACE_ID_LEN` characters (a dash left at the cut is dropped)."""
    return slug[: MAX_WORKSPACE_ID_LEN - len(suffix)].rstrip("-") + suffix


def _collection_prefix(workspace_id: str) -> str:
    """The collection prefix of a non-default workspace (see :func:`workspace_collection`).
    A slug id longer than :data:`MAX_WORKSPACE_ID_LEN` (created before the cap)
    takes the hashed form, so its prefix stays short too."""
    if _SLUG_WORKSPACE_ID.fullmatch(workspace_id) and len(workspace_id) <= MAX_WORKSPACE_ID_LEN:
        # a slug's dashes become underscores (the backend-safe alphabet) — the same
        # sanitizing this prefix always used, so existing collection names hold
        return f"ws_{re.sub(r'[^A-Za-z0-9_]+', '_', workspace_id)}__"
    digest = hashlib.sha256(workspace_id.encode("utf-8")).hexdigest()[:16]
    return f"ws__{digest}__"


class WorkspaceDefaults(BaseModel):
    """Tenant defaults a workspace was set up with (#2323).

    Written by the setup wizard (``routers/setup.py``); every field is optional
    so a partly-finished setup is representable. Recorded config only — nothing
    here is enforced yet: the spendable budget is still the starter series
    (#1571), notifications still read ``SLACK_WEBHOOK_URL`` and approvers are
    still per-policy.
    """

    #: Profile name the workspace runs in by default (``cairn/profiles/<name>.toml``).
    environment: Optional[str] = None
    budget_cap_usd: Optional[float] = None
    approvers: list[str] = Field(default_factory=list)
    notification_channel: Optional[str] = None
    retention_days: Optional[int] = None
    #: Packs whose starter pipelines were enabled by setup.
    starter_packs: list[str] = Field(default_factory=list)
    #: When the optional "Team & defaults" step was first saved or skipped
    #: (ISO-8601 UTC) — what marks that step done, since it may record nothing.
    saved_at: Optional[str] = None


class Workspace(BaseModel):
    """A tenancy boundary. Per-workspace RBAC and encryption keys are still
    later (ADR-0048 §1, issue #224 H); the ACCOUNT binding landed in #1610."""

    id: str
    #: #1610 (ADR-0094 D3) — the account this workspace belongs to. Until this
    #: existed a workspace was bound to no tenant at all, and the middleware
    #: accepted whichever workspace the CALLER asserted, so the boundary was a
    #: filing convention rather than a security one (#1609).
    #:
    #: Defaults to the implicit `default` account so every existing row — and
    #: every single-account deployment — keeps working untouched: the migration
    #: is "the field appears and reads `default`", not a data rewrite.
    account_id: str = DEFAULT_ACCOUNT
    name: str = ""
    settings: dict[str, Any] = Field(default_factory=dict)
    created_at: str = Field(default_factory=utc_now_iso)
    #: Lifecycle (#258). "archived" = soft-deleted: hidden from the switcher, its
    #: installed-workflow records marked inactive — but runs/events/audit are never
    #: deleted. Reversible via restore. Rides in the JSON `data` blob (no migration).
    status: str = "active"
    archived_at: Optional[str] = None
    #: #2323 — tenant defaults from the setup wizard. ``None`` until setup
    #: writes it. Rides in the JSON ``data`` blob every store persists, so
    #: InMemory / SQLite / Postgres need no migration and old rows read ``None``.
    defaults: Optional[WorkspaceDefaults] = None

    def model_post_init(self, _ctx: object) -> None:  # pydantic v2 hook
        if not self.name:
            object.__setattr__(self, "name", self.id)


# ── stores ────────────────────────────────────────────────────────────


class InMemoryWorkspaceStore:
    """Volatile store (tests / no-config default), seeded with ``default``."""

    def __init__(self) -> None:
        self._ws: dict[str, Workspace] = {}
        self._ws[DEFAULT_WORKSPACE] = Workspace(id=DEFAULT_WORKSPACE, name="Default")

    async def put(self, ws: Workspace) -> Workspace:
        self._ws[ws.id] = ws
        return ws

    async def get(self, ws_id: str) -> Optional[Workspace]:
        return self._ws.get(ws_id)

    async def list(self) -> list[Workspace]:
        return sorted(self._ws.values(), key=lambda w: w.id)

    def close(self) -> None:
        pass


class SqliteWorkspaceStore:
    """Embedded-SQLite store — one table (``workspaces``), JSON ``data`` blob;
    async ops via ``asyncio.to_thread`` under a lock. Seeds ``default`` on open.
    Mirrors :class:`SqliteWorkflowRegistry`."""

    def __init__(self, path: str | None = None) -> None:
        if path is None:
            from cairn.core import settings

            # single shared cairn.db (#427/ADR-0058); CAIRN_*_DB overrides
            path = getenv("CAIRN_WORKSPACE_DB", "").strip() or settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("CREATE TABLE IF NOT EXISTS workspaces (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        # seed the implicit default workspace (idempotent).
        self._conn.execute(
            "INSERT OR IGNORE INTO workspaces (id, data) VALUES (?,?)",
            (DEFAULT_WORKSPACE, Workspace(id=DEFAULT_WORKSPACE, name="Default").model_dump_json()),
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    async def put(self, ws: Workspace) -> Workspace:
        async with self._lock:
            def _w() -> None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO workspaces (id, data) VALUES (?,?)",
                    (ws.id, ws.model_dump_json()),
                )
                self._conn.commit()
            await asyncio.to_thread(_w)
        return ws

    async def get(self, ws_id: str) -> Optional[Workspace]:
        row = self._conn.execute("SELECT data FROM workspaces WHERE id=?", (ws_id,)).fetchone()
        return Workspace.model_validate_json(row[0]) if row else None

    async def list(self) -> list[Workspace]:
        rows = self._conn.execute("SELECT data FROM workspaces").fetchall()
        return sorted((Workspace.model_validate_json(r[0]) for r in rows), key=lambda w: w.id)

    def close(self) -> None:
        self._conn.close()


class PostgresWorkspaceStore(PgSchemaInit):
    """Postgres-backed workspace store (#236) — the fleet-shared backend.

    Mirrors :class:`~cairn.core.storage.postgres.runs.PostgresRunStore`: a lazy
    asyncpg pool, a single ``workspaces`` table with a JSONB ``data`` blob, and
    the ``default`` workspace seeded on schema-ready. Same Protocol as the
    InMemory / Sqlite variants, so consumers see no difference.

    Why this matters (ADR-0048): the sqlite store is per-node, so a multi-
    instance fleet wouldn't share workspaces. With ``CAIRN_STORAGE=postgres://…``
    every node reads/writes the same workspace set — "DB authoritative for the
    fleet." Requires the ``storage-postgres`` optional dep group (asyncpg)."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn
        self._pool: Any = None
        self._schema_ready = False

    async def _open_pool(self) -> Any:
        try:
            import asyncpg  # noqa: PLC0415 — optional dep (storage-postgres extra)
        except ImportError as e:
            raise RuntimeError(
                "PostgresWorkspaceStore requires the 'storage-postgres' optional "
                "dep group. Install with: pip install 'cairndev[storage-postgres]'"
            ) from e
        if not self._dsn:
            raise RuntimeError(
                "PostgresWorkspaceStore needs a DSN. Pass dsn= explicitly or set "
                "CAIRN_STORAGE=postgres://… before constructing the store."
            )
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415

        # #614: one pool per DSN, shared across every Postgres store in this process.
        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool) -> None:
        async with pool.acquire() as conn:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS workspaces "
                "(id TEXT PRIMARY KEY, data JSONB NOT NULL)"
            )
            # seed the implicit default workspace (idempotent).
            await conn.execute(
                "INSERT INTO workspaces (id, data) VALUES ($1, $2::jsonb) "
                "ON CONFLICT (id) DO NOTHING",
                DEFAULT_WORKSPACE,
                Workspace(id=DEFAULT_WORKSPACE, name="Default").model_dump_json(),
            )

    async def put(self, ws: Workspace) -> Workspace:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO workspaces (id, data) VALUES ($1, $2::jsonb) "
                "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
                ws.id, ws.model_dump_json(),
            )
        return ws

    async def get(self, ws_id: str) -> Optional[Workspace]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data::text AS data FROM workspaces WHERE id=$1", ws_id)
        return Workspace.model_validate_json(row["data"]) if row else None

    async def list(self) -> list[Workspace]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT data::text AS data FROM workspaces")
        return sorted((Workspace.model_validate_json(r["data"]) for r in rows), key=lambda w: w.id)

    async def aclose(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            self._schema_ready = False

    def close(self) -> None:  # parity with the sync stores (best-effort)
        pass


_store: object | None = None


def get_workspace_store() -> Any:
    """Process-wide workspace store. Honors ``CAIRN_STORAGE`` (#236):

      - ``postgres://…`` / ``postgresql://…`` → :class:`PostgresWorkspaceStore`
        (fleet-shared; same DB as the run store).
      - anything else / unset → :class:`SqliteWorkspaceStore` (per-node;
        ``CAIRN_WORKSPACE_DB`` path knob still applies).

    Same dispatch as :func:`cairn.core.storage.default_store`, so a deployment
    that points ``CAIRN_STORAGE`` at Postgres gets runs *and* workspaces there.
    Tests pass their own store in (``workspace._store = …``)."""
    global _store
    if _store is None:
        url = getenv("CAIRN_STORAGE", "").strip()
        if url.startswith(("postgres://", "postgresql://")):
            _store = PostgresWorkspaceStore(dsn=url)
        else:
            _store = SqliteWorkspaceStore()
    return _store


async def workspace_account(workspace_id: Optional[str] = None, *, store: Any = None) -> str:
    """The account that owns ``workspace_id`` (default: :func:`current_workspace`).

    This is the account a run in that workspace resolves connections and secrets
    in. With auth on, omitting ``workspace_id`` outside any workspace scope (no
    request, no run scoped from its record) raises :class:`WorkspaceScopeRequired`
    rather than answering for the process default. With auth off, a workspace no
    store holds (keyless dev naming an id)
    belongs to the deployment's ``default`` account. With auth on, no account owns
    such a workspace, so it raises :class:`WorkspaceForbidden` — the same answer
    :func:`assert_workspace_allowed` gives. A store error propagates: a caller
    resolving credentials must not fall back to the deployment's on an unknown
    answer."""
    from cairn.core.auth_tokens import auth_enabled  # noqa: PLC0415 — keep the import graph of this module flat

    if not (workspace_id or "").strip() and _active.get() is None and auth_enabled():
        # No workspace named and none in scope (no request, no record-scoped run):
        # work that has lost its run's workspace — a template's operation node on a
        # durable runtime's worker — must not resolve as the deployment.
        raise WorkspaceScopeRequired(
            "no workspace in scope: with auth on, connections and secrets resolve only "
            "inside a run's workspace")
    ws_id = (workspace_id or current_workspace()).strip() or DEFAULT_WORKSPACE
    ws = await (store or get_workspace_store()).get(ws_id)
    if ws is None:
        if auth_enabled():
            raise WorkspaceForbidden(ws_id)
        return DEFAULT_ACCOUNT
    return (getattr(ws, "account_id", "") or "").strip() or DEFAULT_ACCOUNT


# ── authorization (#1610, ADR-0094 D3/D4) ─────────────────────────────────
#
# THE chokepoint. Before this, `_WorkspaceMiddleware` took the workspace from
# the URL or the `X-Cairn-Workspace` header and set it active with no check of
# any kind — so a workspace separated data without protecting it, and any
# caller could name any workspace (#1609).
#
# It is ONE function on purpose. Today it enforces ACCOUNT membership: every
# member of an account may use every workspace in it (ADR-0094 D4). That is
# deliberately simple and deliberately insufficient for later — a company where
# finance must not read the ML team's audit trail needs per-workspace ACLs.
# Keeping the decision here makes that a change to this function instead of to
# every call site, which is the same reachability lesson as #1376/#1402: a
# chokepoint must be a FUNCTION others call, not a BLOCK others copy.


class WorkspaceScopeRequired(RuntimeError):
    """Work that resolves an account's connections or secrets has no workspace to
    resolve them in, and auth is on. Raised instead of resolving as the
    deployment."""


class WorkspaceForbidden(Exception):
    """The caller may not act in this workspace.

    Callers surface this as **404, not 403** — cross-tenant existence must not
    leak, matching `_run_in_ws_or_404` and ADR-0051 §4. "You may not touch it"
    and "it is not there" have to be indistinguishable from outside.
    """


async def assert_workspace_allowed(
    workspace_id: Optional[str],
    account_id: Optional[str],
    *,
    store: Any = None,
) -> None:
    """Raise :class:`WorkspaceForbidden` unless ``account_id`` may act in
    ``workspace_id``.

    Permissive in exactly three places, each on purpose:

    * ``workspace_id`` falsy — no workspace asserted, so the default applies and
      there is nothing to authorize.
    * ``account_id`` falsy — auth is OFF (keyless dev / single-node OSS). The
      deployment has no tenant boundary to enforce, and inventing one here
      would break every existing single-node install.
    * the workspace does not exist AND auth is off — keyless dev may name an id
      before (or without) creating it.

    With auth on, a workspace no store holds is refused like another account's:
    no account owns it, so no caller may act in it. Otherwise any account could
    write records under an id that does not exist yet, and whoever later
    creates that workspace would inherit them. The refusal is the same
    :class:`WorkspaceForbidden` (a 404 to the caller) as a cross-account id, so
    it does not tell a caller which ids exist.
    """
    ws_id = (workspace_id or "").strip()
    acct = (account_id or "").strip()
    if not ws_id or not acct:
        return
    ws = await (store or get_workspace_store()).get(ws_id)
    if ws is None:
        from cairn.core.auth_tokens import auth_enabled  # noqa: PLC0415 — keep the import graph of this module flat

        if auth_enabled():
            raise WorkspaceForbidden(ws_id)
        return
    if (ws.account_id or DEFAULT_ACCOUNT) != acct:
        raise WorkspaceForbidden(ws_id)
