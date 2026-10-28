"""
Single surface for every environment knob templates_v2 reads.

Goals:
  - One place to see *what knobs exist* — the :data:`INVENTORY` list. This is
    the source of truth ``cairn doctor`` (Phase 7) inspects to check what's
    set vs unset per profile.
  - One place to change defaults / rename a key — accessors live here, not
    scattered across 7 modules.
  - **Lazy** reads, not a cached settings object. Tests (and the dispatcher /
    investigation store) rely on per-call ``os.environ.get`` so an env mutation
    in one test takes effect immediately. Caching a frozen object would break that.

What stays out of here:
  - ``OPENAI_API_KEY`` and the rest of LLM/provider config — handled by
    ``src.core.settings`` (the OLD layer that already owns provider creds and
    the LLM gateway). This module is for *templates_v2 operational* knobs.
  - Per-MCP-server vars (``CAIRN_MCP_<NAME>_{URL,COMMAND,TRANSPORT,ARGS,ENV}``) are
    dynamic — they depend on which servers are declared in ``CAIRN_MCP_SERVERS``.
    The inventory marks the dynamic block; ``mcp_client`` expands per server.

The prefix (ADR-0122): every cairn variable is ``CAIRN_<NAME>``. The former
``OBS_<NAME>`` spelling is still read until 1.0 (#2432) — :func:`getenv` is the one
lookup that does it, and every module reads a cairn variable through it.
"""

from __future__ import annotations

import hashlib
import logging
import os
from urllib.parse import urlsplit, urlunsplit
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Mapping, Optional


@dataclass(frozen=True)
class SettingSpec:
    """One declared environment knob, for the inventory + the doctor preflight."""

    name: str
    env_var: str
    default: Optional[str]
    category: str                  # dispatcher / store / actions / notifier / slack / mcp
    description: str
    secret: bool = False           # doctor masks the value when printing
    required: bool = False         # doctor flags as MISSING when unset

    @property
    def legacy_env_var(self) -> Optional[str]:
        """The ``OBS_*`` spelling still read until 1.0, or None for a third-party name."""
        return legacy_name(self.env_var) if self.env_var.startswith(ENV_PREFIX) else None


# ─── The environment prefix (ADR-0122) ──────────────────────────────

#: Every cairn variable is ``CAIRN_<NAME>`` (ADR-0122 D1).
ENV_PREFIX = "CAIRN_"
#: The former prefix: ``OBS_<NAME>`` is read, with a warning, until 1.0 (D2, D5 #2432).
LEGACY_ENV_PREFIX = "OBS_"

_log = logging.getLogger(__name__)
_WARNED: set[str] = set()


def legacy_name(name: str) -> str:
    """``CAIRN_<NAME>`` → ``OBS_<NAME>``, the spelling read until 1.0."""
    if not name.startswith(ENV_PREFIX):
        raise ValueError(f"{name!r} is not a cairn variable (they start {ENV_PREFIX})")
    return LEGACY_ENV_PREFIX + name[len(ENV_PREFIX):]


def getenv(name: str, default: Optional[str] = None, *,
           environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """The value of cairn variable ``name`` (``CAIRN_<NAME>``) — THE lookup (ADR-0122 D2).

    ``CAIRN_<NAME>`` wins. When it is unset or empty, ``OBS_<NAME>`` is read, and the
    first time a process reads a non-empty ``OBS_<NAME>`` it logs one deprecation
    warning naming the new variable. An empty value counts as unset for the
    fallback (the codebase treats ``""`` as unset), but is still returned in
    preference to ``default`` — so ``getenv(n, d)`` behaves like
    ``os.environ.get(n, d)``. ``environ`` reads a mapping other than the process
    environment (a parsed ``.env``, a rendered values file). A name outside the
    prefix (``RAG_VECTOR_STORE``) is a plain lookup.
    """
    src = os.environ if environ is None else environ
    if not name.startswith(ENV_PREFIX):
        return src.get(name, default)
    value = src.get(name)
    if value:
        return value
    old_name = legacy_name(name)
    old = src.get(old_name)
    if old:
        if old_name not in _WARNED:
            _WARNED.add(old_name)
            _log.warning("%s is deprecated: set %s instead (ADR-0122; %s is read until 1.0)",
                         old_name, name, old_name)
        return old
    if value is not None:
        return value
    return old if old is not None else default


# ─── Dispatcher / lanes ─────────────────────────────────────────────


_DEFAULT_LANE_CONCURRENCY: dict[str, int] = {"realtime": 4, "background": 2}


def lane_concurrency(lane: str) -> int:
    """Max in-flight investigations on a lane. Env override per lane."""
    raw = getenv(f"CAIRN_{lane.upper()}_CONCURRENCY")
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return _DEFAULT_LANE_CONCURRENCY.get(lane, 2)


def pg_pool_max(default: int) -> int:
    """Per-store asyncpg pool ceiling (#666). Every Postgres store caps its pool
    here so the TOTAL connections — summed across ~16 stores × every container
    (server + main worker + remote worker) — stays under Postgres
    ``max_connections``. Unset → the caller's own ``default`` (no behavior
    change); set ``CAIRN_PG_POOL_MAX=N`` to cap EVERY pool at N on a
    connection-constrained deploy (e.g. the multi-worker Temporal stack)."""
    raw = getenv("CAIRN_PG_POOL_MAX", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return default


# ─── Shared sqlite db (#427/ADR-0058) ───────────────────────────────


def sqlite_path_from_url(url: str) -> Optional[str]:
    """The file a ``sqlite://…`` URL names — THE one parser (#1471).

    Six call sites parsed this by hand and two conventions were in play:
    ``sqlite:///abs/path`` (cairn's own, `sqlite://` + an absolute path) and
    ``sqlite:////abs/path`` (SQLAlchemy's, where 3 slashes mean *relative*).
    Stripping a different number of slashes per store meant ONE ``CAIRN_STORAGE``
    resolved to two different files — the config family landing on a
    CWD-relative path while runs/artifacts used the absolute one.

    Both spellings now resolve to the same absolute file; ``:memory:`` and
    non-sqlite URLs return ``None`` so callers apply their own default (an
    in-memory DB cannot be *shared* between stores — each connection to
    ``:memory:`` is its own database).
    """
    url = (url or "").strip()
    if not url.startswith("sqlite://"):
        return None
    path = url[len("sqlite://"):]
    if not path or path == ":memory:":
        return None
    while path.startswith("//"):     # sqlite:////abs → //abs → /abs
        path = path[1:]
    return path


def default_sqlite_db() -> str:
    """Default sqlite file shared by the durable config-family stores
    (policy, oauth, triggers, schedules, profiles, connections, installs,
    pilot scores, accounts, workspaces, workflow registry, remote jobs, rag
    catalog, secrets — #427/ADR-0058 decision #5). Table names across these
    stores are unique, so one file is safe.

    ``CAIRN_SQLITE_DB`` overrides this for every store at once; each store's
    own ``CAIRN_*_DB`` env var still takes precedence over this default.
    """
    override = getenv("CAIRN_SQLITE_DB", "").strip()
    if override:
        return override
    # #427 says CAIRN_STORAGE is THE selector — but only the run store parsed a
    # sqlite URL, so `cairn dev --db X` pinned runs to X while pipelines (and
    # every other config-family store) silently fell to ~/.cairn/cairn.db and
    # leaked prior sessions into the dev view (found live, #1195 slice 2).
    # #1471 — through the ONE parser, so this lands on the same file the
    # registry stores use (it previously stripped a slash more than they did).
    return sqlite_path_from_url(getenv("CAIRN_STORAGE", "")) or str(
        Path.home() / ".cairn" / "cairn.db")


def store_identity(sqlite_path: Optional[str] = None) -> Optional[tuple[str, str]]:
    """``(label, fingerprint)`` naming the store this process's config-family stores
    use, or ``None`` for an in-memory one (#229).

    ``CAIRN_STORAGE`` selects it as the stores do: a Postgres DSN is labelled
    ``postgres <host>:<port>/<db>`` (never the user or password), anything else is the
    SQLite file, ``sqlite_path`` when the caller's store has its own path, else
    :func:`default_sqlite_db`, labelled by its resolved path. The fingerprint, a
    short sha256 of the label, is what ``/healthz`` advertises, so the CLI can tell
    whether it writes the store a running engine reads without either side showing
    the other a path.
    """
    url = getenv("CAIRN_STORAGE", "").strip()
    if url.startswith(("postgres://", "postgresql://")):
        parts = urlsplit(url)
        try:
            port = parts.port or 5432
        except ValueError:
            port = 5432
        label = f"postgres {parts.hostname or 'localhost'}:{port}/{parts.path.lstrip('/')}"
    else:
        path = sqlite_path or default_sqlite_db()
        if not path or path == ":memory:":
            return None
        label = f"sqlite {os.path.realpath(path)}"  # as sqlite3 opens it: no `~` expansion
    return label, hashlib.sha256(label.encode()).hexdigest()[:16]


def connection_store_identity() -> Optional[tuple[str, str]]:
    """:func:`store_identity` of the connection store, chosen as
    ``connections.default_connection_store`` chooses it: ``CAIRN_STORAGE`` first, then
    the ``CAIRN_CONNECTIONS_DB`` SQLite path. The engine's ``/healthz`` and the CLI's
    store check both call this, so a ``CAIRN_CONNECTIONS_DB`` set on either side is
    compared, not silently ignored on one of them (#229)."""
    return store_identity(getenv("CAIRN_CONNECTIONS_DB", "").strip() or None)


# ─── Run store ───────────────────────────────────────────────────────


def run_db_path() -> Optional[str]:
    """SQLite path for RunStore. ``None`` → in-memory (volatile)."""
    return getenv("CAIRN_RUN_DB") or None


def run_db_is_set() -> bool:
    return bool(getenv("CAIRN_RUN_DB"))


# ─── Action handling ─────────────────────────────────────────────────


def action_handler() -> str:
    """``in_process`` — the only handler kind (ADR-0024: durability is the
    runtime seam, not a handler tier)."""
    return (getenv("CAIRN_ACTION_HANDLER") or "in_process").lower()


def checkpointer() -> str:
    """Durable-HITL checkpointer tier (ADR-0024 RM3): ``memory`` (default),
    ``sqlite``, or ``postgres``."""
    return (getenv("CAIRN_CHECKPOINTER") or "memory").lower()


def checkpointer_path() -> str:
    """SQLite path for CAIRN_CHECKPOINTER=sqlite."""
    return getenv("CAIRN_CHECKPOINTER_PATH") or str(
        Path.home() / ".cairn" / "checkpoints.sqlite"
    )


def checkpointer_dsn() -> Optional[str]:
    """Postgres DSN for CAIRN_CHECKPOINTER=postgres."""
    return getenv("CAIRN_CHECKPOINTER_DSN")


# ─── Runtime (StateGraph executor) ───────────────────────────────────


def runtime() -> str:
    """Engine runtime: ``inprocess`` (default), ``dbos``, or ``temporal``.

    ``inprocess`` runs the langgraph Pregel engine in this process — fast,
    no external deps, the path every existing test exercises.

    ``dbos`` wraps each run as a DBOS workflow for single-node durable
    execution — crash-recovery on restart + durable HITL, library-only (no
    server), backed by SQLite (dev) or Postgres (prod) per ADR-0035. Requires
    ``pip install dbos``; the LangGraph checkpointer owns resume.

    ``temporal`` submits each run as a Temporal workflow. Per-node retries,
    durable replay, the langgraph plugin handles execution. Requires
    ``pip install 'temporalio[langgraph]'`` and a reachable cluster.
    """
    pv = profile_runtime(getenv("CAIRN_PROFILE"))
    if pv is not None:
        return pv
    return (getenv("CAIRN_RUNTIME") or "inprocess").lower()


# #1852 — the profile's `durable_runtime` was accepted and silently ignored.
# profile_backends resolves profile → env → default for the UI and says of
# itself "it does not itself rewire any consumer (they still read their env
# vars today)". This is the rewiring, done at the one function all twelve
# consumers already call, so a profile that pins the runtime actually pins it.
# Cached per (path, mtime) because runtime() sits on hot paths (dispatcher
# resume, the reconciler loop, every compiled template's run()).
_PROFILE_RUNTIME_CACHE: dict[str, tuple[int, Optional[str]]] = {}
_RUNTIME_VOCAB = {"in_process": "inprocess", "inprocess": "inprocess",
                  "temporal": "temporal", "dbos": "dbos"}


def profile_runtime(profile_path: Optional[str]) -> Optional[str]:
    """The runtime a profile pins, in ``runtime()``'s vocabulary, or None.

    None when no profile is named, the file is not reachable from here (a
    container-only path seen from the host — the doctor's case), or the
    profile sets no ``durable_runtime``. A value that is not one of
    in_process / temporal / dbos RAISES: a governance control that is
    accepted and silently ignored is the defect this exists to close, and a
    typo falling through to the env is that defect again (#1852).
    """
    path = (profile_path or "").strip()
    if not path:
        return None
    from pathlib import Path  # noqa: PLC0415 — stdlib; kept local to match this module's style of tiny helpers

    pf = Path(path)
    if not pf.is_file():
        return None
    try:
        mtime = pf.stat().st_mtime_ns
    except OSError:
        return None
    hit = _PROFILE_RUNTIME_CACHE.get(path)
    if hit is not None and hit[0] == mtime:
        return hit[1]
    # lazy: cairn.core.pack.profile → pack loader → … → settings (cycle)
    from cairn.core.pack.profile import load_profile  # noqa: PLC0415

    raw = (load_profile(pf).backends or {}).get("durable_runtime")
    if raw is None or str(raw).strip() == "":
        val: Optional[str] = None
    else:
        key = str(raw).strip().lower()
        if key not in _RUNTIME_VOCAB:
            raise ValueError(
                f"profile {path} sets durable_runtime={raw!r}, which is not a "
                "runtime. Accepted: in_process, temporal, dbos. A profile value "
                "that is not understood must not fall through to CAIRN_RUNTIME — "
                "the environment would believe the runtime is pinned when it is "
                "not. (#1852)"
            )
        val = _RUNTIME_VOCAB[key]
    _PROFILE_RUNTIME_CACHE[path] = (mtime, val)
    return val


def temporal_address() -> str:
    return getenv("CAIRN_TEMPORAL_ADDRESS") or "localhost:7233"


def temporal_namespace() -> str:
    return getenv("CAIRN_TEMPORAL_NAMESPACE") or "default"


def temporal_worker_build() -> str:
    """Build id stamped onto the task queue so workers running divergent code
    land on different queues and cannot corrupt one another's workflow replays
    (issue #22). Explicit ``CAIRN_WORKER_BUILD`` wins; otherwise the installed
    cairn version (``dev`` if not installed). This approximates Temporal's
    native Worker Versioning (Build IDs / Worker Deployments) without the
    cluster-side version-set/deployment configuration that requires.
    """
    explicit = getenv("CAIRN_WORKER_BUILD")
    if explicit:
        return explicit
    try:
        return version("cairndev")  # the distribution (ADR-0121 D3); the import name stays `cairn`
    except PackageNotFoundError:
        return "dev"


def temporal_task_queue() -> str:
    base = getenv("CAIRN_TEMPORAL_TASK_QUEUE") or "cairn"
    return f"{base}-{temporal_worker_build()}"


def temporal_remote_task_queue() -> Optional[str]:
    """The dedicated task queue for remote-job (SSH/GPU) node activities (#565).

    A pull-based worker (``cairn worker --remote``) polls this queue; the main
    worker's workflow routes remote-job nodes to it via per-node ``task_queue``
    metadata. Returns ``None`` when unset — remote-job nodes then stay on the
    default queue (:func:`temporal_task_queue`), i.e. today's single-worker
    behavior (backward compatible). Set ``CAIRN_TEMPORAL_REMOTE_TASK_QUEUE`` (a
    bare base name; the build id is appended to match the worker) to enable.
    """
    base = getenv("CAIRN_TEMPORAL_REMOTE_TASK_QUEUE")
    if not base:
        return None
    return f"{base}-{temporal_worker_build()}"


def temporal_dataset_task_queue() -> Optional[str]:
    """The dedicated task queue for CPU-bound dataset-batch node activities (#962).

    A worker (``cairn worker --dataset``) on an isolated container — its own
    cgroup CPU quota + ``CAIRN_BATCH_RESERVED_CORES=0`` — polls this queue; the
    main worker routes Shardable dataset-operator nodes to it via per-node
    ``task_queue`` metadata, so a heavy scan never starves the interactive
    worker. Returns ``None`` when unset — dataset nodes then stay on the default
    queue and run the local budgeted pool there (dispatcher "C", no isolation but
    non-blocking), i.e. today's behavior (backward compatible). Set
    ``CAIRN_TEMPORAL_DATASET_TASK_QUEUE`` (a bare base name; the build id is
    appended to match the worker) to enable dispatcher "A"."""
    base = getenv("CAIRN_TEMPORAL_DATASET_TASK_QUEUE")
    if not base:
        return None
    return f"{base}-{temporal_worker_build()}"


def temporal_allow_empty() -> bool:
    """Permit the worker to boot with zero servable graphs (#412 override).

    Default False. Compiling a template instantiates its LLM client, so a
    missing provider key at worker startup (``OPENAI_API_KEY``) makes *every*
    compile fail — historically the worker swallowed those and came up with an
    empty registry, then answered every run "Graph not found". The worker now
    refuses to start in that state. Set ``CAIRN_TEMPORAL_ALLOW_EMPTY=1`` for a
    deliberately empty / activity-only worker.
    """
    return getenv("CAIRN_TEMPORAL_ALLOW_EMPTY", "").strip().lower() in {
        "1", "true", "yes", "on",
    }


# ─── DBOS runtime (ADR-0035) ─────────────────────────────────────────


def masked_dsn(dsn: str) -> str:
    """``dsn`` with any password replaced by ``***`` — for LOG LINES ONLY (#1867).

    The DBOS boot log printed the system-DB URL verbatim, password included, so
    anyone with ``docker logs`` (or a log shipper) held the Postgres credential.
    stdlib only: sqlalchemy is an ingestion-pack dep, not a core one. A URL with
    no password, or no netloc at all (``sqlite:///path``), comes back unchanged.
    """
    try:
        parts = urlsplit(dsn)
    except ValueError:
        return "<unparseable dsn>"
    if not parts.password:
        return dsn
    userinfo, _, hostport = parts.netloc.rpartition("@")
    user, _, _ = userinfo.partition(":")
    return urlunsplit(parts._replace(netloc=f"{user}:***@{hostport}"))


def dbos_dsn() -> str:
    """System database URL for the DBOS durable runtime (CAIRN_RUNTIME=dbos).

    ``sqlite:///path`` for dev (single file, no server) or ``postgres://…``
    for prod (ADR-0035). Unset → a local SQLite file under ~/.cairn.
    """
    return getenv("CAIRN_DBOS_DSN") or (
        f"sqlite:///{Path.home() / '.cairn' / 'dbos.sqlite'}"
    )


# ─── Notifier ────────────────────────────────────────────────────────


_DEFAULT_INBOX = str(Path.home() / ".cairn" / "inbox.jsonl")


def inbox_path() -> Path:
    return Path(getenv("CAIRN_INBOX_PATH", _DEFAULT_INBOX))


# ─── Slack (shared across notifier / approver / executor / mcp_client) ──


def slack_webhook_url() -> Optional[str]:
    return os.environ.get("SLACK_WEBHOOK_URL") or None


def slack_bot_token() -> Optional[str]:
    return os.environ.get("SLACK_BOT_TOKEN") or None


def slack_channel() -> Optional[str]:
    return os.environ.get("SLACK_CHANNEL") or os.environ.get("SLACK_CHANNEL_ID") or None


def approval_base_url() -> Optional[str]:
    return getenv("CAIRN_APPROVAL_BASE_URL") or None


# ─── MCP ─────────────────────────────────────────────────────────────


def mcp_slack_tool() -> str:
    return getenv("CAIRN_MCP_SLACK_TOOL", "slack_post_message")


def mcp_slack_channel_arg() -> str:
    return getenv("CAIRN_MCP_SLACK_CHANNEL_ARG", "channel")


# ─── Inventory (source of truth for doctor) ─────────────────────────


INVENTORY: list[SettingSpec] = [
    SettingSpec(
        "realtime_concurrency", "CAIRN_REALTIME_CONCURRENCY", "4", "dispatcher",
        "Max concurrent realtime runs (webhook-triggered).",
    ),
    SettingSpec(
        "background_concurrency", "CAIRN_BACKGROUND_CONCURRENCY", "2", "dispatcher",
        "Max concurrent background runs (scheduler / retrospective).",
    ),
    SettingSpec(
        "run_db", "CAIRN_RUN_DB", None, "store",
        "SQLite path for RunStore. Unset → in-memory (lost on restart).",
    ),
    SettingSpec(
        "action_handler", "CAIRN_ACTION_HANDLER", "in_process", "actions",
        "Action handler kind: 'in_process' (only kind — ADR-0024).",
    ),
    SettingSpec(
        "checkpointer", "CAIRN_CHECKPOINTER", "memory", "actions",
        "Durable-HITL checkpointer tier: memory | sqlite | postgres (ADR-0024 RM3).",
    ),
    SettingSpec(
        "checkpointer_path", "CAIRN_CHECKPOINTER_PATH",
        str(Path.home() / ".cairn" / "checkpoints.sqlite"), "actions",
        "SQLite path when CAIRN_CHECKPOINTER=sqlite.",
    ),
    SettingSpec(
        "checkpointer_dsn", "CAIRN_CHECKPOINTER_DSN", None, "actions",
        "Postgres DSN when CAIRN_CHECKPOINTER=postgres.", secret=True,
    ),
    SettingSpec(
        "inbox_path", "CAIRN_INBOX_PATH", _DEFAULT_INBOX, "notifier",
        "JSONL path the InboxNotifier appends to.",
    ),
    SettingSpec(
        "slack_webhook_url", "SLACK_WEBHOOK_URL", None, "slack",
        "Slack incoming-webhook URL (used by notifier, executor, approver).",
        secret=True,
    ),
    SettingSpec(
        "slack_bot_token", "SLACK_BOT_TOKEN", None, "slack",
        "Slack bot token (enables chat.postMessage + reversible/delete actions).",
        secret=True,
    ),
    SettingSpec(
        "slack_channel", "SLACK_CHANNEL", None, "slack",
        "Default Slack channel id/name. SLACK_CHANNEL_ID is also accepted.",
    ),
    SettingSpec(
        "approval_base_url", "CAIRN_APPROVAL_BASE_URL", None, "slack",
        "Public base URL for the /triggers/approve callback in Slack approvals.",
    ),
    SettingSpec(
        "mcp_servers", "CAIRN_MCP_SERVERS", "", "mcp",
        "Inline JSON dict of MCP server configs. Per-server vars (CAIRN_MCP_<NAME>_"
        "{URL,COMMAND,TRANSPORT,ARGS,ENV}) are also accepted as an alternative — both "
        "are expanded inside mcp_client (dynamic, not via settings accessors).",
    ),
    SettingSpec(
        "mcp_slack_tool", "CAIRN_MCP_SLACK_TOOL", "slack_post_message", "mcp",
        "Tool name on the configured Slack MCP server for posting messages.",
    ),
    SettingSpec(
        "mcp_slack_channel_arg", "CAIRN_MCP_SLACK_CHANNEL_ARG", "channel", "mcp",
        "Argument name the Slack MCP tool expects for the channel.",
    ),
    SettingSpec(
        "runtime", "CAIRN_RUNTIME", "inprocess", "runtime",
        "StateGraph executor: 'inprocess' (langgraph Pregel, default), 'dbos' "
        "(single-node durable via DBOS, SQLite/Postgres — ADR-0035), or 'temporal' "
        "(multi-node durable, per-node activities via temporalio[langgraph]).",
    ),
    SettingSpec(
        "temporal_address", "CAIRN_TEMPORAL_ADDRESS", "localhost:7233", "runtime",
        "Temporal cluster address (host:port). Used when CAIRN_RUNTIME=temporal.",
    ),
    SettingSpec(
        "temporal_namespace", "CAIRN_TEMPORAL_NAMESPACE", "default", "runtime",
        "Temporal namespace. Used when CAIRN_RUNTIME=temporal.",
    ),
    SettingSpec(
        "temporal_task_queue", "CAIRN_TEMPORAL_TASK_QUEUE", "cairn", "runtime",
        "Base Temporal task queue; the worker build is stamped on as "
        "cairn-<build>. Used when CAIRN_RUNTIME=temporal.",
    ),
    SettingSpec(
        "temporal_worker_build", "CAIRN_WORKER_BUILD", None, "runtime",
        "Build id stamped onto the Temporal task queue (cairn-<build>) so a "
        "worker running divergent code can't share a queue and corrupt another "
        "build's replays (issue #22). Defaults to the installed package version.",
    ),
    SettingSpec(
        "dbos_dsn", "CAIRN_DBOS_DSN", None, "runtime",
        "System database URL for the DBOS durable runtime (CAIRN_RUNTIME=dbos): "
        "sqlite:///path (dev) | postgres://… (prod, ADR-0035). Unset → local SQLite.",
        secret=True,  # a Postgres DSN carries the password
    ),
    # ── Registered here for doctor visibility; read at their use sites ──
    # (audit sweep 2026-06-13: these were read ad-hoc and invisible to doctor).
    SettingSpec(
        "storage_url", "CAIRN_STORAGE", None, "store",
        "Storage URL routing the run store: postgres://… | sqlite:///path. "
        "Unset → CAIRN_RUN_DB / in-memory. Read in core.storage.",
        secret=True,  # a Postgres DSN carries the password
    ),
    SettingSpec(
        "signup", "CAIRN_SIGNUP", "invite", "auth",
        "Self-serve signup mode (ADR-0094 D5): invite (default — joining is by "
        "invite link only) | domain (a work email matching an account's domain "
        "self-serves into it) | open (anyone creates an account; hosted demo "
        "only). Unrecognised values fall back to invite — a typo must fail "
        "CLOSED. Read in core.signup.",
    ),
    # #2336 (ADR-0119) — sign in / sign up with Google and GitHub. A provider
    # with no client id is not offered. Read in core.oauth_login. NOTE: the
    # outbound OAuth broker (core.oauth.broker) reads the same CLIENT_ID/SECRET
    # names for its connections — see ADR-0119 Consequences.
    SettingSpec(
        "oauth_google_client_id", "CAIRN_OAUTH_GOOGLE_CLIENT_ID", None, "auth",
        "Google OAuth client id: offers 'Continue with Google' on sign-in and sign-up.",
    ),
    SettingSpec(
        "oauth_google_client_secret", "CAIRN_OAUTH_GOOGLE_CLIENT_SECRET", None, "auth",
        "Google OAuth client secret (pairs with CAIRN_OAUTH_GOOGLE_CLIENT_ID).", secret=True,
    ),
    SettingSpec(
        "oauth_github_client_id", "CAIRN_OAUTH_GITHUB_CLIENT_ID", None, "auth",
        "GitHub OAuth app client id: offers 'Continue with GitHub' on sign-in and sign-up.",
    ),
    SettingSpec(
        "oauth_github_client_secret", "CAIRN_OAUTH_GITHUB_CLIENT_SECRET", None, "auth",
        "GitHub OAuth app client secret (pairs with CAIRN_OAUTH_GITHUB_CLIENT_ID).", secret=True,
    ),
    SettingSpec(
        "public_url", "CAIRN_PUBLIC_URL", None, "auth",
        "The deployment's public base URL (https://cairn.example.com). Sign-in "
        "callbacks are <base>/auth/oauth/<provider>/callback; unset → the URL the "
        "request arrived on, which is wrong behind a proxy.",
    ),
    SettingSpec(
        "profile", "CAIRN_PROFILE", None, "profile",
        "Active deployment profile (name or path). Resolves [profile.bindings] "
        "for redact.text / audit.sink / rag.search etc. Read in core.pack.profile.",
    ),
    SettingSpec(
        "bus", "CAIRN_BUS", "inprocess", "bus",
        "Bus backend (ADR-0010): inprocess (default) or a postgres://… DSN "
        "(Tier 2). redis://… is refused (ADR-0127 D5). Read in core.bus.",
    ),
    SettingSpec(
        "events_path", "CAIRN_EVENTS_PATH", "<repo>/events.jsonl", "obs",
        "Request-telemetry JSONL sink path. Read in core.obs.events.",
    ),
    SettingSpec(
        "rag_autoindex", "CAIRN_RAG_AUTOINDEX", None, "rag",
        "Opt-in RAG lifecycle indexing at startup + freshness schedule "
        "(ADR-0023). Truthy values: 1/true/yes/on. Read in services.rag.lifecycle.",
    ),
    SettingSpec(
        "rag_catalog", "CAIRN_RAG_CATALOG", None, "rag",
        "SQLite path for the RAG index catalog (persistence opt-in; unset → "
        "in-memory). Read in core.storage.rag_catalog.",
    ),
    SettingSpec(
        "audit_log", "CAIRN_AUDIT_LOG", "audit.jsonl", "safety",
        "Hash-chained audit log path (builtin cairn-operator-audit-store sink).",
    ),
    SettingSpec(
        "artifacts_dir", "CAIRN_ARTIFACTS_DIR", None, "operators",
        "Root dir for cairn-operator-artifact-local stored artifacts.",
    ),
    SettingSpec(
        "deploys_file", "CAIRN_DEPLOYS_FILE", None, "operators",
        "Deploy-history file consumed by cairn-operator-git correlation.",
    ),
    SettingSpec(
        "runs_dir", "CAIRN_RUNS_DIR", None, "cli",
        "Directory for CLI run artifacts (cli.runs reader/writer).",
    ),
    # ── #2412 (ADR-0122 D3) — every CAIRN_* name the code reads, inventoried ──
    # Read at their use sites (not migrated to accessors here); the names are
    # pinned by tests/core/test_env_inventory.py, which fails on a read with no
    # row and on a row nothing reads. A name with <NAME> is built at runtime.

    # control-plane server
    SettingSpec(
        "trigger_port", "CAIRN_TRIGGER_PORT", "9090", "server",
        "Port the HTTP control plane (cairn.core.triggers.main) listens on.",
    ),
    SettingSpec(
        "trigger_host", "CAIRN_TRIGGER_HOST", None, "server",
        "Bind address for the control plane. Unset → 0.0.0.0 when auth is on, "
        "127.0.0.1 when it is off.",
    ),
    SettingSpec(
        "environment", "CAIRN_ENVIRONMENT", "production", "server",
        "Default environment a run executes in (the Runner's default_environment); "
        "see CAIRN_PLAYGROUND_ENV.",
    ),
    SettingSpec(
        "obs_backend", "CAIRN_BACKEND", "mock", "server",
        "Telemetry backend the observability tools query: mock (bundled fixtures) | "
        "otel | clickstack. Read via provider_settings (pydantic) and deploy_check.",
    ),
    SettingSpec(
        "ops_mcp", "CAIRN_OPS_MCP", None, "server",
        "Truthy → mount the reads-first ops MCP surface on the control plane "
        "(core.triggers.mcp). Off by default.",
    ),
    SettingSpec(
        "light", "CAIRN_LIGHT", None, "server",
        "Truthy → the cairn-light preset: the held console surfaces are off "
        "(core.features, #1878).",
    ),
    SettingSpec(
        "feature_flag", "CAIRN_FEATURE_<NAME>", None, "server",
        "Per-feature on|off override, one variable per FEATURE_IDS entry "
        "(CAIRN_FEATURE_PLAYGROUND, …); wins over CAIRN_LIGHT (core.features, #1879).",
    ),
    SettingSpec(
        "admin_host", "CAIRN_ADMIN_HOST", "127.0.0.1", "server",
        "Bind address of the standalone cairn-admin server (core.admin).",
    ),
    SettingSpec(
        "admin_port", "CAIRN_ADMIN_PORT", "9091", "server",
        "Port of the standalone cairn-admin server (core.admin).",
    ),
    SettingSpec(
        "admin_user", "CAIRN_ADMIN_USER", "admin", "server",
        "Username for the cairn-admin view (core.admin).",
    ),
    SettingSpec(
        "admin_password", "CAIRN_ADMIN_PASSWORD", "admin", "server",
        "Password for the cairn-admin view (core.admin).", secret=True,
    ),
    SettingSpec(
        "admin_secret", "CAIRN_ADMIN_SECRET", None, "server",
        "Session-signing secret for cairn-admin. Unset → a dev default and a warning.",
        secret=True,
    ),
    # auth and tenancy
    SettingSpec(
        "jwt_secret", "CAIRN_JWT_SECRET", None, "auth",
        "Control-plane JWT signing secret. Set → auth is on (core.auth_tokens).",
        secret=True,
    ),
    SettingSpec(
        "jwt_algorithm", "CAIRN_JWT_ALGORITHM", "HS256", "auth",
        "JWT signing algorithm (core.auth_tokens).",
    ),
    SettingSpec(
        "access_token_expire_minutes", "CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES", "30", "auth",
        "Lifetime of an issued access token, in minutes (core.auth_tokens).",
    ),
    SettingSpec(
        "refresh_token_days", "CAIRN_REFRESH_TOKEN_DAYS", "30", "auth",
        "Lifetime of a remembered console sign-in (the refresh cookie and its server "
        "row), in days; a sign-in that is not remembered lasts 12 hours "
        "(core.refresh_tokens, ADR-0142).",
    ),
    SettingSpec(
        "api_user", "CAIRN_API_USER", "admin", "auth",
        "Username of the env-configured API user (core.triggers.auth).",
    ),
    SettingSpec(
        "api_password_hash", "CAIRN_API_PASSWORD_HASH", None, "auth",
        "argon2 hash of the API user's password (cairn remote hash-password); wins "
        "over CAIRN_API_PASSWORD.", secret=True,
    ),
    SettingSpec(
        "api_password", "CAIRN_API_PASSWORD", None, "auth",
        "Plain-text API user password (dev); CAIRN_API_PASSWORD_HASH is preferred.",
        secret=True,
    ),
    SettingSpec(
        "require_auth", "CAIRN_REQUIRE_AUTH", None, "auth",
        "Deployment intent that auth is on; cairn deploy doctor warns when it is "
        "explicitly off (#391).",
    ),
    SettingSpec(
        "login_max_failures", "CAIRN_LOGIN_MAX_FAILURES", "5", "auth",
        "Failed logins per (ip, username) before the lockout window starts.",
    ),
    SettingSpec(
        "login_lockout_seconds", "CAIRN_LOGIN_LOCKOUT_SECONDS", "60", "auth",
        "Length of the login lockout window, in seconds.",
    ),
    SettingSpec(
        "workspace", "CAIRN_WORKSPACE", "default", "auth",
        "Workspace id used when no request context names one (core.workspace). With auth "
        "on it must name an existing workspace (`default` always exists).",
    ),
    SettingSpec(
        "oauth_broker", "CAIRN_OAUTH_BROKER", "authlib", "auth",
        "Outbound OAuth broker for connections (core.oauth.broker, #310).",
    ),
    SettingSpec(
        "oauth_catalog", "CAIRN_OAUTH_CATALOG", None, "auth",
        "Path to an OAuth provider catalog that replaces the bundled providers.yaml.",
    ),
    SettingSpec(
        "oauth_redirect_uri", "CAIRN_OAUTH_REDIRECT_URI",
        "http://localhost:9090/oauth/callback", "auth",
        "Callback URL the outbound OAuth broker registers with providers.",
    ),
    # secrets
    SettingSpec(
        "secret_key", "CAIRN_SECRET_KEY", None, "secrets",
        "Fernet master key for the local encrypted secret store. Unset → OS "
        "keyring, then ~/.cairn/secret.key.", secret=True,
    ),
    SettingSpec(
        "secret_backend", "CAIRN_SECRET_BACKEND", "local", "secrets",
        "Where a credential entered in the console is written: local | infisical.",
    ),
    # stores: shared sqlite file and per-store overrides (#427/ADR-0058)
    SettingSpec(
        "sqlite_db", "CAIRN_SQLITE_DB", None, "store",
        "Shared sqlite file for the config-family stores. Unset → the file "
        "CAIRN_STORAGE names, else ~/.cairn/cairn.db.",
    ),
    SettingSpec(
        "pg_pool_max", "CAIRN_PG_POOL_MAX", None, "store",
        "Cap on every Postgres store's connection pool (#614). Unset → each "
        "store's own default (10 for the shared pool).",
    ),
    SettingSpec(
        "pg_lock_pool_max", "CAIRN_PG_LOCK_POOL_MAX", "8", "store",
        "Connection pool size for Postgres advisory locks (dispatch.coordination).",
    ),
    SettingSpec(
        "account_db", "CAIRN_ACCOUNT_DB", None, "store",
        "sqlite path for the account store (overrides CAIRN_SQLITE_DB; #427).",
    ),
    SettingSpec(
        "connections_db", "CAIRN_CONNECTIONS_DB", None, "store",
        "sqlite path for the connection store; :memory: for in-memory (#427).",
    ),
    SettingSpec(
        "eval_ledger_db", "CAIRN_EVAL_LEDGER_DB", None, "store",
        "sqlite path for the eval ledger, knowledge corpora and suggestions stores (#427).",
    ),
    SettingSpec(
        "features_db", "CAIRN_FEATURES_DB", None, "store",
        "sqlite path for the feature-flag store; :memory: for in-memory.",
    ),
    SettingSpec(
        "installs_db", "CAIRN_INSTALLS_DB", None, "store",
        "sqlite path for the registry-install store (#427).",
    ),
    SettingSpec(
        "job_store", "CAIRN_JOB_STORE", None, "store",
        "Remote-job store: memory | (unset) sqlite. Postgres when CAIRN_STORAGE is postgres.",
    ),
    SettingSpec(
        "job_store_db", "CAIRN_JOB_STORE_DB", None, "store",
        "sqlite path for the remote-job store (#427).",
    ),
    SettingSpec(
        "oauth_token_db", "CAIRN_OAUTH_TOKEN_DB", None, "store",
        "sqlite path for the OAuth token store; :memory: for in-memory (#427).",
    ),
    SettingSpec(
        "pilot_db", "CAIRN_PILOT_DB", None, "store",
        "sqlite path for the pilot score store (#427).",
    ),
    SettingSpec(
        "pilot_ledger_db", "CAIRN_PILOT_LEDGER_DB", None, "store",
        "sqlite path for the pilot ledger (#427).",
    ),
    SettingSpec(
        "pipeline_db", "CAIRN_PIPELINE_DB", None, "store",
        "sqlite path for the pipeline record store.",
    ),
    SettingSpec(
        "policies_db", "CAIRN_POLICIES_DB", None, "store",
        "sqlite path for the policy store; :memory: for in-memory (#427).",
    ),
    SettingSpec(
        "profiles_db", "CAIRN_PROFILES_DB", None, "store",
        "sqlite path for the profile store (#427).",
    ),
    SettingSpec(
        "schedules_db", "CAIRN_SCHEDULES_DB", None, "store",
        "sqlite path for the schedule store; :memory: for in-memory (#427).",
    ),
    SettingSpec(
        "secrets_db", "CAIRN_SECRETS_DB", None, "store",
        "sqlite path for the local secret store (#427).",
    ),
    SettingSpec(
        "training_runs_db", "CAIRN_TRAINING_RUNS_DB", None, "store",
        "sqlite path for the training-run history store.",
    ),
    SettingSpec(
        "triggers_db", "CAIRN_TRIGGERS_DB", None, "store",
        "sqlite path for the trigger store; :memory: for in-memory (#427).",
    ),
    SettingSpec(
        "workflow_config_db", "CAIRN_WORKFLOW_CONFIG_DB", None, "store",
        "sqlite path for the workflow config store.",
    ),
    SettingSpec(
        "workflow_registry_db", "CAIRN_WORKFLOW_REGISTRY_DB", None, "store",
        "sqlite path for the workflow registry (#427).",
    ),
    SettingSpec(
        "workspace_db", "CAIRN_WORKSPACE_DB", None, "store",
        "sqlite path for the workspace store (#427).",
    ),
    SettingSpec(
        "workspace_vars_db", "CAIRN_WORKSPACE_VARS_DB", None, "store",
        "sqlite path for the workspace variables store.",
    ),
    # runtime
    SettingSpec(
        "checkpointer_pool_max", "CAIRN_CHECKPOINTER_POOL_MAX", "3", "runtime",
        "Postgres checkpointer pool ceiling (#632).",
    ),
    SettingSpec(
        "checkpointer_pool_max_idle", "CAIRN_CHECKPOINTER_POOL_MAX_IDLE", "60", "runtime",
        "Seconds an idle Postgres checkpointer connection is kept (#632).",
    ),
    SettingSpec(
        "temporal_allow_empty", "CAIRN_TEMPORAL_ALLOW_EMPTY", None, "runtime",
        "Truthy → the Temporal worker may boot with zero servable graphs (#412).",
    ),
    SettingSpec(
        "temporal_remote_task_queue", "CAIRN_TEMPORAL_REMOTE_TASK_QUEUE", None, "runtime",
        "Base name of the task queue for remote-job node activities (#565). Unset "
        "→ they stay on the default queue.",
    ),
    SettingSpec(
        "temporal_dataset_task_queue", "CAIRN_TEMPORAL_DATASET_TASK_QUEUE", None, "runtime",
        "Base name of the task queue for dataset-batch node activities (#962). "
        "Unset → they stay on the default queue.",
    ),
    SettingSpec(
        "flatten_composition", "CAIRN_FLATTEN_COMPOSITION", None, "runtime",
        "Force composed-template flattening on (1) or off (0); unset → on under "
        "temporal only (engine.compiler).",
    ),
    # triggers
    SettingSpec(
        "triggers_dir", "CAIRN_TRIGGERS_DIR", "~/.cairn/triggers", "triggers",
        "User-overlay directory of trigger specs (core.triggers.router).",
    ),
    SettingSpec(
        "trigger_durable", "CAIRN_TRIGGER_DURABLE", None, "triggers",
        "Truthy → triggers go through the durable trigger inbox (CAIRN_TRIGGER_INBOX).",
    ),
    SettingSpec(
        "trigger_inbox", "CAIRN_TRIGGER_INBOX", None, "triggers",
        "Trigger inbox: (unset) sqlite | memory | nats://host:port (core.storage.trigger_inbox).",
    ),
    SettingSpec(
        "trigger_inbox_db", "CAIRN_TRIGGER_INBOX_DB", "~/.cairn/trigger_inbox.db", "triggers",
        "sqlite path for the trigger inbox.",
    ),
    SettingSpec(
        "trigger_inbox_stream", "CAIRN_TRIGGER_INBOX_STREAM", "CAIRN_TRIGGER_INBOX", "triggers",
        "JetStream stream name for the nats trigger inbox.",
    ),
    SettingSpec(
        "trigger_inbox_subject", "CAIRN_TRIGGER_INBOX_SUBJECT", "trigger.inbox", "triggers",
        "JetStream subject for the nats trigger inbox.",
    ),
    SettingSpec(
        "trigger_inbox_consumer", "CAIRN_TRIGGER_INBOX_CONSUMER", "cairn-inbox-worker", "triggers",
        "Durable consumer name for the nats trigger inbox.",
    ),
    SettingSpec(
        "trigger_inbox_ack_wait", "CAIRN_TRIGGER_INBOX_ACK_WAIT", "60", "triggers",
        "Seconds JetStream waits for an ack before redelivering a trigger.",
    ),
    # remote jobs and workers
    SettingSpec(
        "remote_job_durable", "CAIRN_REMOTE_JOB_DURABLE", None, "jobs",
        "Truthy → remote jobs submit, pause and resume from the worker loop "
        "instead of polling inline (ADR-0038, #127).",
    ),
    SettingSpec(
        "remote_job_max_attempts", "CAIRN_REMOTE_JOB_MAX_ATTEMPTS", "2", "jobs",
        "Resume-from-checkpoint attempts for a retriable remote job; 0 disables (#552).",
    ),
    SettingSpec(
        "learning_abort", "CAIRN_LEARNING_ABORT", None, "jobs",
        "Truthy → abort a training job that is not learning (ADR-0039 Gate 2).",
    ),
    SettingSpec(
        "job_channel", "CAIRN_JOB_CHANNEL", None, "jobs",
        "Channel a remote job reports on: nats://… | http (store-backed) | "
        "(unset) in-memory (#2025).",
    ),
    SettingSpec(
        "job_channel_stream", "CAIRN_JOB_CHANNEL_STREAM", "CAIRN_REMOTE_JOBS", "jobs",
        "JetStream stream name for the nats job channel (#2025).",
    ),
    SettingSpec(
        "engine_url", "CAIRN_ENGINE_URL", None, "jobs",
        "Control-plane URL a remote box or flow leg's kernel posts live events to; the "
        "control plane hands it on under the same name (#2429: OBS_ENGINE_URL merged in).",
    ),
    SettingSpec(
        "remote_workers", "CAIRN_REMOTE_WORKERS", None, "jobs",
        "required/truthy → this deployment expects work to be pulled by workers "
        "(core.pull_worker_door).",
    ),
    SettingSpec(
        "train_image", "CAIRN_TRAIN_IMAGE", None, "jobs",
        "Container image for remote ML work: pinned on pull-lane jobs and the "
        "remote_image profile key (ADR-0104 D1, #2071).",
    ),
    # dataflow and ML engines (profile keys in core.profile_backends)
    SettingSpec(
        "dataflow_engine", "CAIRN_DATAFLOW_ENGINE", "builtin", "dataflow",
        "Engine for data flows: builtin | metaflow | ray | skypilot (ADR-0081).",
    ),
    SettingSpec(
        "dataflow_target", "CAIRN_DATAFLOW_TARGET", None, "dataflow",
        "compute.ssh connection the builtin data engine dispatches to; unset → "
        "local (#1050).",
    ),
    SettingSpec(
        "ml_engine", "CAIRN_ML_ENGINE", "builtin", "dataflow",
        "Engine for ML flows: builtin | metaflow | skypilot (#1078).",
    ),
    SettingSpec(
        "ml_target", "CAIRN_ML_TARGET", None, "dataflow",
        "compute.ssh connection, or worker:<pool>, the ML engine dispatches to; "
        "unset → local.",
    ),
    SettingSpec(
        "remote_exec_mode", "CAIRN_REMOTE_EXEC_MODE", "auto", "dataflow",
        "How the SSH engine runs a flow on a box: auto | image | venv (ADR-0104 D1).",
    ),
    SettingSpec(
        "remote_env", "CAIRN_REMOTE_ENV", None, "dataflow",
        "Names of control-plane env variables the SSH engine forwards to the job on the box; "
        "credential-looking names are refused (profile key remote_env).",
    ),
    SettingSpec(
        "dataflow_remote_cmd", "CAIRN_DATAFLOW_REMOTE_CMD", "cairn-dataflow-run", "dataflow",
        "Command the SSH engine runs on a venv box (#1075).",
    ),
    SettingSpec(
        "dataflow_image_cmd", "CAIRN_DATAFLOW_IMAGE_CMD", "cairn-dataflow-run", "dataflow",
        "Command the SSH engine runs inside the image (#1075).",
    ),
    SettingSpec(
        "dataflow_image_gpus", "CAIRN_DATAFLOW_IMAGE_GPUS", "all", "dataflow",
        "--gpus value for an image-mode run on an SSH box.",
    ),
    SettingSpec(
        "metaflow_root", "CAIRN_METAFLOW_ROOT", None, "dataflow",
        "Metaflow local metadata directory; unset → ~/.cairn/metaflow (ADR-0089, #1345).",
    ),
    SettingSpec(
        "metaflow_datastore", "CAIRN_METAFLOW_DATASTORE", None, "dataflow",
        "Metaflow datastore: s3 | (unset/local) files under the metadata root (#1345).",
    ),
    SettingSpec(
        "metaflow_datastore_root", "CAIRN_METAFLOW_DATASTORE_ROOT", None, "dataflow",
        "S3 URI the Metaflow datastore writes under (#1349).",
    ),
    SettingSpec(
        "metaflow_service_url", "CAIRN_METAFLOW_SERVICE_URL", None, "dataflow",
        "URL of a Metaflow metadata service, for cross-box re-attach (#1349).",
    ),
    SettingSpec(
        "metaflow_compute", "CAIRN_METAFLOW_COMPUTE", "local", "dataflow",
        "Where Metaflow runs the execute step: local | kubernetes | batch (#1346).",
    ),
    SettingSpec(
        "metaflow_image", "CAIRN_METAFLOW_IMAGE", None, "dataflow",
        "Container image for remote Metaflow tasks (kubernetes/batch).",
    ),
    SettingSpec(
        "metaflow_queue", "CAIRN_METAFLOW_QUEUE", None, "dataflow",
        "AWS Batch job queue for metaflow compute=batch.",
    ),
    SettingSpec(
        "metaflow_retry", "CAIRN_METAFLOW_RETRY", None, "dataflow",
        "Metaflow @retry count for the execute step; unset/0 → none.",
    ),
    SettingSpec(
        "metaflow_secrets", "CAIRN_METAFLOW_SECRETS", None, "dataflow",
        "Comma-separated connections whose credentials Metaflow flows may use (#1344).",
    ),
    SettingSpec(
        "metaflow_isolation", "CAIRN_METAFLOW_ISOLATION", "auto", "dataflow",
        "Environment a Metaflow task receives: auto | inherit | strict.",
    ),
    SettingSpec(
        "skypilot_image", "CAIRN_SKYPILOT_IMAGE", None, "dataflow",
        "Container image the SkyPilot-provisioned machine runs (ADR-0104 D5, #2073).",
    ),
    SettingSpec(
        "skypilot_use_spot", "CAIRN_SKYPILOT_USE_SPOT", None, "dataflow",
        "Truthy → ask SkyPilot for spot capacity (#2073).",
    ),
    SettingSpec(
        "skypilot_accelerator", "CAIRN_SKYPILOT_ACCELERATOR", None, "dataflow",
        "Accelerator type the SkyPilot target offers, e.g. A100 (#2073).",
    ),
    SettingSpec(
        "skypilot_setup", "CAIRN_SKYPILOT_SETUP", None, "dataflow",
        "Extra setup commands for the SkyPilot task.",
    ),
    SettingSpec(
        "skypilot_hourly_usd", "CAIRN_SKYPILOT_HOURLY_USD", None, "dataflow",
        "Hourly price used for the SkyPilot budget estimate when the target sets none.",
    ),
    # artifacts
    SettingSpec(
        "artifact_store", "CAIRN_ARTIFACT_STORE", "local", "artifacts",
        "Where flow exports are written: local | s3 | s3://bucket/prefix (#1442).",
    ),
    SettingSpec(
        "artifact_s3_bucket", "CAIRN_ARTIFACT_S3_BUCKET", None, "artifacts",
        "S3 bucket for artifact bytes and stage capture; set → capture to S3.",
    ),
    SettingSpec(
        "artifact_s3_prefix", "CAIRN_ARTIFACT_S3_PREFIX", None, "artifacts",
        "Key prefix under CAIRN_ARTIFACT_S3_BUCKET.",
    ),
    SettingSpec(
        "artifact_s3_region", "CAIRN_ARTIFACT_S3_REGION", None, "artifacts",
        "Region for the artifact S3 client; unset → AWS_DEFAULT_REGION.",
    ),
    SettingSpec(
        "artifact_s3_endpoint", "CAIRN_ARTIFACT_S3_ENDPOINT", None, "artifacts",
        "Custom S3 endpoint URL (MinIO and other S3-compatible stores).",
    ),
    # eval and suggestions
    SettingSpec(
        "autoeval", "CAIRN_AUTOEVAL", "on", "eval",
        "off → disable automatic evaluation of finished runs (core.eval.auto_eval).",
    ),
    SettingSpec(
        "autoeval_judge_sample_pct", "CAIRN_AUTOEVAL_JUDGE_SAMPLE_PCT", "0.2", "eval",
        "Fraction (0–1) of runs the automatic eval sends to an LLM judge.",
    ),
    SettingSpec(
        "continuous_eval", "CAIRN_CONTINUOUS_EVAL", None, "eval",
        "Truthy → continuous evaluation is on (core.eval.continuous).",
    ),
    SettingSpec(
        "suggest", "CAIRN_SUGGEST", None, "eval",
        "Truthy → generate improvement suggestions (off by default, #458).",
    ),
    SettingSpec(
        "suggest_actuator", "CAIRN_SUGGEST_ACTUATOR", "off", "eval",
        "Where an accepted suggestion goes: off | auto | github | git-pr | issue | db (#1969).",
    ),
    SettingSpec(
        "suggest_repo", "CAIRN_SUGGEST_REPO", None, "eval",
        "owner/repo for the GitHub REST suggestion actuator (#1969).",
    ),
    SettingSpec(
        "suggest_base", "CAIRN_SUGGEST_BASE", "main", "eval",
        "Base branch for suggestion pull requests.",
    ),
    # LLM
    SettingSpec(
        "gateway", "CAIRN_GATEWAY", None, "llm",
        "Gateway the console Gateway view reads: litellm | openrouter (core.gateway).",
    ),
    SettingSpec(
        "model_roles", "CAIRN_MODEL_ROLES", "~/.cairn/model_roles.json", "llm",
        "Path of the model-role overrides file (core.llm.models).",
    ),
    # policy
    SettingSpec(
        "policies_dir", "CAIRN_POLICIES_DIR", "~/.cairn/policies", "policy",
        "User-overlay directory of policy files (core.policy.engine).",
    ),
    # playground
    SettingSpec(
        "playground_env", "CAIRN_PLAYGROUND_ENV", "playground,sandbox", "playground",
        "Environments in which cheap writes really execute; any other stays stubbed (#844).",
    ),
    SettingSpec(
        "playground_s3_bucket", "CAIRN_PLAYGROUND_S3_BUCKET", None, "playground",
        "Playground scratch bucket (alias of PLAYGROUND_ARTIFACT_S3_BUCKET).",
    ),
    SettingSpec(
        "playground_upload_db", "CAIRN_PLAYGROUND_UPLOAD_DB", None, "playground",
        "sqlite path for Playground uploads; unset → in-memory.",
    ),
    # packs and registry
    SettingSpec(
        "registry_catalog", "CAIRN_REGISTRY_CATALOG", None, "packs",
        "Pack registry catalog (path or URL); unset → a sibling cairn-registry "
        "checkout, else the public cairn-extensions catalog.",
    ),
    SettingSpec(
        "registry_allowlist", "CAIRN_REGISTRY_ALLOWLIST", None, "packs",
        "Comma-separated allowlist for registry installs (core.registry_install).",
    ),
    # operators
    SettingSpec(
        "model_registry", "CAIRN_MODEL_REGISTRY", "~/.cairn/model_registry.jsonl", "operators",
        "JSONL file the model-registry operator appends to and the console reads.",
    ),
    SettingSpec(
        "postmortem_dir", "CAIRN_POSTMORTEM_DIR", "~/.cairn/postmortems", "operators",
        "Directory the postmortem operator writes files to.",
    ),
    SettingSpec(
        "watch_file_roots", "CAIRN_WATCH_FILE_ROOTS", "~/.cairn", "operators",
        "os.pathsep-separated roots a monitor-threshold watch may read files from.",
    ),
    SettingSpec(
        "watch_allow_private_urls", "CAIRN_WATCH_ALLOW_PRIVATE_URLS", None, "operators",
        "Truthy → a monitor-threshold watch may fetch loopback/private URLs.",
    ),
    # obs
    SettingSpec(
        "trace_content", "CAIRN_TRACE_CONTENT", "redacted", "obs",
        "How LLM message content appears in traces (core.obs.telemetry); unknown → redacted.",
    ),
    # rag
    SettingSpec(
        "sqlite_vec_db", "CAIRN_SQLITE_VEC_DB", None, "rag",
        "sqlite-vec store path; unset → CAIRN_RUN_DB, else ~/.cairn/runs.db.",
    ),
    # mcp: per-server variables, expanded by services.mcp.client for each <NAME>
    SettingSpec(
        "mcp_server_url", "CAIRN_MCP_<NAME>_URL", None, "mcp",
        "URL of MCP server <NAME> (http transport).",
    ),
    SettingSpec(
        "mcp_server_transport", "CAIRN_MCP_<NAME>_TRANSPORT", "streamable_http", "mcp",
        "Transport of MCP server <NAME> when CAIRN_MCP_<NAME>_URL is set.",
    ),
    SettingSpec(
        "mcp_server_command", "CAIRN_MCP_<NAME>_COMMAND", None, "mcp",
        "Command that starts MCP server <NAME> (stdio transport).",
    ),
    SettingSpec(
        "mcp_server_args", "CAIRN_MCP_<NAME>_ARGS", "[]", "mcp",
        "JSON list (or space-separated) arguments for CAIRN_MCP_<NAME>_COMMAND.",
    ),
    SettingSpec(
        "mcp_server_env", "CAIRN_MCP_<NAME>_ENV", None, "mcp",
        "JSON object of environment variables for CAIRN_MCP_<NAME>_COMMAND.",
    ),
    # cli
    SettingSpec(
        "remote_endpoint", "CAIRN_REMOTE_ENDPOINT", None, "cli",
        "Control-plane URL the CLI/app client talks to; unset → in-process "
        "(cairn remote falls back to http://localhost:9090).",
    ),
    SettingSpec(
        "remote_token", "CAIRN_REMOTE_TOKEN", None, "cli",
        "Bearer token for CAIRN_REMOTE_ENDPOINT.", secret=True,
    ),
    SettingSpec(
        "config_home", "CAIRN_CONFIG_HOME", "~/.cairn", "cli",
        "Directory holding the CLI's config.toml (cairn remote profiles).",
    ),
]


def get_inventory() -> list[SettingSpec]:
    """Core knobs plus every installed pack's contributed settings.

    Returns a fresh list so callers (e.g. doctor) can't mutate the core
    ``INVENTORY``. Pack settings are appended after the core ones. Imported
    lazily to keep this module free of any pack-registry import cycle.
    """
    from cairn.core.pack import all_settings_inventory, ensure_loaded

    ensure_loaded()
    return list(INVENTORY) + list(all_settings_inventory())
