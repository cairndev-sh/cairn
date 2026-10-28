"""Writable secret backends (#200) — store a credential VALUE the console accepts,
so a :class:`~cairn.core.connections.Connection` only ever holds a *reference*
(ADR-0033). The connection DB never stores the value.

Dispatched by the reference scheme:

  ``${env:NAME}`` / bare ``NAME``   env var (read-only; the existing default)
  ``cairn://secrets#KEY``           local encrypted file (~/.cairn/secrets.db, Fernet)
  ``infisical://<proj>/<path>#KEY`` Infisical (self-hosted/cloud, MIT core; optional ``[infisical]`` extra)
  ``vault://path#KEY``              HashiCorp Vault (future adapter)

:func:`put_secret(ref, value)` writes to the backend the ref names; :func:`resolve_secret(ref)`
reads it back (used by ``resolve_connection``). Values are encrypted at rest in the local
backend; this module never logs a value.

Account scope. A reference minted for a tenant account carries the account
(``cairn://secrets#<account>/<conn>-<KEY>``). Given the caller's ``account_id``,
:func:`resolve_secret` and :func:`put_secret` reach only that account's references:
a bare name or ``${env:…}`` (the server's environment), the deployment's own
``cairn://`` secrets and another account's are refused. With no account, or the
``default`` account a single-node or auth-off deployment runs in, nothing changes, and
references minted before accounts were namespaced keep resolving for the deployment.
"""
from __future__ import annotations
from cairn.core.settings import getenv

from cairn.core._aio import run_sync
from cairn.core.pg_schema_init import schema_init_lock
from cairn.core.settings import pg_pool_max

import os
import re
import sqlite3
from typing import Optional

# ── reference grammar ────────────────────────────────────────────────
_ENV_RE = re.compile(r"^\$\{env:(?P<name>[^}]+)\}$")
_SCHEMES = ("cairn", "infisical", "vault")


def ref_scheme(ref: str) -> str:
    """Backend selector for a reference. Bare names + ``${env:…}`` → env."""
    r = (ref or "").strip()
    for s in _SCHEMES:
        if r.startswith(f"{s}://"):
            return s
    return "env"


def _split(ref: str) -> tuple[str, str]:
    """``cairn://secrets#KEY`` → ("secrets", "KEY"); ``infisical://p/path#K`` → ("p/path", "K")."""
    body = ref.split("://", 1)[1]
    locator, _, key = body.partition("#")
    return locator, key


# ── local encrypted file (Fernet) ───────────────────────────────────
def _load_fernet():
    """Master key: ``CAIRN_SECRET_KEY`` (durable, for headless/prod) → OS keyring →
    a generated key persisted at ``~/.cairn/secret.key`` (0600, dev-grade)."""
    from cryptography.fernet import Fernet  # base dep (already required)

    env_key = getenv("CAIRN_SECRET_KEY", "").strip()
    if env_key:
        return Fernet(env_key.encode())
    try:
        import keyring  # base dep

        k = keyring.get_password("cairn", "secret-master-key")
        if not k:
            k = Fernet.generate_key().decode()
            keyring.set_password("cairn", "secret-master-key", k)
        return Fernet(k.encode())
    except Exception:  # noqa: BLE001 — no usable keyring (headless) → file-backed key
        path = os.path.expanduser("~/.cairn/secret.key")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if os.path.exists(path):
            k = open(path, encoding="utf-8").read().strip()
        else:
            k = Fernet.generate_key().decode()
            with open(path, "w", encoding="utf-8") as f:
                f.write(k)
            os.chmod(path, 0o600)
        return Fernet(k.encode())


class LocalFileSecretBackend:
    """Embedded-SQLite secret vault — values Fernet-encrypted at rest. Ref scheme
    ``cairn://secrets#KEY``. Mirrors the run/connection stores' sqlite pattern."""

    scheme = "cairn"

    def __init__(self, path: str | None = None) -> None:
        if path is None:
            from cairn.core import settings

            # single shared cairn.db (#427/ADR-0058); CAIRN_*_DB overrides
            path = getenv("CAIRN_SECRETS_DB", "").strip() or settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("CREATE TABLE IF NOT EXISTS secrets (key TEXT PRIMARY KEY, ciphertext TEXT NOT NULL)")
        self._conn.commit()
        self._fernet = _load_fernet()

    def put(self, key: str, value: str) -> str:
        token = self._fernet.encrypt(value.encode("utf-8")).decode("ascii")
        self._conn.execute("INSERT OR REPLACE INTO secrets (key, ciphertext) VALUES (?,?)", (key, token))
        self._conn.commit()
        return f"cairn://secrets#{key}"

    def get(self, key: str) -> str:
        row = self._conn.execute("SELECT ciphertext FROM secrets WHERE key=?", (key,)).fetchone()
        if row is None:
            raise KeyError(key)
        return self._fernet.decrypt(row[0].encode("ascii")).decode("utf-8")

    def delete(self, key: str) -> bool:
        cur = self._conn.execute("DELETE FROM secrets WHERE key=?", (key,))
        self._conn.commit()
        return cur.rowcount > 0

    def close(self) -> None:
        self._conn.close()

    def __repr__(self) -> str:  # never echo ciphertext/values
        return f"LocalFileSecretBackend(path={self.path!r})"


# ── Postgres backend (mirrors storage/postgres/runs.py) — #427 ─────────────
# Same Fernet-at-rest as LocalFileSecretBackend, but the ciphertext lives in the
# one prod Postgres (routed by CAIRN_STORAGE) instead of a per-container SQLite
# file. The put/get/delete surface is synchronous to match the sync facade
# (put_secret / resolve_secret), so each call drives asyncpg via a private loop.
_CREATE_SECRETS_PG_SQL = """
    CREATE TABLE IF NOT EXISTS secrets (
        key        TEXT PRIMARY KEY,
        ciphertext TEXT NOT NULL
    )
"""
_UPSERT_SECRET_PG_SQL = """
    INSERT INTO secrets (key, ciphertext) VALUES ($1, $2)
    ON CONFLICT (key) DO UPDATE SET ciphertext = EXCLUDED.ciphertext
"""
_SELECT_SECRET_PG_SQL = "SELECT ciphertext FROM secrets WHERE key = $1"
_DELETE_SECRET_PG_SQL = "DELETE FROM secrets WHERE key = $1"

_SECRETS_TABLE_EXISTS_PG_SQL = "SELECT to_regclass('secrets') IS NOT NULL"


class PostgresSecretBackend:
    """Postgres-backed secret vault — values Fernet-encrypted at rest (same key
    resolution as :class:`LocalFileSecretBackend`). Ref scheme ``cairn://secrets#KEY``.
    Selected by :func:`_local` when ``CAIRN_STORAGE=postgres://…`` so console-entered
    credentials live in the one prod Postgres instead of a per-container SQLite
    file (#427). The put/get/delete surface stays **synchronous** to match the
    facade; each call drives a short-lived asyncpg pool on a private event loop."""

    scheme = "cairn"

    def __init__(self, dsn: str | None = None) -> None:
        if dsn is None:
            dsn = getenv("CAIRN_STORAGE", "").strip()
        self._dsn = dsn
        self._fernet = _load_fernet()

    async def _run(self, fn):
        try:
            import asyncpg  # optional dep (storage-postgres)
        except ImportError as e:
            raise RuntimeError(
                "Postgres secret store requires the 'storage-postgres' optional dep "
                "group. Install with: pip install 'cairndev[storage-postgres]'"
            ) from e
        if not self._dsn:
            raise RuntimeError(
                "PostgresSecretBackend needs a DSN. Pass dsn= or set CAIRN_STORAGE=postgres://…"
            )
        pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=pg_pool_max(2))
        try:
            async with pool.acquire() as c:
                # Stateless (every call builds a private pool): the DDL and the
                # fleet-wide schema lock run only while the table is missing — two
                # replicas on a fresh database otherwise race the CREATE TABLE (#2551)
                if not await c.fetchval(_SECRETS_TABLE_EXISTS_PG_SQL):
                    async with schema_init_lock(c):
                        await c.execute(_CREATE_SECRETS_PG_SQL)
                return await fn(c)
        finally:
            await pool.close()

    def put(self, key: str, value: str) -> str:
        token = self._fernet.encrypt(value.encode("utf-8")).decode("ascii")

        async def _w(c):
            await c.execute(_UPSERT_SECRET_PG_SQL, key, token)
        run_sync(self._run(_w))
        return f"cairn://secrets#{key}"

    def get(self, key: str) -> str:
        async def _r(c):
            return await c.fetchrow(_SELECT_SECRET_PG_SQL, key)
        row = run_sync(self._run(_r))
        if row is None:
            raise KeyError(key)
        return self._fernet.decrypt(row["ciphertext"].encode("ascii")).decode("utf-8")

    def delete(self, key: str) -> bool:
        async def _d(c):
            return await c.execute(_DELETE_SECRET_PG_SQL, key)
        status = run_sync(self._run(_d))
        return status.split()[-1] != "0"  # asyncpg returns e.g. "DELETE 1"

    def close(self) -> None:
        pass  # the pool is per-call (short-lived); nothing persistent to close

    def __repr__(self) -> str:  # never echo ciphertext/values
        return "PostgresSecretBackend(dsn=***)"


# ── Infisical (optional [infisical] extra) ───────────────────────────
class InfisicalSecretBackend:
    """Infisical adapter — ref scheme ``infisical://<project>/<path>#KEY``. Config
    from ``INFISICAL_HOST`` (default cloud), ``INFISICAL_TOKEN`` (or client id/secret),
    ``INFISICAL_ENV`` (default ``dev``). Requires the ``[infisical]`` extra."""

    scheme = "infisical"

    def __init__(self) -> None:
        self.host = os.environ.get("INFISICAL_HOST", "").strip() or "https://app.infisical.com"
        self.token = os.environ.get("INFISICAL_TOKEN", "").strip()
        self.env = os.environ.get("INFISICAL_ENV", "").strip() or "dev"

    def _client(self):
        try:
            # optional dependency — only needed when an infisical:// ref is used
            from infisicalsdk import InfisicalSDKClient  # type: ignore
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(
                "infisical:// secrets need the '[infisical]' extra — `uv pip install cairndev[infisical]`"
            ) from e
        c = InfisicalSDKClient(host=self.host, token=self.token)
        return c

    def put(self, locator: str, key: str, value: str) -> str:
        project_id, _, path = locator.partition("/")
        c = self._client()
        c.secrets.create_secret_by_name(
            secret_name=key, project_id=project_id, secret_path=f"/{path}" if path else "/",
            environment_slug=self.env, secret_value=value,
        )
        return f"infisical://{locator}#{key}"

    def get(self, locator: str, key: str) -> str:
        project_id, _, path = locator.partition("/")
        c = self._client()
        s = c.secrets.get_secret_by_name(
            secret_name=key, project_id=project_id, secret_path=f"/{path}" if path else "/",
            environment_slug=self.env,
        )
        return getattr(s, "secretValue", None) or getattr(s, "secret_value", "")


# ── facade ───────────────────────────────────────────────────────────
def _local():
    """The writable ``cairn://`` backend. **`CAIRN_STORAGE` is the selector** (#427):
    ``postgres://…`` → :class:`PostgresSecretBackend` (the one prod Postgres),
    else the local Fernet-encrypted SQLite vault. ``CAIRN_SECRETS_DB`` overrides the
    sqlite path."""
    url = getenv("CAIRN_STORAGE", "").strip()
    if url.startswith(("postgres://", "postgresql://")):
        return PostgresSecretBackend(dsn=url)
    return LocalFileSecretBackend()


def default_backend_scheme() -> str:
    """Which backend a freshly-entered console credential is written to.
    ``CAIRN_SECRET_BACKEND=local|infisical`` (default ``local``)."""
    return getenv("CAIRN_SECRET_BACKEND", "").strip().lower() or "local"


#: The deployment's own account (mirrors :data:`cairn.core.account.DEFAULT_ACCOUNT`):
#: a single-node or auth-off deployment runs in it, and its references are not namespaced.
_DEPLOYMENT_ACCOUNT = "default"
_UNSAFE = re.compile(r"[^a-zA-Z0-9._-]+")


def scoped_account(account_id: Optional[str]) -> Optional[str]:
    """The tenant account a secret reference is confined to, or ``None`` for the
    deployment (no account, or the ``default`` account)."""
    a = (account_id or "").strip()
    return None if not a or a == _DEPLOYMENT_ACCOUNT else _UNSAFE.sub("-", a)


def _infisical_locator(tenant: Optional[str]) -> str:
    proj = os.environ.get("INFISICAL_PROJECT_ID", "").strip() or "cairn"
    return f"{proj}/cairn/{tenant}" if tenant else f"{proj}/cairn"


def ref_in_account(ref: str, account_id: Optional[str]) -> bool:
    """Whether ``ref`` is one :func:`mint_ref` makes for ``account_id``. The
    deployment may use any reference; a tenant only its own ``cairn://`` or
    ``infisical://`` references, never an env name or another account's."""
    tenant = scoped_account(account_id)
    if tenant is None:
        return True
    scheme = ref_scheme(ref)
    if scheme == "cairn":
        locator, key = _split(ref.strip())
        owner, sep, rest = key.partition("/")
        return locator == "secrets" and bool(sep) and owner == tenant and bool(rest)
    if scheme == "infisical":
        locator, key = _split(ref.strip())
        return locator == _infisical_locator(tenant) and bool(key)
    return False


def mint_ref(
    connection: str, key: str, *, scheme: Optional[str] = None, account_id: Optional[str] = None
) -> str:
    """The reference a new credential will be stored under (scoped by connection so
    two connections never collide on the same logical key, and by ``account_id`` so
    a tenant account's references are its own; the deployment's are unchanged)."""
    scheme = scheme or default_backend_scheme()
    slug = _UNSAFE.sub("-", f"{connection}/{key}".strip("/"))
    tenant = scoped_account(account_id)
    if scheme == "infisical":
        return f"infisical://{_infisical_locator(tenant)}#{slug}"
    return f"cairn://secrets#{tenant}/{slug}" if tenant else f"cairn://secrets#{slug}"


def put_secret(ref: str, value: str, *, account_id: Optional[str] = None) -> str:
    """Write ``value`` to the backend named by ``ref``; return the canonical ref to
    persist on the connection. Env refs are read-only (set them out-of-band). Given a
    tenant ``account_id``, a ref outside that account raises ``ValueError``."""
    if not ref_in_account(ref, account_id):
        raise ValueError(f"secret reference is not in account {account_id!r}")
    scheme = ref_scheme(ref)
    if scheme == "cairn":
        _, key = _split(ref)
        b = _local()
        try:
            return b.put(key, value)
        finally:
            b.close()
    if scheme == "infisical":
        locator, key = _split(ref)
        return InfisicalSecretBackend().put(locator, key, value)
    raise ValueError(f"cannot write secret to a read-only ref scheme: {scheme!r} ({ref!r})")


def resolve_secret(ref: str, *, account_id: Optional[str] = None) -> Optional[str]:
    """Read the value behind a reference, or ``None`` if absent. Dispatches by scheme;
    env/bare names fall back to ``os.environ``. Given a tenant ``account_id``, a
    reference outside that account (an env name, the deployment's or another
    account's) resolves to ``None`` without reaching any backend."""
    if not ref_in_account(ref, account_id):
        return None
    scheme = ref_scheme(ref)
    if scheme == "env":
        m = _ENV_RE.match(ref.strip())
        name = m.group("name") if m else ref.strip()
        return os.environ.get(name)
    if scheme == "cairn":
        _, key = _split(ref)
        b = _local()
        try:
            return b.get(key)
        except KeyError:
            return None
        finally:
            b.close()
    if scheme == "infisical":
        locator, key = _split(ref)
        try:
            return InfisicalSecretBackend().get(locator, key)
        except Exception:  # noqa: BLE001 — unreachable/missing → treat as unresolved
            return None
    return None  # vault:// (future) etc.
