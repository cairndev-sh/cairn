"""Accounts — the tenant entity (ADR-0054, epic #392 phase 1).

ADR-0054 moves the tenant boundary **up one level** from the workspace: an
**Account** is the billing / credential / isolation boundary. It *owns*
workspaces + connectors and *has members* (users) with roles. A solo user is
simply an account with one member — membership + roles are modelled from day
one (retrofitting a tenant boundary later is the worst SaaS migration).

Three entities live here:

  - :class:`Account`     — the tenant (``id``, ``name``, ``created_at``, ``status``).
  - :class:`User`        — a login (``id``, ``username``, ``password_hash`` (argon2),
    ``created_at``, ``disabled``, ``identities``). Passwords are hashed, never
    stored raw; a user who signed up with Google/GitHub has no password at all.
  - :class:`Membership`  — the ``User ↔ Account`` edge with a ``role`` ∈
    ``{owner, admin, member}``.

Storage mirrors :mod:`cairn.core.workspace` exactly — a pydantic record + three
interchangeable backends (InMemory / Sqlite / Postgres) routed by ``CAIRN_STORAGE``
(``CAIRN_ACCOUNT_DB`` path override for the sqlite file), each seeding a ``default``
account on schema-ready so OSS single-tenant sees zero behaviour change.

**Phase 1 = identity foundation only.** Account-scoped connectors + cross-account
denial are phase 2 (ADR-0054 §Phasing); the request→account middleware that scopes
every request is deferred (see :func:`get_account_for_user`, which is the resolver
that middleware will call — it plugs in where ``set_current_workspace`` does today
in ``triggers/server.py``).
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import json
import os
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field
from cairn.core._time import utc_now_iso
from cairn.core.pg_schema_init import PgSchemaInit

#: The implicit account every single-node / OSS deployment runs in. Mirrors
#: :data:`cairn.core.workspace.DEFAULT_WORKSPACE`.
DEFAULT_ACCOUNT = "default"

#: Roles a member can hold, most→least privileged. ``owner``/``admin`` may use
#: the admin-invite API; ``member`` cannot.
ROLES = ("owner", "admin", "member")
#: Roles allowed to invite/manage users via the admin API (ADR-0054 §3).
ADMIN_ROLES = frozenset({"owner", "admin"})


def _new_id() -> str:
    return uuid.uuid4().hex


def _slug(raw: str) -> str:
    """Slug-safe id: lowercase, ``[a-z0-9-]``, collapsed dashes. Same rule as the
    workspace slug in ``triggers/server.py`` so account + workspace ids read alike."""
    s = re.sub(r"[^a-z0-9-]+", "-", (raw or "").strip().lower())
    s = re.sub(r"-{2,}", "-", s)
    return s.strip("-")


# ── entities ──────────────────────────────────────────────────────────


class Account(BaseModel):
    """A tenant: the billing / credential / isolation boundary (ADR-0054 §1)."""

    id: str
    name: str = ""
    created_at: str = Field(default_factory=utc_now_iso)
    #: Lifecycle parity with :class:`~cairn.core.workspace.Workspace`. "archived"
    #: = soft-deleted. Rides in the JSON ``data`` blob (no migration).
    status: str = "active"
    #: #1613 — the work-email domain this account claims, e.g. `acme.com`.
    #: Set at signup; what `CAIRN_SIGNUP=domain` matches a new joiner against, so
    #: a colleague lands in their company rather than creating a second one.
    #: Empty means "claims no domain" — such an account is never domain-joinable,
    #: which is the safe direction. Rides in the blob (no migration).
    domain: str = ""
    #: #2367 — who set :attr:`domain` explicitly (the admin's username), or
    #: ``""`` when signup set it. An explicit claim is honoured even for a public
    #: mail domain (a company that really runs its mail on one); an automatic
    #: claim of one never is (#2351). Rides in the blob (no migration).
    domain_claimed_by: str = ""

    def model_post_init(self, _ctx: object) -> None:  # pydantic v2 hook
        if not self.name:
            object.__setattr__(self, "name", self.id)


class Identity(BaseModel):
    """An external sign-in linked to a :class:`User` (#2336, ADR-0119).

    ``(provider, subject)`` is the key: ``subject`` is the provider's stable
    user id (Google's ``sub`` claim, GitHub's numeric user id), never the
    email — an email can change hands, a subject cannot. ``email`` is the
    VERIFIED address the provider vouched for when the identity was linked.
    """

    provider: str
    subject: str
    email: str = ""
    linked_at: str = Field(default_factory=utc_now_iso)


class User(BaseModel):
    """A login. ``password_hash`` is an argon2 hash (via ``auth.hash_password``),
    never a raw password. ``id`` is a random hex; ``username`` is the login handle
    and is unique across the store.

    ``password_hash == ""`` means the user has NO password (they signed up with
    an external identity): ``POST /token`` refuses them, by the same check that
    refuses an unknown user."""

    id: str = Field(default_factory=_new_id)
    username: str
    password_hash: str = ""
    created_at: str = Field(default_factory=utc_now_iso)
    disabled: bool = False
    #: #2336 — external sign-ins linked to this login. Rides in the JSON
    #: ``data`` blob every store persists, so old rows read ``[]`` (no migration).
    identities: list[Identity] = Field(default_factory=list)


class Invite(BaseModel):
    """A pending invitation to join an account (#1612, ADR-0094 D7).

    Replaces the admin-chooses-the-password flow: the admin creates an invite,
    the invitee opens the link and sets their OWN password. The admin never
    learns the credential and it never has to travel out of band.

    **The token is stored HASHED, never in the clear** — same discipline as
    `User.password_hash`. The raw token exists exactly once, in the link handed
    back at create time; a leaked invites table yields nothing usable.

    Single-use and revocable, which is why this is a stored row rather than a
    self-contained signed token: `consumed_at` and `revoked_at` need somewhere
    to live, and a stateless JWT could be replayed until it expired.
    """

    id: str = Field(default_factory=_new_id)
    account_id: str = DEFAULT_ACCOUNT
    #: Who it was issued for. Checked at accept, so a forwarded link cannot be
    #: redeemed by someone else.
    email: str = ""
    role: str = "member"
    token_hash: str = ""
    expires_at: str = ""
    created_by: str = ""
    created_at: str = Field(default_factory=utc_now_iso)
    #: Set once redeemed — the single-use marker. A second accept must fail.
    consumed_at: Optional[str] = None
    revoked_at: Optional[str] = None

    @property
    def state(self) -> str:
        """`pending` | `consumed` | `revoked` | `expired`.

        Each is reported distinctly on accept: "invalid link" for all four
        leaves the recipient with no idea whether to ask for a new one, wait,
        or check they opened the right link.
        """
        if self.consumed_at:
            return "consumed"
        if self.revoked_at:
            return "revoked"
        if self.expires_at and self.expires_at < utc_now_iso():
            return "expired"
        return "pending"


class Membership(BaseModel):
    """A ``User ↔ Account`` edge carrying a role. The pair
    ``(account_id, user_id)`` is unique — a user has at most one role per account."""

    account_id: str
    user_id: str
    role: str = "member"
    created_at: str = Field(default_factory=utc_now_iso)

    def model_post_init(self, _ctx: object) -> None:
        if self.role not in ROLES:
            raise ValueError(f"role must be one of {ROLES}, got {self.role!r}")


# ── stores ────────────────────────────────────────────────────────────
#
# Every backend implements the same async surface:
#   accounts:    put_account / get_account / list_accounts
#   users:       put_user / get_user / get_user_by_username / get_user_by_identity
#                / list_users
#   memberships: put_membership / list_members / get_account_for_user
# plus the CRUD helpers below live on the *module* (create_account, create_user,
# set_password, add_member, get_account_for_user) so consumers don't care which
# backend is active — same as the workspace module's free functions.


class InMemoryAccountStore:
    """Volatile store (tests / no-config default), seeded with ``default``."""

    def __init__(self) -> None:
        self._accounts: dict[str, Account] = {}
        self._users: dict[str, User] = {}
        self._by_username: dict[str, str] = {}
        self._memberships: dict[tuple[str, str], Membership] = {}
        self._invites: dict[str, "Invite"] = {}
        self._accounts[DEFAULT_ACCOUNT] = Account(id=DEFAULT_ACCOUNT, name="Default")

    # accounts
    async def put_account(self, acct: Account) -> Account:
        self._accounts[acct.id] = acct
        return acct

    async def get_account(self, account_id: str) -> Optional[Account]:
        return self._accounts.get(account_id)

    async def list_accounts(self) -> list[Account]:
        return sorted(self._accounts.values(), key=lambda a: a.id)

    # users
    async def put_user(self, user: User) -> User:
        self._users[user.id] = user
        self._by_username[user.username] = user.id
        return user

    async def get_user(self, user_id: str) -> Optional[User]:
        return self._users.get(user_id)

    async def get_user_by_username(self, username: str) -> Optional[User]:
        uid = self._by_username.get(username)
        return self._users.get(uid) if uid else None

    async def get_user_by_identity(self, provider: str, subject: str) -> Optional[User]:
        return next((u for u in self._users.values()
                     if any(i.provider == provider and i.subject == subject for i in u.identities)), None)

    async def list_users(self) -> list[User]:
        return sorted(self._users.values(), key=lambda u: u.username)

    # memberships
    async def put_membership(self, m: Membership) -> Membership:
        self._memberships[(m.account_id, m.user_id)] = m
        return m

    async def list_members(self, account_id: str) -> list[Membership]:
        return [m for m in self._memberships.values() if m.account_id == account_id]

    async def get_account_for_user(self, user_id: str) -> Optional[str]:
        for (acct_id, uid) in sorted(self._memberships):
            if uid == user_id:
                return acct_id
        return None

    # invites (#1612)
    async def put_invite(self, inv: "Invite") -> "Invite":
        self._invites[inv.id] = inv
        return inv

    async def get_invite(self, invite_id: str) -> Optional["Invite"]:
        return self._invites.get(invite_id)

    async def get_invite_by_token_hash(self, token_hash: str) -> Optional["Invite"]:
        return next((i for i in self._invites.values() if i.token_hash == token_hash), None)

    async def list_invites(self, account_id: str) -> list["Invite"]:
        return sorted((i for i in self._invites.values() if i.account_id == account_id),
                      key=lambda i: i.created_at, reverse=True)

    def close(self) -> None:
        pass


class SqliteAccountStore:
    """Embedded-SQLite store — three tables (``accounts`` / ``users`` /
    ``memberships``), each a JSON ``data`` blob under a routing key column; async
    ops via ``asyncio.to_thread`` under a lock. Seeds ``default`` on open. Mirrors
    :class:`cairn.core.workspace.SqliteWorkspaceStore`."""

    def __init__(self, path: str | None = None) -> None:
        if path is None:
            from cairn.core import settings

            # single shared cairn.db (#427/ADR-0058); CAIRN_*_DB overrides
            path = getenv("CAIRN_ACCOUNT_DB", "").strip() or settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, data TEXT NOT NULL)")
        # `username` is an indexed lookup key duplicated out of the JSON blob.
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, data TEXT NOT NULL)"
        )
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS memberships ("
            "account_id TEXT NOT NULL, user_id TEXT NOT NULL, data TEXT NOT NULL, "
            "PRIMARY KEY (account_id, user_id))"
        )
        # index for the reverse lookup used by get_account_for_user.
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_memberships_user ON memberships (user_id)")
        # invites (#1612). `token_hash` is an indexed lookup key duplicated out
        # of the blob — accept resolves by hash, never by scanning.
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS invites ("
            "id TEXT PRIMARY KEY, account_id TEXT NOT NULL, token_hash TEXT NOT NULL, "
            "data TEXT NOT NULL)"
        )
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_invites_token ON invites (token_hash)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_invites_account ON invites (account_id)")
        # seed the implicit default account (idempotent).
        self._conn.execute(
            "INSERT OR IGNORE INTO accounts (id, data) VALUES (?,?)",
            (DEFAULT_ACCOUNT, Account(id=DEFAULT_ACCOUNT, name="Default").model_dump_json()),
        )
        self._conn.commit()
        self._lock = asyncio.Lock()

    # accounts
    async def put_account(self, acct: Account) -> Account:
        async with self._lock:
            def _w() -> None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO accounts (id, data) VALUES (?,?)",
                    (acct.id, acct.model_dump_json()),
                )
                self._conn.commit()
            await asyncio.to_thread(_w)
        return acct

    async def get_account(self, account_id: str) -> Optional[Account]:
        row = self._conn.execute("SELECT data FROM accounts WHERE id=?", (account_id,)).fetchone()
        return Account.model_validate_json(row[0]) if row else None

    async def list_accounts(self) -> list[Account]:
        rows = self._conn.execute("SELECT data FROM accounts").fetchall()
        return sorted((Account.model_validate_json(r[0]) for r in rows), key=lambda a: a.id)

    # users
    async def put_user(self, user: User) -> User:
        async with self._lock:
            def _w() -> None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO users (id, username, data) VALUES (?,?,?)",
                    (user.id, user.username, user.model_dump_json()),
                )
                self._conn.commit()
            await asyncio.to_thread(_w)
        return user

    async def get_user(self, user_id: str) -> Optional[User]:
        row = self._conn.execute("SELECT data FROM users WHERE id=?", (user_id,)).fetchone()
        return User.model_validate_json(row[0]) if row else None

    async def get_user_by_username(self, username: str) -> Optional[User]:
        row = self._conn.execute("SELECT data FROM users WHERE username=?", (username,)).fetchone()
        return User.model_validate_json(row[0]) if row else None

    async def get_user_by_identity(self, provider: str, subject: str) -> Optional[User]:
        # #2336 — a query over the JSON blob (SQLite JSON1), no new table or
        # column: sign-in by an external identity is rare next to reads by id.
        row = self._conn.execute(
            "SELECT u.data FROM users u, json_each(u.data, '$.identities') i "
            "WHERE json_extract(i.value, '$.provider')=? AND json_extract(i.value, '$.subject')=? "
            "LIMIT 1",
            (provider, subject),
        ).fetchone()
        return User.model_validate_json(row[0]) if row else None

    async def list_users(self) -> list[User]:
        rows = self._conn.execute("SELECT data FROM users").fetchall()
        return sorted((User.model_validate_json(r[0]) for r in rows), key=lambda u: u.username)

    # memberships
    async def put_membership(self, m: Membership) -> Membership:
        async with self._lock:
            def _w() -> None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO memberships (account_id, user_id, data) VALUES (?,?,?)",
                    (m.account_id, m.user_id, m.model_dump_json()),
                )
                self._conn.commit()
            await asyncio.to_thread(_w)
        return m

    async def list_members(self, account_id: str) -> list[Membership]:
        rows = self._conn.execute("SELECT data FROM memberships WHERE account_id=?", (account_id,)).fetchall()
        return [Membership.model_validate_json(r[0]) for r in rows]

    # invites (#1612)
    async def put_invite(self, inv: "Invite") -> "Invite":
        async with self._lock:
            def _w() -> None:
                self._conn.execute(
                    "INSERT OR REPLACE INTO invites (id, account_id, token_hash, data) VALUES (?,?,?,?)",
                    (inv.id, inv.account_id, inv.token_hash, inv.model_dump_json()),
                )
                self._conn.commit()
            await asyncio.to_thread(_w)
        return inv

    async def get_invite(self, invite_id: str) -> Optional["Invite"]:
        row = self._conn.execute("SELECT data FROM invites WHERE id=?", (invite_id,)).fetchone()
        return Invite.model_validate_json(row[0]) if row else None

    async def get_invite_by_token_hash(self, token_hash: str) -> Optional["Invite"]:
        row = self._conn.execute(
            "SELECT data FROM invites WHERE token_hash=?", (token_hash,)).fetchone()
        return Invite.model_validate_json(row[0]) if row else None

    async def list_invites(self, account_id: str) -> list["Invite"]:
        rows = self._conn.execute(
            "SELECT data FROM invites WHERE account_id=?", (account_id,)).fetchall()
        return sorted((Invite.model_validate_json(r[0]) for r in rows),
                      key=lambda i: i.created_at, reverse=True)

    async def get_account_for_user(self, user_id: str) -> Optional[str]:
        row = self._conn.execute(
            "SELECT account_id FROM memberships WHERE user_id=? ORDER BY account_id LIMIT 1", (user_id,)
        ).fetchone()
        return row[0] if row else None

    def close(self) -> None:
        self._conn.close()


class PostgresAccountStore(PgSchemaInit):
    """Postgres-backed account store — the fleet-shared backend (ADR-0054, epic
    #427). Mirrors :class:`cairn.core.workspace.PostgresWorkspaceStore`: a lazy
    asyncpg pool, three tables with JSONB ``data`` blobs, ``default`` seeded on
    schema-ready. Requires the ``storage-postgres`` optional dep (asyncpg)."""

    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn
        self._pool: Any = None
        self._schema_ready = False

    async def _open_pool(self) -> Any:
        try:
            import asyncpg  # noqa: PLC0415 — optional dep (storage-postgres extra)
        except ImportError as e:
            raise RuntimeError(
                "PostgresAccountStore requires the 'storage-postgres' optional "
                "dep group. Install with: pip install 'cairndev[storage-postgres]'"
            ) from e
        if not self._dsn:
            raise RuntimeError(
                "PostgresAccountStore needs a DSN. Pass dsn= explicitly or set "
                "CAIRN_STORAGE=postgres://… before constructing the store."
            )
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415

        # #614: one pool per DSN, shared across every Postgres store in this process.
        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool) -> None:
        async with pool.acquire() as conn:
            await conn.execute("CREATE TABLE IF NOT EXISTS accounts (id TEXT PRIMARY KEY, data JSONB NOT NULL)")
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS users "
                "(id TEXT PRIMARY KEY, username TEXT NOT NULL UNIQUE, data JSONB NOT NULL)"
            )
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS memberships ("
                "account_id TEXT NOT NULL, user_id TEXT NOT NULL, data JSONB NOT NULL, "
                "PRIMARY KEY (account_id, user_id))"
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_memberships_user ON memberships (user_id)")
            # invites (#1612) — #1874: this backend never had the table or the
            # methods, so every auth-on Postgres deployment 500'd on /admin/invites
            # and could not create an invite at all. Same shape as the sqlite
            # store: the token is stored HASHED; `token_hash` is the lookup key.
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS invites ("
                "id TEXT PRIMARY KEY, account_id TEXT NOT NULL, token_hash TEXT NOT NULL, data JSONB NOT NULL)"
            )
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_invites_token ON invites (token_hash)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_invites_account ON invites (account_id)")
            await conn.execute(
                "INSERT INTO accounts (id, data) VALUES ($1, $2::jsonb) ON CONFLICT (id) DO NOTHING",
                DEFAULT_ACCOUNT,
                Account(id=DEFAULT_ACCOUNT, name="Default").model_dump_json(),
            )

    # accounts
    async def put_account(self, acct: Account) -> Account:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO accounts (id, data) VALUES ($1, $2::jsonb) "
                "ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data",
                acct.id, acct.model_dump_json(),
            )
        return acct

    async def get_account(self, account_id: str) -> Optional[Account]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data::text AS data FROM accounts WHERE id=$1", account_id)
        return Account.model_validate_json(row["data"]) if row else None

    async def list_accounts(self) -> list[Account]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT data::text AS data FROM accounts")
        return sorted((Account.model_validate_json(r["data"]) for r in rows), key=lambda a: a.id)

    # users
    async def put_user(self, user: User) -> User:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (id, username, data) VALUES ($1, $2, $3::jsonb) "
                "ON CONFLICT (id) DO UPDATE SET username = EXCLUDED.username, data = EXCLUDED.data",
                user.id, user.username, user.model_dump_json(),
            )
        return user

    async def get_user(self, user_id: str) -> Optional[User]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data::text AS data FROM users WHERE id=$1", user_id)
        return User.model_validate_json(row["data"]) if row else None

    async def get_user_by_username(self, username: str) -> Optional[User]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data::text AS data FROM users WHERE username=$1", username)
        return User.model_validate_json(row["data"]) if row else None

    async def get_user_by_identity(self, provider: str, subject: str) -> Optional[User]:
        # #2336 — JSONB containment over the blob; no new table or column.
        probe = json.dumps({"identities": [{"provider": provider, "subject": subject}]})
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT data::text AS data FROM users WHERE data @> $1::jsonb LIMIT 1", probe)
        return User.model_validate_json(row["data"]) if row else None

    async def list_users(self) -> list[User]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT data::text AS data FROM users")
        return sorted((User.model_validate_json(r["data"]) for r in rows), key=lambda u: u.username)

    # memberships
    async def put_membership(self, m: Membership) -> Membership:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO memberships (account_id, user_id, data) VALUES ($1, $2, $3::jsonb) "
                "ON CONFLICT (account_id, user_id) DO UPDATE SET data = EXCLUDED.data",
                m.account_id, m.user_id, m.model_dump_json(),
            )
        return m

    async def list_members(self, account_id: str) -> list[Membership]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT data::text AS data FROM memberships WHERE account_id=$1", account_id)
        return [Membership.model_validate_json(r["data"]) for r in rows]

    async def get_account_for_user(self, user_id: str) -> Optional[str]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT account_id FROM memberships WHERE user_id=$1 ORDER BY account_id LIMIT 1", user_id
            )
        return row["account_id"] if row else None

    # invites (#1612 / #1874)
    async def put_invite(self, inv: "Invite") -> "Invite":
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO invites (id, account_id, token_hash, data) VALUES ($1, $2, $3, $4::jsonb) "
                "ON CONFLICT (id) DO UPDATE SET account_id = EXCLUDED.account_id, "
                "token_hash = EXCLUDED.token_hash, data = EXCLUDED.data",
                inv.id, inv.account_id, inv.token_hash, inv.model_dump_json(),
            )
        return inv

    async def get_invite(self, invite_id: str) -> Optional["Invite"]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data::text AS data FROM invites WHERE id=$1", invite_id)
        return Invite.model_validate_json(row["data"]) if row else None

    async def get_invite_by_token_hash(self, token_hash: str) -> Optional["Invite"]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data::text AS data FROM invites WHERE token_hash=$1", token_hash)
        return Invite.model_validate_json(row["data"]) if row else None

    async def list_invites(self, account_id: str) -> list["Invite"]:
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT data::text AS data FROM invites WHERE account_id=$1", account_id)
        return sorted((Invite.model_validate_json(r["data"]) for r in rows),
                      key=lambda i: i.created_at, reverse=True)

    async def aclose(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            self._schema_ready = False

    def close(self) -> None:  # parity with the sync stores (best-effort)
        pass


_store: object | None = None


def get_account_store() -> Any:
    """Process-wide account store. Honors ``CAIRN_STORAGE`` exactly like
    :func:`cairn.core.workspace.get_workspace_store`:

      - ``postgres://…`` / ``postgresql://…`` → :class:`PostgresAccountStore`.
      - anything else / unset → :class:`SqliteAccountStore` (``CAIRN_ACCOUNT_DB``
        path knob applies).

    Tests pass their own store in (``account._store = …``)."""
    global _store
    if _store is None:
        url = getenv("CAIRN_STORAGE", "").strip()
        if url.startswith(("postgres://", "postgresql://")):
            _store = PostgresAccountStore(dsn=url)
        else:
            _store = SqliteAccountStore()
    return _store


def is_env_admin_username(name: str) -> bool:
    """Whether ``name`` is the deployment's env admin username (``CAIRN_API_USER``,
    ``admin`` when unset), compared trimmed and case-insensitively.

    A self-serve door (sign-up, an OAuth landing, an invite redemption) never
    creates a store user under that name: login tries the store first, so such
    a user would sit in front of the deployment admin's own sign-in, and a
    second principal answering to the admin's name confuses every record that
    names an actor."""
    admin = (getenv("CAIRN_API_USER") or "").strip() or "admin"
    return bool((name or "").strip()) and name.strip().casefold() == admin.casefold()


# ── module-level CRUD (backend-agnostic; the public API) ────────────────


async def create_account(name: str, *, account_id: Optional[str] = None, store: Any = None) -> Account:
    """Create (or upsert) an account. ``account_id`` defaults to a slug of ``name``."""
    store = store or get_account_store()
    aid = _slug(account_id or name)
    if not aid:
        raise ValueError("account id/name must yield a non-empty slug")
    return await store.put_account(Account(id=aid, name=name.strip() or aid))


async def create_user(
    username: str,
    password: str,
    *,
    disabled: bool = False,
    store: Any = None,
) -> User:
    """Create a user with an argon2-hashed password. Raises ``ValueError`` if the
    username already exists (usernames are unique)."""
    from cairn.core.auth_tokens import hash_password  # lazy: keeps the token module off this import path

    store = store or get_account_store()
    if await store.get_user_by_username(username):
        raise ValueError(f"username {username!r} already exists")
    user = User(username=username, password_hash=hash_password(password), disabled=disabled)
    return await store.put_user(user)


async def set_password(user_id: str, password: str, *, store: Any = None) -> User:
    """Reset a user's password (argon2 re-hash). Raises ``KeyError`` if unknown."""
    from cairn.core.auth_tokens import hash_password  # lazy: see create_user

    store = store or get_account_store()
    user = await store.get_user(user_id)
    if user is None:
        raise KeyError(f"user {user_id!r} not found")
    user.password_hash = hash_password(password)
    saved = await store.put_user(user)
    # ADR-0142 — a new password ends every console sign-in the old one started.
    from cairn.core.refresh_tokens import revoke_user  # noqa: PLC0415 — lazy, like hash_password

    await revoke_user(user_id)
    return saved


async def set_disabled(user_id: str, disabled: bool, *, store: Any = None) -> User:
    """Soft-disable / re-enable a login (#918). A disabled user can no longer
    authenticate (``_resolve_store_user`` refuses them); the record and its
    memberships are KEPT — runs/audit reference user ids for provenance, so
    there is deliberately no hard delete. Raises ``KeyError`` if unknown."""
    store = store or get_account_store()
    user = await store.get_user(user_id)
    if user is None:
        raise KeyError(f"user {user_id!r} not found")
    user.disabled = disabled
    saved = await store.put_user(user)
    if disabled:
        # ADR-0142 — a disabled user's console sign-ins end now, not when they
        # next try to renew one.
        from cairn.core.refresh_tokens import revoke_user  # noqa: PLC0415 — lazy, like hash_password

        await revoke_user(user_id)
    return saved


async def add_member(account_id: str, user_id: str, role: str = "member", *, store: Any = None) -> Membership:
    """Assign a user to an account with a role (∈ :data:`ROLES`). Idempotent upsert
    on ``(account_id, user_id)`` — re-adding updates the role."""
    store = store or get_account_store()
    return await store.put_membership(Membership(account_id=account_id, user_id=user_id, role=role))


async def get_user_by_username(username: str, *, store: Any = None) -> Optional[User]:
    store = store or get_account_store()
    return await store.get_user_by_username(username)


async def get_user_by_identity(provider: str, subject: str, *, store: Any = None) -> Optional[User]:
    """The user an external identity is linked to, or ``None`` (#2336)."""
    store = store or get_account_store()
    return await store.get_user_by_identity(provider, subject)


async def link_identity(user: User, identity: Identity, *, store: Any = None) -> User:
    """Attach ``identity`` to ``user`` (idempotent on ``(provider, subject)``).

    The CALLER decides whether linking is allowed — the only caller,
    :mod:`cairn.core.oauth_login`, links on a provider-verified email that
    exactly matches the user's login (ADR-0119)."""
    store = store or get_account_store()
    if not any(i.provider == identity.provider and i.subject == identity.subject
               for i in user.identities):
        user.identities.append(identity)
        user = await store.put_user(user)
    return user


async def list_members(account_id: str, *, store: Any = None) -> list[Membership]:
    store = store or get_account_store()
    return await store.list_members(account_id)


async def get_account_for_user(user_id: str, *, store: Any = None) -> Optional[str]:
    """The account id a user belongs to (first by id order if multiple).

    This is the resolver the request→account middleware (deferred to a later
    phase, ADR-0054 §Consequences) will call to scope each request — it plugs in
    where ``set_current_workspace`` is set in ``triggers/server.py``. Returns
    ``None`` for a user with no membership."""
    store = store or get_account_store()
    return await store.get_account_for_user(user_id)


# ── invites (#1612, ADR-0094 D7) ──────────────────────────────────────────
#
# The admin API could already create a user, but the ADMIN chose the password —
# so the admin knew the credential and it had to travel out of band. An invite
# inverts that: the admin grants an account and a role, the invitee sets their
# own secret.

#: How long a link stays good. Long enough to survive a weekend, short enough
#: that a forwarded email is not a standing key to the tenant.
INVITE_TTL_HOURS = 72


def _hash_token(raw: str) -> str:
    """SHA-256 of the raw token. What the row stores.

    The raw token has exactly one lifetime: the response that hands back the
    link. Storing the hash means a leaked `invites` table is not a set of
    working invitations — the same reason `User` stores `password_hash`.
    """
    import hashlib
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def create_invite(
    account_id: str, email: str, role: str = "member", *,
    created_by: str = "", ttl_hours: int = INVITE_TTL_HOURS, store: Any = None,
) -> tuple[Invite, str]:
    """Create an invite. Returns ``(invite, raw_token)`` — the raw token is
    returned ONCE and never persisted."""
    import secrets
    from datetime import timedelta

    if role not in ROLES:
        raise ValueError(f"role must be one of {ROLES}, got {role!r}")
    raw = secrets.token_urlsafe(32)
    expires = (datetime.now(timezone.utc) + timedelta(hours=ttl_hours)).isoformat()
    inv = Invite(account_id=account_id, email=email.strip().lower(), role=role,
                 token_hash=_hash_token(raw), expires_at=expires, created_by=created_by)
    return await (store or get_account_store()).put_invite(inv), raw


class InviteError(Exception):
    """An invite could not be redeemed. ``reason`` is one of the
    :attr:`Invite.state` values, or ``unknown`` / ``email_mismatch`` — carried
    separately so the caller can say WHICH, rather than "invalid link"."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


async def peek_invite(raw_token: str, *, store: Any = None) -> Invite:
    """Resolve an invite for display, without consuming it.

    The accept page shows which account and role the link is for BEFORE asking
    for a password — a person should know what they are joining.
    """
    store = store or get_account_store()
    inv = await store.get_invite_by_token_hash(_hash_token(raw_token))
    if inv is None:
        raise InviteError("unknown")
    if inv.state != "pending":
        raise InviteError(inv.state)
    return inv


async def accept_invite(
    raw_token: str, username: str, password_hash: str, *, store: Any = None,
) -> tuple[User, Invite]:
    """Redeem an invite: create the user and its membership, mark it consumed.

    ``password_hash`` is already hashed by the caller — this module never sees
    a plaintext password.

    Consumption is the LAST write, so a failure part-way (duplicate username,
    store hiccup) leaves the invite still redeemable rather than burning it.
    """
    store = store or get_account_store()
    inv = await peek_invite(raw_token, store=store)

    name = (username or "").strip().lower()
    if not name:
        raise InviteError("username_required")
    if is_env_admin_username(name) or await store.get_user_by_username(name) is not None:
        raise InviteError("username_taken")

    user = await store.put_user(User(username=name, password_hash=password_hash))
    await store.put_membership(Membership(account_id=inv.account_id, user_id=user.id,
                                          role=inv.role))
    inv.consumed_at = utc_now_iso()
    await store.put_invite(inv)
    return user, inv


async def revoke_invite(invite_id: str, account_id: str, *, store: Any = None) -> Invite:
    """Revoke a pending invite. Scoped to ``account_id`` so one account cannot
    revoke another's — raises ``KeyError`` rather than reporting the mismatch,
    keeping cross-tenant existence unobservable (ADR-0051 §4)."""
    store = store or get_account_store()
    inv = await store.get_invite(invite_id)
    if inv is None or inv.account_id != account_id:
        raise KeyError(invite_id)
    if inv.state == "pending":
        inv.revoked_at = utc_now_iso()
        await store.put_invite(inv)
    return inv
