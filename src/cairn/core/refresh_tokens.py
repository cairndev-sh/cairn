"""Refresh tokens: the server half of a renewable console sign-in (ADR-0142).

A console sign-in (``POST /token``, the Google/GitHub landing, self-serve sign-up)
returns the usual 30-minute access token and also sets an httpOnly cookie holding
an opaque random refresh token. ``POST /auth/refresh`` trades that cookie for a
new access token and a new cookie; ``POST /auth/logout`` ends the sign-in on the
server. The HTTP side (cookie attributes, the same-origin check, the user-still-
active check) lives in :mod:`cairn.core.triggers.auth`; this module is the record
and the rotation rules, with no web framework.

What a row is:

* ``token_hash`` is the SHA-256 of the raw token. The raw token exists only in the
  cookie; a leaked table is not a set of working sign-ins (the invite store does
  the same, :func:`cairn.core.account._hash_token`).
* Every sign-in starts a **family**. Each refresh marks the presented row used
  (``used_at``, ``replaced_by``) and issues the next row in the same family.
* Presenting a row that was already used is **reuse**: someone holds a copy of a
  rotated token, so the whole family is revoked and both copies stop working.
  Except within :data:`REUSE_GRACE_SECONDS` of that row's own rotation
  (ADR-0144): a browser that lost the response (a dropped connection, two tabs
  refreshing at once) replays the cookie it still holds, so the replay renews
  instead. The child the row was rotated into is revoked (the browser never
  stored it) and a sibling takes its place: a parent never has more than one
  live child, and a child that was itself already rotated makes the replay
  reuse after all.
* A superseded child (revoked, never used) presented
  :data:`REUSE_GRACE_SECONDS` or more after it was superseded ends its family
  with a WARNING (ADR-0145): its holder is the client a replay displaced, so the
  replay may have been a copy of the token.
* The family's lifetime is fixed when it starts: ``CAIRN_REFRESH_TOKEN_DAYS``
  (default 30) when the sign-in asked to be remembered, :data:`SESSION_HOURS`
  otherwise. A rotated row inherits its family's ``expires_at``.
* ``sub`` / ``account_id`` are what the access token is re-minted from, the same
  claims ``POST /token`` issues (``sub`` is a user id, or the env admin's name).

Storage is the usual triad (InMemory / Sqlite / Postgres) selected by
``CAIRN_STORAGE``, in the shared ``cairn.db`` for SQLite. Real columns rather
than a JSON blob, because rotation is a compare-and-set
(``UPDATE … WHERE used_at IS NULL AND revoked_at IS NULL``): of two concurrent
refreshes with the same token exactly one wins the rotation, and the other
takes the replay path above.
Tokens belong to a user, not to a workspace, so the table has no workspace
column.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import hashlib
import logging
import os
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pydantic import BaseModel

from cairn.core import settings
from cairn.core.pg_schema_init import PgSchemaInit

logger = logging.getLogger(__name__)

#: How long a remembered sign-in lasts unless ``CAIRN_REFRESH_TOKEN_DAYS`` says otherwise.
DEFAULT_REMEMBER_DAYS = 30
#: How long a sign-in that was not remembered lasts on the server (its cookie is a
#: session cookie, so a browser restart also ends it).
SESSION_HOURS = 12
#: ADR-0144 — a rotated token presented again within this many seconds of its own
#: rotation renews (a lost response, two tabs) instead of ending the sign-in.
#: A constant by decision, not a setting.
REUSE_GRACE_SECONDS = 10
#: How far a rotation may appear to lie in the future (another replica's clock
#: ahead of this one) and still count as "just now". Beyond that the elapsed time
#: is not trusted and the replay is treated as outside the window: a replica
#: lagging by minutes, or an NTP step, must not turn reuse into a renewal.
_CLOCK_SKEW_SECONDS = 2


def remember_days() -> int:
    """``CAIRN_REFRESH_TOKEN_DAYS``, at least 1; unset or unparseable means 30."""
    raw = getenv("CAIRN_REFRESH_TOKEN_DAYS", "").strip()
    try:
        days = int(raw) if raw else DEFAULT_REMEMBER_DAYS
    except ValueError:
        return DEFAULT_REMEMBER_DAYS
    return max(1, days)


def hash_token(raw: str) -> str:
    """SHA-256 of the raw token: what the row stores and is looked up by."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class RefreshToken(BaseModel):
    """One issued refresh token (never the raw value)."""

    token_hash: str
    family_id: str
    sub: str
    account_id: Optional[str] = None
    remember: bool = True
    issued_at: str
    expires_at: str
    used_at: Optional[str] = None
    replaced_by: Optional[str] = None
    revoked_at: Optional[str] = None

    def expires(self) -> datetime:
        return datetime.fromisoformat(self.expires_at)

    def seconds_left(self, now: Optional[datetime] = None) -> int:
        return int((self.expires() - (now or _now())).total_seconds())


class RefreshError(Exception):
    """A refresh token that cannot be used. ``reason`` is one of ``unknown``,
    ``expired``, ``revoked``, ``reused`` or ``inactive``."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


_COLUMNS = ("token_hash", "family_id", "sub", "account_id", "remember", "issued_at",
            "expires_at", "used_at", "replaced_by", "revoked_at")


def _from_row(row: Any) -> RefreshToken:
    data = dict(zip(_COLUMNS, row)) if isinstance(row, (tuple, list)) else dict(row)
    data["remember"] = bool(data.get("remember"))
    return RefreshToken(**data)


# ─── stores ────────────────────────────────────────────────────────


class InMemoryRefreshTokenStore:
    def __init__(self) -> None:
        self._rows: dict[str, RefreshToken] = {}

    async def insert(self, tok: RefreshToken) -> RefreshToken:
        self._rows[tok.token_hash] = tok
        return tok

    async def get(self, token_hash: str) -> Optional[RefreshToken]:
        return self._rows.get(token_hash)

    async def mark_used(self, token_hash: str, *, at: str, replaced_by: str) -> bool:
        tok = self._rows.get(token_hash)
        if tok is None or tok.used_at is not None or tok.revoked_at is not None:
            return False
        self._rows[token_hash] = tok.model_copy(update={"used_at": at, "replaced_by": replaced_by})
        return True

    async def revoke_unused(self, token_hash: str, *, at: str) -> bool:
        tok = self._rows.get(token_hash)
        if tok is None or tok.used_at is not None or tok.revoked_at is not None:
            return False
        self._rows[token_hash] = tok.model_copy(update={"revoked_at": at})
        return True

    async def set_replaced_by(self, token_hash: str, *, expected: str, new: str) -> bool:
        tok = self._rows.get(token_hash)
        if tok is None or tok.replaced_by != expected or tok.revoked_at is not None:
            return False
        self._rows[token_hash] = tok.model_copy(update={"replaced_by": new})
        return True

    async def revoke_family(self, family_id: str, *, at: str) -> int:
        return self._revoke(lambda t: t.family_id == family_id, at)

    async def revoke_subject(self, sub: str, *, at: str) -> int:
        return self._revoke(lambda t: t.sub == sub, at)

    def _revoke(self, match: Any, at: str) -> int:
        n = 0
        for h, t in list(self._rows.items()):
            if match(t) and t.revoked_at is None:
                self._rows[h] = t.model_copy(update={"revoked_at": at})
                n += 1
        return n

    def close(self) -> None:
        pass


class SqliteRefreshTokenStore:
    """sqlite3 under a lock, ``to_thread``'d, in the shared ``cairn.db``."""

    _CREATE = (
        "CREATE TABLE IF NOT EXISTS refresh_tokens ("
        " token_hash TEXT PRIMARY KEY, family_id TEXT NOT NULL, sub TEXT NOT NULL,"
        " account_id TEXT, remember INTEGER NOT NULL, issued_at TEXT NOT NULL,"
        " expires_at TEXT NOT NULL, used_at TEXT, replaced_by TEXT, revoked_at TEXT)"
    )

    def __init__(self, path: str | None = None) -> None:
        if path is None:
            path = settings.default_sqlite_db()
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute(self._CREATE)
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family ON refresh_tokens (family_id)")
        self._conn.execute("CREATE INDEX IF NOT EXISTS idx_refresh_tokens_sub ON refresh_tokens (sub)")
        self._conn.commit()
        self._lock = asyncio.Lock()

    async def _run(self, fn: Any) -> Any:
        async with self._lock:
            return await asyncio.to_thread(fn)

    async def insert(self, tok: RefreshToken) -> RefreshToken:
        def _w() -> None:
            self._conn.execute(
                f"INSERT INTO refresh_tokens ({', '.join(_COLUMNS)}) VALUES ({', '.join('?' * len(_COLUMNS))})",
                (tok.token_hash, tok.family_id, tok.sub, tok.account_id, int(tok.remember),
                 tok.issued_at, tok.expires_at, tok.used_at, tok.replaced_by, tok.revoked_at),
            )
            self._conn.commit()
        await self._run(_w)
        return tok

    async def get(self, token_hash: str) -> Optional[RefreshToken]:
        def _g() -> Any:
            return self._conn.execute(
                f"SELECT {', '.join(_COLUMNS)} FROM refresh_tokens WHERE token_hash=?", (token_hash,)
            ).fetchone()
        row = await self._run(_g)
        return _from_row(row) if row else None

    async def mark_used(self, token_hash: str, *, at: str, replaced_by: str) -> bool:
        def _u() -> bool:
            cur = self._conn.execute(
                "UPDATE refresh_tokens SET used_at=?, replaced_by=? "
                "WHERE token_hash=? AND used_at IS NULL AND revoked_at IS NULL",
                (at, replaced_by, token_hash),
            )
            self._conn.commit()
            return cur.rowcount == 1
        return await self._run(_u)

    async def revoke_unused(self, token_hash: str, *, at: str) -> bool:
        def _u() -> bool:
            cur = self._conn.execute(
                "UPDATE refresh_tokens SET revoked_at=? "
                "WHERE token_hash=? AND used_at IS NULL AND revoked_at IS NULL",
                (at, token_hash),
            )
            self._conn.commit()
            return cur.rowcount == 1
        return await self._run(_u)

    async def set_replaced_by(self, token_hash: str, *, expected: str, new: str) -> bool:
        def _u() -> bool:
            cur = self._conn.execute(
                "UPDATE refresh_tokens SET replaced_by=? "
                "WHERE token_hash=? AND replaced_by=? AND revoked_at IS NULL",
                (new, token_hash, expected),
            )
            self._conn.commit()
            return cur.rowcount == 1
        return await self._run(_u)

    async def revoke_family(self, family_id: str, *, at: str) -> int:
        return await self._revoke("family_id", family_id, at)

    async def revoke_subject(self, sub: str, *, at: str) -> int:
        return await self._revoke("sub", sub, at)

    async def _revoke(self, column: str, value: str, at: str) -> int:
        def _r() -> int:
            cur = self._conn.execute(
                f"UPDATE refresh_tokens SET revoked_at=? WHERE {column}=? AND revoked_at IS NULL",
                (at, value),
            )
            self._conn.commit()
            return cur.rowcount
        return await self._run(_r)

    def close(self) -> None:
        self._conn.close()


class PostgresRefreshTokenStore(PgSchemaInit):
    """asyncpg on the shared pool; the pool is published only once the table
    exists (:class:`~cairn.core.pg_schema_init.PgSchemaInit`)."""

    _CREATE = (
        "CREATE TABLE IF NOT EXISTS refresh_tokens ("
        " token_hash TEXT PRIMARY KEY, family_id TEXT NOT NULL, sub TEXT NOT NULL,"
        " account_id TEXT, remember BOOLEAN NOT NULL, issued_at TEXT NOT NULL,"
        " expires_at TEXT NOT NULL, used_at TEXT, replaced_by TEXT, revoked_at TEXT)"
    )

    def __init__(self, dsn: str | None = None) -> None:
        self._dsn = dsn
        self._pool = None
        self._schema_ready = False

    async def _open_pool(self) -> Any:
        try:
            import asyncpg  # noqa: F401, PLC0415 — optional dep (storage-postgres)
        except ImportError as e:
            raise RuntimeError(
                "the Postgres refresh-token store requires the 'storage-postgres' optional dep."
            ) from e
        if not self._dsn:
            raise RuntimeError("PostgresRefreshTokenStore needs a DSN (CAIRN_STORAGE=postgres://…).")
        from cairn.core.storage.postgres.pool import shared_pool  # noqa: PLC0415 — optional dep

        return await shared_pool(self._dsn)

    async def _ensure_schema(self, pool: Any) -> None:
        async with pool.acquire() as c:
            await c.execute(self._CREATE)
            await c.execute("CREATE INDEX IF NOT EXISTS idx_refresh_tokens_family ON refresh_tokens (family_id)")
            await c.execute("CREATE INDEX IF NOT EXISTS idx_refresh_tokens_sub ON refresh_tokens (sub)")

    async def insert(self, tok: RefreshToken) -> RefreshToken:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            await c.execute(
                f"INSERT INTO refresh_tokens ({', '.join(_COLUMNS)}) VALUES "
                f"({', '.join(f'${i}' for i in range(1, len(_COLUMNS) + 1))})",
                tok.token_hash, tok.family_id, tok.sub, tok.account_id, tok.remember,
                tok.issued_at, tok.expires_at, tok.used_at, tok.replaced_by, tok.revoked_at,
            )
        return tok

    async def get(self, token_hash: str) -> Optional[RefreshToken]:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            row = await c.fetchrow(
                f"SELECT {', '.join(_COLUMNS)} FROM refresh_tokens WHERE token_hash=$1", token_hash)
        return _from_row(row) if row else None

    async def mark_used(self, token_hash: str, *, at: str, replaced_by: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            status = await c.execute(
                "UPDATE refresh_tokens SET used_at=$1, replaced_by=$2 "
                "WHERE token_hash=$3 AND used_at IS NULL AND revoked_at IS NULL",
                at, replaced_by, token_hash,
            )
        return status.split()[-1] == "1"  # asyncpg returns e.g. "UPDATE 1"

    async def revoke_unused(self, token_hash: str, *, at: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            status = await c.execute(
                "UPDATE refresh_tokens SET revoked_at=$1 "
                "WHERE token_hash=$2 AND used_at IS NULL AND revoked_at IS NULL",
                at, token_hash,
            )
        return status.split()[-1] == "1"

    async def set_replaced_by(self, token_hash: str, *, expected: str, new: str) -> bool:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            status = await c.execute(
                "UPDATE refresh_tokens SET replaced_by=$1 "
                "WHERE token_hash=$2 AND replaced_by=$3 AND revoked_at IS NULL",
                new, token_hash, expected,
            )
        return status.split()[-1] == "1"

    async def revoke_family(self, family_id: str, *, at: str) -> int:
        return await self._revoke("family_id", family_id, at)

    async def revoke_subject(self, sub: str, *, at: str) -> int:
        return await self._revoke("sub", sub, at)

    async def _revoke(self, column: str, value: str, at: str) -> int:
        pool = await self._get_pool()
        async with pool.acquire() as c:
            status = await c.execute(
                f"UPDATE refresh_tokens SET revoked_at=$1 WHERE {column}=$2 AND revoked_at IS NULL",
                at, value,
            )
        return int(status.split()[-1])

    def close(self) -> None:
        pass  # the pool lives for the process (parity with the sibling stores)


_default_store: Any = None


def default_refresh_token_store() -> Any:
    """Process-wide store. ``CAIRN_STORAGE`` selects it (``postgres://…`` →
    Postgres, else the shared ``cairn.db``). Tests swap in their own."""
    global _default_store
    if _default_store is None:
        url = getenv("CAIRN_STORAGE", "").strip()
        if url.startswith(("postgres://", "postgresql://")):
            _default_store = PostgresRefreshTokenStore(dsn=url)
        else:
            _default_store = SqliteRefreshTokenStore()
    return _default_store


# ─── the rules ─────────────────────────────────────────────────────


def _new_raw() -> str:
    return secrets.token_urlsafe(32)


async def issue(sub: str, account_id: Optional[str], *, remember: bool,
                store: Any = None) -> tuple[str, RefreshToken]:
    """Start a new family for a sign-in. Returns the raw token (for the cookie,
    nowhere else) and the stored row."""
    store = store or default_refresh_token_store()
    now = _now()
    lifetime = timedelta(days=remember_days()) if remember else timedelta(hours=SESSION_HOURS)
    raw = _new_raw()
    row = RefreshToken(
        token_hash=hash_token(raw), family_id=uuid.uuid4().hex, sub=sub,
        account_id=account_id, remember=remember, issued_at=_iso(now),
        expires_at=_iso(now + lifetime),
    )
    await store.insert(row)
    return raw, row


def _within_grace(row: RefreshToken, now: datetime) -> bool:
    """Whether ``row`` was rotated less than :data:`REUSE_GRACE_SECONDS` ago. A
    rotation more than :data:`_CLOCK_SKEW_SECONDS` in the future is not."""
    if row.used_at is None:
        return False
    elapsed = (now - datetime.fromisoformat(row.used_at)).total_seconds()
    return -_CLOCK_SKEW_SECONDS <= elapsed < REUSE_GRACE_SECONDS


def _revoked_long_enough_ago(row: RefreshToken, now: datetime) -> bool:
    """Whether ``row`` was revoked at least :data:`REUSE_GRACE_SECONDS` ago."""
    if row.revoked_at is None:
        return False
    return (now - datetime.fromisoformat(row.revoked_at)).total_seconds() >= REUSE_GRACE_SECONDS


async def validate(raw: str, *, store: Any = None) -> RefreshToken:
    """The row for ``raw`` if it may be refreshed, or :class:`RefreshError`.

    A row that was already rotated is reuse, and its whole family is revoked
    before refusing, unless it was rotated less than :data:`REUSE_GRACE_SECONDS`
    ago (ADR-0144): then it is returned, and :func:`rotate` renews it as a replay.

    A revoked row is refused. If it was never used and was revoked at least
    :data:`REUSE_GRACE_SECONDS` ago, it is a child a replay superseded (ADR-0145):
    whoever holds it is the client the replay displaced, so the replay may have
    been a copy of the token. The family is revoked, and if that ended anything
    (rows a sign-out, password change or earlier reuse already revoked end
    nothing) it is logged as a WARNING. Within the window the row is refused
    without that, so a request already in flight with the just-superseded child
    cannot end the sibling that replaced it."""
    store = store or default_refresh_token_store()
    row = await store.get(hash_token(raw)) if raw else None
    if row is None:
        raise RefreshError("unknown")
    if row.revoked_at is not None:
        if row.used_at is None and _revoked_long_enough_ago(row, _now()):
            n = await store.revoke_family(row.family_id, at=_iso(_now()))
            if n > 0:
                logger.warning("superseded refresh token presented, family revoked: sub=%s family=%s…",
                               row.sub, row.family_id[:8])
        raise RefreshError("revoked")
    if row.used_at is not None and not _within_grace(row, _now()):
        await revoke_family(row, reason="reuse of a rotated token", store=store)
        raise RefreshError("reused")
    if row.seconds_left() <= 0:
        raise RefreshError("expired")
    return row


async def rotate(row: RefreshToken, *, store: Any = None) -> tuple[str, RefreshToken]:
    """Mark ``row`` used and issue the next token in its family (same expiry).

    The next row is written BEFORE the compare-and-set. A loser's revocation
    runs only after the winner's compare-and-set, so it always finds the
    winner's new row in place and revokes it too; written after, the winner's
    row could land behind the revocation and keep the family alive.

    If ``row`` was already rotated (a replay, or a concurrent refresh that won the
    compare-and-set), :func:`_renew_replay` decides: within the grace window the
    new row replaces ``row``'s child as a sibling; otherwise it is reuse."""
    store = store or default_refresh_token_store()
    now = _iso(_now())
    raw = _new_raw()
    nxt = row.model_copy(update={
        "token_hash": hash_token(raw), "issued_at": now,
        "used_at": None, "replaced_by": None, "revoked_at": None,
    })
    await store.insert(nxt)
    if row.used_at is None and await store.mark_used(row.token_hash, at=now, replaced_by=nxt.token_hash):
        return raw, nxt
    return raw, await _renew_replay(row, nxt, store=store)


#: How many times a replay tries to make itself its parent's child. A pass fails
#: only when the parent's compare-and-set fails, that is when another replay of
#: the same parent installed its own sibling in between (or the family was revoked,
#: which the next pass refuses), so only real changes use up attempts; a replay
#: that is merely slow, or stopped half way, uses none.
_REPLAY_ATTEMPTS = 5


async def _renew_replay(row: RefreshToken, nxt: RefreshToken, *, store: Any) -> RefreshToken:
    """``row`` was already rotated; make ``nxt`` (already written, unused) its
    only live child, or refuse (ADR-0144).

    Within :data:`REUSE_GRACE_SECONDS` of ``row``'s rotation, and while its family
    is live: revoke the child ``row`` was rotated into while that child is unused,
    then point ``row`` at ``nxt`` with a compare-and-set on ``replaced_by``. That
    compare-and-set is the decision: of two concurrent replays exactly one
    installs its sibling, and the other re-reads the parent and revokes that
    sibling in turn. A child another replay already revoked is dead either way,
    so a replay that finds it so goes straight to the compare-and-set rather than
    waiting for the other replay to finish. A child that was already used means a
    second chain grows from it, so that is reuse. Outside the window it is reuse.
    Reuse revokes the family, as before."""
    for _ in range(_REPLAY_ATTEMPTS):
        now = _now()
        parent = await store.get(row.token_hash)
        if parent is None or parent.revoked_at is not None:
            await store.revoke_unused(nxt.token_hash, at=_iso(now))
            raise RefreshError("revoked")
        if not _within_grace(parent, now):
            await revoke_family(parent, reason="reuse of a rotated token", store=store)
            raise RefreshError("reused")
        child = parent.replaced_by or ""
        if child and not await store.revoke_unused(child, at=_iso(now)):
            current = await store.get(child)
            if current is None or current.used_at is not None:
                await revoke_family(parent, reason="replay of a token whose child was already rotated",
                                    store=store)
                raise RefreshError("reused")
            # revoked already (by another replay of this parent): dead either way
        if await store.set_replaced_by(parent.token_hash, expected=child, new=nxt.token_hash):
            logger.info("refresh token replayed within %ss of its rotation, renewed: sub=%s family=%s…",
                        REUSE_GRACE_SECONDS, parent.sub, parent.family_id[:8])
            return nxt
        # the parent moved on: another replay installed its sibling first; go again
    await revoke_family(row, reason="replays of one token kept racing", store=store)
    raise RefreshError("reused")


async def revoke_family(row: RefreshToken, *, reason: str, store: Any = None) -> int:
    """Revoke ``row``'s family because something is wrong (reuse, a user who may
    no longer sign in), with a WARNING naming the user and the family id's first
    8 characters: never the token or its hash."""
    store = store or default_refresh_token_store()
    n = await store.revoke_family(row.family_id, at=_iso(_now()))
    logger.warning("refresh-token family revoked (%s): sub=%s family=%s…",
                   reason, row.sub, row.family_id[:8])
    return n


async def revoke_family_of(raw: str, *, store: Any = None) -> bool:
    """Revoke the family ``raw`` belongs to (sign-out). ``False`` when the token is
    unknown, so sign-out stays idempotent."""
    store = store or default_refresh_token_store()
    row = await store.get(hash_token(raw)) if raw else None
    if row is None:
        return False
    await store.revoke_family(row.family_id, at=_iso(_now()))
    logger.info("refresh-token family signed out: sub=%s family=%s…", row.sub, row.family_id[:8])
    return True


async def revoke_user(sub: str, *, store: Any = None) -> int:
    """Revoke every family of ``sub`` (a password change, a disabled user)."""
    store = store or default_refresh_token_store()
    n = await store.revoke_subject(sub, at=_iso(_now()))
    logger.info("refresh tokens revoked for sub=%s (%d row(s))", sub, n)
    return n


__all__ = [
    "DEFAULT_REMEMBER_DAYS",
    "InMemoryRefreshTokenStore",
    "PostgresRefreshTokenStore",
    "RefreshError",
    "RefreshToken",
    "REUSE_GRACE_SECONDS",
    "SESSION_HOURS",
    "SqliteRefreshTokenStore",
    "default_refresh_token_store",
    "hash_token",
    "issue",
    "remember_days",
    "revoke_family",
    "revoke_family_of",
    "revoke_user",
    "rotate",
    "validate",
]
