"""Auth tokens and password hashing — the pure helpers under the HTTP auth layer.

Minting and verifying the control plane's JWTs and hashing passwords need no web
framework, so they live here, below the HTTP layer (#2416, ADR-0120 D6): the
account store hashes a password and the operator layer mints a worker token
without importing ``cairn.core.triggers``. :mod:`cairn.core.triggers.auth` keeps
the FastAPI side (``POST /token``, the bearer middleware, ``require_auth`` /
``require_admin``) and imports these from here; its names are unchanged.

Moved out of ``cairn/core/triggers/auth.py`` unchanged. Configuration is the same
environment (read lazily so tests can monkeypatch): ``CAIRN_JWT_SECRET`` (its
presence enables auth), ``CAIRN_JWT_ALGORITHM``, ``CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES``.
Password hashing uses ``pwdlib[argon2]`` — a base dependency since 0.1.2, imported lazily;
token verification uses only PyJWT, a base dependency.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import jwt

_DEFAULT_ALGORITHM = "HS256"
_DEFAULT_EXPIRE_MINUTES = 30


# ─── config (env, read lazily so tests can monkeypatch) ────────────


def auth_enabled() -> bool:
    """Auth is enforced iff a signing secret is configured."""
    return bool(getenv("CAIRN_JWT_SECRET", "").strip())


def _secret() -> str:
    s = getenv("CAIRN_JWT_SECRET", "").strip()
    if not s:
        raise RuntimeError("CAIRN_JWT_SECRET is not set — auth is disabled")
    return s


def _algorithm() -> str:
    return getenv("CAIRN_JWT_ALGORITHM", "").strip() or _DEFAULT_ALGORITHM


def _expire_minutes() -> int:
    raw = getenv("CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES", "").strip()
    try:
        return int(raw) if raw else _DEFAULT_EXPIRE_MINUTES
    except ValueError:
        return _DEFAULT_EXPIRE_MINUTES


# ─── password hashing (lazy pwdlib import) ─────────────────────────

_PH = None
_DUMMY_HASH: Optional[str] = None


class PasswordHashingUnavailable(RuntimeError):
    """The ``auth`` extra (``pwdlib[argon2]``) is not installed. A RuntimeError as
    before; typed so the HTTP layer answers 501 naming the extra, not 500 (#2536)."""


def _password_hash():
    global _PH
    if _PH is None:
        try:
            from pwdlib import PasswordHash  # a base dependency since 0.1.2; guarded for older installs
        except ImportError as e:
            raise PasswordHashingUnavailable(
                "password hashing needs the 'auth' extra: "
                "pip install 'cairndev[auth]' (pwdlib[argon2])"
            ) from e
        _PH = PasswordHash.recommended()
    return _PH


def hash_password(password: str) -> str:
    """argon2 hash for the admin password (used by ``remote hash-password`` + tests)."""
    return _password_hash().hash(password)


def _dummy_hash() -> str:
    global _DUMMY_HASH
    if _DUMMY_HASH is None:
        _DUMMY_HASH = _password_hash().hash("cairn-dummy-password")
    return _DUMMY_HASH


# ─── token issue / verify ──────────────────────────────────────────


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = dict(data)
    expire = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=_expire_minutes())
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, _secret(), algorithm=_algorithm())


def decode_subject(token: str) -> Optional[str]:
    """Return the token's subject if the signature + expiry validate, else ``None``."""
    try:
        payload = jwt.decode(token, _secret(), algorithms=[_algorithm()])
    except jwt.InvalidTokenError:
        return None
    sub = payload.get("sub")
    return sub if isinstance(sub, str) and sub else None


def decode_claims(token: str) -> Optional[dict[str, Any]]:
    """Return the full validated claim set (``sub``, ``account_id``, …) or ``None``.

    Used by the admin API to read the ``account_id`` claim (ADR-0054) without a
    second store round-trip. Same signature/expiry validation as
    :func:`decode_subject`."""
    try:
        payload = jwt.decode(token, _secret(), algorithms=[_algorithm()])
    except jwt.InvalidTokenError:
        return None
    sub = payload.get("sub")
    return payload if isinstance(sub, str) and sub else None
