"""JWT auth for the cairn trigger server (an internal issue).

OAuth2 *password flow* + JWT, following the FastAPI tutorial
(https://fastapi.tiangolo.com/tutorial/security/oauth2-jwt/): ``POST /token``
issues a signed JWT (PyJWT); a middleware validates the
``Authorization: Bearer <jwt>`` header on every control-plane route.

**Enforcement is opt-in** — auth is required *iff* ``CAIRN_JWT_SECRET`` is set.
Unset → the server runs open (keyless local-dev) with a loud warning, matching
the engine's "optional to turn ON, not to ESCAPE" convention. ``make serve``
and the existing demos keep working unchanged; a deployment turns auth on by
exporting a secret + admin credentials before exposing the server.

Env knobs
---------
``CAIRN_JWT_SECRET``                   HS256 signing secret; *its presence enables auth*.
``CAIRN_JWT_ALGORITHM``                default ``"HS256"``.
``CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES``  default ``30``.
``CAIRN_API_USER``                     admin username (default ``"admin"``).
``CAIRN_API_PASSWORD_HASH``            argon2 hash of the admin password (preferred).
``CAIRN_API_PASSWORD``                 plaintext admin password (dev fallback; hashed in memory).

Password hashing uses ``pwdlib[argon2]`` — the ``auth`` extra
(``pip install 'cairndev[auth]'``). It is imported lazily, so the base install
and unauthenticated dev never pull it in; only ``POST /token`` needs it. Token
*verification* (the hot path) uses only PyJWT, which is a base dependency.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import logging
import os
import time
from typing import Annotated, Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel

from cairn.core._aio import run_sync
from cairn.core.auth_tokens import (  # the pure helpers live below the HTTP layer (#2416)
    PasswordHashingUnavailable,
    _dummy_hash,
    _password_hash,
    _secret,  # noqa: F401 — re-exported: routers import it from here
    auth_enabled,
    create_access_token,
    decode_claims,
    decode_subject,
    hash_password,
)

logger = logging.getLogger(__name__)

_DEFAULT_USER = "admin"

# Paths reachable without a token even when auth is enabled: health checks and
# the login endpoint itself. Nothing here may describe the control plane.
_OPEN_PATHS = frozenset({
    "/", "/healthz", "/token", "/auth/status",
    # #1612 — accepting an invite is necessarily unauthenticated: the invitee
    # has no account yet, and the token in the link IS the credential. Gating
    # it would make invites impossible to redeem, which is how this was caught
    # (both routes 401'd end-to-end while every unit test passed).
    "/auth/accept-invite",
    # #1613 — signup is necessarily unauthenticated (that is the point). The GET
    # reports whether the door is open at all, so the login page can offer a
    # link only when it would work; the POST is the door. When
    # `CAIRN_SIGNUP=invite` (the default) the POST refuses with `signup_closed`,
    # so being routable is not being open.
    "/auth/signup",
})

#: #1612 — the invite PREVIEW is a prefix route (`/auth/invite/{token}`), so it
#: cannot live in the exact-match set above. Deliberately narrow: nothing else
#: is mounted under this prefix.
#:
#: What it exposes to a caller holding a valid token: the account NAME, the
#: role, and the email it was issued to — i.e. what the invitation itself says.
#: The token is 32 random bytes, single-use and expiring, so this is not a
#: browsable surface; without one you get a 404 that does not distinguish
#: "wrong token" from "no such account".
#:
#: #2336 — ``/auth/oauth/`` holds only the Google/GitHub sign-in ``start`` and
#: ``callback`` redirects: the caller has no session yet by definition. Each
#: is guarded by its own signed state + browser-bound cookie, not a JWT.
_PUBLIC_PATH_PREFIXES = ("/auth/invite/", "/auth/oauth/")

# The interactive API docs. Open in keyless dev, where they are the fastest way
# to learn the surface — and behind auth once a secret is set, because on a
# deployed control plane they publish the whole route and schema map to anyone
# who can reach the port (#1507).
#
# Nothing consumes these over HTTP: `console/openapi.json` is generated offline
# by `scripts/dump_openapi.py` building the app in-process, so gating them
# breaks no build step.
_DOCS_PATHS = frozenset({
    "/docs", "/openapi.json", "/redoc", "/docs/oauth2-redirect",
})

# Spec-declared trigger ingress paths (webhook receivers) that authenticate via
# their OWN signature verification (``ingress.signature`` / ``provider.verify``),
# NOT a JWT — a real Alertmanager/PagerDuty/GitHub sender presents an HMAC
# signature, never a bearer token, so JWT-gating them 401s every real webhook
# before its signature is ever checked (issue #686). Populated at app-build time
# by ``_mount_trigger_routes`` for each signed ingress; a receiver with no
# ``signature`` block is NOT registered here and stays JWT-gated. The legacy
# ``/triggers/manual`` · ``/triggers/scheduled`` · ``/triggers/inbox*`` routes
# are console/CLI-facing and are never registered — they keep the JWT guard.
_PUBLIC_TRIGGER_PATHS: set[str] = set()


def register_public_trigger_path(path: str) -> None:
    """Mark a spec-declared trigger ingress path as reachable without a JWT.

    Its authn is signature verification in the trigger router (``provider.verify``
    — the mechanism real webhook senders use), so the JWT middleware must let it
    through. Called by ``_mount_trigger_routes`` per signed ingress (#686).
    """
    _PUBLIC_TRIGGER_PATHS.add(path)


def _is_public_path(path: str) -> bool:
    """Whether ``path`` is reachable without a bearer token when auth is enabled.

    The exact ``_OPEN_PATHS`` set, the console SPA's static assets, PLUS any
    signature-authenticated trigger ingress registered via
    ``register_public_trigger_path`` (#686): a browser must load the SPA (and its
    login gate) *before* it has a token, so ``/console`` and its bundled assets
    are public; and external webhook senders authenticate by signature, not JWT.
    The console's read-API (``/console/api``) and the flat control-plane routes
    stay protected — the SPA calls them with the token it gets from
    ``POST /token``. (Without the console rule, the first auth-ON deployment 401s
    the login page itself — issue #391 follow-up.)
    """
    if path in _OPEN_PATHS:
        return True
    # Docs are public only in keyless dev (#1507). `_is_public_path` is reached
    # solely when auth_enabled() is already true, so this is a plain deny —
    # kept as an explicit named set so the reason survives the next reader.
    if path in _DOCS_PATHS:
        return False
    if path in _PUBLIC_TRIGGER_PATHS:
        return True
    if any(path.startswith(pfx) for pfx in _PUBLIC_PATH_PREFIXES):
        return True
    if path == "/console" or (
        path.startswith("/console/") and not path.startswith("/console/api")
    ):
        return True
    return False


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ─── config (env, read lazily so tests can monkeypatch) ────────────


def require_auth_enabled() -> bool:
    """Whether the deployment INSISTS on auth (``CAIRN_REQUIRE_AUTH``, #391).

    When set, an unset ``CAIRN_JWT_SECRET`` is a hard startup failure instead of a
    warning — so a prod stack can never come up with an open control plane.
    """
    return getenv("CAIRN_REQUIRE_AUTH", "").strip().lower() in {"1", "true", "yes", "on"}


_HASH_PROBLEM_LOGGED = False


def password_hash_problem() -> Optional[str]:
    """Why ``CAIRN_API_PASSWORD_HASH`` cannot work, or ``None`` if it can (#1853).

    Unset is fine (keyless dev, or ``CAIRN_API_PASSWORD`` / store users instead).
    SET but unparseable is the case this exists for: compose interpolates the
    ``$`` segments out of an argon2 hash — ``$argon2id$v=19$m=65536,…`` reaches
    the process as ``=19=65536,…`` — and pwdlib then raises UnknownHashError,
    which the login path reported as "incorrect username or password". That
    misdirection was the expensive part: every login fails, nothing says why,
    and the natural response is to reset a password that was already right.
    Verified on prod 2026-09-01; the fix on that box was ``$$`` escaping.

    Called at boot (server lifespan, fail closed), by ``cairn deploy doctor``,
    and by the login path to log the real cause once.
    """
    h = getenv("CAIRN_API_PASSWORD_HASH", "").strip()
    if not h:
        return None
    try:
        _password_hash().verify("cairn-hash-format-probe", h)  # bool either way if parseable
        return None
    except Exception as e:  # noqa: BLE001 — UnknownHashError, or a malformed argon2 payload
        return (
            f"CAIRN_API_PASSWORD_HASH is set but cannot be parsed ({type(e).__name__}: "
            f"starts {h[:12]!r}). Every login will fail as 'incorrect password' "
            "with the real cause hidden. Most likely compose interpolated the '$' "
            "segments out of the argon2 hash — write it with '$$' (compose "
            "un-escapes to '$'), or move it out of .env. See #1853."
        )


def _configured_user() -> Optional[tuple[str, str]]:
    """``(username, password_hash)`` from env, or ``None`` if no admin configured."""
    user = getenv("CAIRN_API_USER", "").strip() or _DEFAULT_USER
    h = getenv("CAIRN_API_PASSWORD_HASH", "").strip()
    if h:
        return (user, h)
    pw = getenv("CAIRN_API_PASSWORD", "")
    if pw:
        return (user, hash_password(pw))
    return None


def _verify_env_admin(username: str, password: str, ph) -> Optional[str]:
    """The legacy single-admin check (``CAIRN_API_USER``/``CAIRN_API_PASSWORD``).

    Returns the admin username on success, else ``None``. Verifies against a
    dummy hash on no-config so timing stays roughly constant."""
    cfg = _configured_user()
    if cfg is None:
        ph.verify(password, _dummy_hash())
        return None
    cfg_user, cfg_hash = cfg
    global _HASH_PROBLEM_LOGGED
    try:
        ok_pass = ph.verify(password, cfg_hash)
    except Exception:  # noqa: BLE001 - any verify error == auth failure
        ok_pass = False
        # #1853 — a hash that cannot be PARSED is a deployment fault, not a
        # wrong password. It still fails closed, but the real cause is logged
        # (once per process, so a brute-force attempt cannot flood the log)
        # instead of being indistinguishable from a typo.
        if not _HASH_PROBLEM_LOGGED:
            problem = password_hash_problem()
            if problem:
                _HASH_PROBLEM_LOGGED = True
                logger.error("login rejected because the configured hash is unusable: %s", problem)
    return cfg_user if (username == cfg_user and ok_pass) else None


class AuthIdentity(BaseModel):
    """A resolved login (ADR-0054 phase 1). ``sub`` is what lands in the JWT
    ``sub`` claim — a **user id** for store users, or the admin **username** for
    the legacy env-admin (which has no user record). ``account_id`` is the tenant
    the user belongs to (``None`` for the env-admin, resolved to ``default``)."""

    sub: str
    username: str
    account_id: Optional[str] = None


async def _resolve_store_user(username: str, password: str, ph) -> Optional["AuthIdentity"]:
    """Verify ``username``/``password`` against the account User store (async).

    Returns an :class:`AuthIdentity` on success, else ``None``. A disabled user
    never authenticates. Any store/import error is swallowed → the caller falls
    back to the env admin (back-compat: a deploy with no user store configured
    behaves exactly as before)."""
    try:
        from cairn.core import account as _acct  # noqa: PLC0415 — avoids an auth↔account import cycle

        store = _acct.get_account_store()
        user = await store.get_user_by_username(username)
    except Exception:  # noqa: BLE001 — no store / import error → fall back to env admin
        return None
    if user is None or user.disabled or not user.password_hash:
        # keep timing roughly constant when the user is absent/disabled
        ph.verify(password, _dummy_hash())
        return None
    try:
        ok = ph.verify(password, user.password_hash)
    except Exception:  # noqa: BLE001 — any verify error == auth failure
        ok = False
    if not ok:
        return None
    try:
        from cairn.core import account as _acct  # noqa: PLC0415 — cycle guard (see above)

        acct_id = await _acct.get_account_store().get_account_for_user(user.id)
    except Exception:  # noqa: BLE001
        acct_id = None
    return AuthIdentity(sub=user.id, username=user.username, account_id=acct_id)


async def resolve_identity_async(username: str, password: str) -> Optional["AuthIdentity"]:
    """Async authentication resolver — the source of truth (ADR-0054 phase 1).

    Precedence: the **User store first** (multi-user), then a **fallback to the
    legacy env admin** (``CAIRN_API_USER``/``CAIRN_API_PASSWORD``) so existing
    single-admin deploys keep working unchanged. Returns ``None`` on failure.
    Awaited directly by ``POST /token`` (which already runs on the event loop);
    the sync :func:`resolve_identity` wraps it for non-async callers."""
    ph = _password_hash()
    ident = await _resolve_store_user(username, password, ph)
    if ident is not None:
        return ident
    admin = _verify_env_admin(username, password, ph)
    if admin is not None:
        # env admin has no user record / membership → the implicit default account.
        return AuthIdentity(sub=admin, username=admin, account_id="default")
    return None


def resolve_identity(username: str, password: str) -> Optional["AuthIdentity"]:
    """Sync wrapper over :func:`resolve_identity_async` for non-async callers
    (CLI, tests). Safe to call from inside a running loop (runs on a private
    loop in a worker thread)."""
    return run_sync(resolve_identity_async(username, password))


async def _user_store_populated_async() -> bool:
    """Whether the account User store has at least one user (ADR-0054).

    Lets ``POST /token`` accept logins when the only admin lives in the store and
    no env admin is configured. Any store/import error → ``False`` (treat as
    "no store", back-compat)."""
    try:
        from cairn.core import account as _acct  # noqa: PLC0415 — avoids an auth↔account cycle

        return bool(await _acct.get_account_store().list_users())
    except Exception:  # noqa: BLE001
        return False


def authenticate_user(username: str, password: str) -> Optional[str]:
    """Return the auth *subject* on success, else ``None`` (back-compat surface).

    Now backed by :func:`resolve_identity`: the User store is consulted first,
    falling back to the legacy env admin. The returned ``sub`` is the user id for
    store users and the admin username for the env admin (which has no id) — so a
    deploy with no user store behaves exactly as before.
    """
    ident = resolve_identity(username, password)
    return ident.sub if ident is not None else None


async def caller_identity(token: Optional[str]) -> dict[str, Any]:
    """The authenticated caller's identity for the console's ``GET /auth/me``
    (ADR-0054). Returns ``{auth_enabled, username, account_id, role, is_admin}`` so
    the SPA can show/hide admin-only surfaces — the API stays the real gate.

    Auth-off (keyless dev) → a full-access ``owner`` identity. Auth-on with an
    invalid/absent token raises ``401``. The legacy env admin resolves to
    ``owner``; a store user's role comes from its membership in the token's
    account (defaulting to ``member`` if none is found)."""
    if not auth_enabled():
        return {"auth_enabled": False, "username": "dev", "account_id": "default", "role": "owner", "is_admin": True}
    claims = decode_claims(token) if token else None
    if claims is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    sub = str(claims["sub"])
    account_id = str(claims.get("account_id") or "default")
    cfg = _configured_user()
    if cfg is not None and sub == cfg[0]:  # legacy env admin — full control
        return {"auth_enabled": True, "username": sub, "account_id": account_id, "role": "owner", "is_admin": True}
    from cairn.core import account as _acct  # noqa: PLC0415 — avoids an auth↔account cycle
    store = _acct.get_account_store()
    username, role = sub, "member"
    try:
        u = await store.get_user(sub)
        if u is not None:
            username = u.username
        # #1611 (ADR-0094 D2) — MEMBERSHIP is authoritative for the account, not
        # the token claim. The claim above is a hint and a fast path; it stays
        # in the token (it is what the env-admin path and a store outage fall
        # back to), but it stops deciding which tenant a request acts in.
        #
        # Two reasons this matters more than it looks:
        #  * a membership change takes effect on the NEXT request instead of
        #    whenever the token happens to expire — moving or removing someone
        #    is otherwise not enforceable without invalidating sessions;
        #  * it is the whole seam for multi-account later. "A user belongs to
        #    several accounts" becomes "this lookup returns more than one, plus
        #    a switcher" — no token format change, no re-issued credentials.
        #    Today it returns exactly one (ADR-0094 D2: one email, one account).
        live = await store.get_account_for_user(sub)
        if live:
            account_id = live
        for m in await store.list_members(account_id):
            if m.user_id == sub:
                role = m.role
                break
    except Exception:  # noqa: BLE001 — a store hiccup shouldn't 500 /auth/me
        pass
    return {"auth_enabled": True, "username": username, "account_id": account_id,
            "role": role, "is_admin": role in _acct.ADMIN_ROLES}


# ─── reusable auth dependency ──────────────────────────────────────

# `auto_error=False`: a missing/blank Authorization header yields ``None``
# instead of an auto-401, so ``require_auth`` can wave requests through when
# auth is disabled (no ``CAIRN_JWT_SECRET``) — matching the middleware's
# fail-open contract. The ``tokenUrl`` only feeds the OpenAPI "Authorize"
# button; it's the same login route ``install_auth`` mounts.
_oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token", auto_error=False)


def require_auth(token: Annotated[Optional[str], Depends(_oauth2_scheme)]) -> Optional[str]:
    """FastAPI dependency that requires a valid bearer token on a route/router.

    Reuses :func:`decode_subject` for verification — no parallel validation
    logic. Fail-OPEN exactly like the ``install_auth`` middleware: when
    ``CAIRN_JWT_SECRET`` is unset (auth disabled) it returns ``None`` and never
    rejects, so keyless local-dev and the existing tests are unaffected. When
    auth is enabled it returns the authenticated subject or raises ``401``.

    Composes with the global ``_bearer_guard`` middleware rather than replacing
    it: the middleware is the blanket guard for every control-plane path, while
    this dependency lets an individual route/router opt in explicitly (and hand
    the verified subject to the handler). Apply via
    ``APIRouter(dependencies=[Depends(require_auth)])`` or a route's
    ``dependencies=[...]``.
    """
    if not auth_enabled():
        return None
    subject = decode_subject(token) if token else None
    if subject is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return subject


async def _subject_is_admin(subject: str) -> bool:
    """Whether ``subject`` (a JWT ``sub``) is an owner/admin (ADR-0054 §3).

    Two ways to be an admin:
      1. The subject is a **store user** with an owner/admin membership.
      2. The subject is the **legacy env admin** username (single-admin mode) —
         it has full control by definition, preserving back-compat.
    Any store/import error → treat only the env-admin match as admin.

    Awaited on the server's own loop: the Postgres account store shares one
    asyncpg pool per DSN, bound to that loop, so a lookup on a private loop
    (``run_sync``) failed every time and every signed-up owner read as
    not-admin on prod — 403 on setup, invites and users."""
    env_admin = None
    cfg = _configured_user()
    if cfg is not None:
        env_admin = cfg[0]
    if subject == env_admin:
        return True
    try:
        from cairn.core import account as _acct  # noqa: PLC0415 — avoids an auth↔account cycle

        store = _acct.get_account_store()
        acct_id = await store.get_account_for_user(subject)
        if acct_id is None:
            return False
        for m in await store.list_members(acct_id):
            if m.user_id == subject:
                return m.role in _acct.ADMIN_ROLES
        return False
    except Exception:  # noqa: BLE001
        logger.warning("admin check failed for subject=%s; treating as not admin", subject, exc_info=True)
        return False


async def require_admin(token: Annotated[Optional[str], Depends(_oauth2_scheme)]) -> Optional[str]:
    """FastAPI dependency: require an authenticated **owner/admin** (ADR-0054 §3).

    Gates the admin-invite API. Fail-OPEN like :func:`require_auth` when auth is
    disabled (keyless dev returns ``None``). When auth is on it verifies the token
    then checks the subject holds an admin role (or is the legacy env admin),
    raising ``403`` otherwise."""
    if not auth_enabled():
        return None
    subject = decode_subject(token) if token else None
    if subject is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not await _subject_is_admin(subject):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="admin or owner role required",
        )
    return subject


# ─── FastAPI wiring ────────────────────────────────────────────────


#: Failed-login throttle (#1507). `POST /token` had no lockout, so an open port
#: was an offline-speed password oracle. Keyed by (client-ip, username) so one
#: attacker cannot lock every account out by guessing at it — the classic
#: side-effect of a username-only counter.
_LOGIN_MAX_FAILURES = int(getenv("CAIRN_LOGIN_MAX_FAILURES", "5") or "5")
_LOGIN_LOCKOUT_SECONDS = float(getenv("CAIRN_LOGIN_LOCKOUT_SECONDS", "60") or "60")
#: (ip, username) -> (failure count, epoch seconds when the window started)
_login_failures: dict[tuple[str, str], tuple[int, float]] = {}


def _throttle_key(request: Optional[Request], username: str) -> tuple[str, str]:
    ip = ""
    if request is not None and request.client is not None:
        ip = request.client.host or ""
    return (ip, (username or "").strip().lower())


def login_retry_after(key: tuple[str, str]) -> Optional[int]:
    """Seconds the caller must wait, or None when it may attempt a login."""
    entry = _login_failures.get(key)
    if entry is None:
        return None
    count, started = entry
    elapsed = time.time() - started
    if elapsed >= _LOGIN_LOCKOUT_SECONDS:
        _login_failures.pop(key, None)  # window expired — forget it
        return None
    if count < _LOGIN_MAX_FAILURES:
        return None
    return max(1, int(_LOGIN_LOCKOUT_SECONDS - elapsed))


def _record_login_failure(key: tuple[str, str]) -> None:
    count, started = _login_failures.get(key, (0, time.time()))
    if time.time() - started >= _LOGIN_LOCKOUT_SECONDS:
        count, started = 0, time.time()
    _login_failures[key] = (count + 1, started)


def _clear_login_failures(key: tuple[str, str]) -> None:
    _login_failures.pop(key, None)


async def login_for_access_token(
    request: Request,
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    """``POST /token`` — verify admin credentials, return a signed JWT.

    Module-level (not nested in ``install_auth``) so FastAPI can resolve the
    ``Annotated[..., Depends()]`` dependency against this module's globals — a
    nested function under ``from __future__ import annotations`` cannot, and the
    form then mis-parses as a query param (422).
    """
    if not auth_enabled():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="auth is disabled (CAIRN_JWT_SECRET unset) — no token needed",
        )
    # An admin can be configured via env OR live only in the user store (ADR-0054).
    # Only refuse when NEITHER source can possibly authenticate anyone.
    if _configured_user() is None and not await _user_store_populated_async():
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="auth enabled but no admin configured (set CAIRN_API_USER + "
                   "CAIRN_API_PASSWORD or CAIRN_API_PASSWORD_HASH, or create a user)",
        )
    key = _throttle_key(request, form_data.username)
    retry_after = login_retry_after(key)
    if retry_after is not None:
        # 429 BEFORE verifying, so a locked-out caller cannot keep paying for
        # argon2 hashing — the throttle protects CPU as well as the password.
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="too many failed login attempts — try again shortly",
            headers={"Retry-After": str(retry_after)},
        )
    ident = await resolve_identity_async(form_data.username, form_data.password)
    if ident is None:
        _record_login_failure(key)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    _clear_login_failures(key)
    # JWT: `sub` = user id (or admin username); `account_id` = the resolved tenant.
    claims: dict[str, Any] = {"sub": ident.sub}
    if ident.account_id:
        claims["account_id"] = ident.account_id
    return Token(access_token=create_access_token(claims))


def install_auth(app: FastAPI) -> None:
    """Mount ``POST /token`` and a bearer-token guard middleware on ``app``.

    Enforcement is a no-op when ``CAIRN_JWT_SECRET`` is unset (the middleware
    still registers but waves every request through, with a one-time warning).
    Call once per app, inside ``build_app``.
    """
    if not auth_enabled():
        if require_auth_enabled():
            # #391 hardening: prod refuses to start an open control plane.
            raise RuntimeError(
                "CAIRN_REQUIRE_AUTH is set but CAIRN_JWT_SECRET is not — refusing to "
                "start with an OPEN control plane. Set CAIRN_JWT_SECRET + "
                "CAIRN_API_PASSWORD_HASH, or unset CAIRN_REQUIRE_AUTH for keyless dev."
            )
        logger.warning(
            "AUTH DISABLED — CAIRN_JWT_SECRET is unset; the trigger server's "
            "control plane is OPEN. Set CAIRN_JWT_SECRET + CAIRN_API_PASSWORD[_HASH] "
            "before exposing it (issue #2)."
        )

    app.post("/token", response_model=Token, tags=["auth"])(login_for_access_token)

    @app.exception_handler(PasswordHashingUnavailable)
    async def _no_auth_extra(request: Request, exc: PasswordHashingUnavailable) -> JSONResponse:
        # #2536: login, signup and invite redemption hash a password; on a base
        # install without pwdlib (only possible before 0.1.2) that is a missing feature, not a crash.
        return JSONResponse({"detail": str(exc)}, status_code=status.HTTP_501_NOT_IMPLEMENTED)

    @app.get("/auth/status", tags=["auth"])
    async def auth_status() -> dict[str, bool]:
        """Whether the control plane requires a login. Public (in ``_OPEN_PATHS``)
        so the web console can decide to show the login gate — when auth is
        disabled (no ``CAIRN_JWT_SECRET``, the dev default) the gate is skipped."""
        return {"enabled": auth_enabled(), "login_required": auth_enabled()}

    @app.middleware("http")
    async def _bearer_guard(request: Request, call_next):
        if (
            auth_enabled()
            and request.method != "OPTIONS"
            and not _is_public_path(request.url.path)
        ):
            authz = request.headers.get("Authorization", "")
            token = authz[7:].strip() if authz[:7].lower() == "bearer " else ""
            # SSE: the browser's EventSource API can't set an Authorization
            # header (#967), so accept a same-origin `?access_token=<jwt>` query
            # param as a fallback. The JWT is still validated below; short-lived
            # + same-origin keeps the query-param exposure acceptable.
            if not token:
                token = (request.query_params.get("access_token") or "").strip()
            if not token or decode_subject(token) is None:
                return JSONResponse(
                    {"detail": "Could not validate credentials"},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    headers={"WWW-Authenticate": "Bearer"},
                )
        return await call_next(request)
