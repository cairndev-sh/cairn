"""The OAuth-broker seam (#100).

An :class:`OAuthBroker` brokers *outbound* OAuth — connecting cairn **to**
GitHub / Slack / Jira / … on a user's behalf — and hands cairn's own process a
**fresh access token** (the broker owns the auth-code dance + refresh). cairn
then makes the actual API call itself, so the request/response never leave the
box and every use still passes the ``invoke_operator`` chokepoint. This is the
deliberate contrast with execute-on-their-cloud tool platforms.

Replaceability is the whole point:

* Every call site depends only on this Protocol + ``Connection.config["broker"]``
  — never on a vendor type.
* Swapping the broker = a new :class:`OAuthBroker` impl + flipping
  ``config["broker"]`` (per-connection) or ``CAIRN_OAUTH_BROKER`` (global default).
* Token custody is a property of *which broker* you pick, not of cairn's code.

:class:`AuthlibBroker` is the only shipped implementation (#310, ADR-0052 §8 —
amends ADR-0037): the auth-code dance runs in cairn's own process via Authlib,
cairn keeps full token custody, no sidecar. The Nango impl was removed from the
shipped path (git history has it); the seam stays so a deployment can register
its own broker and flip ``CAIRN_OAUTH_BROKER``.
"""
from __future__ import annotations
from cairn.core.settings import ENV_PREFIX, getenv, legacy_name

import logging
import os
import secrets
import time
from typing import Any, Optional, Protocol, runtime_checkable

from pydantic import BaseModel

from cairn.core.account import DEFAULT_ACCOUNT


class OAuthBrokerError(Exception):
    """A broker call failed (transport error, or no usable token returned)."""


class AuthSession(BaseModel):
    """Handle to an in-progress authorization. The user opens ``redirect_url``
    to complete the provider's consent; the broker finalizes the connection
    keyed by ``connection_id``."""

    redirect_url: str
    connection_id: str
    expires_at: Optional[str] = None


@runtime_checkable
class OAuthBroker(Protocol):
    """Provider-agnostic outbound-OAuth broker. The seam cairn depends on."""

    name: str

    async def start_authorization(
        self, *, provider: str, connection_id: str, scopes: Optional[list[str]] = None
    ) -> AuthSession:
        """Begin an authorization-code flow; return a redirect URL to open."""
        ...

    async def get_access_token(self, *, provider: str, connection_id: str) -> str:
        """Return a **fresh** access token (the broker handles refresh)."""
        ...

    async def revoke(self, *, provider: str, connection_id: str) -> None:
        """Revoke / delete the brokered connection."""
        ...


# ─── registry ───────────────────────────────────────────────────────────

_BROKERS: dict[str, type] = {}


def register_broker(name: str, cls: type) -> None:
    """Register an :class:`OAuthBroker` implementation under ``name``."""
    _BROKERS[name] = cls


def get_oauth_broker(name: Optional[str] = None) -> OAuthBroker:
    """Resolve a broker by name (default ``CAIRN_OAUTH_BROKER`` → ``authlib``,
    the built-in in-process broker — #310, ADR-0052 §8).
    Raises ``KeyError`` if the name is not registered."""
    name = (name or getenv("CAIRN_OAUTH_BROKER", "authlib")).strip() or "authlib"
    cls = _BROKERS.get(name)
    if cls is None:
        raise KeyError(f"unknown oauth broker {name!r}; known: {sorted(_BROKERS)}")
    return cls()


# NangoBroker (impl #1) was removed from the shipped path (#310, ADR-0052 §8 —
# amends ADR-0037): AuthlibBroker below is the only shipped implementation.
# The OAuthBroker Protocol + register_broker() seam stay — a deployment that
# wants Nango (or any other broker) registers its own impl and flips
# CAIRN_OAUTH_BROKER / Connection.config["broker"]. The last shipped NangoBroker
# lives in git history (pre-#310).


# ─── Authlib (the built-in, in-process broker — no sidecar) ──────────────


async def _initiating_account() -> str:
    """The account an authorization is started for: the workspace in scope's (a
    console request's). With auth on and nothing in scope — the operator's own
    process (``cairn connect --via oauth``) — the deployment's ``default``, where
    that command stores its connection row."""
    from cairn.core.workspace import WorkspaceScopeRequired, workspace_account  # lazy: store stack off the import path

    try:
        return await workspace_account()
    except WorkspaceScopeRequired:
        return DEFAULT_ACCOUNT


def _token_expired(rec: dict[str, Any]) -> bool:
    """True if the stored token is at/near expiry (60s skew). No ``expires_at``
    (e.g. GitHub tokens) → treated as non-expiring."""
    exp = rec.get("expires_at")
    if not exp:
        return False
    try:
        return float(exp) <= time.time() + 60
    except (TypeError, ValueError):
        return False


class AuthlibBroker:
    """Built-in OAuth broker — runs the authorization-code dance **in cairn's own
    process** (no sidecar), using Authlib. Provider endpoints + scopes come from
    the file-based catalog (``providers.yaml``); client id/secret are read from
    the SecretStore/env; captured tokens live in the writable token store
    (refresh handled here). The browser is redirected back to cairn's own
    ``/oauth/callback`` (``CAIRN_OAUTH_REDIRECT_URI``), which calls
    :meth:`complete_authorization`.

    cairn keeps full token custody (no sidecar). ``authlib``
    is the optional ``[oauth]`` extra (lazy-imported). A test may inject an httpx
    ``transport`` (MockTransport) to exercise the token endpoint hermetically."""

    name = "authlib"

    def __init__(
        self,
        *,
        redirect_uri: Optional[str] = None,
        store: Any = None,
        transport: Any = None,
    ) -> None:
        self._redirect_uri = redirect_uri or getenv(
            "CAIRN_OAUTH_REDIRECT_URI", "http://localhost:9090/oauth/callback"
        )
        self._store = store
        self._transport = transport  # tests inject httpx.MockTransport

    # ── helpers ──
    def _tokens(self):
        if self._store is not None:
            return self._store
        from cairn.core.oauth.store import default_token_store

        return default_token_store()

    def _spec(self, provider: str):
        from cairn.core.oauth.catalog import get_oauth_provider

        spec = get_oauth_provider(provider)
        if spec is None:
            raise OAuthBrokerError(f"unknown OAuth provider {provider!r} — not in the catalog (providers.yaml)")
        return spec

    def _secret(self, ref: str) -> str:
        # lazy: avoid import-time coupling to the operator/secret stack
        from cairn.core.operator.secrets import get_secret_provider

        provider = get_secret_provider()
        cairn_var = ref.startswith(ENV_PREFIX)
        if provider is not None:
            # ADR-0122 — a secret stored under the OBS_* name still resolves until 1.0
            for name in (ref, legacy_name(ref)) if cairn_var else (ref,):
                try:
                    val = provider.get(name)
                    if val:
                        return val
                except Exception:  # noqa: BLE001 — undeclared/missing → env fallback
                    pass
        return (getenv(ref, "") if cairn_var else os.environ.get(ref, "")) or ""

    def _client_creds(self, provider: str) -> tuple[str, str]:
        p = provider.strip().upper().replace("-", "_")
        cid = self._secret(f"CAIRN_OAUTH_{p}_CLIENT_ID") or self._secret(f"{p}_CLIENT_ID")
        sec = self._secret(f"CAIRN_OAUTH_{p}_CLIENT_SECRET") or self._secret(f"{p}_CLIENT_SECRET")
        if not cid:
            raise OAuthBrokerError(
                f"missing OAuth client id for {provider!r} — set CAIRN_OAUTH_{p}_CLIENT_ID "
                f"(and CAIRN_OAUTH_{p}_CLIENT_SECRET)"
            )
        return cid, sec

    def _client(self, **kw):
        from authlib.integrations.httpx_client import AsyncOAuth2Client  # [oauth] extra

        kw.setdefault("redirect_uri", self._redirect_uri)
        if self._transport is not None:
            kw["transport"] = self._transport
        return AsyncOAuth2Client(**{k: v for k, v in kw.items() if v is not None})

    # ── Protocol ──
    async def start_authorization(
        self, *, provider: str, connection_id: str, scopes: Optional[list[str]] = None
    ) -> AuthSession:
        spec = self._spec(provider)
        cid, _ = self._client_creds(provider)
        chosen = scopes or spec.scopes
        code_verifier = secrets.token_urlsafe(64) if spec.pkce else None
        client = self._client(
            client_id=cid,
            scope=" ".join(chosen),
            code_verifier=code_verifier,
            code_challenge_method="S256" if spec.pkce else None,
        )
        try:
            url, state = client.create_authorization_url(spec.authorize_url, **spec.extra_authorize_params)
        finally:
            await client.aclose()
        await self._tokens().put_pending(
            state,
            {"provider": provider, "connection_id": connection_id, "code_verifier": code_verifier or "", "scopes": chosen,
             # the account the authorization is started for: the callback (which
             # arrives with no caller scope) creates the connection row in it
             "account_id": await _initiating_account()},
        )
        return AuthSession(redirect_url=url, connection_id=connection_id)

    async def complete_authorization(self, *, state: str, code: str) -> str:
        """Finalize the dance (called by ``/oauth/callback``): exchange ``code``
        for tokens and persist them. Returns the connection_id."""
        pending = await self._tokens().pop_pending(state)
        if pending is None:
            raise OAuthBrokerError("unknown or expired OAuth state — restart the authorization")
        provider, connection_id = pending["provider"], pending["connection_id"]
        spec = self._spec(provider)
        cid, sec = self._client_creds(provider)
        client = self._client(client_id=cid, client_secret=sec or None, code_verifier=pending.get("code_verifier") or None)
        try:
            token = await client.fetch_token(spec.token_url, code=code)
        finally:
            await client.aclose()
        await self._tokens().put_token(connection_id, provider, dict(token))
        # A pending record written before the account was recorded is the deployment's.
        account_id = str(pending.get("account_id") or "").strip() or DEFAULT_ACCOUNT
        await self._ensure_connection_record(provider, connection_id, account_id=account_id)
        return connection_id

    async def _ensure_connection_record(self, provider: str, connection_id: str, *,
                                        account_id: str = DEFAULT_ACCOUNT) -> None:
        """Make the authorized token *usable* (#310): upsert a ``kind=oauth``
        Connection so ``resolve_connection(connection_id)`` hands operators a
        fresh ``ACCESS_TOKEN`` via this broker. Idempotent — an existing record
        (re-authorization) is left as-is. Best-effort: a store hiccup must not
        fail the completed authorization (the token IS captured).

        The row is created in ``account_id``, the account the authorization was
        started for, not in the implicit ``default``. The lookup is by id across
        accounts (a connection id is unique in the store, and ``create`` replaces
        by id): a row another account holds is never replaced, only reported."""
        try:
            # lazy: connections pulls the store/env stack; keep it off the import path
            from cairn.core.connections import Connection, default_connection_store

            store = default_connection_store()
            try:
                existing = await store.get(connection_id)
                if existing is None:
                    await store.create(Connection(
                        name=connection_id, kind="oauth", account_id=account_id,
                        description=f"{provider} via OAuth (authlib)",
                        config={"broker": self.name, "provider": provider,
                                "connection_id": connection_id},
                    ))
                elif (existing.account_id or DEFAULT_ACCOUNT) != account_id:
                    logging.getLogger("cairn.oauth").warning(
                        "authorized %s for connection %r, which another account holds; "
                        "its record is left as-is", provider, connection_id)
            finally:
                close = getattr(store, "close", None)
                if callable(close):
                    close()
        except Exception:  # noqa: BLE001 — token capture already succeeded
            logging.getLogger("cairn.oauth").warning(
                "authorized %s but could not upsert the connection record %r",
                provider, connection_id, exc_info=True,
            )

    async def get_access_token(self, *, provider: str, connection_id: str) -> str:
        store = self._tokens()
        rec = await store.get_token(connection_id)
        if rec is None:
            raise OAuthBrokerError(
                f"no token for connection {connection_id!r} — authorize first (cairn connect authorize {connection_id})"
            )
        if not _token_expired(rec):
            token = rec.get("access_token") or ""
            if not token:
                raise OAuthBrokerError(f"stored token for {connection_id!r} has no access_token")
            return token
        # expired → refresh
        refresh = rec.get("refresh_token")
        if not refresh:
            raise OAuthBrokerError(f"token for {connection_id!r} expired with no refresh_token — re-authorize")
        spec = self._spec(provider)
        cid, sec = self._client_creds(provider)
        client = self._client(client_id=cid, client_secret=sec or None)
        try:
            new = await client.refresh_token(spec.token_url, refresh_token=refresh)
        finally:
            await client.aclose()
        merged = {**rec, **dict(new)}
        merged.setdefault("refresh_token", refresh)  # some providers omit a new one on refresh
        await store.put_token(connection_id, provider, merged)
        return merged.get("access_token") or ""

    async def revoke(self, *, provider: str, connection_id: str) -> None:
        await self._tokens().delete_token(connection_id)


register_broker("authlib", AuthlibBroker)
