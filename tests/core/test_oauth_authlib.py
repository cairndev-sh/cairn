"""Built-in AuthlibBroker + OAuth catalog + token store (#116).

Hermetic: the provider token endpoint is a httpx MockTransport, so the
authorization-code exchange + refresh are exercised with no real provider.
"""
from __future__ import annotations

import importlib.util
import os
from urllib.parse import parse_qs, urlparse

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import httpx
import pytest

from cairn.core.oauth.catalog import get_oauth_provider, list_oauth_providers, load_oauth_catalog
from cairn.core.oauth.store import OAuthTokenStore

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("authlib") is None, reason="needs the [oauth] extra (authlib)"
)


# ── catalog (scopes live in the file) ────────────────────────────────────


def test_catalog_loads_providers_and_scopes():
    cat = load_oauth_catalog()
    assert {"github", "slack", "google", "notion", "jira"} <= set(cat)
    gh = get_oauth_provider("github")
    assert gh.scopes == ["repo", "read:org"]
    assert gh.authorize_url.startswith("https://github.com/login/oauth/authorize")
    g = get_oauth_provider("google")
    assert g.refresh is True and g.pkce is True and g.extra_authorize_params.get("access_type") == "offline"
    assert any(p.provider == "jira" for p in list_oauth_providers())


# ── writable token store ─────────────────────────────────────────────────


async def test_token_store_round_trips():
    s = OAuthTokenStore(":memory:")
    await s.put_pending("st8", {"provider": "github", "connection_id": "gh1"})
    assert (await s.pop_pending("st8"))["connection_id"] == "gh1"
    assert await s.pop_pending("st8") is None  # consumed
    await s.put_token("gh1", "github", {"access_token": "AT", "refresh_token": "RT"})
    assert (await s.get_token("gh1"))["access_token"] == "AT"
    assert await s.delete_token("gh1") is True
    assert await s.get_token("gh1") is None
    s.close()


# ── AuthlibBroker (mock token endpoint) ──────────────────────────────────


def _broker(handler, monkeypatch):
    from cairn.core.oauth.broker import AuthlibBroker

    monkeypatch.setenv("OBS_OAUTH_GITHUB_CLIENT_ID", "cid-test")
    monkeypatch.setenv("OBS_OAUTH_GITHUB_CLIENT_SECRET", "sec-test")
    return AuthlibBroker(store=OAuthTokenStore(":memory:"), transport=httpx.MockTransport(handler))


def test_broker_registered_under_authlib():
    from cairn.core.oauth import AuthlibBroker, get_oauth_broker

    assert isinstance(get_oauth_broker("authlib"), AuthlibBroker)


async def test_start_authorization_builds_url_and_stores_pending(monkeypatch):
    broker = _broker(lambda r: httpx.Response(500), monkeypatch)  # no token call here
    session = await broker.start_authorization(provider="github", connection_id="gh1")
    q = parse_qs(urlparse(session.redirect_url).query)
    assert session.redirect_url.startswith("https://github.com/login/oauth/authorize")
    assert q["client_id"] == ["cid-test"] and q["state"] and q["scope"] == ["repo read:org"]
    assert "redirect_uri" in q
    # pending was persisted under the state
    assert (await broker._tokens().pop_pending(q["state"][0]))["connection_id"] == "gh1"


async def test_complete_then_get_access_token(monkeypatch):
    def handler(request):
        return httpx.Response(200, json={"access_token": "AT-1", "refresh_token": "RT-1", "token_type": "bearer", "expires_in": 3600})

    broker = _broker(handler, monkeypatch)
    session = await broker.start_authorization(provider="github", connection_id="gh1")
    state = parse_qs(urlparse(session.redirect_url).query)["state"][0]
    cid = await broker.complete_authorization(state=state, code="the-code")
    assert cid == "gh1"
    assert await broker.get_access_token(provider="github", connection_id="gh1") == "AT-1"


async def test_get_access_token_refreshes_when_expired(monkeypatch):
    import time

    def handler(request):
        return httpx.Response(200, json={"access_token": "AT-2", "token_type": "bearer", "expires_in": 3600})

    broker = _broker(handler, monkeypatch)
    # seed an expired token with a refresh token
    await broker._tokens().put_token(
        "gh1", "github", {"access_token": "AT-old", "refresh_token": "RT-1", "expires_at": time.time() - 100}
    )
    assert await broker.get_access_token(provider="github", connection_id="gh1") == "AT-2"


async def test_get_access_token_without_token_errors(monkeypatch):
    from cairn.core.oauth import OAuthBrokerError

    broker = _broker(lambda r: httpx.Response(500), monkeypatch)
    with pytest.raises(OAuthBrokerError, match="no token"):
        await broker.get_access_token(provider="github", connection_id="nope")


async def test_revoke_deletes_token(monkeypatch):
    broker = _broker(lambda r: httpx.Response(500), monkeypatch)
    await broker._tokens().put_token("gh1", "github", {"access_token": "AT"})
    await broker.revoke(provider="github", connection_id="gh1")
    with_token = await broker._tokens().get_token("gh1")
    assert with_token is None


async def test_complete_authorization_creates_usable_connection(monkeypatch, tmp_path):
    """#310 — completing the dance upserts a kind=oauth Connection so
    resolve_connection hands operators a fresh ACCESS_TOKEN (connected → usable)."""
    monkeypatch.setenv("OBS_CONNECTIONS_DB", str(tmp_path / "conn.db"))

    def handler(request):
        return httpx.Response(200, json={"access_token": "AT-slack", "token_type": "bearer"})

    broker = _broker(handler, monkeypatch)
    session = await broker.start_authorization(provider="github", connection_id="gh-conn")
    state = parse_qs(urlparse(session.redirect_url).query)["state"][0]
    await broker.complete_authorization(state=state, code="the-code")

    from cairn.core.connections import default_connection_store
    store = default_connection_store()
    try:
        conn = await store.get("gh-conn")
    finally:
        store.close()
    assert conn is not None and conn.kind == "oauth"
    assert conn.config == {"broker": "authlib", "provider": "github", "connection_id": "gh-conn"}

    # idempotent on re-authorization: no duplicate/overwrite
    session2 = await broker.start_authorization(provider="github", connection_id="gh-conn")
    state2 = parse_qs(urlparse(session2.redirect_url).query)["state"][0]
    await broker.complete_authorization(state=state2, code="again")
    store = default_connection_store()
    try:
        assert (await store.get("gh-conn")) is not None
    finally:
        store.close()


# ── the connection row lands in the account that started the authorization ──


def _state(session) -> str:
    return parse_qs(urlparse(session.redirect_url).query)["state"][0]


@pytest.fixture
def _tenant_workspace(monkeypatch, tmp_path):
    from cairn.core import workspace
    from cairn.core.workspace import InMemoryWorkspaceStore, Workspace

    monkeypatch.setenv("OBS_CONNECTIONS_DB", str(tmp_path / "conn.db"))
    wstore = InMemoryWorkspaceStore()
    monkeypatch.setattr(workspace, "_store", wstore)

    async def _seed():
        await wstore.put(Workspace(id="ws-a", account_id="acct-a"))
    return _seed


async def _stored(conn_id: str):
    from cairn.core.connections import default_connection_store

    store = default_connection_store()
    try:
        return await store.get(conn_id)
    finally:
        store.close()


async def test_the_callback_creates_the_connection_in_the_initiating_account(monkeypatch, _tenant_workspace):
    """The authorization starts inside a workspace (a console request); the
    callback arrives with no caller scope. The row is the initiating account's,
    not the implicit ``default``."""
    from cairn.core.workspace import workspace_scope

    await _tenant_workspace()
    broker = _broker(lambda r: httpx.Response(200, json={"access_token": "AT-a", "token_type": "bearer"}),
                     monkeypatch)
    with workspace_scope("ws-a"):
        session = await broker.start_authorization(provider="github", connection_id="gh-a")
    await broker.complete_authorization(state=_state(session), code="the-code")
    conn = await _stored("gh-a")
    assert conn is not None and conn.kind == "oauth" and conn.account_id == "acct-a"


async def test_an_unscoped_start_keeps_the_deployments_account(monkeypatch, _tenant_workspace):
    """The operator's own process (``cairn connect --via oauth``), auth on and no
    workspace in scope: the deployment's ``default``, as before."""
    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret")
    broker = _broker(lambda r: httpx.Response(200, json={"access_token": "AT", "token_type": "bearer"}),
                     monkeypatch)
    session = await broker.start_authorization(provider="github", connection_id="gh-ops")
    await broker.complete_authorization(state=_state(session), code="the-code")
    assert (await _stored("gh-ops")).account_id == "default"


async def test_the_callback_never_replaces_a_row_another_account_holds(monkeypatch, _tenant_workspace):
    from cairn.core.connections import Connection, default_connection_store
    from cairn.core.workspace import workspace_scope

    await _tenant_workspace()
    store = default_connection_store()
    try:
        await store.create(Connection(name="gh-b", kind="oauth", account_id="acct-b",
                                      config={"broker": "authlib", "provider": "github", "connection_id": "gh-b"}))
    finally:
        store.close()
    broker = _broker(lambda r: httpx.Response(200, json={"access_token": "AT", "token_type": "bearer"}),
                     monkeypatch)
    with workspace_scope("ws-a"):
        session = await broker.start_authorization(provider="github", connection_id="gh-b")
    await broker.complete_authorization(state=_state(session), code="the-code")
    assert (await _stored("gh-b")).account_id == "acct-b"


async def test_a_pending_record_without_an_account_lands_in_default(monkeypatch, _tenant_workspace):
    """A pending authorization written before the account was recorded."""
    broker = _broker(lambda r: httpx.Response(200, json={"access_token": "AT", "token_type": "bearer"}),
                     monkeypatch)
    await broker._tokens().put_pending("st-legacy", {"provider": "github", "connection_id": "gh-old",
                                                    "code_verifier": "", "scopes": ["repo"]})
    await broker.complete_authorization(state="st-legacy", code="the-code")
    assert (await _stored("gh-old")).account_id == "default"
