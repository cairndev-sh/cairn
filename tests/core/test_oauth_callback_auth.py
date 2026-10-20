"""``/oauth/callback`` is reachable without a bearer token when auth is on (#62).

The OAuth provider redirects the user's browser to ``/oauth/callback``; that
redirect carries ``state`` + ``code``, never a cairn JWT, so the bearer guard
401'd it and ``cairn connect --via oauth`` could never complete on an
auth-enabled deployment. The route authenticates by its own single-use
``state`` (``AuthlibBroker.complete_authorization`` pops it or refuses), the same
posture as a signature-checked webhook. Driven through the real app with auth on.
"""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.oauth import broker as B
from cairn.core.triggers import auth
from cairn.core.triggers import router_providers
from cairn.core.triggers.server import build_app
from tests.core.test_trigger_server_dispatch import FakeRunner


@pytest.fixture
def client(monkeypatch):
    pending = {"s1": "conn-1"}

    async def _complete(self, *, state, code):
        cid = pending.pop(state, None)  # single-use state, like the real broker's pop_pending
        if cid is None:
            raise B.OAuthBrokerError("unknown or expired OAuth state — restart the authorization")
        return cid

    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret")
    monkeypatch.setattr(router_providers, "load_router_providers", lambda: [])
    monkeypatch.setattr(B.AuthlibBroker, "complete_authorization", _complete)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        yield c


def test_callback_completes_without_a_bearer_token(client):
    r = client.get("/oauth/callback", params={"state": "s1", "code": "c1"})
    assert r.status_code == 200 and "Connected: conn-1" in r.text, (r.status_code, r.text)


def test_callback_state_is_the_gate(client):
    assert client.get("/oauth/callback", params={"state": "s1", "code": "c1"}).status_code == 200
    replay = client.get("/oauth/callback", params={"state": "s1", "code": "c1"})
    assert replay.status_code == 200 and "Authorization failed" in replay.text
    assert client.get("/oauth/callback").status_code == 400


def test_only_the_callback_is_opened(client):
    assert auth._is_public_path("/oauth/callback")
    assert not auth._is_public_path("/oauth/callback/x")
    assert not auth._is_public_path("/oauth")
    assert client.get("/runs").status_code == 401


PAYLOAD = '<img src=x onerror=alert(1)><script>alert(2)</script>'


def test_the_callback_page_escapes_what_it_echoes(client, monkeypatch):
    """The callback is reachable without a token, so nothing it echoes may run as markup on the
    console's origin: the provider's `error`, a broker failure message, or a connection id."""
    r = client.get("/oauth/callback", params={"error": PAYLOAD})
    assert r.status_code == 200 and "<img" not in r.text and "<script" not in r.text, r.text
    assert "&lt;img src=x onerror=alert(1)&gt;" in r.text

    async def _boom(self, *, state, code):
        raise B.OAuthBrokerError(PAYLOAD)
    monkeypatch.setattr(B.AuthlibBroker, "complete_authorization", _boom)
    r = client.get("/oauth/callback", params={"state": "s1", "code": "c1"})
    assert "<script" not in r.text and "&lt;script&gt;" in r.text, r.text

    async def _cid(self, *, state, code):
        return PAYLOAD
    monkeypatch.setattr(B.AuthlibBroker, "complete_authorization", _cid)
    r = client.get("/oauth/callback", params={"state": "s1", "code": "c1"})
    assert "<img" not in r.text and "Connected: &lt;img" in r.text, r.text
