"""Sign in / sign up with Google and GitHub (#2336, ADR-0119).

A real app, auth ON, the provider mocked at the HTTP layer (an httpx
MockTransport behind Authlib) — Google and GitHub are never called. The
property under test: an identity method is not a sign-up door. ``OBS_SIGNUP``
and the first-run bootstrap decide exactly as they do for a password, linking
happens only on a verified matching email, and an unverified email never
creates or reaches an account.
"""
from __future__ import annotations

import asyncio
import getpass
import importlib.util
import os
import time
import types
import uuid
from urllib.parse import parse_qs, urlparse

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import httpx
import jwt
import pytest

from cairn.core import account as A
from cairn.core import oauth_login as OL

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("authlib") is None, reason="needs the [oauth] extra (authlib)")

GOOGLE_ID = "google-cid.apps.googleusercontent.com"


class FakeProvider:
    """Google's token endpoint and GitHub's token + API endpoints."""

    def __init__(self) -> None:
        self.google = {"sub": "g-1", "email": "founder@acme.com", "email_verified": True}
        self.github_user = {"id": 4242, "login": "octo"}
        self.github_emails = [{"email": "octo@acme.com", "primary": True, "verified": True}]
        self.token_bodies: list[dict] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if url == "https://oauth2.googleapis.com/token":
            self.token_bodies.append(parse_qs(request.content.decode()))
            claims = {"iss": "https://accounts.google.com", "aud": GOOGLE_ID,
                      "exp": int(time.time()) + 300, **self.google}
            return httpx.Response(200, json={"access_token": "g-at", "token_type": "Bearer",
                                             "expires_in": 300,
                                             "id_token": jwt.encode(claims, "google-test-signing-key-32-bytes-long!", algorithm="HS256")})
        if url == "https://github.com/login/oauth/access_token":
            self.token_bodies.append(parse_qs(request.content.decode()))
            return httpx.Response(200, json={"access_token": "gh-at", "token_type": "bearer"})
        if url == "https://api.github.com/user":
            return httpx.Response(200, json=self.github_user)
        if url == "https://api.github.com/user/emails":
            return httpx.Response(200, json=self.github_emails)
        return httpx.Response(404)


@pytest.fixture()
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.delenv("OBS_SIGNUP", raising=False)  # invite: the default
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))
    monkeypatch.delenv("OBS_API_USER", raising=False)
    monkeypatch.delenv("OBS_API_PASSWORD", raising=False)
    monkeypatch.delenv("OBS_API_PASSWORD_HASH", raising=False)
    monkeypatch.delenv("OBS_PUBLIC_URL", raising=False)
    monkeypatch.setenv("OBS_OAUTH_GOOGLE_CLIENT_ID", GOOGLE_ID)
    monkeypatch.setenv("OBS_OAUTH_GOOGLE_CLIENT_SECRET", "g-secret")
    monkeypatch.setenv("OBS_OAUTH_GITHUB_CLIENT_ID", "gh-cid")
    monkeypatch.setenv("OBS_OAUTH_GITHUB_CLIENT_SECRET", "gh-secret")
    # process-wide singletons: patch the instances (see test_two_tenant_isolation)
    from cairn.core import workspace as _ws
    monkeypatch.setattr(A, "_store", A.SqliteAccountStore(path=str(tmp_path / "a.db")))
    monkeypatch.setattr(_ws, "_store", _ws.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    provider = FakeProvider()
    monkeypatch.setattr(OL, "_transport", httpx.MockTransport(provider))

    from fastapi.testclient import TestClient

    from cairn.core.triggers.server import build_app

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    return TestClient(build_app(FakeRunner(), enable_scheduler=False)), provider


def _sign_in(c, provider: str = "google", *, tamper: str = "") -> tuple[str, dict]:
    """Start, then follow the provider back to the callback. Returns the final
    redirect's path and its parsed fragment (or query, for a refusal)."""
    start = c.get(f"/auth/oauth/{provider}/start", follow_redirects=False)
    assert start.status_code == 302, start.text
    state = parse_qs(urlparse(start.headers["location"]).query).get("state", [""])[0]
    cb = c.get(f"/auth/oauth/{provider}/callback", follow_redirects=False,
               params={"code": "the-code", "state": tamper or state})
    assert cb.status_code == 302, cb.text
    loc = urlparse(cb.headers["location"])
    return loc.path, {k: v[0] for k, v in parse_qs(loc.fragment or loc.query).items()}


def _refused(c, reason: str, **kw) -> None:
    path, q = _sign_in(c, **kw)
    assert (path, q.get("error")) == ("/console/login", reason)


# ── the sign-up door ─────────────────────────────────────────────────────


def test_an_empty_deployment_makes_the_first_oauth_user_the_owner(env):
    c, _ = env
    path, frag = _sign_in(c)
    assert path == "/console/oauth/callback" and frag["new_account"] == "1"
    assert frag["workspace"] == "acme-com", "the first workspace was created"
    me = c.get("/auth/me", headers={"Authorization": f"Bearer {frag['access_token']}"})
    assert me.status_code == 200, me.text
    assert me.json()["role"] == "owner" and me.json()["username"] == "founder@acme.com"
    u = asyncio.run(A.get_account_store().get_user_by_username("founder@acme.com"))
    assert u.password_hash == "" and [(i.provider, i.subject) for i in u.identities] == [("google", "g-1")]


def test_an_oauth_sign_up_announces_its_first_workspace(env, monkeypatch):
    """#2421 decision 7: the new account's first workspace fires `workspace.created`
    (source `oauth`, the new user as actor); a returning sign-in creates none."""
    from cairn.core.pack import run_hooks

    heard = []

    async def probe(event, payload):
        heard.append((event, dict(payload)))

    monkeypatch.setattr(run_hooks, "load_run_hooks", lambda: {"probe": probe})
    c, _ = env
    frag = _sign_in(c)[1]
    sub = jwt.decode(frag["access_token"], options={"verify_signature": False})["sub"]
    assert heard == [("workspace.created", {"event": "workspace.created", "workspace_id": frag["workspace"],
                                            "actor": sub, "source": "oauth"})]
    _sign_in(c)  # the same person again: no new workspace
    assert len(heard) == 1


def test_invite_mode_refuses_an_unknown_oauth_email_once_an_account_exists(env):
    c, provider = env
    r = c.post("/auth/signup", json={"company": "Acme", "email": "founder@acme.com", "password": "pw12345"})
    assert r.status_code == 200, r.text
    provider.google = {"sub": "g-9", "email": "stranger@globex.com", "email_verified": True}
    _refused(c, "signup_closed")


def test_domain_mode_refuses_a_public_mail_domain_oauth_sign_up(env, monkeypatch):
    """#2351 — the same door as a password: a second Gmail user does not
    self-serve into the first Gmail founder's account."""
    c, provider = env
    provider.google = {"sub": "g-1", "email": "founder@gmail.com", "email_verified": True}
    assert _sign_in(c)[1]["new_account"] == "1"  # first-run bootstrap still applies
    monkeypatch.setenv("OBS_SIGNUP", "domain")
    provider.google = {"sub": "g-2", "email": "stranger@gmail.com", "email_verified": True}
    _refused(c, "public_domain")
    assert asyncio.run(A.get_account_store().get_user_by_username("stranger@gmail.com")) is None


def test_an_oauth_landing_never_creates_a_user_under_the_env_admin_name(env, monkeypatch):
    """A verified provider email equal to the env admin's name is refused like
    a taken address, on an empty deployment too, and creates nothing."""
    c, provider = env
    monkeypatch.setenv("CAIRN_API_USER", "Founder@Acme.com ")
    _refused(c, "already_registered")
    assert asyncio.run(A.get_account_store().list_users()) == []
    assert c.get("/auth/signup").json()["first_run"] is True


def test_an_unverified_email_is_refused_and_creates_nothing(env):
    c, provider = env
    provider.google = {**provider.google, "email_verified": False}
    _refused(c, "email_not_verified")
    assert c.get("/auth/signup").json()["first_run"] is True, "no account was created"


def test_github_uses_the_primary_address_only_when_verified(env):
    c, provider = env
    provider.github_emails = [{"email": "octo@acme.com", "primary": True, "verified": False},
                              {"email": "other@acme.com", "primary": False, "verified": True}]
    _refused(c, "email_not_verified", provider="github")
    provider.github_emails[0]["verified"] = True
    path, frag = _sign_in(c, provider="github")
    assert path == "/console/oauth/callback" and frag["new_account"] == "1"


# ── linking ──────────────────────────────────────────────────────────────


def test_a_password_user_signing_in_with_google_is_linked_not_duplicated(env):
    c, _ = env
    body = c.post("/auth/signup", json={"company": "Acme", "email": "founder@acme.com",
                                        "password": "pw12345"}).json()
    path, frag = _sign_in(c)
    assert path == "/console/oauth/callback" and frag["new_account"] == "0"
    assert frag["workspace"] == body["workspace_id"], "lands in the account's workspace"
    claims = jwt.decode(frag["access_token"], options={"verify_signature": False})
    store = A.get_account_store()
    users = asyncio.run(store.list_users())
    assert len(users) == 1 and claims["sub"] == users[0].id and claims["account_id"] == "acme"
    # the password still works, and the next Google sign-in resolves by identity
    assert c.post("/token", data={"username": "founder@acme.com", "password": "pw12345"}).status_code == 200
    assert _sign_in(c)[1]["new_account"] == "0"


# ── state ────────────────────────────────────────────────────────────────


def test_a_tampered_state_is_refused(env):
    c, _ = env
    _refused(c, "oauth_state", tamper="not-a-signed-state")


def test_a_state_without_the_browsers_cookie_is_refused(env):
    """Login CSRF: a callback URL replayed into another browser does not land."""
    c, _ = env
    start = c.get("/auth/oauth/google/start", follow_redirects=False)
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
    c.cookies.clear()
    cb = c.get("/auth/oauth/google/callback", params={"code": "x", "state": state}, follow_redirects=False)
    assert cb.headers["location"] == "/console/login?error=oauth_state"


def test_an_expired_state_is_refused(env, monkeypatch):
    c, _ = env
    real = time.time
    monkeypatch.setattr(OL.time, "time", lambda: real() - OL.STATE_TTL_SECONDS - 5)
    start = c.get("/auth/oauth/google/start", follow_redirects=False)
    monkeypatch.setattr(OL.time, "time", real)
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
    cb = c.get("/auth/oauth/google/callback", params={"code": "x", "state": state}, follow_redirects=False)
    assert cb.headers["location"] == "/console/login?error=oauth_state"


def test_google_uses_pkce(env):
    c, provider = env
    start = c.get("/auth/oauth/google/start", follow_redirects=False)
    q = parse_qs(urlparse(start.headers["location"]).query)
    assert q["code_challenge_method"] == ["S256"] and q["code_challenge"]
    assert q["redirect_uri"] == ["http://testserver/auth/oauth/google/callback"]
    _sign_in(c)
    assert provider.token_bodies[-1].get("code_verifier"), "the verifier went to the token endpoint"


# ── passwords and config ─────────────────────────────────────────────────


def test_a_user_with_no_password_cannot_log_in_by_password(env):
    c, _ = env
    _sign_in(c)
    for pw in ("", "anything"):
        r = c.post("/token", data={"username": "founder@acme.com", "password": pw})
        assert r.status_code in (401, 422), r.text


def test_signup_status_lists_only_configured_providers(env, monkeypatch):
    c, _ = env
    assert c.get("/auth/signup").json()["providers"] == ["google", "github"]
    monkeypatch.delenv("OBS_OAUTH_GOOGLE_CLIENT_ID")
    assert c.get("/auth/signup").json()["providers"] == ["github"]
    monkeypatch.delenv("OBS_OAUTH_GITHUB_CLIENT_ID")
    assert c.get("/auth/signup").json()["providers"] == []
    start = c.get("/auth/oauth/github/start", follow_redirects=False)
    assert start.headers["location"] == "/console/login?error=oauth_unavailable"


def test_oauth_routes_are_reachable_without_a_token(env):
    c, _ = env
    assert c.get("/auth/oauth/nope/start", follow_redirects=False).headers["location"] \
        == "/console/login?error=oauth_unavailable"


# ── same-name tenants (the upsert that OAuth would hit every time) ───────


def test_two_signups_from_one_domain_get_two_accounts_and_workspaces(env, monkeypatch):
    """No company name on an OAuth sign-up → the account is named after the
    email domain. Two such people must not share (or overwrite) one tenant."""
    c, provider = env
    monkeypatch.setenv("OBS_SIGNUP", "open")
    first = _sign_in(c)[1]
    provider.google = {"sub": "g-2", "email": "second@acme.com", "email_verified": True}
    second = _sign_in(c)[1]
    assert first["workspace"] != second["workspace"]
    a = jwt.decode(first["access_token"], options={"verify_signature": False})["account_id"]
    b = jwt.decode(second["access_token"], options={"verify_signature": False})["account_id"]
    assert a != b
    me = c.get("/auth/me", headers={"Authorization": f"Bearer {first['access_token']}"}).json()
    assert me["role"] == "owner" and me["account_id"] == a


# ── stores ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("make", [A.InMemoryAccountStore,
                                  lambda: A.SqliteAccountStore(path=":memory:")])
async def test_store_lookup_by_identity(make):
    store = make()
    u = await store.put_user(A.User(username="a@x.com"))
    assert await store.get_user_by_identity("google", "s1") is None
    await A.link_identity(u, A.Identity(provider="google", subject="s1", email="a@x.com"), store=store)
    await A.link_identity(u, A.Identity(provider="google", subject="s1", email="a@x.com"), store=store)
    found = await store.get_user_by_identity("google", "s1")
    assert found.id == u.id and len(found.identities) == 1
    assert await store.get_user_by_identity("github", "s1") is None


def _pg_reachable() -> bool:
    try:
        import psycopg
        with psycopg.connect(f"postgres://{getpass.getuser()}@localhost:5432/postgres", connect_timeout=2):
            return True
    except Exception:
        return False


@pytest.mark.skipif(not _pg_reachable(), reason="no local Postgres on :5432")
async def test_postgres_store_lookup_by_identity():
    import psycopg
    admin = f"postgres://{getpass.getuser()}@localhost:5432/postgres"
    db = f"cairn_oauth_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {db}")
    store = A.PostgresAccountStore(dsn=f"postgres://{getpass.getuser()}@localhost:5432/{db}")
    try:
        u = await store.put_user(A.User(username="a@x.com"))
        await A.link_identity(u, A.Identity(provider="github", subject="7"), store=store)
        assert (await store.get_user_by_identity("github", "7")).id == u.id
        assert await store.get_user_by_identity("github", "8") is None
    finally:
        await store.aclose()
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(f"DROP DATABASE IF EXISTS {db} WITH (FORCE)")
