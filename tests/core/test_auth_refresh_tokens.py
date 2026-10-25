"""Renewable console sign-ins: the refresh cookie, rotation, server-side sign-out (ADR-0142).

A real app with auth ON and the account, workspace and refresh-token stores in a
temporary directory. Every sign-in door (``POST /token``, sign-up, the Google
landing) sets the cookie; ``POST /auth/refresh`` rotates it and refuses reuse,
expiry, revocation, a cross-origin caller and a missing header; ``POST
/auth/logout`` (behind the same header and same-origin check), a password change
and disabling a user revoke it. The store tests run against the in-memory and
SQLite stores, and against Postgres when ``PGSTORE_DSN`` is set.
"""
from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import types
import uuid
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlparse

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core import account as A
from cairn.core import refresh_tokens as RT
from cairn.core.auth_tokens import decode_claims
from cairn.core.triggers import auth

SECRET = "test-secret-long-enough-for-hs256-abc"
ORIGIN = "http://testserver"
REFRESH_HEADERS = {"X-Cairn-Refresh": "1", "Origin": ORIGIN}


def _clean_env(monkeypatch) -> None:
    for name in ("JWT_SECRET", "JWT_ALGORITHM", "ACCESS_TOKEN_EXPIRE_MINUTES", "API_USER",
                 "API_PASSWORD", "API_PASSWORD_HASH", "REQUIRE_AUTH", "PUBLIC_URL",
                 "REFRESH_TOKEN_DAYS", "SIGNUP"):
        monkeypatch.delenv(f"CAIRN_{name}", raising=False)
        monkeypatch.delenv(f"OBS_{name}", raising=False)


def _app(tmp_path, monkeypatch):
    from cairn.core import workspace as _ws

    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))
    monkeypatch.setattr(A, "_store", A.SqliteAccountStore(path=str(tmp_path / "a.db")))
    monkeypatch.setattr(_ws, "_store", _ws.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    monkeypatch.setattr(RT, "_default_store", RT.SqliteRefreshTokenStore(path=str(tmp_path / "rt.db")))
    monkeypatch.setattr(auth, "_login_failures", {})

    from fastapi.testclient import TestClient

    from cairn.core.triggers.server import build_app

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    return TestClient(build_app(FakeRunner(), enable_scheduler=False))


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """Auth on, with the env admin ``admin`` / ``pw``."""
    _clean_env(monkeypatch)
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    monkeypatch.setenv("CAIRN_API_USER", "admin")
    monkeypatch.setenv("CAIRN_API_PASSWORD", "pw")
    return _app(tmp_path, monkeypatch)


# ─── helpers ───────────────────────────────────────────────────────


def _cookie(resp) -> dict[str, str]:
    """The ``cairn_refresh`` Set-Cookie as ``{"value": …, <attr lower>: …}``."""
    for header in resp.headers.get_list("set-cookie"):
        if header.startswith(f"{auth.REFRESH_COOKIE}="):
            parts = [p.strip() for p in header.split(";")]
            out = {"value": parts[0].split("=", 1)[1]}
            for p in parts[1:]:
                k, _, v = p.partition("=")
                out[k.lower()] = v
            return out
    raise AssertionError(f"no {auth.REFRESH_COOKIE} cookie in {resp.headers.get_list('set-cookie')}")


def _no_cookie(resp) -> bool:
    return not any(h.startswith(f"{auth.REFRESH_COOKIE}=") for h in resp.headers.get_list("set-cookie"))


def _cleared(resp) -> bool:
    c = _cookie(resp)
    return c["value"] in ("", '""') and (c.get("max-age") == "0" or "expires" in c)


def _login(c, username="admin", password="pw", **extra):
    c.cookies.clear()
    r = c.post("/token", data={"username": username, "password": password, **extra})
    assert r.status_code == 200, r.text
    return r


def _refresh(c, raw, headers=REFRESH_HEADERS):
    c.cookies.clear()
    return c.post("/auth/refresh", headers={**headers, "Cookie": f"{auth.REFRESH_COOKIE}={raw}"})


def _logout(c, raw=None, headers=REFRESH_HEADERS):
    c.cookies.clear()
    extra = {"Cookie": f"{auth.REFRESH_COOKIE}={raw}"} if raw is not None else {}
    return c.post("/auth/logout", headers={**headers, **extra})


def _advance(monkeypatch, seconds: float) -> None:
    """Move the refresh-token module's clock ``seconds`` ahead of real time."""
    real = datetime.now
    monkeypatch.setattr(RT, "_now", lambda: real(timezone.utc) + timedelta(seconds=seconds))


def _row(raw):
    return asyncio.run(RT.default_refresh_token_store().get(RT.hash_token(raw)))


def _store_user(username="ada@acme.com", password="pw12345"):
    async def go():
        acct = await A.create_account("Acme", account_id="acme")
        u = await A.create_user(username, password)
        await A.add_member(acct.id, u.id, "member")
        return u
    return asyncio.run(go())


# ─── minting on POST /token ────────────────────────────────────────


def test_token_sets_a_remembered_refresh_cookie(client):
    r = _login(client)
    body = r.json()
    assert set(body) == {"access_token", "token_type"} and body["token_type"] == "bearer"
    c = _cookie(r)
    assert "httponly" in c and c["samesite"].lower() == "strict" and c["path"] == "/auth"
    assert "secure" not in c, "plain http on the test server: not Secure"
    assert abs(int(c["max-age"]) - 30 * 86400) <= 5
    row = _row(c["value"])
    assert row is not None and row.remember and row.sub == "admin" and row.account_id == "default"
    assert row.token_hash == RT.hash_token(c["value"]) and row.token_hash != c["value"]
    assert abs(row.seconds_left() - 30 * 86400) <= 5


def test_token_remember_false_is_a_session_cookie_with_a_12_hour_server_expiry(client):
    c = _cookie(_login(client, remember="false"))
    assert "max-age" not in c and "expires" not in c
    assert "httponly" in c and c["samesite"].lower() == "strict" and c["path"] == "/auth"
    row = _row(c["value"])
    assert row.remember is False and abs(row.seconds_left() - 12 * 3600) <= 5


def test_refresh_token_days_sets_the_remembered_lifetime(client, monkeypatch):
    monkeypatch.setenv("CAIRN_REFRESH_TOKEN_DAYS", "7")
    c = _cookie(_login(client))
    assert abs(int(c["max-age"]) - 7 * 86400) <= 5
    assert abs(_row(c["value"]).seconds_left() - 7 * 86400) <= 5


def test_the_cookie_is_secure_when_the_public_url_is_https(client, monkeypatch):
    monkeypatch.setenv("CAIRN_PUBLIC_URL", "https://cairn.example.com")
    assert "secure" in _cookie(_login(client))


def test_a_failed_login_sets_no_cookie(client):
    client.cookies.clear()
    r = client.post("/token", data={"username": "admin", "password": "wrong"})
    assert r.status_code == 401 and _no_cookie(r)


# ─── POST /auth/refresh ────────────────────────────────────────────


def test_refresh_rotates_and_re_mints_the_same_claims(client):
    first = _login(client)
    old = _cookie(first)["value"]
    r = _refresh(client, old)
    assert r.status_code == 200, r.text
    assert set(r.json()) == {"access_token", "token_type"}
    claims = decode_claims(r.json()["access_token"])
    assert (claims["sub"], claims["account_id"]) == ("admin", "default")
    new = _cookie(r)
    assert new["value"] != old and new["path"] == "/auth" and "httponly" in new
    before, after = _row(old), _row(new["value"])
    assert before.used_at and before.replaced_by == after.token_hash
    assert after.family_id == before.family_id and after.expires_at == before.expires_at
    assert int(new["max-age"]) <= int(_cookie(first)["max-age"])
    me = client.get("/auth/me", headers={"Authorization": f"Bearer {r.json()['access_token']}"})
    assert me.status_code == 200 and me.json()["username"] == "admin"
    assert _refresh(client, new["value"]).status_code == 200, "the new token rotates in turn"


def test_reusing_a_rotated_token_revokes_the_whole_family(client, caplog, monkeypatch):
    a = _cookie(_login(client))["value"]
    b = _cookie(_refresh(client, a))["value"]
    _advance(monkeypatch, RT.REUSE_GRACE_SECONDS + 1)  # past the grace window (ADR-0144)
    with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
        reuse = _refresh(client, a)
    assert reuse.status_code == 401 and "reused" in reuse.json()["detail"]
    assert _cleared(reuse)
    assert _row(b).revoked_at, "the live token of the family is revoked too"
    assert _refresh(client, b).status_code == 401
    # a WARNING names the user and the family prefix, never the token or its hash
    warned = [r.getMessage() for r in caplog.records
              if r.levelno == logging.WARNING and r.name == "cairn.core.refresh_tokens"]
    family = _row(a).family_id
    assert len(warned) == 1 and "sub=admin" in warned[0] and f"family={family[:8]}" in warned[0]
    for secret in (a, b, RT.hash_token(a), RT.hash_token(b), family):
        assert secret not in warned[0]


def test_a_replay_within_the_grace_window_renews(client, caplog):
    """ADR-0144: the browser lost the response to a refresh and sends the cookie
    it still holds. It renews: the child it never stored is revoked, a sibling
    takes its place, the family stays alive, and it is logged at INFO."""
    a = _cookie(_login(client))["value"]
    b = _cookie(_refresh(client, a))["value"]
    with caplog.at_level(logging.INFO, logger="cairn.core.refresh_tokens"):
        again = _refresh(client, a)
    assert again.status_code == 200, again.text
    assert decode_claims(again.json()["access_token"])["sub"] == "admin"
    c = _cookie(again)["value"]
    assert c not in (a, b)
    assert _row(b).revoked_at, "the child the browser never stored is revoked"
    assert _row(a).replaced_by == RT.hash_token(c) and _row(c).revoked_at is None
    assert _row(c).family_id == _row(a).family_id
    assert _refresh(client, b).status_code == 401
    assert _refresh(client, c).status_code == 200, "the family is alive"
    records = [r for r in caplog.records if r.name == "cairn.core.refresh_tokens"]
    assert [r.levelno for r in records] == [logging.INFO]
    msg = records[0].getMessage()
    family = _row(a).family_id
    assert "sub=admin" in msg and f"family={family[:8]}" in msg
    for secret in (a, b, c, RT.hash_token(a), RT.hash_token(b), family):
        assert secret not in msg


def test_a_superseded_cookie_presented_after_the_window_ends_the_sign_in(client, caplog, monkeypatch):
    """ADR-0145: a copy of the cookie was replayed within the window and took the
    sign-in over (sibling ``c``). When the displaced client next presents its
    own token ``b``, ten seconds or more after it was superseded, the family
    ends: both are refused, and one WARNING names the user and family prefix."""
    a = _cookie(_login(client))["value"]
    b = _cookie(_refresh(client, a))["value"]          # the legitimate client
    c = _cookie(_refresh(client, a))["value"]          # a copy of a, within the window
    _advance(monkeypatch, RT.REUSE_GRACE_SECONDS + 1)
    with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
        r = _refresh(client, b)
    assert r.status_code == 401 and _cleared(r)
    assert _row(c).revoked_at, "the sibling the copy got is revoked"
    assert _refresh(client, c).status_code == 401
    warned = [x.getMessage() for x in caplog.records if x.levelno == logging.WARNING]
    family = _row(a).family_id
    assert len(warned) == 1 and "superseded" in warned[0]
    assert "sub=admin" in warned[0] and f"family={family[:8]}" in warned[0]
    for secret in (a, b, c, RT.hash_token(b), family):
        assert secret not in warned[0]


def test_a_replay_after_logout_is_refused_even_within_the_window(client):
    a = _cookie(_login(client))["value"]
    b = _cookie(_refresh(client, a))["value"]
    assert _logout(client, b).status_code == 204
    r = _refresh(client, a)
    assert r.status_code == 401 and "revoked" in r.json()["detail"]


def test_an_expired_token_is_refused(client):
    raw = _cookie(_login(client))["value"]
    store = RT.default_refresh_token_store()
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    store._conn.execute("UPDATE refresh_tokens SET expires_at=? WHERE token_hash=?",
                        (past, RT.hash_token(raw)))
    store._conn.commit()
    r = _refresh(client, raw)
    assert r.status_code == 401 and "expired" in r.json()["detail"] and _cleared(r)


def test_an_unknown_token_and_no_cookie_are_refused(client):
    r = _refresh(client, "not-a-token")
    assert r.status_code == 401 and "unknown" in r.json()["detail"]
    client.cookies.clear()
    r = client.post("/auth/refresh", headers=REFRESH_HEADERS)
    assert r.status_code == 401


def test_a_store_user_refreshes_into_their_account(client):
    _store_user()
    raw = _cookie(_login(client, "ada@acme.com", "pw12345"))["value"]
    r = _refresh(client, raw)
    assert r.status_code == 200, r.text
    assert decode_claims(r.json()["access_token"])["account_id"] == "acme"


def test_a_disabled_user_cannot_refresh(client):
    u = _store_user()
    raw = _cookie(_login(client, "ada@acme.com", "pw12345"))["value"]
    asyncio.run(A.set_disabled(u.id, True))
    assert _row(raw).revoked_at, "disabling revokes"
    assert _refresh(client, raw).status_code == 401


def test_a_user_disabled_behind_the_hook_is_still_refused_and_revoked(client):
    """The refresh-time check holds even when the record was changed without
    :func:`account.set_disabled` (a direct store write)."""
    u = _store_user()
    raw = _cookie(_login(client, "ada@acme.com", "pw12345"))["value"]
    u.disabled = True
    asyncio.run(A.get_account_store().put_user(u))
    r = _refresh(client, raw)
    assert r.status_code == 401 and "inactive" in r.json()["detail"]
    assert _row(raw).revoked_at


def test_the_env_admin_cannot_refresh_once_it_is_no_longer_configured(client, monkeypatch):
    raw = _cookie(_login(client))["value"]
    monkeypatch.delenv("CAIRN_API_PASSWORD")
    assert _refresh(client, raw).status_code == 401


def test_a_password_change_revokes_every_sign_in(client):
    u = _store_user()
    one = _cookie(_login(client, "ada@acme.com", "pw12345"))["value"]
    two = _cookie(_login(client, "ada@acme.com", "pw12345", remember="false"))["value"]
    asyncio.run(A.set_password(u.id, "new-password"))
    assert _row(one).revoked_at and _row(two).revoked_at
    assert _refresh(client, one).status_code == 401
    assert _refresh(client, two).status_code == 401


def test_a_cross_origin_refresh_is_refused(client):
    raw = _cookie(_login(client))["value"]
    for headers in ({"X-Cairn-Refresh": "1", "Origin": "https://evil.example"},
                    {"X-Cairn-Refresh": "1", "Referer": "https://evil.example/console"},
                    {"X-Cairn-Refresh": "1", "Origin": "null"},
                    {"X-Cairn-Refresh": "1"}):
        r = _refresh(client, raw, headers)
        assert r.status_code == 403, (headers, r.text)
        assert _no_cookie(r), "a 403 leaves the cookie alone"
    assert _row(raw).used_at is None, "a refused request rotates nothing"
    ok = _refresh(client, raw, {"X-Cairn-Refresh": "1", "Referer": f"{ORIGIN}/console/runs"})
    assert ok.status_code == 200, "a same-origin Referer stands in for Origin"


def test_the_public_url_host_is_same_origin(client, monkeypatch):
    monkeypatch.setenv("CAIRN_PUBLIC_URL", "https://cairn.example.com")
    raw = _cookie(_login(client))["value"]
    r = _refresh(client, raw, {"X-Cairn-Refresh": "1", "Origin": "https://cairn.example.com"})
    assert r.status_code == 200, r.text


def test_a_refresh_without_the_header_is_refused(client):
    raw = _cookie(_login(client))["value"]
    for headers in ({"Origin": ORIGIN}, {"Origin": ORIGIN, "X-Cairn-Refresh": "0"}):
        assert _refresh(client, raw, headers).status_code == 403
    assert _row(raw).used_at is None


# ─── POST /auth/logout ─────────────────────────────────────────────


def test_logout_revokes_the_family_and_clears_the_cookie(client):
    a = _cookie(_login(client))["value"]
    b = _cookie(_refresh(client, a))["value"]
    r = _logout(client, b)
    assert r.status_code == 204 and _cleared(r)
    assert _row(a).revoked_at and _row(b).revoked_at
    assert _refresh(client, b).status_code == 401


def test_logout_is_idempotent_and_needs_no_access_token(client):
    raw = _cookie(_login(client))["value"]
    for _ in range(2):
        assert _logout(client, raw).status_code == 204
    assert _logout(client).status_code == 204
    assert _logout(client, "unknown").status_code == 204


def test_logout_needs_the_header_and_the_same_origin(client):
    raw = _cookie(_login(client))["value"]
    for headers in ({"Origin": ORIGIN},
                    {"X-Cairn-Refresh": "1", "Origin": "https://evil.example"},
                    {"X-Cairn-Refresh": "1", "Referer": "https://evil.example/"},
                    {"X-Cairn-Refresh": "1"}):
        r = _logout(client, raw, headers)
        assert r.status_code == 403, (headers, r.text)
        assert _no_cookie(r), "a 403 leaves the cookie alone"
    assert _row(raw).revoked_at is None, "a refused sign-out revokes nothing"
    assert _logout(client, raw, {"X-Cairn-Refresh": "1", "Referer": f"{ORIGIN}/console"}).status_code == 204
    assert _row(raw).revoked_at


# ─── auth off ──────────────────────────────────────────────────────


def test_auth_off_sets_no_cookie_and_the_endpoints_answer_like_token(tmp_path, monkeypatch):
    _clean_env(monkeypatch)
    monkeypatch.setenv("CAIRN_SIGNUP", "open")
    c = _app(tmp_path, monkeypatch)
    token = c.post("/token", data={"username": "a", "password": "b"})
    assert token.status_code == 400 and _no_cookie(token)
    for path in ("/auth/refresh", "/auth/logout"):
        r = c.post(path, headers={**REFRESH_HEADERS, "Cookie": f"{auth.REFRESH_COOKIE}=x"})
        assert (r.status_code, r.json()["detail"]) == (400, token.json()["detail"]), path
    s = c.post("/auth/signup", json={"company": "Acme", "email": "a@acme.com", "password": "pw12345"})
    assert s.status_code == 200 and s.json()["access_token"] is None and _no_cookie(s)


# ─── the other sign-in doors ───────────────────────────────────────


def test_signup_sets_a_remembered_refresh_cookie(client):
    client.cookies.clear()
    r = client.post("/auth/signup", json={"company": "Acme", "email": "a@acme.com", "password": "pw12345"})
    assert r.status_code == 200, r.text
    assert r.json()["access_token"], "the body is unchanged: it still carries the token"
    c = _cookie(r)
    assert "httponly" in c and c["samesite"].lower() == "strict" and c["path"] == "/auth"
    assert abs(int(c["max-age"]) - 30 * 86400) <= 5
    row = _row(c["value"])
    assert row.remember and row.sub == decode_claims(r.json()["access_token"])["sub"]
    assert _refresh(client, c["value"]).status_code == 200


@pytest.mark.skipif(importlib.util.find_spec("authlib") is None, reason="needs the [oauth] extra (authlib)")
def test_the_google_landing_sets_a_remembered_refresh_cookie(client, monkeypatch):
    import httpx

    from cairn.core import oauth_login as OL
    from tests.core.test_oauth_login import GOOGLE_ID, FakeProvider

    monkeypatch.setenv("CAIRN_OAUTH_GOOGLE_CLIENT_ID", GOOGLE_ID)
    monkeypatch.setenv("CAIRN_OAUTH_GOOGLE_CLIENT_SECRET", "g-secret")
    monkeypatch.setattr(OL, "_transport", httpx.MockTransport(FakeProvider()))
    client.cookies.clear()
    start = client.get("/auth/oauth/google/start", follow_redirects=False)
    state = parse_qs(urlparse(start.headers["location"]).query)["state"][0]
    cb = client.get("/auth/oauth/google/callback", follow_redirects=False,
                    params={"code": "the-code", "state": state})
    assert cb.status_code == 302 and urlparse(cb.headers["location"]).path == "/console/oauth/callback"
    c = _cookie(cb)
    assert "httponly" in c and c["samesite"].lower() == "strict" and c["path"] == "/auth"
    assert abs(int(c["max-age"]) - 30 * 86400) <= 5
    frag = {k: v[0] for k, v in parse_qs(urlparse(cb.headers["location"]).fragment).items()}
    assert _row(c["value"]).sub == decode_claims(frag["access_token"])["sub"]
    assert _refresh(client, c["value"]).status_code == 200


# ─── store parity ──────────────────────────────────────────────────


def _pg_dsn():
    dsn = os.environ.get("PGSTORE_DSN")
    if not dsn:
        pytest.skip("PGSTORE_DSN not set — skipping live Postgres integration")
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        pytest.skip("storage-postgres optional dep group not installed")
    return dsn


@pytest.fixture(params=["memory", "sqlite", "postgres"])
def store(request, tmp_path):
    if request.param == "memory":
        yield RT.InMemoryRefreshTokenStore()
    elif request.param == "sqlite":
        s = RT.SqliteRefreshTokenStore(path=str(tmp_path / "rt.db"))
        yield s
        s.close()
    else:
        yield RT.PostgresRefreshTokenStore(dsn=_pg_dsn())


def test_store_parity(store, monkeypatch):
    async def go():
        sub = f"user-{uuid.uuid4().hex}"
        raw, row = await RT.issue(sub, None, remember=False, store=store)
        got = await store.get(RT.hash_token(raw))
        assert got == row and got.remember is False and got.account_id is None
        assert await RT.validate(raw, store=store) == row
        raw2, row2 = await RT.rotate(row, store=store)
        assert (row2.family_id, row2.expires_at, row2.remember) == (row.family_id, row.expires_at, False)
        used = await store.get(row.token_hash)
        assert used.used_at and used.replaced_by == row2.token_hash
        assert await store.mark_used(row.token_hash, at="x", replaced_by="y") is False, "compare-and-set"
        _advance(monkeypatch, RT.REUSE_GRACE_SECONDS + 1)
        with pytest.raises(RT.RefreshError, match="reused"):
            await RT.validate(raw, store=store)
        assert (await store.get(row2.token_hash)).revoked_at, "reuse revoked the family"
        with pytest.raises(RT.RefreshError, match="revoked"):
            await RT.validate(raw2, store=store)
        # a second family for the same user, and one for someone else
        raw3, row3 = await RT.issue(sub, "acme", remember=True, store=store)
        _, other = await RT.issue(f"other-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        assert (await store.get(row3.token_hash)).remember is True
        assert await RT.revoke_user(sub, store=store) == 1, "only the live family is revoked again"
        assert (await store.get(other.token_hash)).revoked_at is None
        assert await RT.revoke_family_of(raw3, store=store) is True
        assert await RT.revoke_family_of("nope", store=store) is False
        assert await store.get(RT.hash_token("nope")) is None
    asyncio.run(go())


async def _live(store, *rows) -> list:
    return [r for r in rows if (await store.get(r.token_hash)).revoked_at is None]


def test_two_concurrent_rotations_the_loser_within_the_window_gets_a_sibling(store):
    """Two refreshes with the same cookie at once (two tabs): one wins the
    compare-and-set, the other is a replay within the grace window (ADR-0144)
    and gets a sibling. Both answers carry a token, and exactly one is live:
    the parent's only child."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        live = await RT.validate(raw, store=store)
        results = await asyncio.gather(RT.rotate(live, store=store), RT.rotate(live, store=store),
                                       return_exceptions=True)
        assert not [r for r in results if isinstance(r, BaseException)], results
        children = [r[1] for r in results]
        alive = await _live(store, *children)
        assert len(alive) == 1, "only one live child per parent"
        assert (await store.get(row.token_hash)).replaced_by == alive[0].token_hash
        assert (await store.get(row.token_hash)).revoked_at is None, "the family is alive"
        raw_alive = next(r[0] for r in results if r[1] == alive[0])
        assert await RT.validate(raw_alive, store=store) == alive[0]
    asyncio.run(go())


def test_two_concurrent_rotations_outside_the_window_revoke_the_family(store, monkeypatch):
    """With no grace window the pre-ADR-0144 rule holds: the loser is reuse and
    the whole family, the winner's new token included, ends revoked."""
    monkeypatch.setattr(RT, "REUSE_GRACE_SECONDS", 0)

    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        live = await RT.validate(raw, store=store)
        results = await asyncio.gather(RT.rotate(live, store=store), RT.rotate(live, store=store),
                                       return_exceptions=True)
        won = [r for r in results if not isinstance(r, BaseException)]
        lost = [r for r in results if isinstance(r, BaseException)]
        assert len(won) == 1 and len(lost) == 1, results
        assert isinstance(lost[0], RT.RefreshError) and lost[0].reason == "reused"
        new_raw, new_row = won[0]
        assert (await store.get(new_row.token_hash)).revoked_at, "the winner's token is revoked too"
        with pytest.raises(RT.RefreshError, match="revoked"):
            await RT.validate(new_raw, store=store)
    asyncio.run(go())


def test_replays_within_the_window_keep_one_live_child(store):
    """ADR-0144: a replay renews as a sibling and revokes the previous child; a
    second replay of the same parent does it again. Never two live children."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        _, c1 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        raw2, c2 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        raw3, c3 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert await _live(store, c1, c2, c3) == [c3]
        parent = await store.get(row.token_hash)
        assert parent.replaced_by == c3.token_hash and parent.revoked_at is None
        with pytest.raises(RT.RefreshError, match="revoked"):
            await RT.validate(raw2, store=store)
        _, d = await RT.rotate(await RT.validate(raw3, store=store), store=store)
        assert d.family_id == row.family_id and (await store.get(d.token_hash)).revoked_at is None
    asyncio.run(go())


def test_a_replay_after_the_window_revokes_the_family(store, monkeypatch):
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        _, c1 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        _advance(monkeypatch, RT.REUSE_GRACE_SECONDS + 1)
        with pytest.raises(RT.RefreshError, match="reused"):
            await RT.validate(raw, store=store)
        assert (await store.get(c1.token_hash)).revoked_at, "the family is revoked"
    asyncio.run(go())


@pytest.mark.parametrize("skew,renews", [(-1, True), (-(RT._CLOCK_SKEW_SECONDS + 1), False)])
def test_a_rotation_from_the_future_is_not_trusted_beyond_the_skew(store, monkeypatch, skew, renews):
    """The rotation was stamped by a replica whose clock is ahead (or this one
    stepped back): a second of lag still renews; more than the allowed skew is
    treated as outside the window, so the replay is reuse (ADR-0144)."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        _, c1 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        _advance(monkeypatch, skew)
        if renews:
            _, c2 = await RT.rotate(await RT.validate(raw, store=store), store=store)
            assert await _live(store, c1, c2) == [c2]
        else:
            with pytest.raises(RT.RefreshError, match="reused"):
                await RT.validate(raw, store=store)
            assert (await store.get(c1.token_hash)).revoked_at, "the family is revoked"
    asyncio.run(go())


def test_a_replay_of_a_parent_whose_child_was_already_rotated_revokes_the_family(store):
    """Renewing beside a child that already has a successor would leave two live
    chains, so that replay is reuse, window or not."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        raw1, c1 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        _, d1 = await RT.rotate(await RT.validate(raw1, store=store), store=store)
        with pytest.raises(RT.RefreshError, match="reused"):
            await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert (await store.get(d1.token_hash)).revoked_at, "the grandchild's chain is revoked"
    asyncio.run(go())


def test_a_replay_after_the_family_was_signed_out_is_refused(store):
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        raw1, c1 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert await RT.revoke_family_of(raw1, store=store) is True
        with pytest.raises(RT.RefreshError, match="revoked"):
            await RT.validate(raw, store=store)
    asyncio.run(go())


def test_a_replay_takes_over_from_one_that_stopped_half_way(store):
    """Another replay of the same parent revoked the child and then stopped (slow
    database, crashed process) before pointing the parent at its sibling. The
    revoked child is dead either way, so the next replay renews instead of
    spending its attempts waiting and revoking the family."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        _, c1 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert await store.revoke_unused(c1.token_hash, at=RT._iso(RT._now()))  # the stalled replay
        raw2, c2 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert await _live(store, c1, c2) == [c2]
        parent = await store.get(row.token_hash)
        assert parent.replaced_by == c2.token_hash and parent.revoked_at is None
    asyncio.run(go())


def test_a_family_revoked_in_the_middle_of_a_replay_refuses_it():
    """The family is signed out between the replay's read of the parent and its
    compare-and-set: the compare-and-set fails, the next pass finds the parent
    revoked, and the replay is refused with its own new token revoked."""
    class SignOutMidway(RT.InMemoryRefreshTokenStore):
        async def set_replaced_by(self, token_hash, *, expected, new):
            await self.revoke_family(self._rows[token_hash].family_id, at=RT._iso(RT._now()))
            return await super().set_replaced_by(token_hash, expected=expected, new=new)

    async def go():
        store = SignOutMidway()
        raw, row = await RT.issue("u-1", "acme", remember=True, store=store)
        _, c1 = await RT.rotate(await RT.validate(raw, store=store), store=store)
        replay = await RT.validate(raw, store=store)
        with pytest.raises(RT.RefreshError, match="revoked"):
            await RT.rotate(replay, store=store)
        assert all(t.revoked_at for t in store._rows.values()), "nothing in the family is live"
    asyncio.run(go())


def test_a_replay_that_keeps_losing_the_race_gives_up_and_revokes_the_family(caplog):
    """Every time this replay reaches the compare-and-set, another replay of the
    same parent has just installed its own sibling. After the allowed number of
    such changes the replay gives up and the family is revoked (WARNING)."""
    class AlwaysBeaten(RT.InMemoryRefreshTokenStore):
        calls = 0

        async def set_replaced_by(self, token_hash, *, expected, new):
            self.calls += 1
            parent = self._rows[token_hash]
            racer = parent.model_copy(update={"token_hash": f"racer-{self.calls}", "used_at": None,
                                              "replaced_by": None, "revoked_at": None})
            await self.insert(racer)
            assert await super().set_replaced_by(token_hash, expected=parent.replaced_by,
                                                 new=racer.token_hash)
            return await super().set_replaced_by(token_hash, expected=expected, new=new)

    async def go():
        store = AlwaysBeaten()
        raw, row = await RT.issue("u-1", "acme", remember=True, store=store)
        await RT.rotate(await RT.validate(raw, store=store), store=store)
        replay = await RT.validate(raw, store=store)
        with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
            with pytest.raises(RT.RefreshError, match="reused"):
                await RT.rotate(replay, store=store)
        assert store.calls == RT._REPLAY_ATTEMPTS
        assert all(t.revoked_at for t in store._rows.values()), "the family is revoked"
        assert any("kept racing" in r.getMessage() for r in caplog.records)
    asyncio.run(go())


def _warnings(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records
            if r.levelno == logging.WARNING and r.name == "cairn.core.refresh_tokens"]


def test_a_superseded_token_after_the_window_revokes_the_family_once(store, monkeypatch, caplog):
    """ADR-0145: B was superseded by a replay's sibling C. B presented ten seconds
    or more later ends the family (C dies), with one WARNING; presenting B again
    changes nothing and logs nothing."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        raw_b, b = await RT.rotate(await RT.validate(raw, store=store), store=store)
        raw_c, c = await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert await _live(store, b, c) == [c]
        _advance(monkeypatch, RT.REUSE_GRACE_SECONDS)
        with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
            with pytest.raises(RT.RefreshError, match="revoked"):
                await RT.validate(raw_b, store=store)
            assert (await store.get(c.token_hash)).revoked_at, "the sibling is revoked"
            assert (await store.get(row.token_hash)).revoked_at
            with pytest.raises(RT.RefreshError, match="revoked"):
                await RT.validate(raw_b, store=store)
            with pytest.raises(RT.RefreshError, match="revoked"):
                await RT.validate(raw_c, store=store)
        warned = _warnings(caplog)
        assert len(warned) == 1 and "superseded" in warned[0] and f"family={row.family_id[:8]}" in warned[0]
    asyncio.run(go())


def test_a_superseded_token_within_the_window_is_refused_without_ending_the_sign_in(
        store, monkeypatch, caplog):
    """A request already in flight with the just-superseded child (a reload that
    raced the replay) is refused, and the sibling that replaced it lives on."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        raw_b, b = await RT.rotate(await RT.validate(raw, store=store), store=store)
        raw_c, c = await RT.rotate(await RT.validate(raw, store=store), store=store)
        _advance(monkeypatch, RT.REUSE_GRACE_SECONDS - 1)
        with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
            with pytest.raises(RT.RefreshError, match="revoked"):
                await RT.validate(raw_b, store=store)
        assert (await store.get(c.token_hash)).revoked_at is None, "the sibling is alive"
        assert await RT.validate(raw_c, store=store) == c
        assert _warnings(caplog) == []
    asyncio.run(go())


def _freeze(monkeypatch, at: datetime) -> None:
    """Stop the refresh-token module's clock at ``at``."""
    monkeypatch.setattr(RT, "_now", lambda: at)


def test_the_thiefs_later_tokens_die_with_the_family(store, monkeypatch, caplog):
    """The copy already rotated its sibling C into D before the displaced client
    came back: presenting B after the window ends the family, D included."""
    async def go():
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        raw_b, b = await RT.rotate(await RT.validate(raw, store=store), store=store)
        raw_c, c = await RT.rotate(await RT.validate(raw, store=store), store=store)
        raw_d, d = await RT.rotate(await RT.validate(raw_c, store=store), store=store)
        _advance(monkeypatch, RT.REUSE_GRACE_SECONDS + 1)
        with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
            with pytest.raises(RT.RefreshError, match="revoked"):
                await RT.validate(raw_b, store=store)
        assert (await store.get(d.token_hash)).revoked_at, "the thief's latest token is revoked"
        with pytest.raises(RT.RefreshError, match="revoked"):
            await RT.validate(raw_d, store=store)
        assert len(_warnings(caplog)) == 1
    asyncio.run(go())


@pytest.mark.parametrize("after,ends", [(RT.REUSE_GRACE_SECONDS, True),
                                        (RT.REUSE_GRACE_SECONDS - 0.001, False),
                                        (-5, False)])
def test_the_superseded_boundary_with_a_frozen_clock(store, monkeypatch, caplog, after, ends):
    """Pinned with a frozen clock: exactly REUSE_GRACE_SECONDS after it was
    superseded ends the family; a millisecond less only refuses; a ``revoked_at``
    that lies in the future (a lagging replica) only refuses."""
    t0 = datetime(2026, 10, 22, 12, 0, 0, tzinfo=timezone.utc)

    async def go():
        _freeze(monkeypatch, t0)
        raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
        raw_b, b = await RT.rotate(await RT.validate(raw, store=store), store=store)
        raw_c, c = await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert (await store.get(b.token_hash)).revoked_at == t0.isoformat()
        _freeze(monkeypatch, t0 + timedelta(seconds=after))
        with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
            with pytest.raises(RT.RefreshError, match="revoked"):
                await RT.validate(raw_b, store=store)
        assert bool((await store.get(c.token_hash)).revoked_at) is ends
        assert len(_warnings(caplog)) == (1 if ends else 0)
    asyncio.run(go())


def test_tokens_revoked_by_sign_out_or_a_password_change_end_nothing_more(store, monkeypatch, caplog):
    """A token revoked with its whole family (sign-out, password change, disabling,
    earlier reuse) presented later is refused: nothing more is revoked and
    nothing is logged, because the family has nothing live left to end."""
    async def go():
        sub = f"user-{uuid.uuid4().hex}"
        raw, row = await RT.issue(sub, "acme", remember=True, store=store)
        raw_b, b = await RT.rotate(await RT.validate(raw, store=store), store=store)
        assert await RT.revoke_family_of(raw_b, store=store) is True          # sign-out
        raw2, row2 = await RT.issue(sub, "acme", remember=False, store=store)
        assert await RT.revoke_user(sub, store=store) == 1                     # password change
        _, other = await RT.issue(sub, "acme", remember=True, store=store)    # signed in again since
        _advance(monkeypatch, RT.REUSE_GRACE_SECONDS + 60)
        with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
            for presented in (raw_b, raw2):
                with pytest.raises(RT.RefreshError, match="revoked"):
                    await RT.validate(presented, store=store)
        assert _warnings(caplog) == []
        assert (await store.get(other.token_hash)).revoked_at is None, "a later sign-in is untouched"
    asyncio.run(go())


def test_two_tabs_outcome_is_unchanged(store, monkeypatch, caplog):
    """Two tabs refreshed at once: one child was superseded by the other's sibling.
    If the browser kept the live sibling, the next refresh renews as before. If it
    kept the superseded one, the next refresh is refused as before; the family,
    which nobody holds a live token for any more, ends with a WARNING."""
    async def go():
        for kept_live in (True, False):
            raw, row = await RT.issue(f"user-{uuid.uuid4().hex}", "acme", remember=True, store=store)
            live = await RT.validate(raw, store=store)
            results = await asyncio.gather(RT.rotate(live, store=store), RT.rotate(live, store=store))
            alive = await _live(store, *(r[1] for r in results))
            assert len(alive) == 1
            raw_alive = next(r[0] for r in results if r[1] == alive[0])
            raw_dead = next(r[0] for r in results if r[1] != alive[0])
            monkeypatch.undo()
            _advance(monkeypatch, RT.REUSE_GRACE_SECONDS + 1800)   # the next refresh, half an hour on
            caplog.clear()
            with caplog.at_level(logging.WARNING, logger="cairn.core.refresh_tokens"):
                if kept_live:
                    assert await RT.validate(raw_alive, store=store) == alive[0]
                    await RT.rotate(alive[0], store=store)
                    assert _warnings(caplog) == []
                else:
                    with pytest.raises(RT.RefreshError, match="revoked"):
                        await RT.validate(raw_dead, store=store)
                    assert len(_warnings(caplog)) == 1
            monkeypatch.undo()
    asyncio.run(go())


def test_the_winners_token_is_revoked_even_when_the_loser_revokes_first(monkeypatch):
    """The interleaving ``gather`` alone does not force: the winner's
    compare-and-set succeeds, then the loser fails its own and revokes the family
    BEFORE the winner continues. The winner's new token must already be in the
    family by then (written before the compare-and-set), or it survives. Outside
    the grace window, where the loser revokes (ADR-0144)."""
    monkeypatch.setattr(RT, "REUSE_GRACE_SECONDS", 0)
    class Paused(RT.InMemoryRefreshTokenStore):
        def __init__(self) -> None:
            super().__init__()
            self.revoked = asyncio.Event()

        async def mark_used(self, token_hash, *, at, replaced_by):
            won = await super().mark_used(token_hash, at=at, replaced_by=replaced_by)
            if won:  # hold the winner here until the loser has revoked
                try:
                    await asyncio.wait_for(self.revoked.wait(), 2)
                except asyncio.TimeoutError:
                    pass
            return won

        async def revoke_family(self, family_id, *, at):
            n = await super().revoke_family(family_id, at=at)
            self.revoked.set()
            return n

    async def go():
        store = Paused()
        raw, row = await RT.issue("u-1", "acme", remember=True, store=store)
        results = await asyncio.gather(RT.rotate(row, store=store), RT.rotate(row, store=store),
                                       return_exceptions=True)
        won = [r for r in results if not isinstance(r, BaseException)]
        assert len(won) == 1, results
        assert (await store.get(won[0][1].token_hash)).revoked_at, "the winner's token survived"
    asyncio.run(go())


def test_the_factory_follows_cairn_storage(monkeypatch, tmp_path):
    monkeypatch.setattr(RT, "_default_store", None)
    monkeypatch.setenv("CAIRN_STORAGE", "postgres://u:p@h/db")
    assert isinstance(RT.default_refresh_token_store(), RT.PostgresRefreshTokenStore)
    monkeypatch.setattr(RT, "_default_store", None)
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{tmp_path}/x.db")
    s = RT.default_refresh_token_store()
    assert isinstance(s, RT.SqliteRefreshTokenStore) and s.path == f"{tmp_path}/x.db"
    s.close()
    monkeypatch.setattr(RT, "_default_store", None)
