"""With auth on, a workspace id no store holds is refused like another account's.

``assert_workspace_allowed`` let a caller act in a workspace that does not exist:
no row, so no account to compare. Any signed-in user could name an id that no
one had created and read or write records under it, and whoever later created a
workspace with that id would inherit them. Now, with auth on, the id is refused
with the same ``WorkspaceForbidden`` (a 404 to the caller) as a cross-account
id. Auth off is unchanged.

Driven through the real app (``build_app``) with auth on and real account and
workspace stores.
"""
from __future__ import annotations

import asyncio
import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core import account as A
from cairn.core import workspace as W
from cairn.core.triggers import auth

SECRET = "test-secret-long-enough-for-hs256-abc"


class _FakeRunner:
    default_environment = "production"
    skill_engine = types.SimpleNamespace(list_skills=lambda: [])

    async def run(self, *a, **k):
        return {}


def _auth_off(monkeypatch) -> None:
    for k in ("OBS_JWT_SECRET", "CAIRN_JWT_SECRET"):
        monkeypatch.delenv(k, raising=False)


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE",
              "CAIRN_API_USER", "CAIRN_API_PASSWORD", "CAIRN_API_PASSWORD_HASH",
              "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH", "CAIRN_JWT_SECRET"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", SECRET)
    monkeypatch.setenv("OBS_SIGNUP", "open")
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))

    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)

    async def _seed() -> dict[str, tuple[str, str]]:
        for acct in ("acme", "globex"):
            await accounts.put_account(A.Account(id=acct, name=acct.title()))
            await workspaces.put(W.Workspace(id=f"{acct}-ops", name=f"{acct} ops", account_id=acct))
        ids = {}
        for name, acct in (("bob@acme.com", "acme"),):
            u = await A.create_user(name, f"{name}-password", store=accounts)
            await A.add_member(acct, u.id, "member", store=accounts)
            ids[name] = (u.id, acct)
        return ids
    users = asyncio.run(_seed())

    def headers(who: str, ws: str | None = None) -> dict[str, str]:
        uid, acct = users[who]
        h = {"Authorization": f"Bearer {auth.create_access_token({'sub': uid, 'account_id': acct})}"}
        if ws:
            h["X-Cairn-Workspace"] = ws
        return h

    from cairn.core.triggers.server import build_app
    return types.SimpleNamespace(
        client=lambda: TestClient(build_app(_FakeRunner(), enable_scheduler=False)),
        headers=headers, workspaces=workspaces)


# ═══ the chokepoint ════════════════════════════════════════════════════════


async def test_with_auth_on_an_unknown_workspace_is_refused(monkeypatch):
    monkeypatch.setenv("OBS_JWT_SECRET", SECRET)
    with pytest.raises(W.WorkspaceForbidden):
        await W.assert_workspace_allowed("no-such-ws", "acme", store=W.InMemoryWorkspaceStore())


async def test_with_auth_off_an_unknown_workspace_is_still_permitted(monkeypatch):
    _auth_off(monkeypatch)
    await W.assert_workspace_allowed("no-such-ws", "default", store=W.InMemoryWorkspaceStore())


# ═══ through the app ═══════════════════════════════════════════════════════


def test_naming_an_unknown_workspace_by_header_is_404(env):
    with env.client() as c:
        r = c.get("/runs", headers=env.headers("bob@acme.com", "no-such-ws"))
    assert r.status_code == 404, (r.status_code, r.text)


def test_naming_an_unknown_workspace_by_path_is_404(env):
    with env.client() as c:
        r = c.get("/workspaces/no-such-ws/runs", headers=env.headers("bob@acme.com"))
    assert r.status_code == 404, (r.status_code, r.text)


def test_an_unknown_id_and_another_accounts_id_answer_alike(env):
    """The refusal does not tell a caller which ids exist."""
    with env.client() as c:
        unknown = c.get("/workspaces/no-such-ws/runs", headers=env.headers("bob@acme.com"))
        foreign = c.get("/workspaces/globex-ops/runs", headers=env.headers("bob@acme.com"))
    assert (unknown.status_code, unknown.text) == (foreign.status_code, foreign.text)


def test_a_write_cannot_land_in_an_unknown_workspace(env):
    with env.client() as c:
        r = c.put("/variables/region", json={"value": "eu"},
                  headers=env.headers("bob@acme.com", "no-such-ws"))
    assert r.status_code == 404, (r.status_code, r.text)


# ═══ unchanged ═════════════════════════════════════════════════════════════


def test_a_member_still_uses_their_own_workspace(env):
    with env.client() as c:
        r = c.get("/workspaces/acme-ops/runs", headers=env.headers("bob@acme.com"))
    assert r.status_code == 200, (r.status_code, r.text)


def test_a_workspace_created_over_the_api_is_usable_at_once(env):
    with env.client() as c:
        h = env.headers("bob@acme.com")
        made = c.post("/workspaces", json={"name": "Acme Research"}, headers=h)
        assert made.status_code == 200, made.text
        r = c.get(f"/workspaces/{made.json()['id']}/runs", headers=h)
    assert r.status_code == 200, (r.status_code, r.text)


def test_a_signed_up_founder_uses_their_first_workspace(env):
    with env.client() as c:
        out = c.post("/auth/signup", json={"company": "Initech", "email": "p@initech.com",
                                           "password": "pw12345"}).json()
        h = {"Authorization": f"Bearer {out['access_token']}"}
        r = c.get(f"/workspaces/{out['workspace_id']}/runs", headers=h)
    assert r.status_code == 200, (r.status_code, r.text)


def test_auth_off_may_name_an_unknown_workspace(env, monkeypatch):
    _auth_off(monkeypatch)
    with env.client() as c:
        r = c.get("/runs", headers={"X-Cairn-Workspace": "no-such-ws"})
    assert r.status_code == 200, (r.status_code, r.text)
