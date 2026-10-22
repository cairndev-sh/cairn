"""Only the owning account manages a workspace, and a create never takes one over.

``DELETE /workspaces/{id}`` and ``PATCH /workspaces/{id}`` archived or restored
any workspace by id for any signed-in user. ``POST /workspaces`` wrote with an
upserting ``put``, so a create naming an existing id (``default`` included)
re-stamped that workspace with the caller's account.

Now archive and restore need an owner/admin of the owning account (another
account's workspace answers 404, as a cross-account read does), and a create
whose id exists answers 404 (another account's) or 409 (the caller's own).

Driven through the real app (``build_app``) with auth on and real account and
workspace stores (sqlite, whose ``put`` is INSERT OR REPLACE).
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


class _FakeRunner:
    default_environment = "production"
    skill_engine = types.SimpleNamespace(list_skills=lambda: [])

    async def run(self, *a, **k):
        return {}


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE", "CAIRN_JWT_SECRET",
              "CAIRN_API_USER", "CAIRN_API_PASSWORD", "CAIRN_API_PASSWORD_HASH",
              "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))

    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)

    async def _seed() -> dict[str, tuple[str, str]]:
        for acct in ("acme", "globex"):
            await accounts.put_account(A.Account(id=acct, name=acct.title()))
        await workspaces.put(W.Workspace(id="acme-ops", name="Acme ops", account_id="acme"))
        ids = {}
        for name, acct, role in (("alice", "acme", "admin"), ("bob", "acme", "member"),
                                 ("gina", "globex", "admin")):
            u = await A.create_user(name, f"{name}-password", store=accounts)
            await A.add_member(acct, u.id, role, store=accounts)
            ids[name] = (u.id, acct)
        return ids
    users = asyncio.run(_seed())

    def headers(who: str) -> dict[str, str]:
        uid, acct = users[who]
        return {"Authorization": f"Bearer {auth.create_access_token({'sub': uid, 'account_id': acct})}"}

    def row(ws_id: str):
        return asyncio.run(workspaces.get(ws_id))

    from cairn.core.triggers.server import build_app
    return types.SimpleNamespace(
        client=lambda: TestClient(build_app(_FakeRunner(), enable_scheduler=False)),
        headers=headers, row=row)


def _archive(c, ws, h):
    return c.delete(f"/workspaces/{ws}", params={"confirm": "delete"}, headers=h)


def _restore(c, ws, h):
    return c.patch(f"/workspaces/{ws}", json={"status": "active"}, headers=h)


# ═══ archive / restore ═════════════════════════════════════════════════════


def test_another_accounts_admin_cannot_archive(env):
    with env.client() as c:
        r = _archive(c, "acme-ops", env.headers("gina"))
    assert r.status_code == 404, (r.status_code, r.text)
    assert env.row("acme-ops").status == "active"


def test_another_accounts_admin_cannot_restore_or_archive_by_patch(env):
    with env.client() as c:
        assert _archive(c, "acme-ops", env.headers("alice")).status_code == 200
        r = _restore(c, "acme-ops", env.headers("gina"))
        assert r.status_code == 404, (r.status_code, r.text)
        assert env.row("acme-ops").status == "archived"
        assert _restore(c, "acme-ops", env.headers("alice")).status_code == 200
        r = c.patch("/workspaces/acme-ops", json={"status": "archived"}, headers=env.headers("gina"))
    assert r.status_code == 404, (r.status_code, r.text)
    assert env.row("acme-ops").status == "active"


def test_another_accounts_workspace_answers_like_a_missing_one(env):
    with env.client() as c:
        foreign = _archive(c, "acme-ops", env.headers("gina"))
        missing = _archive(c, "no-such-ws", env.headers("gina"))
    assert foreign.status_code == missing.status_code == 404
    assert foreign.json()["detail"].replace("acme-ops", "X") == missing.json()["detail"].replace("no-such-ws", "X")


def test_a_member_who_is_not_an_admin_gets_403(env):
    with env.client() as c:
        a = _archive(c, "acme-ops", env.headers("bob"))
        p = c.patch("/workspaces/acme-ops", json={"status": "archived"}, headers=env.headers("bob"))
    assert (a.status_code, p.status_code) == (403, 403), (a.text, p.text)
    assert env.row("acme-ops").status == "active"


def test_the_owning_admin_archives_and_restores(env):
    with env.client() as c:
        a = _archive(c, "acme-ops", env.headers("alice"))
        assert a.status_code == 200, a.text
        assert env.row("acme-ops").status == "archived"
        p = _restore(c, "acme-ops", env.headers("alice"))
    assert p.status_code == 200, p.text
    assert env.row("acme-ops").status == "active"


# ═══ create never takes a workspace over ═══════════════════════════════════


@pytest.mark.parametrize("victim", ["acme-ops", "default"])
def test_another_account_cannot_take_over_an_existing_id(env, victim):
    before = env.row(victim)
    with env.client() as c:
        r = c.post("/workspaces", json={"name": "mine now", "id": victim}, headers=env.headers("gina"))
    assert r.status_code == 404, (r.status_code, r.text)
    after = env.row(victim)
    assert (after.account_id, after.name, after.created_at) == (before.account_id, before.name, before.created_at)


def test_an_id_the_callers_own_account_holds_is_409(env):
    before = env.row("acme-ops")
    with env.client() as c:
        r = c.post("/workspaces", json={"name": "renamed", "id": "acme-ops"}, headers=env.headers("alice"))
    assert r.status_code == 409, (r.status_code, r.text)
    assert env.row("acme-ops").name == before.name


def test_a_fresh_id_still_creates(env):
    with env.client() as c:
        r = c.post("/workspaces", json={"name": "Globex Research"}, headers=env.headers("gina"))
    assert r.status_code == 200, (r.status_code, r.text)
    assert env.row(r.json()["id"]).account_id == "globex"


def test_the_instance_admin_still_archives_any_workspace(env, monkeypatch):
    monkeypatch.setenv("CAIRN_API_USER", "root")
    monkeypatch.setenv("CAIRN_API_PASSWORD", "root-password-for-tests")
    h = {"Authorization": f"Bearer {auth.create_access_token({'sub': 'root'})}"}
    with env.client() as c:
        assert _archive(c, "acme-ops", h).status_code == 200
        assert _restore(c, "acme-ops", h).status_code == 200
    assert env.row("acme-ops").status == "active"


def test_a_name_another_account_already_uses_gets_the_next_free_slug(env):
    """The console creates by name; the id is the name's slug. Another account's
    `acme-ops` must neither be taken over nor block the name: the create gets
    `acme-ops-2` and the owner's row is unchanged."""
    before = env.row("acme-ops")
    with env.client() as c:
        r = c.post("/workspaces", json={"name": "Acme Ops"}, headers=env.headers("gina"))
    assert r.status_code == 200, (r.status_code, r.text)
    assert r.json()["id"] == "acme-ops-2"
    assert env.row("acme-ops-2").account_id == "globex"
    after = env.row("acme-ops")
    assert (after.account_id, after.name) == (before.account_id, before.name)


def test_a_missing_workspace_is_404_for_its_would_be_owner_too(env):
    with env.client() as c:
        d = _archive(c, "no-such-ws", env.headers("alice"))
        p = c.patch("/workspaces/no-such-ws", json={"status": "active"}, headers=env.headers("alice"))
    assert (d.status_code, p.status_code) == (404, 404), (d.text, p.text)
    assert env.row("no-such-ws") is None
