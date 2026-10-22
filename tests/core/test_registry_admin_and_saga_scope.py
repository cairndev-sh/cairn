"""Deployment-level registry writes need the deployment admin; a saga decision
checks the saga's workspace (cairn-platform#223).

1. ``POST /registry/uninstall`` and ``POST /registry/install`` act on the whole
   deployment (a pack is process-wide) but needed only a signed-in caller, so any
   member of any account could ask to remove a console-installed pack. With auth
   on they now need the deployment's admin (``CAIRN_API_USER``); anyone else,
   an account owner included, gets 403.
2. ``POST /triggers/approve/{workflow_id}`` checked the caller's own workspace,
   never that the saga being decided belongs to it. The decision now resolves the
   saga's workspace from the action handler (``workspace_of``) and requires the
   caller's account to own it; otherwise, and when the handler cannot say, the
   answer is 404 (the deployment admin still decides any saga).

Driven through the real app with auth on and real SQLite account, workspace and
install stores.
"""
from __future__ import annotations

import asyncio
import types

import pytest
from fastapi.testclient import TestClient

from cairn.core import account as A
from cairn.core import registry_install as RI
from cairn.core import workspace as W
from cairn.core.triggers import auth

SECRET = "test-secret-long-enough-for-hs256-223"
VICTIM_WS, INTRUDER_WS = "acme-ops", "initech-ops"
OWNER, MEMBER, INTRUDER = "bob@acme.com", "dee@acme.com", "carl@initech.com"
ENV_ADMIN = "root"
PACK = "acme/console-pack"
SAGA, ORPHAN_SAGA = "wf-acme-1", "wf-nobody"


class _Handler:
    """A durable action handler: one paused saga in acme's workspace."""

    def __init__(self, *, attributes: bool = True) -> None:
        self.decided: list[tuple[str, bool]] = []
        if attributes:
            self.workspace_of = self._workspace_of

    async def _workspace_of(self, workflow_id: str):
        return {SAGA: VICTIM_WS}.get(workflow_id)

    async def resume(self, workflow_id, record):
        self.decided.append((workflow_id, record.approved))
        return types.SimpleNamespace(status="success")


class _FakeRunner:
    default_environment = "production"
    skill_engine = types.SimpleNamespace(list_skills=lambda: [])

    def __init__(self, handler) -> None:
        self.action_handler = handler

    async def run(self, *a, **k):
        return {}


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE",
              "CAIRN_API_PASSWORD_HASH", "OBS_API_USER", "OBS_API_PASSWORD",
              "OBS_API_PASSWORD_HASH", "OBS_JWT_SECRET"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    monkeypatch.setenv("CAIRN_API_USER", ENV_ADMIN)
    monkeypatch.setenv("CAIRN_API_PASSWORD", "root-password-223")
    monkeypatch.setenv("CAIRN_SQLITE_DB", str(tmp_path / "cairn.db"))
    monkeypatch.setenv("CAIRN_SCHEDULES_DB", ":memory:")
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))

    installs = RI.SqliteInstallStore(path=str(tmp_path / "installs.db"))
    monkeypatch.setattr(RI, "_store", installs)
    asyncio.run(installs.upsert(RI.InstallRecord(ref=PACK, id=PACK, status="installed",
                                                 detail="installed by the deployment admin")))

    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)

    async def _seed() -> dict[str, tuple[str, str, str]]:
        ids = {}
        for acct, ws in (("acme", VICTIM_WS), ("initech", INTRUDER_WS)):
            await accounts.put_account(A.Account(id=acct, name=acct.title()))
            await workspaces.put(W.Workspace(id=ws, name=ws, account_id=acct))
        for user, acct, ws, role in ((OWNER, "acme", VICTIM_WS, "owner"),
                                     (MEMBER, "acme", VICTIM_WS, "member"),
                                     (INTRUDER, "initech", INTRUDER_WS, "owner")):
            u = await A.create_user(user, f"{user}-password", store=accounts)
            await A.add_member(acct, u.id, role, store=accounts)
            ids[user] = (u.id, acct, ws)
        return ids
    users = asyncio.run(_seed())

    def user(who: str) -> dict[str, str]:
        uid, acct, ws = users[who]
        tok = auth.create_access_token({"sub": uid, "account_id": acct})
        return {"Authorization": f"Bearer {tok}", "X-Cairn-Workspace": ws}

    def client(handler=None):
        from cairn.core.triggers.server import build_app
        return TestClient(build_app(_FakeRunner(handler or _Handler()), enable_scheduler=False))

    return types.SimpleNamespace(client=client, user=user, installs=installs)


def _env_admin() -> dict[str, str]:
    return {"Authorization": f"Bearer {auth.create_access_token({'sub': ENV_ADMIN, 'account_id': 'default'})}"}


def _record(env) -> RI.InstallRecord:
    rec = asyncio.run(env.installs.get(PACK))
    assert rec is not None
    return rec


# ─── 1. registry install/uninstall: the deployment admin only ────────────


@pytest.mark.parametrize("who", [MEMBER, OWNER, INTRUDER])
def test_a_signed_in_user_cannot_uninstall_a_pack(env, who):
    before = _record(env)
    with env.client() as c:
        r = c.post("/registry/uninstall", json={"ref": PACK}, headers=env.user(who))
    assert r.status_code == 403, (r.status_code, r.text)
    after = _record(env)
    assert (after.status, after.detail, after.updated_at) == (before.status, before.detail, before.updated_at)


@pytest.mark.parametrize("who", [MEMBER, OWNER, INTRUDER])
def test_a_signed_in_user_cannot_install_a_pack(env, who):
    with env.client() as c:
        r = c.post("/registry/install", json={"ref": "acme/other-pack"}, headers=env.user(who))
    assert r.status_code == 403, (r.status_code, r.text)
    assert asyncio.run(env.installs.get("acme/other-pack")) is None


def test_the_deployment_admin_gets_the_routes_own_answer(env):
    """The admin reaches the door; the door then answers as it always did:
    uninstall by posture (501, #1545 D2), install of a ref outside the catalog
    with a ``failed`` record."""
    with env.client() as c:
        un = c.post("/registry/uninstall", json={"ref": PACK}, headers=_env_admin())
        ins = c.post("/registry/install", json={"ref": "acme/other-pack"}, headers=_env_admin())
    assert un.status_code == 501, (un.status_code, un.text)
    assert ins.status_code == 200 and ins.json()["status"] == "failed", (ins.status_code, ins.text)


def test_the_deployment_admin_uninstalls_with_a_working_uninstaller(env, monkeypatch):
    """With an uninstaller that works, the admin's uninstall goes through."""
    real = RI.uninstall_pack
    removed: list[str] = []

    async def _uninstall(ref, actor="console", **kw):
        return await real(ref, actor, uninstaller=removed.append, catalog_idx={})
    monkeypatch.setattr(RI, "uninstall_pack", _uninstall)
    with env.client() as c:
        r = c.post("/registry/uninstall", json={"ref": PACK}, headers=_env_admin())
    assert r.status_code == 200 and r.json()["status"] == "removed", (r.status_code, r.text)
    assert removed == [PACK]


def test_auth_off_registry_doors_are_unchanged(monkeypatch, tmp_path):
    for k in ("CAIRN_JWT_SECRET", "OBS_JWT_SECRET", "CAIRN_REQUIRE_AUTH", "OBS_REQUIRE_AUTH",
              "OBS_STORAGE", "CAIRN_STORAGE", "CAIRN_WORKSPACE", "OBS_WORKSPACE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_SCHEDULES_DB", ":memory:")
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))
    installs = RI.SqliteInstallStore(path=str(tmp_path / "installs.db"))
    monkeypatch.setattr(RI, "_store", installs)
    asyncio.run(installs.upsert(RI.InstallRecord(ref=PACK, id=PACK, status="installed")))
    from cairn.core.triggers.server import build_app
    with TestClient(build_app(_FakeRunner(_Handler()), enable_scheduler=False)) as c:
        assert c.post("/registry/uninstall", json={"ref": PACK}).status_code == 501
        assert c.post("/registry/install", json={"ref": "acme/other-pack"}).status_code == 200


# ─── 2. a saga is decided only from its own account ──────────────────────


def test_another_account_cannot_decide_a_saga(env):
    handler = _Handler()
    with env.client(handler) as c:
        r = c.post(f"/triggers/approve/{SAGA}", json={"decision": "approve"}, headers=env.user(INTRUDER))
    assert r.status_code == 404, (r.status_code, r.text)
    assert handler.decided == []


@pytest.mark.parametrize("who", [OWNER, MEMBER])
def test_the_sagas_own_account_decides_it(env, who):
    handler = _Handler()
    with env.client(handler) as c:
        r = c.post(f"/triggers/approve/{SAGA}", json={"decision": "reject"}, headers=env.user(who))
    assert r.status_code == 200 and r.json()["approved"] is False, (r.status_code, r.text)
    assert handler.decided == [(SAGA, False)]


def test_a_saga_no_workspace_claims_is_404(env):
    handler = _Handler()
    with env.client(handler) as c:
        r = c.post(f"/triggers/approve/{ORPHAN_SAGA}", json={"decision": "approve"}, headers=env.user(OWNER))
    assert r.status_code == 404, (r.status_code, r.text)
    assert handler.decided == []


def test_a_handler_that_cannot_attribute_sagas_leaves_them_to_the_deployment_admin(env):
    """Fail closed: a handler without ``workspace_of`` cannot say whose a saga is,
    so only the deployment admin decides one."""
    handler = _Handler(attributes=False)
    with env.client(handler) as c:
        user = c.post(f"/triggers/approve/{SAGA}", json={"decision": "approve"}, headers=env.user(OWNER))
        admin = c.post(f"/triggers/approve/{SAGA}", json={"decision": "approve"}, headers=_env_admin())
    assert user.status_code == 404, (user.status_code, user.text)
    assert admin.status_code == 200, (admin.status_code, admin.text)
    assert handler.decided == [(SAGA, True)]


@pytest.mark.parametrize("answer", ["raise", "  "])
def test_a_handler_whose_workspace_of_fails_or_is_blank_fails_closed(env, answer):
    """An exception or a blank workspace id from ``workspace_of`` leaves the saga to
    the deployment admin; it never reads as "no workspace asserted"."""
    handler = _Handler()

    async def _broken(workflow_id):
        if answer == "raise":
            raise RuntimeError("saga store unavailable")
        return answer
    handler.workspace_of = _broken
    with env.client(handler) as c:
        user = c.post(f"/triggers/approve/{SAGA}", json={"decision": "approve"}, headers=env.user(OWNER))
    assert user.status_code == 404, (user.status_code, user.text)
    assert handler.decided == []


def test_the_deployment_admin_decides_any_saga(env):
    handler = _Handler()
    with env.client(handler) as c:
        r = c.post(f"/triggers/approve/{SAGA}", json={"decision": "approve"}, headers=_env_admin())
    assert r.status_code == 200, (r.status_code, r.text)
    assert handler.decided == [(SAGA, True)]


def test_auth_off_saga_decisions_are_unchanged(monkeypatch, tmp_path):
    for k in ("CAIRN_JWT_SECRET", "OBS_JWT_SECRET", "CAIRN_REQUIRE_AUTH", "OBS_REQUIRE_AUTH",
              "OBS_STORAGE", "CAIRN_STORAGE", "CAIRN_WORKSPACE", "OBS_WORKSPACE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_SCHEDULES_DB", ":memory:")
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))
    handler = _Handler(attributes=False)
    from cairn.core.triggers.server import build_app
    with TestClient(build_app(_FakeRunner(handler), enable_scheduler=False)) as c:
        r = c.post(f"/triggers/approve/{SAGA}", json={"decision": "approve"})
    assert r.status_code == 200, (r.status_code, r.text)
    assert handler.decided == [(SAGA, True)]
