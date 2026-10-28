"""The durable trigger inbox is scoped to the caller's workspace.

``GET /triggers/inbox`` listed every queued delivery, input payload included, for
every workspace, and ``POST /triggers/inbox/replay`` re-enqueued every workspace's
finished deliveries; both needed only a signed-in caller. Since a queued delivery
runs in its receiver's workspace, any user could re-run another tenant's
deliveries in that tenant's context.

Now the list shows only the caller's workspace (a record with no workspace belongs
to ``default``). Replay needs an owner/admin (``require_admin``) and replays only
the caller's workspace; only the instance admin (the deployment's env admin,
``CAIRN_API_USER``) replays across workspaces.

Driven through the real app (``build_app``) with auth on and real account and
workspace stores (accounts in sqlite in a temp dir, workspaces in memory).
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
from cairn.core.engine.runner import Runner
from cairn.core.storage import trigger_inbox as inbox_mod
from cairn.core.triggers import auth
from cairn.core.triggers import router as router_mod
from cairn.core.triggers.server import build_app

SINCE = "1970-01-01T00:00:00"


@pytest.fixture
def app_env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "CAIRN_TRIGGER_DURABLE", "OBS_TRIGGER_DURABLE",
              "CAIRN_API_USER", "CAIRN_API_PASSWORD", "CAIRN_API_PASSWORD_HASH",
              "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("OBS_TRIGGER_DURABLE", "1")
    monkeypatch.setenv("OBS_TRIGGER_INBOX", "memory")
    # the deployment's env admin (the instance admin)
    monkeypatch.setenv("CAIRN_API_USER", "root")
    monkeypatch.setenv("CAIRN_API_PASSWORD", "root-password-for-tests")
    monkeypatch.setattr(auth, "_PUBLIC_TRIGGER_PATHS", set())

    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.InMemoryWorkspaceStore()
    monkeypatch.setattr(W, "_store", workspaces)

    async def _seed() -> dict[str, str]:
        await accounts.put_account(A.Account(id="acme", name="Acme"))
        await accounts.put_account(A.Account(id="globex", name="Globex"))
        await workspaces.put(W.Workspace(id="acme-ops", name="Acme ops", account_id="acme"))
        await workspaces.put(W.Workspace(id="globex-ops", name="Globex ops", account_id="globex"))
        ids = {}
        for name, acct, role in (("alice", "acme", "admin"), ("bob", "acme", "member"),
                                 ("gina", "globex", "admin")):
            u = await A.create_user(name, f"{name}-password", store=accounts)
            await A.add_member(acct, u.id, role, store=accounts)
            ids[name] = (u.id, acct)
        return ids
    users = asyncio.run(_seed())

    # no receivers, no discovered pipelines; the drain loop is captured (never run)
    monkeypatch.setattr(router_mod, "load_trigger_specs", lambda *_a, **_k: [])
    import cairn.core.storage.trigger_store as ts
    store = types.SimpleNamespace(list_sync=lambda *a, **k: [], close=lambda: None)
    monkeypatch.setattr(ts, "default_trigger_store", lambda: store)

    async def _no_sync(**_kw):
        return 0
    import cairn.core.workflow_registry as wr
    monkeypatch.setattr(wr, "sync_discovered_pipelines", _no_sync)
    captured: dict = {}

    async def _capture(inbox, handler, **_kw):
        captured["inbox"] = inbox
    monkeypatch.setattr(inbox_mod, "run_drain_loop", _capture)

    def headers(who: str, ws: str | None = None) -> dict[str, str]:
        if who == "root":
            claims = {"sub": "root"}
        else:
            uid, acct = users[who]
            claims = {"sub": uid, "account_id": acct}
        h = {"Authorization": f"Bearer {auth.create_access_token(claims)}"}
        if ws:
            h["X-Cairn-Workspace"] = ws
        return h

    return types.SimpleNamespace(captured=captured, headers=headers)


def _seed_inbox(c: TestClient, env) -> dict[str, str]:
    """One finished delivery per workspace (and one with no workspace)."""
    inbox = env.captured["inbox"]

    async def _put():
        ids = {}
        for ws in ("acme-ops", "globex-ops", None):
            rec = await inbox.enqueue("custom", "pipeline:p/x", {"payload": f"of-{ws}"}, workspace=ws)
            await inbox.mark_done(rec.id)
            ids[ws or "default"] = rec.id
        return ids
    return c.portal.call(_put)


def _pending(c: TestClient, env) -> list:
    return [r for r in c.portal.call(env.captured["inbox"].list) if r.status == "pending"]


# ═══ the list ═════════════════════════════════════════════════════════════


def test_a_user_sees_only_their_workspaces_deliveries(app_env):
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.get("/triggers/inbox", headers=app_env.headers("bob", "acme-ops"))
        assert r.status_code == 200, (r.status_code, r.text)
        recs = r.json()["records"]
    assert [x["workspace"] for x in recs] == ["acme-ops"]
    assert "of-globex-ops" not in r.text


def test_a_record_with_no_workspace_belongs_to_default(app_env):
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.get("/triggers/inbox", headers=app_env.headers("root"))
        assert r.status_code == 200, (r.status_code, r.text)
    assert [x["workspace"] for x in r.json()["records"]] == [None]


def test_another_tenants_workspace_cannot_be_named(app_env):
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.get("/triggers/inbox", headers=app_env.headers("bob", "globex-ops"))
    assert r.status_code == 404, (r.status_code, r.text)


def test_without_a_header_another_accounts_member_cannot_read_default(app_env):
    """No X-Cairn-Workspace resolves to `default`, which belongs to the default
    account; a member of another account acts in their home workspace (#220), so
    they see only their own account's deliveries."""
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.get("/triggers/inbox", headers=app_env.headers("bob"))
    assert r.status_code == 200, (r.status_code, r.text)
    assert [x["workspace"] for x in r.json()["records"]] == ["acme-ops"]
    assert "of-None" not in r.text and "of-globex-ops" not in r.text


# ═══ replay ═══════════════════════════════════════════════════════════════


def test_a_non_admin_replay_is_refused(app_env):
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.post("/triggers/inbox/replay", params={"since": SINCE},
                   headers=app_env.headers("bob", "acme-ops"))
        assert r.status_code == 403, (r.status_code, r.text)
        assert _pending(c, app_env) == []


def test_an_admin_replay_in_a_workspace_replays_only_that_workspace(app_env):
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.post("/triggers/inbox/replay", params={"since": SINCE},
                   headers=app_env.headers("alice", "acme-ops"))
        assert r.status_code == 200, (r.status_code, r.text)
        assert r.json()["replayed"] == 1
        assert [(x.workspace, x.input) for x in _pending(c, app_env)] == [
            ("acme-ops", {"payload": "of-acme-ops"})]


def test_an_admin_of_another_account_replays_only_their_own(app_env):
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.post("/triggers/inbox/replay", params={"since": SINCE},
                   headers=app_env.headers("gina", "globex-ops"))
        assert r.status_code == 200, (r.status_code, r.text)
        assert [x.workspace for x in _pending(c, app_env)] == ["globex-ops"]
        r = c.post("/triggers/inbox/replay", params={"since": SINCE},
                   headers=app_env.headers("gina", "acme-ops"))
        assert r.status_code == 404, (r.status_code, r.text)


def test_without_a_header_another_accounts_admin_cannot_replay_default(app_env):
    """Without a header the admin of another account acts in their home workspace
    (#220): only that workspace's deliveries are replayed, never default's."""
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.post("/triggers/inbox/replay", params={"since": SINCE}, headers=app_env.headers("alice"))
        assert r.status_code == 200, (r.status_code, r.text)
        assert r.json()["replayed"] == 1
        assert [(x.workspace, x.input) for x in _pending(c, app_env)] == [
            ("acme-ops", {"payload": "of-acme-ops"})]


def test_the_instance_admin_replays_every_workspace(app_env):
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        _seed_inbox(c, app_env)
        r = c.post("/triggers/inbox/replay", params={"since": SINCE},
                   headers=app_env.headers("root"))
        assert r.status_code == 200, (r.status_code, r.text)
        assert r.json()["replayed"] == 3
        assert sorted(str(x.workspace) for x in _pending(c, app_env)) == [
            "None", "acme-ops", "globex-ops"]
