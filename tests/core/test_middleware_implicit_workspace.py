"""The workspace a request acts in is authorized even when the request names none.

``_WorkspaceMiddleware`` authorized a workspace only when the request named one
(``X-Cairn-Workspace`` or the ``/workspaces/{id}/`` path). With neither, the
request acted in the implicit workspace (``CAIRN_WORKSPACE``, else ``default``)
and nothing checked that the caller's account owns it. ``default`` belongs to
the default account, so with auth on, a member of any other account could read
it by sending no header.

Now the implicit workspace is authorized like a named one. A caller whose
account does not own it acts in their account's home workspace (its oldest
active one, where sign-in lands); an account with no workspace gets 404.

Driven through the real app (``build_app``) with auth on and real account,
workspace, run and pipeline stores.
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
from cairn.core.engine.schema import TriggerEvent
from cairn.core.storage import Run, SqliteRunStore
from cairn.core.storage import pipeline_store as P
from cairn.core.triggers import auth

DEFAULT_RUN, ACME_RUN = "run-default-1", "run-acme-1"
NOROW_WS, NOROW_RUN = "no-row-ws", "run-norow-1"  # a run stamped with a workspace that has no row
DEFAULT_PIPE, ACME_PIPE = "pipe-default-1", "pipe-acme-1"


class _FakeRunner:
    default_environment = "production"
    skill_engine = types.SimpleNamespace(list_skills=lambda: [])

    async def run(self, *a, **k):
        return {}


def _seed_runs(path: str) -> None:
    async def _write():
        s = SqliteRunStore(path)
        for run_id, ws in ((DEFAULT_RUN, "default"), (ACME_RUN, "acme-ops"), (NOROW_RUN, NOROW_WS)):
            trig = TriggerEvent(source="manual", matched_pattern="p", service="svc")
            await s.create(Run(id=run_id, trigger=trig, fingerprint=run_id, workspace_id=ws))
            await s.append_event(run_id, "done", {})
        s.close()

    asyncio.run(_write())


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE",
              "CAIRN_API_USER", "CAIRN_API_PASSWORD", "CAIRN_API_PASSWORD_HASH",
              "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("CAIRN_API_USER", "root")  # the deployment's env admin
    monkeypatch.setenv("CAIRN_API_PASSWORD", "root-password-for-tests")
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))
    _seed_runs(str(tmp_path / "r.db"))

    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)
    pipes = P.InMemoryPipelineStore()
    monkeypatch.setattr(P, "_default_store", pipes)

    async def _seed() -> dict[str, tuple[str, str]]:
        await accounts.put_account(A.Account(id="acme", name="Acme"))
        await workspaces.put(W.Workspace(id="acme-ops", name="Acme ops", account_id="acme"))
        await pipes.put(P.PipelineRecord(pipeline_id=DEFAULT_PIPE, pipeline_name="p", workspace_id="default"))
        await pipes.put(P.PipelineRecord(pipeline_id=ACME_PIPE, pipeline_name="p", workspace_id="acme-ops"))
        ids = {}
        await accounts.put_account(A.Account(id="initech", name="Initech"))  # no workspace
        await accounts.put_account(A.Account(id="umbrella", name="Umbrella"))
        await workspaces.put(W.Workspace(id="umbrella-ops", name="Umbrella ops", account_id="umbrella",
                                         status="archived"))  # its only workspace, archived
        for name, acct in (("bob@acme.com", "acme"), ("dana", A.DEFAULT_ACCOUNT),
                           ("carl@initech.com", "initech"), ("uma@umbrella.com", "umbrella")):
            u = await A.create_user(name, f"{name}-password", store=accounts)
            await A.add_member(acct, u.id, "admin" if name == "uma@umbrella.com" else "member",
                               store=accounts)
            ids[name] = (u.id, acct)
        return ids
    users = asyncio.run(_seed())

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

    from cairn.core.triggers.server import build_app
    return types.SimpleNamespace(
        client=lambda: TestClient(build_app(_FakeRunner(), enable_scheduler=False)),
        headers=headers)


def _run_ids(r) -> set[str]:
    return {i["id"] for i in r.json()["investigations"]}


def _pipe_ids(r) -> set[str]:
    return {p["pipeline_id"] for p in r.json()["pipelines"]}


# ═══ the defect: no header, another account, the default workspace ═══════


def test_without_a_header_another_accounts_member_lists_only_their_own_runs(env):
    with env.client() as c:
        r = c.get("/runs", headers=env.headers("bob@acme.com"))
    assert r.status_code == 200, (r.status_code, r.text)
    assert _run_ids(r) == {ACME_RUN}
    assert DEFAULT_RUN not in r.text


def test_without_a_header_another_accounts_member_lists_only_their_own_pipelines(env):
    with env.client() as c:
        r = c.get("/pipelines", headers=env.headers("bob@acme.com"))
    assert r.status_code == 200, (r.status_code, r.text)
    assert _pipe_ids(r) == {ACME_PIPE}
    assert DEFAULT_PIPE not in r.text


def test_the_request_reports_the_home_workspace_as_current(env):
    with env.client() as c:
        r = c.get("/workspaces", headers=env.headers("bob@acme.com"))
    assert r.status_code == 200, (r.status_code, r.text)
    assert r.json()["current"] == "acme-ops"


def test_an_account_with_no_workspace_gets_404_but_can_create_one(env):
    """No active workspace: every workspace-scoped route answers 404 (never
    `default`); the account-level routes answer, so it can create its first."""
    h = env.headers("carl@initech.com")
    with env.client() as c:
        r = c.get("/runs", headers=h)
        assert r.status_code == 404 and DEFAULT_RUN not in r.text, (r.status_code, r.text)
        for method, path, body in (("PUT", "/variables/K", {"value": "v"}),
                                   ("POST", "/triggers/manual", {"template": "x", "input": {}}),
                                   ("POST", "/pipelines/launch", {"ref": "x"}),
                                   ("POST", "/schedules", {"name": "s", "cron": "* * * * *", "template": "x"})):
            w = c.request(method, path, json=body, headers=h)
            assert w.status_code == 404, (method, path, w.status_code, w.text)
        assert c.get("/workspaces", headers=h).status_code == 200
        assert c.get("/auth/me", headers=h).status_code == 200
        created = c.post("/workspaces", json={"name": "Initech"}, headers=h)
    assert created.status_code == 200, (created.status_code, created.text)


def test_two_accounts_with_no_workspace_share_nothing(env):
    """The no-workspace id is one value for every such account, so nothing may be
    written under it: A's trigger is refused, and B sees no run of A's."""
    with env.client() as c:
        a = c.post("/triggers/manual", json={"template": "x", "input": {}}, headers=env.headers("carl@initech.com"))
        b = c.get("/runs", headers=env.headers("uma@umbrella.com"))
    assert a.status_code == 404, (a.status_code, a.text)
    assert b.status_code == 404, (b.status_code, b.text)


def test_cairn_workspace_is_authorized_like_the_default(env, monkeypatch):
    """The env-pinned implicit workspace is the resolved workspace too: its
    own account uses it, any other account acts in its home workspace."""
    monkeypatch.setenv("CAIRN_WORKSPACE", "acme-ops")
    with env.client() as c:
        bob = c.get("/runs", headers=env.headers("bob@acme.com"))
        dana = c.get("/runs", headers=env.headers("dana"))
    assert bob.status_code == 200 and _run_ids(bob) == {ACME_RUN}
    assert dana.status_code == 200 and _run_ids(dana) == {DEFAULT_RUN}


def test_a_cairn_workspace_with_no_row_is_not_every_accounts(env, monkeypatch):
    """With auth on a workspace with no row is refused (#156), so a
    `CAIRN_WORKSPACE` naming one sends another account's member to their home
    workspace, not into it."""
    monkeypatch.setenv("CAIRN_WORKSPACE", NOROW_WS)
    with env.client() as c:
        h = env.headers("bob@acme.com")
        cur = c.get("/workspaces", headers=h).json()["current"]
        r = c.get("/runs", headers=h)
    assert cur == "acme-ops"
    assert r.status_code == 200 and _run_ids(r) == {ACME_RUN}


def test_a_cairn_workspace_with_no_row_sends_a_default_member_to_default(env, monkeypatch):
    monkeypatch.setenv("CAIRN_WORKSPACE", NOROW_WS)
    with env.client() as c:
        h = env.headers("dana")
        cur = c.get("/workspaces", headers=h).json()["current"]
        r = c.get("/runs", headers=h)
    assert cur == "default"
    assert r.status_code == 200 and _run_ids(r) == {DEFAULT_RUN}
    assert NOROW_RUN not in r.text


def test_a_worker_with_a_cairn_workspace_with_no_row_never_acts_in_it(env, monkeypatch):
    """A per-run worker token is the default account's: with no header it acts
    in `default`, so its run there still reports, and a run stamped with the
    row-less workspace answers 404 (the run guard compares it to `default`)."""
    monkeypatch.setenv("CAIRN_WORKSPACE", NOROW_WS)

    def _post(run_id):
        tok = auth.create_access_token({"sub": f"worker:{run_id}", "scope": "run-ingest", "run_id": run_id})
        return c.post(f"/runs/{run_id}/events/ingest", headers={"Authorization": f"Bearer {tok}"},
                      json={"events": [{"type": "log", "data": {"node": "n", "seq": 1}}]})
    with env.client() as c:
        in_default, in_norow = _post(DEFAULT_RUN), _post(NOROW_RUN)
    assert in_default.status_code == 200, (in_default.status_code, in_default.text)
    assert in_norow.status_code == 404, (in_norow.status_code, in_norow.text)


def test_a_cairn_workspace_with_no_row_is_warned_about_once_at_startup(env, monkeypatch, caplog):
    monkeypatch.setenv("CAIRN_WORKSPACE", NOROW_WS)
    with caplog.at_level("WARNING", logger="cairn.trigger_server"):
        with env.client() as c:
            c.get("/runs", headers=env.headers("dana"))
            c.get("/runs", headers=env.headers("bob@acme.com"))
    hits = [r for r in caplog.records if "CAIRN_WORKSPACE" in r.getMessage() and NOROW_WS in r.getMessage()]
    assert len(hits) == 1, [r.getMessage() for r in caplog.records]


def test_an_existing_cairn_workspace_is_not_warned_about(env, monkeypatch, caplog):
    monkeypatch.setenv("CAIRN_WORKSPACE", "acme-ops")
    with caplog.at_level("WARNING", logger="cairn.trigger_server"):
        with env.client():
            pass
    assert not [r for r in caplog.records if "names no workspace" in r.getMessage()]


def test_an_account_whose_only_workspace_is_archived_can_still_restore_it(env):
    h = env.headers("uma@umbrella.com")
    with env.client() as c:
        r = c.get("/runs", headers=h)
        assert r.status_code == 404 and DEFAULT_RUN not in r.text, (r.status_code, r.text)
        listed = c.get("/workspaces", params={"include_archived": "true"}, headers=h)
        assert listed.status_code == 200, (listed.status_code, listed.text)
        archived = [w["id"] for w in listed.json().get("workspaces", []) if w.get("status") == "archived"]
        assert archived, listed.text
        restored = c.patch(f"/workspaces/{archived[0]}", json={"status": "active"}, headers=h)
    assert restored.status_code == 200, (restored.status_code, restored.text)
    assert restored.json().get("status") == "active", restored.text


def test_a_headerless_start_launches_in_the_home_workspace_never_default(env, monkeypatch):
    """The sharpest write: where a header-less POST /start would launch."""
    from cairn.core.dispatch import start as start_mod

    seen: list[str] = []

    def _resolve(ref, kind=None):
        seen.append(W.current_workspace())
        raise start_mod.UnknownStartRefError(ref)
    monkeypatch.setattr(start_mod, "resolve_start_ref", _resolve)
    with env.client() as c:
        r = c.post("/start", json={"ref": "pipeline:p/x"}, headers=env.headers("bob@acme.com"))
    assert r.status_code == 404, (r.status_code, r.text)  # the stubbed ref, after authorization
    assert seen == ["acme-ops"]


def test_an_account_resolution_failure_fails_closed(env, monkeypatch):
    """The guard authenticated the caller; failing to decide where they act is a
    503, never the implicit workspace unchecked."""
    from cairn.core.triggers import console_support

    async def _boom(_claims):
        raise RuntimeError("account store down")
    monkeypatch.setattr(console_support, "account_from_claims", _boom)
    with env.client() as c:
        r = c.get("/runs", headers=env.headers("bob@acme.com"))
    assert r.status_code == 503, (r.status_code, r.text)
    assert DEFAULT_RUN not in r.text


def test_a_named_workspace_also_fails_closed_when_account_resolution_raises(env, monkeypatch):
    """The named-workspace branch: an authenticated caller whose account cannot be
    resolved gets 503, never the named workspace unchecked."""
    from cairn.core.triggers import console_support

    async def _boom(_claims):
        raise RuntimeError("account store down")
    monkeypatch.setattr(console_support, "account_from_claims", _boom)
    with env.client() as c:
        r = c.get("/runs", headers=env.headers("bob@acme.com", "acme-ops"))
    assert r.status_code == 503, (r.status_code, r.text)
    assert ACME_RUN not in r.text


# ═══ unchanged ═════════════════════════════════════════════════════════════


def test_a_default_account_member_still_reads_default_without_a_header(env):
    with env.client() as c:
        r = c.get("/runs", headers=env.headers("dana"))
        p = c.get("/pipelines", headers=env.headers("dana"))
    assert r.status_code == 200 and _run_ids(r) == {DEFAULT_RUN}
    assert p.status_code == 200 and _pipe_ids(p) == {DEFAULT_PIPE}


def test_the_env_admin_still_reads_default_without_a_header(env):
    with env.client() as c:
        r = c.get("/runs", headers=env.headers("root"))
    assert r.status_code == 200 and _run_ids(r) == {DEFAULT_RUN}


def test_a_member_naming_their_own_workspace_is_unchanged(env):
    with env.client() as c:
        r = c.get("/runs", headers=env.headers("bob@acme.com", "acme-ops"))
        p = c.get("/workspaces/acme-ops/pipelines", headers=env.headers("bob@acme.com"))
    assert r.status_code == 200 and _run_ids(r) == {ACME_RUN}
    assert p.status_code == 200 and _pipe_ids(p) == {ACME_PIPE}


def test_auth_off_is_unchanged(env, monkeypatch):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.delenv("CAIRN_API_USER", raising=False)
    monkeypatch.delenv("CAIRN_API_PASSWORD", raising=False)
    with env.client() as c:
        r = c.get("/runs")
    assert r.status_code == 200 and _run_ids(r) == {DEFAULT_RUN}


def test_public_paths_are_unchanged_for_another_accounts_member(env):
    """A token on a public path is not judged against the implicit workspace."""
    with env.client() as c:
        h = env.headers("bob@acme.com")
        assert c.get("/healthz", headers=h).status_code == 200
        assert c.get("/auth/status", headers=h).status_code == 200
        tok = c.post("/token", data={"username": "bob@acme.com", "password": "bob@acme.com-password"},
                     headers=h)
        assert tok.status_code == 200, tok.text


def test_a_worker_token_still_reports_its_run(env):
    tok = auth.create_access_token({"sub": f"worker:{DEFAULT_RUN}", "scope": "run-ingest",
                                    "run_id": DEFAULT_RUN})
    with env.client() as c:
        r = c.post(f"/runs/{DEFAULT_RUN}/events/ingest", headers={"Authorization": f"Bearer {tok}"},
                   json={"events": [{"type": "log", "data": {"node": "n", "seq": 1}}]})
    assert r.status_code == 200, (r.status_code, r.text)



def test_account_level_routes_still_answer_without_a_header(env):
    """What a signed-up user calls before any workspace is chosen (the console's
    sign-in: who am I, which workspaces are mine) must not depend on owning the
    implicit workspace."""
    with env.client() as c:
        h = env.headers("bob@acme.com")
        got = {p: c.get(p, headers=h).status_code
               for p in ("/workspaces", "/auth/me", "/operators/reports")}
    assert got == {"/workspaces": 200, "/auth/me": 200, "/operators/reports": 200}, got
