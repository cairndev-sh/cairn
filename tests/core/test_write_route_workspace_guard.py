"""Every write route asserts the caller may write in the workspace it acts in (#221).

``_WorkspaceMiddleware`` is the workspace boundary: it authorizes the named and
the implicit workspace before any route runs. Until #221 only three routes
(``POST /start``, inbox replay, the variable writes) re-asserted it where the
write happens; every other write trusted ``current_workspace()``. So any gap in
the middleware (#220 was one) silently became a write path into another
tenant's workspace.

Now every route that writes a workspace-scoped record or starts work carries
ONE dependency, ``console_support.writable_workspace`` (which is
``assert_can_write_workspace``: 403 for a caller whose account does not own the
workspace) — or, on the one route a per-run worker token also calls (run event
ingest), its variant ``writable_workspace_unless_worker``, which leaves the
worker token to the guard's run scoping (#120) and checks every other caller
the same way. The middleware stays the primary check.

Driven through the real app (``build_app``) with auth on and real account,
workspace, run and pipeline stores. The middleware gap is simulated by making
the middleware's own authorization a no-op (``server.assert_workspace_allowed``,
the name only the middleware resolves), so the request reaches the route acting
in a workspace the caller's account does not own, and only the route-level
dependency stands between it and the write.

A structural test fails any new write route that carries neither the
dependency nor a stated exemption (with its reason) in ``EXEMPT`` below.
"""
from __future__ import annotations

import asyncio
import types

import pytest
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from cairn.core import account as A
from cairn.core import workflow_config_store as WCS
from cairn.core import workspace as W
from cairn.core import workspace_variables as WV
from cairn.core.engine.schema import TriggerEvent
from cairn.core.storage import Run, SqliteRunStore
from cairn.core.storage import pipeline_store as P
from cairn.core.triggers import auth
from cairn.core.triggers import console_support as support

VICTIM_WS, INTRUDER_WS = "acme-ops", "initech-ops"
ACME_RUN, ACME_PIPE = "run-acme-1", "pipe-acme-1"
DEFAULT_RUN = "run-default-1"  # a run a default-account worker reports
OWNER, INTRUDER = "bob@acme.com", "carl@initech.com"
EVENTS = {"events": [{"type": "metrics", "data": {"node": "train", "seq": 1, "step": 1,
                                                  "series": {"loss": 1.0}}}]}


class _FakeRunner:
    default_environment = "production"
    skill_engine = types.SimpleNamespace(list_skills=lambda: [])

    async def run(self, *a, **k):
        return {}


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE",
              "CAIRN_API_USER", "CAIRN_API_PASSWORD", "CAIRN_API_PASSWORD_HASH",
              "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH",
              "OBS_JWT_SECRET", "CAIRN_TRIGGER_DURABLE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("CAIRN_SQLITE_DB", str(tmp_path / "cairn.db"))
    monkeypatch.setenv("CAIRN_SCHEDULES_DB", ":memory:")
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))

    async def _seed_runs():
        s = SqliteRunStore(str(tmp_path / "r.db"))
        trig = TriggerEvent(source="manual", matched_pattern="p", service="svc")
        await s.create(Run(id=ACME_RUN, trigger=trig, fingerprint=ACME_RUN, workspace_id=VICTIM_WS))
        await s.create(Run(id=DEFAULT_RUN, trigger=trig, fingerprint=DEFAULT_RUN, workspace_id="default"))
        s.close()
    asyncio.run(_seed_runs())

    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)
    pipes = P.InMemoryPipelineStore()
    monkeypatch.setattr(P, "_default_store", pipes)
    monkeypatch.setattr(WV, "_default_store", WV.InMemoryWorkspaceVariableStore())
    monkeypatch.setattr(WCS, "_default_store", WCS.InMemoryWorkflowConfigStore())

    # The registry routes compile every discovered template; their stores are not
    # what is under test, so they answer from stubs.
    import cairn.core.workflow_registry as WR

    async def _sync(**_k):
        return 0

    async def _toggle(_ref, **_k):
        return False
    monkeypatch.setattr(WR, "sync_discovered_workflows", _sync)
    monkeypatch.setattr(WR, "enable_workflow", _toggle)
    monkeypatch.setattr(WR, "disable_workflow", _toggle)

    async def _seed() -> dict[str, tuple[str, str]]:
        await pipes.put(P.PipelineRecord(pipeline_id=ACME_PIPE, pipeline_name="p",
                                         workspace_id=VICTIM_WS))
        ids = {}
        for acct, ws, user in (("acme", VICTIM_WS, OWNER), ("initech", INTRUDER_WS, INTRUDER)):
            await accounts.put_account(A.Account(id=acct, name=acct.title()))
            await workspaces.put(W.Workspace(id=ws, name=ws, account_id=acct))
            u = await A.create_user(user, f"{user}-password", store=accounts)
            # admins, so a route's own require_admin is never what answers
            await A.add_member(acct, u.id, "admin", store=accounts)
            ids[user] = (u.id, acct)
        return ids
    users = asyncio.run(_seed())

    def headers(who: str, ws: str) -> dict[str, str]:
        uid, acct = users[who]
        tok = auth.create_access_token({"sub": uid, "account_id": acct})
        return {"Authorization": f"Bearer {tok}", "X-Cairn-Workspace": ws}

    from cairn.core.triggers.server import build_app
    return types.SimpleNamespace(
        client=lambda: TestClient(build_app(_FakeRunner(), enable_scheduler=False)),
        headers=headers)


@pytest.fixture
def middleware_gap(monkeypatch):
    """The middleware stops authorizing the workspace a request names: the shape
    of #220. Only ``_WorkspaceMiddleware`` (and the spec-declared receivers,
    not exercised here) resolve ``assert_workspace_allowed`` through
    ``cairn.core.triggers.server``; every route-level check imports it from
    ``cairn.core.workspace`` and is untouched."""
    import cairn.core.triggers.server as server

    async def _allow(*_a, **_k):
        return None
    monkeypatch.setattr(server, "assert_workspace_allowed", _allow)


# (method, path, request kwargs, the owner's status — the route's own answer)
GUARDED = [
    ("POST", "/start", {"json": {"ref": "nope/missing"}}, 404),
    ("POST", "/pipelines/launch", {"json": {"ref": "pipeline:nope/missing"}}, 404),
    ("POST", "/pipelines/presets", {"json": {"ref": "pipeline:nope/missing", "name": "p1"}}, 404),
    ("DELETE", "/pipelines/presets", {"params": {"ref": "pipeline:nope/missing", "name": "p1"}}, 200),
    ("POST", f"/pipelines/{ACME_PIPE}/steps/s1/advance", {}, 409),
    ("POST", f"/pipelines/{ACME_PIPE}/steps/s1/approve", {"json": {}}, 409),
    ("POST", f"/pipelines/{ACME_PIPE}/steps/s1/reject", {"json": {"reason": "no"}}, 409),
    ("POST", f"/pipelines/{ACME_PIPE}/cancel", {}, 200),
    ("POST", "/triggers/scheduled", {"json": {}}, 400),
    ("PATCH", "/triggers/scheduled/sched-x", {"json": {"enabled": False}}, 404),
    ("DELETE", "/triggers/scheduled/sched-x", {}, 404),
    ("PATCH", "/triggers/stored/trig-x", {"json": {"enabled": True}}, 404),  # ADR-0160
    ("POST", "/registry/sync-workflows", {}, 200),
    ("POST", "/registry/enable", {"json": {"ref": "x/y"}}, 404),
    ("POST", "/registry/disable", {"json": {"ref": "x/y"}}, 404),
    ("POST", "/packs/x/y/run", {"json": {}}, 400),
    ("POST", f"/runs/{ACME_RUN}/resume", {"json": {"decision": "reject"}}, 409),
    ("POST", f"/runs/{ACME_RUN}/control", {"json": {"action": "abort"}}, 200),
    ("POST", f"/runs/{ACME_RUN}/events/ingest", {"json": EVENTS}, 200),
    ("POST", f"/runs/{ACME_RUN}/cancel", {}, 200),
    ("POST", f"/runs/{ACME_RUN}/expire", {}, 409),
    ("POST", "/approvals/dismiss", {"json": {"run_ids": [ACME_RUN]}}, 200),
    ("POST", "/triggers/alert", {"json": {}}, 422),
    ("POST", "/triggers/pattern", {"json": {}}, 422),
    ("POST", "/triggers/manual", {"json": {"payload": "not-a-dict"}}, 422),
    ("POST", "/triggers/approve/wf-1", {"json": {"decision": "reject"}}, 501),
    ("POST", "/triggers/inbox/replay", {"params": {"since": "2026-01-01T00:00:00Z"}}, 409),
    ("PUT", "/variables/k1", {"json": {"value": "v"}}, 200),
    ("DELETE", "/variables/k1", {}, 404),
]
_IDS = [f"{m} {p}" for m, p, _kw, _st in GUARDED]


@pytest.mark.parametrize(("method", "path", "kw", "_owner"), GUARDED, ids=_IDS)
def test_a_caller_acting_in_another_accounts_workspace_is_refused_at_the_route(
        env, middleware_gap, method, path, kw, _owner):
    with env.client() as c:
        r = c.request(method, path, headers=env.headers(INTRUDER, VICTIM_WS), **kw)
    assert r.status_code == 403, (r.status_code, r.text)
    assert r.json()["detail"] == f"not a member of workspace {VICTIM_WS!r}"


@pytest.mark.parametrize(("method", "path", "kw", "owner_status"), GUARDED, ids=_IDS)
def test_the_owner_gets_the_routes_own_answer(env, method, path, kw, owner_status):
    with env.client() as c:
        r = c.request(method, path, headers=env.headers(OWNER, VICTIM_WS), **kw)
    assert r.status_code == owner_status, (r.status_code, r.text)
    assert "not a member of workspace" not in r.text
    assert r.text != '{"detail":"workspace not found"}'


def test_a_non_member_is_refused_before_the_body_is_validated(env, middleware_gap):
    """The check is a dependency, so it answers before body validation: a
    non-member sending a bad body gets 403, not 422 (on main ``POST /start``
    checked inside the handler, after the body)."""
    with env.client() as c:
        intruder = c.post("/start", json={}, headers=env.headers(INTRUDER, VICTIM_WS))
        owner = c.post("/start", json={}, headers=env.headers(OWNER, VICTIM_WS))
    assert intruder.status_code == 403, (intruder.status_code, intruder.text)
    assert owner.status_code == 422, (owner.status_code, owner.text)


def test_a_worker_token_still_ingests_its_own_run(env, middleware_gap):
    """A per-run worker token belongs to no account's membership; the guard
    scopes it to its own run (#120), and the ingest route leaves it there."""
    tok = auth.create_access_token({"sub": f"worker:{DEFAULT_RUN}", "scope": "run-ingest",
                                    "run_id": DEFAULT_RUN})
    with env.client() as c:
        r = c.post(f"/runs/{DEFAULT_RUN}/events/ingest", json=EVENTS,
                   headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 200 and r.json()["accepted"] == 1, (r.status_code, r.text)


def test_without_a_gap_the_middleware_still_answers_first(env):
    """The dependency re-asserts; it does not replace. A named workspace the
    caller's account does not own is still the middleware's 404 (ADR-0051 §4:
    cross-tenant existence never leaks), before any route runs."""
    with env.client() as c:
        r = c.post(f"/runs/{ACME_RUN}/cancel", headers=env.headers(INTRUDER, VICTIM_WS))
    assert r.status_code == 404, (r.status_code, r.text)
    assert r.json() == {"detail": "workspace not found"}


def test_auth_off_a_keyless_workspace_write_is_unchanged(monkeypatch, tmp_path):
    """Keyless dev: the caller is the default account, which owns every
    workspace a keyless deployment creates, so a write there goes through."""
    for k in ("CAIRN_JWT_SECRET", "OBS_JWT_SECRET", "CAIRN_REQUIRE_AUTH", "OBS_REQUIRE_AUTH",
              "CAIRN_WORKSPACE", "OBS_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(W, "_store", W.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    monkeypatch.setattr(WV, "_default_store", WV.InMemoryWorkspaceVariableStore())
    with TestClient(_app()) as c:
        assert c.post("/workspaces", json={"name": "Team"}).status_code == 200
        r = c.put("/variables/k1", json={"value": "v"}, headers={"X-Cairn-Workspace": "team"})
    assert r.status_code == 200, (r.status_code, r.text)
    assert r.json()["workspace_id"] == "team"


def test_auth_off_another_accounts_workspace_is_refused(monkeypatch, tmp_path):
    """Keyless dev: the caller is the default account. A workspace another
    account owns (one created while auth was on) refuses the write with 403, as
    ``POST /start`` already did (owner decision on #221: one rule)."""
    for k in ("CAIRN_JWT_SECRET", "OBS_JWT_SECRET", "CAIRN_REQUIRE_AUTH", "OBS_REQUIRE_AUTH",
              "CAIRN_WORKSPACE", "OBS_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE"):
        monkeypatch.delenv(k, raising=False)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)
    monkeypatch.setattr(WV, "_default_store", WV.InMemoryWorkspaceVariableStore())
    asyncio.run(workspaces.put(W.Workspace(id=VICTIM_WS, name=VICTIM_WS, account_id="acme")))
    with TestClient(_app()) as c:
        var = c.put("/variables/k1", json={"value": "v"}, headers={"X-Cairn-Workspace": VICTIM_WS})
        dismiss = c.post("/approvals/dismiss", json={"run_ids": []},
                         headers={"X-Cairn-Workspace": VICTIM_WS})
    for r in (var, dismiss):
        assert r.status_code == 403, (r.status_code, r.text)
        assert r.json()["detail"] == f"not a member of workspace {VICTIM_WS!r}"


# ── structural: no write route without the guard or a stated exemption ──────

_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

#: (method, path) → why this write route does not assert the workspace.
EXEMPT: dict[tuple[str, str], str] = {
    ("POST", "/token"): "public: the login itself; no workspace yet",
    ("POST", "/auth/refresh"):
        "public: renews a sign-in from its refresh cookie; acts in no workspace (ADR-0142)",
    ("POST", "/auth/logout"):
        "public: revokes the refresh cookie's sign-in; acts in no workspace (ADR-0142)",
    ("POST", "/auth/signup"): "public: creates an account, acts in no workspace",
    ("POST", "/auth/accept-invite"): "public: the invite token is the credential; joins an account",
    ("POST", "/workspaces"): "account-level: creates a workspace for the caller's account",
    ("DELETE", "/workspaces/{workspace_id}"):
        "account-level: archives one; needs an admin of the owning account (#157)",
    ("PATCH", "/workspaces/{workspace_id}"):
        "account-level: archives/restores one; needs an admin of the owning account (#157)",
    # The pull lane is not a workspace route: these routes act in no workspace and
    # `claim_next` takes any account's queued job, so there is no workspace to
    # assert. Who may call them is a credential rule instead (an internal issue;
    # tests/core/test_jobs_claim_worker_credential.py): only a worker credential
    # (the deployment admin or a worker-pool token) claims or heartbeats; a
    # run-scoped token heartbeats only its own run's job; a user's token gets 403.
    ("POST", "/jobs/{job_id}/heartbeat"):
        "pull lane, no workspace: a worker credential, or a run-scoped token for its own job",
    ("POST", "/jobs/claim"):
        "pull lane, no workspace: only a worker credential (deployment admin or worker-pool token)",
    ("POST", "/registry/install"):
        "deployment-level, the deployment admin only (an internal issue); refused by posture (501, #1545 D2)",
    ("POST", "/registry/uninstall"):
        "deployment-level, the deployment admin only (an internal issue): a pack is process-wide",
    ("POST", "/pipelines/preview"): "side-effect free: nothing is submitted or recorded",
    ("POST", "/pipelines/preview-config"): "side-effect free: composes the config surface",
    ("POST", "/triggers/scheduled/preview"): "side-effect free: computes the next fire times",
    ("POST", "/packs/{pack}/{template}/validate"): "side-effect free: validates form values",
    ("POST", "/packs/{pack}/{template}/preflight"):
        "side-effect free: the dry-run verdict (ADR-0072); no run row, no spend",
}


def _calls(dependant) -> set:
    out = set()
    for d in dependant.dependencies:
        out.add(d.call)
        out |= _calls(d)
    return out


def _guarded(route: APIRoute) -> bool:
    guards = (support.writable_workspace, support.writable_workspace_unless_worker)
    return any(g in _calls(route.dependant) for g in guards)


def _app():
    from cairn.core.triggers.server import build_app
    return build_app(_FakeRunner(), enable_scheduler=False)


#: The handler ``server._mount_trigger_routes`` mounts for each spec-declared
#: webhook receiver, matched by where it is defined rather than by route name.
_SPEC_RECEIVER = ("cairn.core.triggers.server", "_mount_trigger_routes.<locals>.handler")


def _core_write_routes(app) -> list[tuple[str, str, APIRoute]]:
    """Write routes the public core declares. A spec-declared trigger receiver
    (:data:`_SPEC_RECEIVER`) is not one: it binds the run to the workspace that
    declared it and asserts the caller there itself (#209). Routes a
    ``cairn.routers`` provider mounts belong to that provider, which audits
    them the same way."""
    out = []
    for r in app.routes:
        if not isinstance(r, APIRoute) or not r.endpoint.__module__.startswith("cairn.core."):
            continue
        if (r.endpoint.__module__, r.endpoint.__qualname__) == _SPEC_RECEIVER:
            continue
        for m in sorted(r.methods & _WRITE_METHODS):
            out.append((m, r.path, r))
    return out


def test_every_write_route_asserts_the_workspace_or_says_why_not():
    missing = [f"{m} {p}" for m, p, r in _core_write_routes(_app())
               if (m, p) not in EXEMPT and not _guarded(r)]
    assert not missing, (
        "write routes without Depends(console_support.writable_workspace[_unless_worker]) and no "
        f"exemption in EXEMPT: {missing}")


def test_every_guarded_route_is_exercised_by_the_behaviour_tests():
    guarded = {(m, p) for m, p, r in _core_write_routes(_app())
               if _guarded(r)}
    exercised = set()
    for m, p, _kw, _st in GUARDED:
        exercised |= {(gm, gp) for gm, gp in guarded if gm == m and _same_route(gp, p)}
    assert guarded == exercised, sorted(guarded - exercised)


def test_every_exemption_names_a_live_route_and_none_is_also_guarded():
    routes = {(m, p): r for m, p, r in _core_write_routes(_app())}
    stale = sorted(k for k in EXEMPT if k not in routes)
    assert not stale, f"EXEMPT names routes the app no longer has: {stale}"
    both = sorted(k for k in EXEMPT if _guarded(routes[k]))
    assert not both, f"exempted yet guarded (drop the exemption): {both}"


def _same_route(template: str, concrete: str) -> bool:
    a, b = template.strip("/").split("/"), concrete.strip("/").split("/")
    return len(a) == len(b) and all(x == y or (x.startswith("{") and x.endswith("}"))
                                    for x, y in zip(a, b))


def test_the_spec_receiver_exclusion_matches_the_mounted_handler(vendor_trigger_specs):
    """:data:`_SPEC_RECEIVER` names the handler ``_mount_trigger_routes`` really
    mounts, so the exclusion cannot silently stop matching (or match by name)."""
    from fastapi import FastAPI

    from cairn.core.triggers.server import _mount_trigger_routes

    app = FastAPI()
    fake = types.SimpleNamespace(specs=list(vendor_trigger_specs.values()),
                                 workspace_for=lambda _s: "default")
    _mount_trigger_routes(app, fake)
    mounted = [r for r in app.routes if isinstance(r, APIRoute)]
    assert mounted and all((r.endpoint.__module__, r.endpoint.__qualname__) == _SPEC_RECEIVER
                           for r in mounted)
