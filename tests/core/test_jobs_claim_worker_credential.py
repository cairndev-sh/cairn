"""Only a worker credential claims pull-lane work (cairn-platform#224).

``POST /jobs/claim`` hands out ANY queued job with its input, plus a run-scoped
token for the job's run. It refused only per-run worker tokens, so every other
signed-in caller counted as an operator: a self-serve sign-up of any account
could take another account's job, read its input and report into its run.

Now a claim (and a heartbeat that is not a run-scoped token's own job) needs a
worker credential: the deployment's admin (``CAIRN_API_USER``) or a worker-pool
token (``scope=worker-pool``) minted with ``cairn remote worker-token``. A user's
token gets 403 whatever its role. A worker-pool token reaches the pull lane and
nothing else. Driven through the real app with auth on and real stores.
"""
from __future__ import annotations

import asyncio
import types

import jwt
import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from cairn.core import account as A
from cairn.core import workspace as W
from cairn.core.engine.schema import TriggerEvent
from cairn.core.operator.job_store import PULL_BACKEND, RemoteJobRecord, SqliteRemoteJobStore
from cairn.core.storage import Run, SqliteRunStore
from cairn.core.triggers import auth
from cairn.core.triggers.routers import jobs as jobs_router

SECRET = "test-secret-long-enough-for-hs256-224"
VICTIM_WS, INTRUDER_WS = "acme-ops", "initech-ops"
ACME_RUN = "run-acme-224"
JOB = f"{ACME_RUN}:train"
OWNER, INTRUDER = "bob@acme.com", "carl@initech.com"
ENV_ADMIN, ENV_ADMIN_PW = "root", "root-password-224"
CLAIM = {"worker_id": "w1", "capabilities": {}}


class _FakeRunner:
    default_environment = "production"
    skill_engine = types.SimpleNamespace(list_skills=lambda: [])

    async def run(self, *a, **k):
        return {}


@pytest.fixture
def env(monkeypatch, tmp_path):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "OBS_STORAGE", "CAIRN_STORAGE",
              "CAIRN_API_PASSWORD_HASH", "OBS_API_USER", "OBS_API_PASSWORD",
              "OBS_API_PASSWORD_HASH", "OBS_JWT_SECRET", "OBS_JOB_CHANNEL", "CAIRN_JOB_CHANNEL"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_JWT_SECRET", SECRET)
    monkeypatch.setenv("CAIRN_API_USER", ENV_ADMIN)
    monkeypatch.setenv("CAIRN_API_PASSWORD", ENV_ADMIN_PW)
    # set, so a claim hands back a run-scoped token for the job's run: the prize
    monkeypatch.setenv("CAIRN_ENGINE_URL", "http://engine.test")
    monkeypatch.setenv("CAIRN_SQLITE_DB", str(tmp_path / "cairn.db"))
    monkeypatch.setenv("CAIRN_SCHEDULES_DB", ":memory:")
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))

    async def _seed_run():
        s = SqliteRunStore(str(tmp_path / "r.db"))
        trig = TriggerEvent(source="manual", matched_pattern="p", service="svc")
        await s.create(Run(id=ACME_RUN, trigger=trig, fingerprint=ACME_RUN, workspace_id=VICTIM_WS))
        s.close()
    asyncio.run(_seed_run())

    jobs = SqliteRemoteJobStore(path=str(tmp_path / "jobs.db"))
    monkeypatch.setattr(jobs_router, "get_remote_job_store", lambda: jobs)
    asyncio.run(jobs.register(RemoteJobRecord(
        id=JOB, run_id=ACME_RUN, node_name="train", workspace_id=VICTIM_WS,
        backend=PULL_BACKEND, provider="train/stub", input={"dataset": "acme-private-corpus"})))

    accounts = A.SqliteAccountStore(path=str(tmp_path / "accounts.db"))
    monkeypatch.setattr(A, "_store", accounts)
    workspaces = W.SqliteWorkspaceStore(path=str(tmp_path / "w.db"))
    monkeypatch.setattr(W, "_store", workspaces)

    async def _seed() -> dict[str, tuple[str, str]]:
        ids = {}
        for acct, ws, user in (("acme", VICTIM_WS, OWNER), ("initech", INTRUDER_WS, INTRUDER)):
            await accounts.put_account(A.Account(id=acct, name=acct.title()))
            await workspaces.put(W.Workspace(id=ws, name=ws, account_id=acct))
            u = await A.create_user(user, f"{user}-password", store=accounts)
            # owners, so no route's own role check is what answers
            await A.add_member(acct, u.id, "owner", store=accounts)
            ids[user] = (u.id, acct)
        return ids
    users = asyncio.run(_seed())

    def user(who: str) -> dict[str, str]:
        uid, acct = users[who]
        return _bearer(auth.create_access_token({"sub": uid, "account_id": acct}))

    from cairn.core.triggers.server import build_app
    return types.SimpleNamespace(
        client=lambda: TestClient(build_app(_FakeRunner(), enable_scheduler=False)),
        user=user, jobs=jobs)


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _env_admin() -> dict[str, str]:
    return _bearer(auth.create_access_token({"sub": ENV_ADMIN, "account_id": "default"}))


def _pool(name: str = "gpu-a") -> dict[str, str]:
    """The token ``cairn remote worker-token`` prints — the real minting path."""
    return _bearer(auth.mint_worker_pool_token(name))


def _run_scoped(run_id: str) -> dict[str, str]:
    from cairn.core.operator.jobs import live_channel_env_for  # the token a claim hands out
    return _bearer(live_channel_env_for(run_id, "train")["CAIRN_ENGINE_TOKEN"])


def _unclaimed(env) -> bool:
    rec = asyncio.run(env.jobs.get(JOB))
    return rec is not None and not rec.claimed_by and not rec.worker_status


# ─── the defect: any signed-in user claimed any account's job ─────────────


@pytest.mark.parametrize("who", [INTRUDER, OWNER])
def test_a_users_token_cannot_claim(env, who):
    """A self-serve user of another account (the defect), and even an owner of
    the job's own account: claiming is a worker's act, not a user's."""
    with env.client() as c:
        r = c.post("/jobs/claim", json=CLAIM, headers=env.user(who))
    assert r.status_code == 403, (r.status_code, r.text)
    assert "acme-private-corpus" not in r.text and "CAIRN_ENGINE_TOKEN" not in r.text
    assert _unclaimed(env)


@pytest.mark.parametrize("who", [INTRUDER, OWNER])
def test_a_users_token_cannot_heartbeat(env, who):
    with env.client() as c:
        r = c.post(f"/jobs/{JOB}/heartbeat", headers=env.user(who),
                   json={"worker_id": "w1", "status": "succeeded", "output": {"x": 1}})
    assert r.status_code == 403, (r.status_code, r.text)
    assert _unclaimed(env)


# ─── worker credentials still claim ──────────────────────────────────────


@pytest.mark.parametrize("cred", ["env_admin", "pool"])
def test_a_worker_credential_claims_and_heartbeats(env, cred):
    headers = _env_admin() if cred == "env_admin" else _pool()
    with env.client() as c:
        r = c.post("/jobs/claim", json=CLAIM, headers=headers)
        assert r.status_code == 200, (r.status_code, r.text)
        body = r.json()
        assert body["id"] == JOB and body["input"] == {"dataset": "acme-private-corpus"}
        assert body["env"]["CAIRN_ENGINE_TOKEN"]
        hb = c.post(f"/jobs/{JOB}/heartbeat", json={"worker_id": "w1"}, headers=headers)
    assert hb.status_code == 200 and hb.json()["accepted"] is True, (hb.status_code, hb.text)


def test_an_empty_queue_is_still_204_for_a_worker(env):
    with env.client() as c:
        assert c.post("/jobs/claim", json=CLAIM, headers=_pool()).status_code == 200
        assert c.post("/jobs/claim", json=CLAIM, headers=_pool("gpu-b")).status_code == 204


def test_a_run_scoped_token_still_heartbeats_its_own_job_and_still_cannot_claim(env):
    with env.client() as c:
        claim = c.post("/jobs/claim", json=CLAIM, headers=_run_scoped(ACME_RUN))
        assert claim.status_code == 403, (claim.status_code, claim.text)
        assert c.post("/jobs/claim", json=CLAIM, headers=_pool()).status_code == 200
        hb = c.post(f"/jobs/{JOB}/heartbeat", json={"worker_id": "w1"}, headers=_run_scoped(ACME_RUN))
    assert hb.status_code == 200 and hb.json()["accepted"] is True, (hb.status_code, hb.text)


# ─── a worker-pool token is the pull lane and nothing else ───────────────


@pytest.mark.parametrize("method,path,body", [
    ("GET", "/runs", None),
    ("GET", f"/runs/{ACME_RUN}", None),
    ("POST", f"/runs/{ACME_RUN}/events/ingest", {"events": []}),
    ("GET", f"/runs/{ACME_RUN}/control", None),
    ("POST", "/triggers/manual", {"skill_name": "x", "wait": False}),
    ("GET", "/auth/me", None),
    ("GET", f"/workspaces/{VICTIM_WS}/runs", None),
])
def test_a_worker_pool_token_is_refused_everywhere_else(env, method, path, body):
    with env.client() as c:
        r = c.request(method, path, json=body, headers=_pool())
    assert r.status_code == 403, (method, path, r.status_code, r.text)


def test_a_worker_pool_token_carrying_a_run_scope_is_run_scoped(env):
    """A token naming both scopes is held to the narrower one: its run."""
    tok = auth.create_access_token({"sub": f"worker:{ACME_RUN}", "scope": "run-ingest",
                                    "run_id": ACME_RUN})
    bad = auth.create_access_token({"sub": f"worker:{ACME_RUN}", "scope": auth.WORKER_POOL_SCOPE})
    with env.client() as c:
        assert c.post("/jobs/claim", json=CLAIM, headers=_bearer(tok)).status_code == 403
        assert c.post("/jobs/claim", json=CLAIM, headers=_bearer(bad)).status_code == 403
    assert _unclaimed(env)


# ─── minting: `cairn remote worker-token` ────────────────────────────────


def test_the_cli_mints_a_worker_pool_token(env):
    from cairn.cli.commands.remote import app
    out = CliRunner().invoke(app, ["worker-token", "--name", "gpu-a", "--days", "2"])
    assert out.exit_code == 0, out.output
    claims = jwt.decode(out.output.strip(), SECRET, algorithms=["HS256"])
    assert claims["scope"] == "worker-pool" and claims["sub"] == "worker-pool:gpu-a"
    assert "account_id" not in claims
    with env.client() as c:
        r = c.post("/jobs/claim", json=CLAIM, headers=_bearer(out.output.strip()))
    assert r.status_code == 200, (r.status_code, r.text)


def test_the_cli_refuses_without_a_signing_secret(monkeypatch):
    for k in ("CAIRN_JWT_SECRET", "OBS_JWT_SECRET"):
        monkeypatch.delenv(k, raising=False)
    from cairn.cli.commands.remote import app
    out = CliRunner().invoke(app, ["worker-token", "--name", "gpu-a"])
    assert out.exit_code != 0
    assert "CAIRN_JWT_SECRET" in out.output


# ─── auth off is unchanged ───────────────────────────────────────────────


def test_auth_off_a_keyless_worker_still_claims(monkeypatch, tmp_path):
    for k in ("CAIRN_JWT_SECRET", "OBS_JWT_SECRET", "CAIRN_REQUIRE_AUTH", "OBS_REQUIRE_AUTH",
              "OBS_STORAGE", "CAIRN_STORAGE", "CAIRN_WORKSPACE", "OBS_WORKSPACE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_SCHEDULES_DB", ":memory:")
    monkeypatch.setenv("CAIRN_RUN_DB", str(tmp_path / "r.db"))
    jobs = SqliteRemoteJobStore(path=str(tmp_path / "jobs.db"))
    monkeypatch.setattr(jobs_router, "get_remote_job_store", lambda: jobs)
    asyncio.run(jobs.register(RemoteJobRecord(id=JOB, run_id=ACME_RUN, node_name="train",
                                              backend=PULL_BACKEND)))
    from cairn.core.triggers.server import build_app
    with TestClient(build_app(_FakeRunner(), enable_scheduler=False)) as c:
        r = c.post("/jobs/claim", json=CLAIM)
        hb = c.post(f"/jobs/{JOB}/heartbeat", json={"worker_id": "w1"})
    assert r.status_code == 200 and r.json()["id"] == JOB, (r.status_code, r.text)
    assert hb.status_code == 200 and hb.json()["accepted"] is True, (hb.status_code, hb.text)


@pytest.mark.parametrize("days", [0, 366, 10**9])
def test_a_worker_pool_token_lifetime_is_bounded(env, days):
    """It cannot be revoked short of rotating the signing secret, so it must expire."""
    with pytest.raises(ValueError, match="1 to 365 days"):
        auth.mint_worker_pool_token("gpu-a", days)
