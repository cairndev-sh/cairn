"""The run stream's query-string token is tenant-checked like the header (#205).

The bearer guard accepts ``?access_token=`` on the run SSE stream (#58), because a
browser's ``EventSource`` cannot set an ``Authorization`` header. The workspace
middleware, which decides "may this caller act in this workspace", resolved the
caller from the ``Authorization`` header only — so a credential the guard accepted
from the query string was authenticated but never tenant-checked, and the run
guard (``_run_in_ws_or_404``) only compares the run to the workspace named in the
request. The two checks now judge the same credential: the one the guard accepted.

Driven through the real app (``build_app``) with auth on, two accounts created
through the real signup route, and a finished run seeded in each workspace.
"""
from __future__ import annotations

import asyncio
import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.engine.schema import TriggerEvent
from cairn.core.storage import Run, SqliteRunStore

ACME_WS, GLOBEX_WS = "acme-inc", "globex"
ACME_RUN, GLOBEX_RUN = "run-acme-1", "run-globex-1"


def _seed_runs(path: str) -> None:
    async def _write():
        s = SqliteRunStore(path)
        for run_id, ws in ((ACME_RUN, ACME_WS), (GLOBEX_RUN, GLOBEX_WS)):
            trig = TriggerEvent(source="manual", matched_pattern="p", service="svc")
            await s.create(Run(id=run_id, trigger=trig, fingerprint=run_id, workspace_id=ws))
            await s.append_event(run_id, "running", {"secret": f"{ws}-evidence"})
            await s.append_event(run_id, "done", {})
        s.close()

    asyncio.run(_write())


def _app(tmp_path, monkeypatch, *, auth: bool = True):
    if auth:
        monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    else:
        monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
        monkeypatch.delenv("CAIRN_JWT_SECRET", raising=False)
    monkeypatch.setenv("OBS_SIGNUP", "open")
    monkeypatch.setenv("OBS_ACCOUNT_DB", str(tmp_path / "a.db"))
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "r.db"))
    monkeypatch.setenv("OBS_WORKSPACE_DB", str(tmp_path / "w.db"))
    for k in ("OBS_API_USER", "OBS_API_PASSWORD", "OBS_STORAGE", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE"):
        monkeypatch.delenv(k, raising=False)
    # Both stores are process-wide singletons; patch them, not just the env.
    from cairn.core import account as _acct
    from cairn.core import workspace as _ws
    monkeypatch.setattr(_acct, "_store", _acct.SqliteAccountStore(path=str(tmp_path / "a.db")))
    monkeypatch.setattr(_ws, "_store", _ws.SqliteWorkspaceStore(path=str(tmp_path / "w.db")))
    _seed_runs(str(tmp_path / "r.db"))

    from cairn.core.triggers.server import build_app

    class FakeRunner:
        default_environment = "production"
        skill_engine = types.SimpleNamespace(list_skills=lambda: [])

        async def run(self, *a, **k):
            return {}

    return build_app(FakeRunner(), enable_scheduler=False)


@pytest.fixture()
def two_tenants(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    c = TestClient(_app(tmp_path, monkeypatch))

    def token(company: str, email: str) -> str:
        c.post("/auth/signup", json={"company": company, "email": email, "password": "pw12345"})
        return c.post("/token", data={"username": email, "password": "pw12345"}).json()["access_token"]

    acme, globex = token("Acme Inc", "a@acme.com"), token("Globex", "b@globex.com")
    ws = {w["id"] for w in c.get("/workspaces", headers={"Authorization": f"Bearer {globex}"})
          .json()["workspaces"]}
    assert GLOBEX_WS in ws, ws  # the seeded run's workspace is Globex's
    return c, acme, globex


def _bearer(tok: str) -> dict:
    return {"Authorization": f"Bearer {tok}"}


# ─── the comparison: the header form is refused cross-tenant ───────────


def test_header_token_cannot_stream_another_tenants_run(two_tenants):
    c, acme, _ = two_tenants
    r = c.get(f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream", headers=_bearer(acme))
    assert r.status_code == 404, (r.status_code, r.text)
    assert "globex-evidence" not in r.text


# ─── the defect (#205): the query form was not tenant-checked ──────────


def test_query_token_cannot_stream_another_tenants_run(two_tenants):
    c, acme, _ = two_tenants
    r = c.get(f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream?access_token={acme}")
    assert r.status_code == 404, (r.status_code, r.text)
    assert "globex-evidence" not in r.text


def test_query_token_cross_tenant_matches_the_header_answer(two_tenants):
    """Same refusal, same body: the two spellings are indistinguishable."""
    c, acme, _ = two_tenants
    path = f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream"
    by_header = c.get(path, headers=_bearer(acme))
    by_query = c.get(f"{path}?access_token={acme}")
    assert (by_query.status_code, by_query.json()) == (by_header.status_code, by_header.json())


def test_query_token_with_workspace_header_on_flat_stream_is_refused(two_tenants):
    """The flat route takes the workspace from ``X-Cairn-Workspace``; naming the
    other tenant there must not become the way around the path check."""
    c, acme, _ = two_tenants
    r = c.get(f"/runs/{GLOBEX_RUN}/stream?access_token={acme}",
              headers={"X-Cairn-Workspace": GLOBEX_WS})
    assert r.status_code == 404, (r.status_code, r.text)
    assert "globex-evidence" not in r.text


def test_flat_stream_query_token_cannot_read_another_workspaces_run(two_tenants):
    """No workspace named → the default workspace; a run in another workspace is
    not found there (the run guard), so the flat form is no cross-tenant read."""
    c, acme, _ = two_tenants
    r = c.get(f"/runs/{GLOBEX_RUN}/stream?access_token={acme}")
    assert r.status_code == 404, (r.status_code, r.text)
    assert "globex-evidence" not in r.text


# ─── the legitimate paths keep working ─────────────────────────────────


def test_query_token_streams_its_own_tenants_run(two_tenants):
    c, _, globex = two_tenants
    r = c.get(f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream?access_token={globex}")
    assert r.status_code == 200, (r.status_code, r.text)
    assert "globex-evidence" in r.text and "event: done" in r.text


def test_header_token_streams_its_own_tenants_run(two_tenants):
    c, _, globex = two_tenants
    r = c.get(f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream", headers=_bearer(globex))
    assert r.status_code == 200, (r.status_code, r.text)
    assert "globex-evidence" in r.text


def test_header_wins_over_query_token_and_is_what_is_checked(two_tenants):
    """With both present the guard authenticates the header (the query token is a
    fallback only), so the header identity is the one tenant-checked."""
    c, acme, globex = two_tenants
    path = f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream"
    assert c.get(f"{path}?access_token={globex}", headers=_bearer(acme)).status_code == 404
    assert c.get(f"{path}?access_token={acme}", headers=_bearer(globex)).status_code == 200


# ─── keyless dev is unchanged ──────────────────────────────────────────


def test_keyless_dev_streams_without_a_token(tmp_path, monkeypatch):
    from fastapi.testclient import TestClient

    with TestClient(_app(tmp_path, monkeypatch, auth=False)) as c:
        r = c.get(f"/workspaces/{GLOBEX_WS}/runs/{GLOBEX_RUN}/stream")
        assert r.status_code == 200, (r.status_code, r.text)
        assert "globex-evidence" in r.text
