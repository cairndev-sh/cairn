"""A per-run worker token reaches its own run's reporting routes and nothing else (#120).

``live_channel_env_for`` mints a bearer for a container (``sub=worker:<run>``,
``scope=run-ingest``, ``run_id=<run>``) so it can report ONE run: post its events
and read its control requests, and (pull lane) heartbeat that run's job. The guard
used to check only signature + expiry, so the same token was an operator
credential on every route. Driven through the real app with auth on.
"""
from __future__ import annotations

import asyncio
import itertools
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.operator import report_ingest as ri
from cairn.core.operator.job_store import InMemoryRemoteJobStore, RemoteJobRecord
from cairn.core.operator.jobs import live_channel_env_for
from cairn.core.triggers import auth
from cairn.core.triggers.routers import jobs as jobs_router
from cairn.core.triggers.server import build_app


_SEQ = itertools.count()


class _Runner:
    """Runs stay live for the test, so ingest/control have a running run to talk to."""

    async def run(self, *a, **k):
        await asyncio.sleep(20)
        return {"summary": "ok"}


@pytest.fixture
def job_store(monkeypatch):
    store = InMemoryRemoteJobStore()
    monkeypatch.setattr(jobs_router, "get_remote_job_store", lambda: store)
    return store


@pytest.fixture
def client(monkeypatch, job_store):
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret")
    monkeypatch.setenv("OBS_JOB_CHANNEL", "http")
    monkeypatch.setenv("CAIRN_ENGINE_URL", "http://engine.test")
    ri._HWM.clear()
    app = build_app(_Runner(), enable_scheduler=False, enable_spec_routes=False)
    with TestClient(app) as c:
        yield c


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def operator() -> dict[str, str]:
    return _bearer(auth.create_access_token({"sub": "admin"}))


def _seed_run(client, operator) -> str:
    # a distinct service per call: identical manual triggers coalesce onto one run
    body = {"skill_name": "db-pool", "service": f"svc-{next(_SEQ)}", "wait": False}
    r = client.post("/triggers/manual", json=body, headers=operator)
    assert r.status_code == 200 and not r.json().get("coalesced"), r.text
    return r.json()["run_id"]


def _worker(run_id: str) -> dict[str, str]:
    """The token the control plane hands a container for ``run_id`` — the real minting path."""
    tok = live_channel_env_for(run_id, "train")["CAIRN_ENGINE_TOKEN"]
    return _bearer(tok)


def _batch(seq: int) -> dict:
    return {"events": [{"type": "metrics", "data": {"node": "train", "seq": seq, "step": seq, "series": {"loss": 1.0}}}]}


# ─── the worker's own surface keeps working ───────────────────────────


def test_worker_token_ingests_and_reads_control_for_its_own_run(client, operator):
    run_x = _seed_run(client, operator)
    w = _worker(run_x)
    r = client.post(f"/runs/{run_x}/events/ingest", json=_batch(1), headers=w)
    assert r.status_code == 200 and r.json()["accepted"] == 1, r.text
    r = client.get(f"/runs/{run_x}/control", headers=w)
    assert r.status_code == 200 and r.json()["control"] is None, r.text


def test_worker_token_heartbeats_its_own_runs_job(client, operator, job_store):
    run_x = _seed_run(client, operator)
    asyncio.run(job_store.register(RemoteJobRecord(id=f"{run_x}:train", run_id=run_x, node_name="train")))
    r = client.post(f"/jobs/{run_x}:train/heartbeat", json={"worker_id": "w1"}, headers=_worker(run_x))
    assert r.status_code == 200, r.text


# ─── the defect: the same token worked as an operator credential ──────


def test_worker_token_cannot_touch_another_run(client, operator, job_store):
    run_x, run_y = _seed_run(client, operator), _seed_run(client, operator)
    w = _worker(run_x)
    assert client.post(f"/runs/{run_y}/events/ingest", json=_batch(1), headers=w).status_code == 403
    assert client.get(f"/runs/{run_y}/control", headers=w).status_code == 403
    asyncio.run(job_store.register(RemoteJobRecord(id=f"{run_y}:train", run_id=run_y, node_name="train")))
    assert client.post(f"/jobs/{run_y}:train/heartbeat", json={"worker_id": "w1"}, headers=w).status_code == 403


@pytest.mark.parametrize("method,path_tpl,body", [
    ("POST", "/runs/{run}/control", {"action": "abort"}),     # an operator's request, not the worker's
    ("POST", "/runs/{run}/cancel", None),
    ("GET", "/runs/{run}/events", None),
    ("GET", "/runs", None),
    ("POST", "/triggers/manual", {"skill_name": "db-pool", "wait": False}),
    ("POST", "/jobs/claim", {"worker_id": "w1"}),
    ("POST", "/workspaces/default/runs/{run}/cancel", None),
])
def test_worker_token_is_refused_everywhere_else(client, operator, method, path_tpl, body):
    run_x = _seed_run(client, operator)
    r = client.request(method, path_tpl.format(run=run_x), json=body, headers=_worker(run_x))
    assert r.status_code == 403, (method, path_tpl, r.status_code, r.text)


def test_a_token_whose_scope_and_subject_disagree_is_refused(client, operator):
    run_x, run_y = _seed_run(client, operator), _seed_run(client, operator)
    tok = auth.create_access_token({"sub": f"worker:{run_y}", "scope": "run-ingest", "run_id": run_x})
    r = client.post(f"/runs/{run_x}/events/ingest", json=_batch(1), headers=_bearer(tok))
    assert r.status_code == 403, r.text


# ─── operator credentials are unaffected ──────────────────────────────


def test_operator_token_still_reaches_every_run(client, operator):
    run_y = _seed_run(client, operator)
    assert client.post(f"/runs/{run_y}/events/ingest", json=_batch(1), headers=operator).status_code == 200
    assert client.post(f"/runs/{run_y}/control", json={"action": "abort"}, headers=operator).status_code == 200
    assert client.get("/runs", headers=operator).status_code == 200
