"""The action-saga approval decides on POST only (#63).

``GET /triggers/approve/{id}`` took the decision from the query string and
defaulted to approve, so any logged, prefetched or replayed URL was an approval.
A decision is now ``POST /triggers/approve/{id}`` with an explicit ``decision`` in
the body (no default, the #628 rule); the GET answers what to do and never
resumes the saga. Driven through the real app.
"""
from __future__ import annotations

import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.triggers.auth import create_access_token
from cairn.core.triggers.server import build_app
from tests.core.test_trigger_server_dispatch import FakeRunner


def _runner(seen: list) -> FakeRunner:
    async def resume(workflow_id, record):
        seen.append((workflow_id, record))
        return types.SimpleNamespace(status="success")

    runner = FakeRunner()
    runner.action_handler = types.SimpleNamespace(resume=resume)
    return runner


@pytest.fixture
def auth_on(monkeypatch):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-saga-approve-63")
    monkeypatch.setenv("OBS_API_USER", "maya")
    monkeypatch.setenv("OBS_API_PASSWORD", "maya-pw")
    monkeypatch.delenv("OBS_API_PASSWORD_HASH", raising=False)
    return {"Authorization": f"Bearer {create_access_token({'sub': 'maya'})}"}


# ─── the defect: a GET decided ────────────────────────────────────────


@pytest.mark.parametrize("query", ["", "?decision=approve", "?decision=approve&approver=maya"])
def test_get_never_decides_under_auth(auth_on, query):
    seen: list = []
    with TestClient(build_app(_runner(seen), enable_scheduler=False)) as c:
        r = c.get(f"/triggers/approve/wf-1{query}", headers=auth_on)
        assert r.status_code == 200, r.text
        assert r.json()["decided"] is False
    assert seen == []


def test_get_never_decides_keyless(monkeypatch):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    seen: list = []
    with TestClient(build_app(_runner(seen), enable_scheduler=False)) as c:
        assert c.get("/triggers/approve/wf-1?decision=approve").status_code == 200
    assert seen == []


# ─── the decision path: POST with an explicit decision ────────────────


def test_post_approve_resumes_with_the_caller_as_approver(auth_on):
    seen: list = []
    with TestClient(build_app(_runner(seen), enable_scheduler=False)) as c:
        r = c.post("/triggers/approve/wf-1", json={"decision": "approve", "approver": "someone-else"},
                   headers=auth_on)
        assert r.status_code == 200 and r.json()["approved"] is True, r.text
    (wf, rec), = seen
    assert (wf, rec.approved, rec.approver, rec.channel) == ("wf-1", True, "maya", "slack")


def test_post_reject_is_honoured(auth_on):
    seen: list = []
    with TestClient(build_app(_runner(seen), enable_scheduler=False)) as c:
        r = c.post("/triggers/approve/wf-1", json={"decision": "reject"}, headers=auth_on)
        assert r.status_code == 200 and r.json()["approved"] is False, r.text
    assert seen[0][1].approved is False


@pytest.mark.parametrize("body", [{}, {"decision": ""}, {"decision": "maybe"}])
def test_post_without_a_clear_decision_is_refused(auth_on, body):
    seen: list = []
    with TestClient(build_app(_runner(seen), enable_scheduler=False)) as c:
        assert c.post("/triggers/approve/wf-1", json=body, headers=auth_on).status_code == 422
    assert seen == []


def test_post_without_a_token_is_401(auth_on):
    seen: list = []
    with TestClient(build_app(_runner(seen), enable_scheduler=False)) as c:
        assert c.post("/triggers/approve/wf-1", json={"decision": "approve"}).status_code == 401
    assert seen == []
