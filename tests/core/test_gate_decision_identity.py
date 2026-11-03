"""#2310 — a gate decision records WHO decided from the token, not from the body.

The console's Approver field was free text prefilled ``console``, and both
decision routes recorded whatever the request said. Under auth the recorded
approver is now the authenticated caller (the ``/auth/me`` username); a claimed
``approver`` is still accepted for compatibility but ignored. With auth off
there is no principal, so the claimed name stands (pinned elsewhere too:
``test_resume_fail_closed.py``).
"""
from __future__ import annotations

import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState
from cairn.core.triggers.auth import create_access_token
from cairn.core.triggers.server import build_app
from tests.core.test_trigger_server_dispatch import FakeRunner


@pytest.fixture
def harness(monkeypatch):
    """A server whose pipeline approve and run resume record the decision they
    were handed instead of driving a bridge / runtime."""
    seen: dict = {}
    store = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: store)

    async def fake_advance(self, pipeline_id, step_id, *, approver="", rationale=""):
        seen["pipeline"] = approver
        return types.SimpleNamespace(status="running")

    async def fake_get(self, run_id):
        return types.SimpleNamespace(workspace_id="default", status="awaiting_approval")

    async def fake_resume(self, run_id, decision):
        seen["run"] = decision["approver"]
        return {"run_id": run_id, "resumed": True}

    monkeypatch.setattr(PipelineBridge, "advance_step", fake_advance)
    monkeypatch.setattr(RunDispatcher, "get", fake_get)
    monkeypatch.setattr(RunDispatcher, "resume_run", fake_resume)
    import asyncio
    asyncio.run(store.put(PipelineRecord(pipeline_id="pl_gate", pipeline_name="p", status="paused",
                                         workspace_id="default")))
    return seen


def _decide(c: TestClient, headers: dict) -> None:
    r = c.post("/pipelines/pl_gate/steps/promote-model/approve",
               json={"approver": "someone-else", "rationale": "ok"}, headers=headers)
    assert r.status_code == 200, r.text
    r = c.post("/runs/run-1/resume", json={"decision": "approve", "approver": "someone-else"}, headers=headers)
    assert r.status_code == 200, r.text


def test_auth_on_records_the_token_identity_not_the_body(monkeypatch, harness, caplog):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-the-gate-decision-identity-2310")
    monkeypatch.setenv("OBS_API_USER", "maya")
    monkeypatch.setenv("OBS_API_PASSWORD", "maya-pw")
    monkeypatch.delenv("OBS_API_PASSWORD_HASH", raising=False)
    tok = create_access_token({"sub": "maya"})
    with caplog.at_level("WARNING"), TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        _decide(c, {"Authorization": f"Bearer {tok}"})
    assert harness == {"pipeline": "maya", "run": "maya"}
    # the claimed name is ignored, and saying so is logged
    assert any("someone-else" in r.getMessage() for r in caplog.records)


def test_auth_on_without_a_token_cannot_decide(monkeypatch, harness):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-the-gate-decision-identity-2310")
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post("/pipelines/pl_gate/steps/promote-model/approve", json={"approver": "maya"})
    assert r.status_code == 401
    assert harness == {}


def test_auth_off_keeps_the_claimed_approver(monkeypatch, harness):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        _decide(c, {})
    assert harness == {"pipeline": "someone-else", "run": "someone-else"}


# ─── #2358 — the Slack magic-link door (/triggers/approve) follows the same rule ───


def _slack_runner(seen: dict) -> FakeRunner:
    """A runner whose durable action handler records the ApprovalRecord it resumes with."""
    async def resume(workflow_id, record):
        seen["record"] = record
        return types.SimpleNamespace(status="success")

    runner = FakeRunner()
    runner.action_handler = types.SimpleNamespace(resume=resume)
    return runner


def test_slack_approve_under_auth_records_the_caller_not_the_query(monkeypatch, caplog):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-the-gate-decision-identity-2310")
    monkeypatch.setenv("OBS_API_USER", "maya")
    monkeypatch.setenv("OBS_API_PASSWORD", "maya-pw")
    monkeypatch.delenv("OBS_API_PASSWORD_HASH", raising=False)
    seen: dict = {}
    tok = create_access_token({"sub": "maya"})
    with caplog.at_level("WARNING"), TestClient(build_app(_slack_runner(seen), enable_scheduler=False)) as c:
        r = c.post("/triggers/approve/wf-1", json={"decision": "approve", "approver": "someone-else"},
                   headers={"Authorization": f"Bearer {tok}"})
        assert r.status_code == 200, r.text
        # no token → the bearer guard refuses before any decision is recorded
        assert c.post("/triggers/approve/wf-2", json={"decision": "approve", "approver": "maya"}).status_code == 401
    rec = seen["record"]
    assert (rec.approver, rec.channel, rec.approved) == ("maya", "slack", True)
    assert any("someone-else" in r.getMessage() for r in caplog.records)


def test_slack_approve_auth_off_keeps_the_claimed_name(monkeypatch):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    seen: dict = {}
    with TestClient(build_app(_slack_runner(seen), enable_scheduler=False)) as c:
        assert c.post("/triggers/approve/wf-1", json={"decision": "reject"}).status_code == 200
    assert (seen["record"].approver, seen["record"].channel, seen["record"].approved) == ("slack-user", "slack", False)



# ─── #2388 — POST …/advance releases a parked step but never decides a gate ───


@pytest.fixture
def parked(monkeypatch):
    """A pipeline with a gate (`awaiting_approval`) and a manual step (`awaiting_advance`);
    the bridge records what each release was handed (None = no approver at all)."""
    seen: list = []
    store = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: store)

    async def fake_advance(self, pipeline_id, step_id, *, approver=None, rationale=""):
        seen.append((step_id, approver))
        return types.SimpleNamespace(status="running")

    monkeypatch.setattr(PipelineBridge, "advance_step", fake_advance)
    import asyncio
    asyncio.run(store.put(PipelineRecord(
        pipeline_id="pl_park", pipeline_name="p", status="paused", workspace_id="default",
        step_state={"promote-model": StepState(status="awaiting_approval"),
                    "release": StepState(status="awaiting_advance")})))
    return seen


def test_advance_refuses_a_gate_and_points_at_approve(monkeypatch, parked):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post("/pipelines/pl_park/steps/promote-model/advance")
        assert r.status_code == 409, r.text
        assert "/pipelines/pl_park/steps/promote-model/approve" in r.json()["detail"]
        assert parked == []                                   # the bridge never saw it
        # a step parked for a manual release still advances — with no approver recorded
        assert c.post("/pipelines/pl_park/steps/release/advance").status_code == 200
    assert parked == [("release", None)]


def test_advance_under_auth_without_a_token_is_refused(monkeypatch, parked):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-the-gate-decision-identity-2310")
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        assert c.post("/pipelines/pl_park/steps/release/advance").status_code == 401
    assert parked == []


# ─── POST /approvals/dismiss records the token's identity, like a gate decision ───


@pytest.fixture
def dismissed(monkeypatch):
    seen: dict = {}

    async def fake_dismiss(store, run_ids, *, actor="console", workspace_id=None, now=None):
        seen["actor"] = actor
        return {"expired": list(run_ids), "skipped": []}

    monkeypatch.setattr("cairn.core.approval.reaper.dismiss_runs", fake_dismiss)
    return seen


def test_dismiss_under_auth_records_the_token_identity(monkeypatch, dismissed, caplog):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-the-gate-decision-identity-2310")
    monkeypatch.setenv("OBS_API_USER", "maya")
    monkeypatch.setenv("OBS_API_PASSWORD", "maya-pw")
    monkeypatch.delenv("OBS_API_PASSWORD_HASH", raising=False)
    tok = create_access_token({"sub": "maya"})
    with caplog.at_level("WARNING"), TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post("/approvals/dismiss", json={"run_ids": ["run-1"], "actor": "someone-else"},
                   headers={"Authorization": f"Bearer {tok}"})
        assert r.status_code == 200, r.text
        assert c.post("/approvals/dismiss", json={"run_ids": ["run-1"]}).status_code == 401
    assert dismissed == {"actor": "maya"}
    assert any("someone-else" in x.getMessage() for x in caplog.records)


def test_dismiss_auth_off_keeps_the_claimed_actor(monkeypatch, dismissed):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        assert c.post("/approvals/dismiss", json={"run_ids": ["r"], "actor": "ops"}).status_code == 200
        assert c.post("/approvals/dismiss", json={"run_ids": ["r"]}).status_code == 200
    assert dismissed == {"actor": "console"}
