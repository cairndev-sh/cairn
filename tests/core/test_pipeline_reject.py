"""ADR-0156 D4/D5 — an attributable reject for a pipeline gate.

``POST /pipelines/{id}/steps/{step}/reject``: approve's guards in approve's order, the decider
from the token, a required reason; the pipeline ends ``failed`` with the step's verdict
``rejected``, finalizers run, the review is decided ``rejected``, ``pipeline.rejected`` is
published. A step that is not at ``awaiting_approval`` answers 409.
"""
from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState
from cairn.core.storage.reviews import ReviewRecord, build_review_store, pipeline_review_id
from cairn.core.triggers.auth import create_access_token
from cairn.core.triggers.server import build_app
from tests.core.test_trigger_server_dispatch import FakeRunner

_SECRET = "test-secret-for-the-pipeline-gate-reject-adr-0156"


class _CaptureBus:
    def __init__(self):
        self.events: list[tuple[str, dict]] = []

    async def publish(self, topic, payload, **kw):
        self.events.append((topic, payload))

    async def subscribe(self, _t, _h):
        return SimpleNamespace(unsubscribe=lambda: None)

    async def replay(self, _t, _s):
        return
        yield  # pragma: no cover


def _plan(*, finalizer: bool = False, approvers: str | None = None) -> PipelinePlan:
    steps = [
        PipelinePlanStep(id="prep", ref="task:x/prep"),
        PipelinePlanStep(id="review", ref="gate:review", depends_on=["prep"], approval=True,
                         approvers=approvers),
    ]
    if finalizer:
        steps.append(PipelinePlanStep(id="cleanup", ref="task:x/cleanup", finalizer=True))
    return PipelinePlan(name="pl", steps=steps)


def _record(plan: PipelinePlan, *, gate_status: str = "awaiting_approval", **kw) -> PipelineRecord:
    state = {"prep": StepState(status="done"), "review": StepState(status=gate_status)}
    if any(s.finalizer for s in plan.steps):
        state["cleanup"] = StepState(status="pending")
    return PipelineRecord(pipeline_id="pl_gate", pipeline_name="pl", status="paused", plan=plan,
                          inputs={}, step_state=state, workspace_id="default", **kw)


@pytest.fixture
def reviews(tmp_path, monkeypatch):
    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{tmp_path / 'rev.db'}")
    monkeypatch.delenv("CAIRN_STORAGE", raising=False)
    return build_review_store()


async def _open_review(store, pid="pl_gate"):
    await store.open_review(ReviewRecord(
        review_id=pipeline_review_id(pid, "review"), subject_ref=f"{pid}/review",
        title="Approve review", pipeline_id=pid, step_id="review"))


def _bridge(plan, **kw):
    bus = _CaptureBus()
    bridge = PipelineBridge(SimpleNamespace(store=None), store=InMemoryPipelineStore(), bus=bus)
    return bridge, bus, plan


# ── the bridge ───────────────────────────────────────────────────────────────


async def test_reject_fails_the_pipeline_and_records_decider_reason_review_event(reviews):
    bridge, bus, plan = _bridge(_plan())
    rec = await bridge.store.put(_record(plan))
    await _open_review(reviews)

    out = await bridge.reject_step(rec.pipeline_id, "review", decider="alice", reason="eval regressed")

    assert out.status == "failed"                                   # status vocabulary unchanged
    st = out.step_state["review"]
    assert (st.status, st.verdict, st.approver) == ("skipped", "rejected", "alice")
    assert out.trigger["failure_reason"] == "step 'review' rejected by alice: eval regressed"
    review = await reviews.get_review(pipeline_review_id(rec.pipeline_id, "review"))
    assert (review.status, review.reviewer, review.rationale) == ("rejected", "alice", "eval regressed")
    rejected = [p for t, p in bus.events if t == "pipeline.rejected"]
    assert rejected == [{"pipeline_id": "pl_gate", "step_id": "review", "step": "review",
                         "decider": "alice", "reason": "eval regressed"}]
    assert any(t == "pipeline.pipeline_failed" for t, _ in bus.events)


async def test_reject_runs_the_finalizers(reviews, monkeypatch):
    bridge, bus, plan = _bridge(_plan(finalizer=True))
    rec = await bridge.store.put(_record(plan))
    ran: list[str] = []

    async def _fin(r):
        ran.append(r.pipeline_id)
        r.step_state["cleanup"].status = "done"

    monkeypatch.setattr(bridge, "_run_finalizers", _fin)
    out = await bridge.reject_step(rec.pipeline_id, "review", decider="alice", reason="no")
    assert ran == [rec.pipeline_id]
    assert out.status == "failed" and out.step_state["review"].verdict == "rejected"


@pytest.mark.parametrize("status", ["awaiting_advance", "pending", "running", "done"])
async def test_only_a_gate_at_awaiting_approval_can_be_rejected(reviews, status):
    bridge, _bus, plan = _bridge(_plan())
    rec = await bridge.store.put(_record(plan, gate_status=status))
    with pytest.raises(ValueError, match="not awaiting approval"):
        await bridge.reject_step(rec.pipeline_id, "review", decider="a", reason="r")
    assert (await bridge.store.get(rec.pipeline_id)).status == "paused"     # untouched


async def test_a_terminal_pipeline_cannot_be_rejected(reviews):
    bridge, _bus, plan = _bridge(_plan())
    rec = _record(plan)
    rec.status = "cancelled"
    await bridge.store.put(rec)
    with pytest.raises(ValueError, match="cancelled"):
        await bridge.reject_step(rec.pipeline_id, "review", decider="a", reason="r")


async def test_temporal_owner_records_verdict_then_signals(reviews, monkeypatch):
    bridge, _bus, plan = _bridge(_plan())
    rec = await bridge.store.put(_record(plan, trigger={"owner": "temporal"}))
    signals: list[tuple] = []

    async def _sig(pid, name, *args):
        # the verdict and decider are on the record BEFORE the signal goes out
        st = (await bridge.store.get(pid)).step_state["review"]
        signals.append((pid, name, args, st.verdict, st.approver))

    monkeypatch.setattr(bridge, "_signal_temporal", _sig)
    await bridge.reject_step(rec.pipeline_id, "review", decider="alice", reason="no")
    assert signals == [("pl_gate", "reject_step", ("review",), "rejected", "alice")]


# ── the route ────────────────────────────────────────────────────────────────


@pytest.fixture
def server(monkeypatch, reviews):
    """A server over an in-memory pipeline store holding one pipeline parked at a gate whose
    `approvers:` names `bob`. Auth on: `maya` is the env admin (owner), `bob` may decide the
    gate, `carol` is a plain member the gate does not name."""
    store = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: store)
    monkeypatch.setenv("OBS_JWT_SECRET", _SECRET)
    monkeypatch.setenv("OBS_API_USER", "maya")
    monkeypatch.setenv("OBS_API_PASSWORD", "maya-pw")
    monkeypatch.delenv("OBS_API_PASSWORD_HASH", raising=False)
    asyncio.run(store.put(_record(_plan(approvers="bob"))))
    asyncio.run(_open_review(reviews))
    return SimpleNamespace(store=store, reviews=reviews)


def _auth(user: str) -> dict:
    return {"Authorization": f"Bearer {create_access_token({'sub': user})}"}


URL = "/pipelines/pl_gate/steps/review/reject"


def test_an_allowed_approver_rejects_and_the_token_is_the_decider(server):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post(URL, json={"reason": "eval regressed", "approver": "someone-else"},
                   headers=_auth("bob"))
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "failed" and r.json()["decider"] == "bob"
    rec = asyncio.run(server.store.get("pl_gate"))
    st = rec.step_state["review"]
    assert (st.status, st.verdict, st.approver) == ("skipped", "rejected", "bob")   # not the body's name
    assert rec.status == "failed"
    assert rec.trigger["failure_reason"] == "step 'review' rejected by bob: eval regressed"
    review = asyncio.run(server.reviews.get_review(pipeline_review_id("pl_gate", "review")))
    assert (review.status, review.reviewer, review.rationale) == ("rejected", "bob", "eval regressed")


def test_an_owner_may_always_reject(server):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post(URL, json={"reason": "no"}, headers=_auth("maya"))
    assert r.status_code == 200, r.text
    assert r.json()["decider"] == "maya"


def test_a_caller_the_gate_does_not_name_is_refused_before_anything_is_recorded(server):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post(URL, json={"reason": "no"}, headers=_auth("carol"))
    assert r.status_code == 403, r.text
    assert "may be decided by" in r.json()["detail"]
    rec = asyncio.run(server.store.get("pl_gate"))
    assert rec.status == "paused" and rec.step_state["review"].status == "awaiting_approval"
    review = asyncio.run(server.reviews.get_review(pipeline_review_id("pl_gate", "review")))
    assert review.status == "open"


def test_reason_is_required(server):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        h = _auth("bob")
        assert c.post(URL, json={}, headers=h).status_code == 422
        assert c.post(URL, json={"reason": ""}, headers=h).status_code == 422
        assert c.post(URL, json={"reason": "   "}, headers=h).status_code == 422
    assert asyncio.run(server.store.get("pl_gate")).status == "paused"


def test_a_step_not_at_awaiting_approval_is_409(server):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post("/pipelines/pl_gate/steps/prep/reject", json={"reason": "no"}, headers=_auth("maya"))
    assert r.status_code == 409, r.text


def test_another_workspaces_pipeline_is_404(server):
    rec = asyncio.run(server.store.get("pl_gate"))
    rec.workspace_id = "other"
    asyncio.run(server.store.put(rec))
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post(URL, json={"reason": "no"}, headers=_auth("maya"))
        assert r.status_code == 404
        assert c.post("/pipelines/pl_nope/steps/review/reject", json={"reason": "no"},
                      headers=_auth("maya")).status_code == 404


def test_without_a_token_under_auth_it_is_401(server):
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        assert c.post(URL, json={"reason": "no"}).status_code == 401


def test_auth_off_records_the_claimed_name(monkeypatch, server):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        r = c.post(URL, json={"reason": "no", "approver": "dana"})
    assert r.status_code == 200, r.text
    assert r.json()["decider"] == "dana"
