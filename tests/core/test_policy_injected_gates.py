"""Policy-injected plan-time gates (#1252, ADR-0087 P0b).

A policy-injected gate is satisfied only by its own decision: an author gate
that can be routed off does not stand in for it, the policy's ``approval_role``
is the injected gate's audience (and the approve door applies it), and a policy
that sets only ``pipeline_legs`` is a plan-time policy, not a per-call one (#34).
"""
from __future__ import annotations

import os
import textwrap
from types import SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient

from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.engine.pipeline_gate import required_approvers
from cairn.core.engine.schema import ActionCandidate, ActionRequest, PolicySpec
from cairn.core.policy.engine import PolicyEngine, policy_applies
from cairn.core.policy.pipeline_gates import inject_policy_gates
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord
from tests.core.test_pipeline_bridge import FakeDispatcher, _complete, _NullBus


def _policy(role: str | None = None, **applies) -> PolicySpec:
    rule = {"effect": "require_hitl", **({"approval_role": role} if role else {})}
    return PolicySpec.model_validate({
        "name": "review-before-fine-tune",
        "applies_to": {"pipeline_legs": ["*/lora"], **applies},
        "rules": [rule],
    })


class _Engine:
    def __init__(self, *policies: PolicySpec):
        self.policies = list(policies)

    def _is_active(self, _p) -> bool:
        return True


_AUTHOR_GATE = """
name: p
kind: pipeline
{governance}
steps:
  - {{id: prep, template: prep}}
  - gate: looks-fine
    {when}
    depends_on: [prep]
    {approvers}
  - {{id: train, template: lora, depends_on: [looks-fine]}}
"""


def _plan(when: str = "", approvers: str = "", governance: str = ""):
    body = _AUTHOR_GATE.format(when=when, approvers=approvers, governance=governance)
    return plan_pipeline(parse_pipeline_def(textwrap.dedent(body), pack="training", source="<t>"))


# ── an author gate that can be routed off does not stand in for the policy ───

def test_a_conditional_author_gate_does_not_replace_the_policy_gate():
    plan = _plan(when="when: $input.ask_a_human")
    assert inject_policy_gates(plan, environment="production", engine=_Engine(_policy())) == [
        "policy-gate-review-before-fine-tune-train"]
    train = next(s for s in plan.steps if s.id == "train")
    assert train.depends_on == ["policy-gate-review-before-fine-tune-train"]


def test_an_unconditional_author_gate_is_still_not_doubled():
    plan = _plan()
    assert inject_policy_gates(plan, environment="production", engine=_Engine(_policy())) == []


async def test_a_routed_off_author_gate_still_leaves_the_leg_waiting_on_a_decision():
    plan = _plan(when="when: $input.ask_a_human")
    inject_policy_gates(plan, environment="production", engine=_Engine(_policy()))
    disp = FakeDispatcher()
    bridge = PipelineBridge(disp, store=InMemoryPipelineStore(), bus=_NullBus())
    rec = await bridge.start_pipeline(plan, inputs={"ask_a_human": False})
    await _complete(bridge, disp, "run_1", output={"verdict": "pass"})
    rec = await bridge.store.get(rec.pipeline_id)
    assert [c["step_id"] for c in disp.calls] == ["prep"]
    assert rec.step_state["looks-fine"].status == "skipped"
    assert rec.step_state["policy-gate-review-before-fine-tune-train"].status == "awaiting_approval"
    assert rec.step_state["train"].status == "pending"


def test_an_author_gate_for_another_audience_does_not_replace_a_role_policy():
    plan = _plan(approvers="approvers: release-managers")
    assert inject_policy_gates(plan, environment="production", engine=_Engine(_policy("ml-leads")))
    plan = _plan(approvers="approvers: ml-leads")
    assert inject_policy_gates(plan, environment="production", engine=_Engine(_policy("ml-leads"))) == []


# ── approval_role is the injected gate's audience ────────────────────────────

def test_the_injected_gate_carries_the_policys_approval_role():
    plan = _plan(governance="governance: {approvers: member}")
    plan.steps = [s for s in plan.steps if s.id != "looks-fine"]
    next(s for s in plan.steps if s.id == "train").depends_on = ["prep"]
    inject_policy_gates(plan, environment="production", engine=_Engine(_policy("ml-leads")))
    gate = next(s for s in plan.steps if s.injected_by)
    assert gate.approvers == "ml-leads"
    assert required_approvers(gate, plan.governance) == "ml-leads"


@pytest.fixture
def door(monkeypatch):
    """The approve door over a record holding a policy-injected, role-staffed gate."""
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-the-injected-gate-audience")
    seen: dict = {}
    store = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: store)

    async def fake_advance(self, pipeline_id, step_id, *, approver="", rationale=""):
        seen["approver"] = approver
        return SimpleNamespace(status="running")

    monkeypatch.setattr(PipelineBridge, "advance_step", fake_advance)
    plan = _plan(governance="governance: {approvers: member}")
    plan.steps = [s for s in plan.steps if s.id != "looks-fine"]
    next(s for s in plan.steps if s.id == "train").depends_on = ["prep"]
    inject_policy_gates(plan, environment="production", engine=_Engine(_policy("ml-leads")))
    import asyncio
    asyncio.run(store.put(PipelineRecord(
        pipeline_id="pl_role", pipeline_name="p", status="paused", workspace_id="default",
        plan=plan, governance=plan.governance)))
    return seen


def _decide_as(monkeypatch, username: str, role: str, *, is_admin: bool = False):
    from cairn.core.triggers.auth import create_access_token
    from cairn.core.triggers.server import build_app
    from tests.core.test_trigger_server_dispatch import FakeRunner

    async def ident(_token):
        return {"username": username, "role": role, "is_admin": is_admin}

    monkeypatch.setattr("cairn.core.triggers.routers.pipelines.caller_identity", ident)
    monkeypatch.setattr("cairn.core.triggers.routers.reviews.caller_identity", ident)
    tok = create_access_token({"sub": username})
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as c:
        return c.post("/pipelines/pl_role/steps/policy-gate-review-before-fine-tune-train/approve",
                      json={"rationale": "ok"}, headers={"Authorization": f"Bearer {tok}"})


def test_the_approve_door_refuses_a_caller_outside_the_policys_role(monkeypatch, door):
    r = _decide_as(monkeypatch, "sam", "member")
    assert r.status_code == 403 and "ml-leads" in r.text
    assert door == {}


def test_the_approve_door_accepts_the_policys_role(monkeypatch, door):
    r = _decide_as(monkeypatch, "lee", "ml-leads")
    assert r.status_code == 200, r.text
    assert door == {"approver": "lee"}


# ── pipeline_legs is a plan-time match only (#34) ────────────────────────────

def _request(op: str = "data/profile", risk: str = "low") -> ActionRequest:
    return ActionRequest(candidate=ActionCandidate(id="x", type=op, risk=risk), environment="production")


def test_a_pipeline_legs_only_policy_does_not_apply_to_a_per_call_request():
    assert not policy_applies(_policy(), _request())


def test_a_pipeline_legs_policy_with_other_filters_keeps_them():
    assert policy_applies(_policy(action_types=["data/profile"]), _request())
    assert not policy_applies(_policy(action_types=["storage/put"]), _request())


def test_a_read_only_call_is_not_gated_at_dispatch_but_the_leg_is_gated_at_plan_time():
    engine = PolicyEngine()
    engine.policies = [_policy()]
    decision = engine.evaluate(_request())
    assert decision.matched_policies == []
    plan = _plan()
    plan.steps = [s for s in plan.steps if s.id != "looks-fine"]
    next(s for s in plan.steps if s.id == "train").depends_on = ["prep"]
    assert inject_policy_gates(plan, environment="production", engine=engine) == [
        "policy-gate-review-before-fine-tune-train"]
