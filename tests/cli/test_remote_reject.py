"""``cairn remote reject`` (ADR-0156 D6): both forms, against the real in-process server.

``reject <pipeline_id> <step> --reason TEXT`` rejects a pipeline gate; ``reject <run_id>`` is the
unchanged run-level verb; a lone ``pl_…`` argument is refused.
"""
from __future__ import annotations

import asyncio
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from typer.testing import CliRunner

from cairn.cli.commands import remote
from cairn.core.engine.bundle_pipeline import PipelinePlan, PipelinePlanStep
from cairn.core.engine.runner import Runner
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState
from cairn.core.triggers.auth import create_access_token
from cairn.core.triggers.server import build_app
from tests.cli.test_remote import _patch_client

cli = CliRunner()
EP = ["--endpoint", "http://testserver"]


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    monkeypatch.setenv("OBS_CONFIG_HOME", str(tmp_path / "cfg"))
    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{tmp_path / 'rev.db'}")
    for k in ("OBS_REMOTE_ENDPOINT", "OBS_REMOTE_TOKEN", "CAIRN_REMOTE_ENDPOINT", "CAIRN_REMOTE_TOKEN",
              "OBS_JWT_SECRET", "OBS_API_USER", "OBS_API_PASSWORD", "OBS_API_PASSWORD_HASH"):
        monkeypatch.delenv(k, raising=False)


@pytest.fixture
def served(monkeypatch):
    store = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: store)
    plan = PipelinePlan(name="pl", steps=[
        PipelinePlanStep(id="prep", ref="task:x/prep"),
        PipelinePlanStep(id="review", ref="gate:review", depends_on=["prep"], approval=True,
                         approvers="bob")])
    asyncio.run(store.put(PipelineRecord(
        pipeline_id="pl_cli1", pipeline_name="pl", status="paused", plan=plan, inputs={},
        workspace_id="default",
        step_state={"prep": StepState(status="done"),
                    "review": StepState(status="awaiting_approval")})))
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    return store


def test_pipeline_form_rejects_the_gate(served):
    res = cli.invoke(remote.app, ["reject", "pl_cli1", "review", "--reason", "eval regressed",
                                  "--approver", "dana", *EP])
    assert res.exit_code == 0, res.output
    assert "rejected" in res.output and "failed" in res.output
    rec = asyncio.run(served.get("pl_cli1"))
    assert rec.status == "failed"
    assert (rec.step_state["review"].verdict, rec.step_state["review"].approver) == ("rejected", "dana")
    assert "eval regressed" in rec.trigger["failure_reason"]


def test_reason_is_required_with_a_step(served):
    res = cli.invoke(remote.app, ["reject", "pl_cli1", "review", *EP])
    assert res.exit_code == 1
    assert "--reason is required" in res.output
    assert asyncio.run(served.get("pl_cli1")).status == "paused"


def test_a_lone_pipeline_id_is_refused(served):
    res = cli.invoke(remote.app, ["reject", "pl_cli1", *EP])
    assert res.exit_code == 1
    assert "name the gate step" in res.output
    assert asyncio.run(served.get("pl_cli1")).status == "paused"


def test_run_form_is_unchanged(served):
    res = cli.invoke(remote.app, ["reject", "no-such-run", *EP])
    assert res.exit_code == 1
    assert "404" in res.output          # POST /runs/{id}/resume, as before
    res = cli.invoke(remote.app, ["approve", "no-such-run", *EP])
    assert res.exit_code == 1 and "404" in res.output


def test_unknown_pipeline_is_reported_as_404(served):
    res = cli.invoke(remote.app, ["reject", "pl_missing", "review", "--reason", "x", *EP])
    assert res.exit_code == 1 and "404" in res.output


def test_a_step_that_is_not_a_gate_wait_is_reported_as_409(served):
    res = cli.invoke(remote.app, ["reject", "pl_cli1", "prep", "--reason", "x", *EP])
    assert res.exit_code == 1 and "409" in res.output


def test_a_caller_the_gate_does_not_name_is_reported_as_403(served, monkeypatch):
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-remote-reject-adr-0156-cli")
    monkeypatch.setenv("OBS_API_USER", "maya")
    monkeypatch.setenv("OBS_API_PASSWORD", "maya-pw")
    _patch_client(monkeypatch, build_app(Runner(), enable_scheduler=False, enable_spec_routes=False))
    tok = create_access_token({"sub": "carol"})
    res = cli.invoke(remote.app, ["reject", "pl_cli1", "review", "--reason", "x", "--token", tok, *EP])
    assert res.exit_code == 1 and "403" in res.output
    assert asyncio.run(served.get("pl_cli1")).status == "paused"
