"""#2388 — a gate approved with `cairn decide … approve --step` is recorded with who decided.

The CLI sent ``--approver`` to ``POST …/steps/{id}/advance``, a route that ignores
its body, so a gate released from the CLI was audited with no approver (and its
review stayed open with reviewer ""). `/advance` now refuses a gate, and the CLI
reads the step first: a gate (``awaiting_approval``) goes to ``/approve`` — the one
door that decides a gate and records the approver (#2310, #2358) — while a step
parked ``awaiting_advance`` is still released through ``/advance``.

These drive the real chain — CliRunner → ``cairn_client.CairnClient`` → the FastAPI
routes → the bridge door. The first two replace only ``PipelineBridge.advance_step``
with a recorder; the last runs the REAL bridge and reads its audit event and review.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from cairn.cli.commands import remote
from cairn.cli.main import app as cairn
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.storage.pipeline_store import InMemoryPipelineStore, PipelineRecord, StepState
from cairn.core.triggers.auth import create_access_token
from cairn.core.triggers.server import build_app
from tests.core.test_trigger_server_dispatch import FakeRunner

cli = CliRunner()


def _auth_on(monkeypatch) -> str:
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-for-the-cli-advance-approver-2388")
    monkeypatch.setenv("OBS_API_USER", "maya")
    monkeypatch.setenv("OBS_API_PASSWORD", "maya-pw")
    monkeypatch.delenv("OBS_API_PASSWORD_HASH", raising=False)
    return create_access_token({"sub": "maya"})


def _wire(monkeypatch, tc: TestClient, token) -> None:
    """Point the CLI at ``tc``'s app through the real client."""
    from cairn_client import CairnClient

    @contextlib.contextmanager
    def _session(endpoint, tok, profile):
        yield "http://testserver", token, False

    monkeypatch.setattr(remote, "_session", _session)
    monkeypatch.setattr(remote, "_api", lambda ep, tok, timeout=30.0: CairnClient(
        ep, token=tok, transport=tc._transport))


@pytest.fixture
def parked(monkeypatch):
    """A gate (`awaiting_approval`) and a manual step (`awaiting_advance`); the bridge
    records each release as (step, approver) — None means no approver was handed."""
    seen: list = []
    store = InMemoryPipelineStore()
    monkeypatch.setattr("cairn.core.dispatch.pipeline_bridge.default_pipeline_store", lambda: store)

    async def fake_advance(self, pipeline_id, step_id, *, approver=None, rationale=""):
        seen.append((step_id, approver))
        return types.SimpleNamespace(status="running")

    monkeypatch.setattr(PipelineBridge, "advance_step", fake_advance)
    asyncio.run(store.put(PipelineRecord(
        pipeline_id="pl_gate", pipeline_name="p", status="paused", workspace_id="default",
        step_state={"promote-model": StepState(status="awaiting_approval"),
                    "release": StepState(status="awaiting_advance")})))
    return seen


def _advance(step: str, *extra: str):
    return cli.invoke(cairn, ["decide", "pl_gate", "approve", "--step", step, *extra])


def test_cli_gate_approval_under_auth_records_the_signed_in_user(monkeypatch, parked):
    tok = _auth_on(monkeypatch)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as tc:
        _wire(monkeypatch, tc, tok)
        for extra in ((), ("--approver", "someone-else")):   # a claim never overrides the token
            r = _advance("promote-model", *extra)
            assert r.exit_code == 0, r.output
            assert "approved" in r.output
        r = _advance("release")                               # not a gate: /advance, no approver
        assert r.exit_code == 0, r.output
    assert parked == [("promote-model", "maya"), ("promote-model", "maya"), ("release", None)]


def test_cli_gate_approval_keyless_records_the_claimed_name(monkeypatch, parked):
    monkeypatch.delenv("OBS_JWT_SECRET", raising=False)
    with TestClient(build_app(FakeRunner(), enable_scheduler=False)) as tc:
        _wire(monkeypatch, tc, None)
        assert _advance("promote-model", "--approver", "alice").exit_code == 0
        assert _advance("promote-model").exit_code == 0
    # keyless with no --approver: /approve's own fallback — never empty
    assert parked == [("promote-model", "alice"), ("promote-model", "console")]


class _RecordingBus:
    """The bridge's audit sink, kept: every `pipeline.<event>` it publishes."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    async def publish(self, topic, payload, **_kw):
        self.events.append((topic, dict(payload)))

    async def subscribe(self, _topics, _handler):
        return types.SimpleNamespace(unsubscribe=lambda: None)

    async def replay(self, _topic, _since):
        return
        yield  # pragma: no cover — an async generator


def test_cli_gate_approval_through_the_real_bridge_names_the_approver(monkeypatch, tmp_path):
    """No stubbed door: the real PipelineBridge parks a gate, `cairn decide`
    releases it, and the audit event and the review both name the token identity."""
    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{tmp_path / 'rev.db'}")
    tok = _auth_on(monkeypatch)
    from cairn.core.storage.reviews import build_review_store, pipeline_review_id
    from cairn.core.triggers.routers.pipelines import build_pipelines_router
    from tests.core.test_pipeline_bridge import FakeDispatcher, _complete, _prep_then

    disp, bus = FakeDispatcher(), _RecordingBus()
    bridge = PipelineBridge(disp, store=InMemoryPipelineStore(), bus=bus)

    async def _park() -> str:
        rec = await bridge.start_pipeline(_prep_then("train", approval=True), inputs={})
        await _complete(bridge, disp, "run_1", output={"staged_dataset_uri": "s3://s"})
        return rec.pipeline_id

    pid = asyncio.run(_park())
    assert asyncio.run(bridge.store.get(pid)).step_state["train"].status == "awaiting_approval"

    app = FastAPI()
    app.include_router(build_pipelines_router(disp, bridge))
    with TestClient(app) as tc:
        _wire(monkeypatch, tc, tok)
        assert tc.post(f"/pipelines/{pid}/steps/train/advance",
                       headers={"Authorization": f"Bearer {tok}"}).status_code == 409
        r = cli.invoke(cairn, ["decide", pid, "approve", "--step", "train", "--approver", "someone-else"])
        assert r.exit_code == 0, r.output

    approved = [p for t, p in bus.events if t == "pipeline.approved"]
    assert [p["approver"] for p in approved] == ["maya"]
    assert not [t for t, _ in bus.events if t == "pipeline.advanced"]
    review = asyncio.run(build_review_store().get_review(pipeline_review_id(pid, "train")))
    assert review is not None and review.status == "approved" and review.reviewer == "maya"
    assert asyncio.run(bridge.store.get(pid)).step_state["train"].approver == "maya"
