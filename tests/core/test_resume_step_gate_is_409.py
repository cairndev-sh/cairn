"""Resuming a pipeline STEP gate through the run-level verb is a client error.

0.1.2 answered ``POST /runs/{id}/resume`` (``cairn remote approve/reject``) on a
pipeline step gate with HTTP 502 (verification bug 15), which reads as an engine
fault. The run has no template interrupt; the step is released by advancing it.
The real dispatcher decides here; only the route's workspace guard is stubbed.
"""
from __future__ import annotations

import asyncio
import types

import pytest
from fastapi.testclient import TestClient

from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.engine.runner import Runner
from cairn.core.storage.protocols import Run, TriggerEvent
from cairn.core.triggers.server import build_app


@pytest.fixture
def client_and_dispatcher(monkeypatch):
    monkeypatch.delenv("CAIRN_STORAGE", raising=False)
    monkeypatch.delenv("OBS_STORAGE", raising=False)
    monkeypatch.setenv("CAIRN_RUNTIME", "inprocess")
    made: list[RunDispatcher] = []
    real_init = RunDispatcher.__init__

    def _init(self, *a, **kw):
        real_init(self, *a, **kw)
        made.append(self)

    monkeypatch.setattr(RunDispatcher, "__init__", _init)
    app = build_app(Runner(), enable_scheduler=False, enable_spec_routes=False)
    return TestClient(app), made[0]


def test_reject_on_a_step_gate_is_409_with_the_advance_command(client_and_dispatcher):
    client, disp = client_and_dispatcher
    run = Run(id="run-step-gate", trigger=TriggerEvent(source="manual", payload={}),
              status="awaiting_approval")
    run.bundle_pipeline_id = "pl_abc"  # a leg of a pipeline, paused at its step gate
    run.step_id = "review"
    asyncio.run(disp.store.create(run))
    r = client.post("/runs/run-step-gate/resume", json={"decision": "reject"})
    assert r.status_code == 409, (r.status_code, r.text)
    assert "cairn remote advance pl_abc review" in r.json()["detail"]
