"""A queued webhook delivery starts through the same door as an inline one.

With ``CAIRN_TRIGGER_DURABLE`` on, a matched webhook event is queued in the trigger
inbox and a drain loop starts it later. The drain used to call
``start_template_run``: a bare compile and run of a template, with no launch door,
no plan-time policy gates, no admission and no run record. A declared pipeline ref
(``pipeline:<pack>/<name>``, what a pipeline's ``triggers:`` webhook names) never
resolved as a template at all, so every durable delivery for a pipeline was retried
and dead-lettered. The drain now hands the record to the router's own starter, the
launch door the inline delivery uses, inside the record's workspace.

Driven through the real app (``build_app``) with auth on, a real dispatcher,
pipeline bridge and policy engine; only the stores are in memory.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import textwrap
import types

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel, ConfigDict

from cairn.core.approval import policy_source
from cairn.core.engine.runner import Runner
from cairn.core.operator import Operation, register_operator
from cairn.core.policy.engine import PolicyEngine
from cairn.core.storage import pipeline_store as pipeline_store_mod
from cairn.core.storage import trigger_inbox as inbox_mod
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from cairn.core.storage.trigger_inbox import DEAD, DONE, drain_once
from cairn.core.storage.trigger_store import StoredTrigger
from cairn.core.triggers import auth
from cairn.core.triggers import router as router_mod
from cairn.core.triggers.server import build_app

SECRET = "hook-secret"
BODY = json.dumps({"type": "push"}).encode()
PIPELINE_REF = "pipeline:t217/echo"


class _P(BaseModel):
    model_config = ConfigDict(extra="allow")


@register_operator(name="_t217/echo", version="0.1.0")
class _Echo(Operation[_P, _P]):
    """A read-only task leg: proves the leg ran as a governed Run."""
    input_schema = _P
    output_schema = _P
    risk = "read_only"

    def run(self, inp, ctx):
        return _P(echoed=True)


@pytest.fixture(autouse=True)
def _op_registered():
    from cairn.core.operator.registry import default_registry
    try:
        default_registry.register(_Echo, strict=False)
    except Exception:  # noqa: BLE001 — already registered is fine
        pass


def _spec_text(path: str, ref: str) -> str:
    return (
        "source: custom\n"
        f"ingress: {{transport: http, path: '{path}', method: POST, signature: "
        "{header: X-Signature, secret: '${env:HOOK_SECRET}', required: true}}\n"
        "normalize: {event_type: type}\n"
        f"run: {{template: '{ref}'}}\n"
    )


def _sig() -> dict[str, str]:
    return {"X-Signature": hmac.new(SECRET.encode(), BODY, hashlib.sha256).hexdigest(),
            "content-type": "application/json"}


@pytest.fixture
def app_env(monkeypatch, tmp_path):
    """Auth on, durable ingress on an in-memory inbox, one declared pipeline
    (``t217/echo``: one read-only task leg), in-memory pipeline store, no policies."""
    for k in ("OBS_JWT_ALGORITHM", "OBS_ACCESS_TOKEN_EXPIRE_MINUTES", "OBS_REQUIRE_AUTH",
              "OBS_WORKSPACE", "CAIRN_WORKSPACE", "CAIRN_TRIGGER_DURABLE", "OBS_TRIGGER_DURABLE"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("OBS_JWT_SECRET", "test-secret-long-enough-for-hs256-abc")
    monkeypatch.setenv("HOOK_SECRET", SECRET)
    monkeypatch.setenv("OBS_TRIGGER_DURABLE", "1")
    monkeypatch.setenv("OBS_TRIGGER_INBOX", "memory")
    monkeypatch.setattr(auth, "_PUBLIC_TRIGGER_PATHS", set())

    # the declared pipeline the receivers start
    import cairn.core.engine.pipeline_resolve as R
    pdir = tmp_path / "t217" / "pipelines"
    pdir.mkdir(parents=True)
    (pdir / "echo.yaml").write_text(textwrap.dedent("""
        name: echo
        kind: pipeline
        steps:
          - {id: first, task: _t217/echo}
    """))
    monkeypatch.setattr(R, "all_pipeline_dirs", lambda: [str(pdir)])

    pipelines = InMemoryPipelineStore()
    monkeypatch.setattr(pipeline_store_mod, "_default_store", pipelines)
    # the receivers' workspace exists, as it does once a pipeline is enabled in it
    # (with auth on, a workspace id no store holds is refused)
    from cairn.core import workspace as W
    workspaces = W.InMemoryWorkspaceStore()
    asyncio.run(workspaces.put(W.Workspace(id="acme-ops", name="Acme ops")))
    monkeypatch.setattr(W, "_store", workspaces)
    # no policies unless a test writes one
    pol_dir = tmp_path / "policies"
    pol_dir.mkdir()
    monkeypatch.setattr(policy_source, "_engine", PolicyEngine(policies_directory=pol_dir))

    # receivers come from the trigger store; no overlay files, no discovered pipelines
    rows: list[StoredTrigger] = []
    store = types.SimpleNamespace(list_sync=lambda *a, **k: list(rows), close=lambda: None)
    import cairn.core.storage.trigger_store as ts
    monkeypatch.setattr(ts, "default_trigger_store", lambda: store)
    monkeypatch.setattr(router_mod, "load_trigger_specs", lambda *_a, **_k: [])

    async def _no_sync(**_kw):
        return 0
    import cairn.core.workflow_registry as wr
    monkeypatch.setattr(wr, "sync_discovered_pipelines", _no_sync)

    # the lifespan's drain loop is captured so the test drains it on the app's loop
    captured: dict = {}

    async def _capture(inbox, handler, **_kw):
        captured["inbox"], captured["handler"] = inbox, handler
    monkeypatch.setattr(inbox_mod, "run_drain_loop", _capture)

    # the bare in-process path the drain used to take: recorded, never run
    bare: list[str] = []

    async def _bare(ref, inputs):
        bare.append(ref)
        return await _real_bare(ref, inputs)
    _real_bare = router_mod.start_template_run
    monkeypatch.setattr(router_mod, "start_template_run", _bare)

    return types.SimpleNamespace(rows=rows, pipelines=pipelines, captured=captured,
                                 bare=bare, pol_dir=pol_dir, monkeypatch=monkeypatch)


def _receiver(env, *, ws: str = "acme-ops", path: str = "/triggers/acme",
              ref: str = PIPELINE_REF) -> None:
    env.rows.append(StoredTrigger(id=f"hook-{ws}", workspace_id=ws, name=f"hook-{ws}",
                                  source="custom", yaml=_spec_text(path, ref), enabled=True))


def _drain(c: TestClient, env) -> dict:
    return c.portal.call(drain_once, env.captured["inbox"], env.captured["handler"])


async def _settle(store, timeout: float = 5.0) -> list:
    """Every pipeline record once none is still running."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        recs = await store.list()
        if recs and all(r.status not in ("pending", "running") for r in recs):
            return recs
        if loop.time() > deadline:
            return recs
        await asyncio.sleep(0.05)


def _inbox_records(c: TestClient, env) -> list:
    return c.portal.call(env.captured["inbox"].list)


# ═══ the defect: a queued delivery for a declared pipeline ═════════════════


def test_a_queued_pipeline_delivery_starts_the_pipeline_through_the_launch_door(app_env):
    """Through the launch door: a PipelineRecord in the receiver's workspace, the
    task leg run as its own Run, and nothing on the bare compile-and-run path."""
    _receiver(app_env)
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        r = c.post("/triggers/acme", content=BODY, headers=_sig())
        assert r.status_code == 200, (r.status_code, r.text)
        tally = _drain(c, app_env)
        assert tally["dispatched"] == 1, (tally, [x.last_error for x in _inbox_records(c, app_env)])
        recs = c.portal.call(_settle, app_env.pipelines)
        assert app_env.bare == []
        assert len(recs) == 1, recs
        rec = recs[0]
        assert rec.workspace_id == "acme-ops"
        assert rec.status == "done", (rec.status, rec.step_state)
        assert (rec.trigger or {}).get("source") == "webhook"
        assert rec.admission.get("verdict"), rec.admission  # decided once at the launch door
        leg = rec.step_state["first"]
        assert leg.run_id, leg
        tok = auth.create_access_token({"sub": "admin"})
        run = c.get(f"/runs/{leg.run_id}", headers={"Authorization": f"Bearer {tok}",
                                                     "X-Cairn-Workspace": "acme-ops"})
        assert run.status_code == 200, (run.status_code, run.text)
        assert run.json().get("status") == "done", run.json()


def test_a_policy_deny_refuses_a_queued_delivery(app_env):
    """The plan-time gate fires for a queued delivery as for an inline one: a
    ``deny`` on the pipeline's leg starts nothing and records nothing."""
    (app_env.pol_dir / "deny.yaml").write_text(textwrap.dedent("""
        name: no-echo
        description: echo legs may not launch
        applies_to:
          pipeline_legs: ["_t217/*"]
        rules:
          - effect: deny
            reason: "echo is not allowed here"
    """))
    app_env.monkeypatch.setattr(policy_source, "_engine",
                                PolicyEngine(policies_directory=app_env.pol_dir))
    _receiver(app_env)
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        assert c.post("/triggers/acme", content=BODY, headers=_sig()).status_code == 200
        tally = _drain(c, app_env)
        (rec,) = _inbox_records(c, app_env)
        recs = c.portal.call(app_env.pipelines.list)
    assert tally["dispatched"] == 0 and tally["retried"] + tally["dead"] == 1, tally
    assert "PolicyDenied" in (rec.last_error or "") and "no-echo" in rec.last_error, rec
    assert recs == []
    assert app_env.bare == []


# ═══ negative cases: unchanged behaviour ═══════════════════════════════════


def test_a_queued_delivery_for_a_default_workspace_receiver_still_runs(app_env):
    _receiver(app_env, ws="default", path="/triggers/dflt")
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        assert c.post("/triggers/dflt", content=BODY, headers=_sig()).status_code == 200
        assert _drain(c, app_env)["dispatched"] == 1
        recs = c.portal.call(_settle, app_env.pipelines)
    assert [(r.workspace_id, r.status) for r in recs] == [("default", "done")]


def test_the_inline_delivery_is_unchanged_and_matches_the_queued_one(app_env):
    """Durable off: the inline path starts the same pipeline the same way."""
    app_env.monkeypatch.delenv("OBS_TRIGGER_DURABLE")
    _receiver(app_env)
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        r = c.post("/triggers/acme", content=BODY, headers=_sig())
        assert r.status_code == 200, (r.status_code, r.text)
        recs = c.portal.call(_settle, app_env.pipelines)
    assert "inbox" not in app_env.captured  # no drain without the durable inbox
    assert [(r.workspace_id, r.status, (r.trigger or {}).get("source")) for r in recs] == [
        ("acme-ops", "done", "webhook")]
    assert app_env.bare == []


def test_a_queued_bare_flow_ref_is_refused_as_the_inline_one_is(app_env):
    """A bare (non-pipeline) ref is refused at the door (ADR-0106 D1), never run
    as a bare template by the drain."""
    _receiver(app_env, ref="t217/not-a-pipeline")
    with TestClient(build_app(Runner(), enable_scheduler=False)) as c:
        assert c.post("/triggers/acme", content=BODY, headers=_sig()).status_code == 200
        tally = _drain(c, app_env)
        (rec,) = _inbox_records(c, app_env)
    assert tally["dispatched"] == 0, tally
    assert rec.status in ("pending", DEAD) and rec.status != DONE
    assert app_env.bare == []
