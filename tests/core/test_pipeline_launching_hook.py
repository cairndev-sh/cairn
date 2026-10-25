"""The ``pipeline.launching`` run hook: an installed ``cairn.run_hooks`` provider sees
a pipeline launch (its ref, inputs, workspace, account and trigger) and may refuse it
before the pipeline record exists.

Fired from ``launch_pipeline_def`` — the one door every pipeline launch goes through —
after the policy gates, the service doors and the admission decision, before
``bridge.start_pipeline``. A refusal reaches the caller as the door's admission
refusal (``AdmissionRefusedError``); a provider that fails any other way refuses the
launch too (this event fails closed, unlike every other run-hook event). No provider
installed: nothing is fired and nothing changes.

Every refusal test asserts the bridge was never called: the bridge is where the
record is created, so a refusal that reaches it has already failed.
"""
from __future__ import annotations

import os
import textwrap
from types import MappingProxyType, SimpleNamespace

os.environ.setdefault("OPENAI_API_KEY", "sk-validate-offline")

import pytest  # noqa: E402

from cairn.core import workspace as W  # noqa: E402
from cairn.core.admission_door import REFUSED, AdmissionRefusedError  # noqa: E402
from cairn.core.dispatch.pipeline_launch import launch_pipeline  # noqa: E402
from cairn.core.pack import run_hooks as RH  # noqa: E402

PIPE = "hookpack/importlike"

_PIPELINE = """
name: importlike
kind: pipeline
inputs:
  path: {type: str}
  eval_path: {type: str, default: ""}
  tags: {type: str, default: "a"}
steps:
  - {id: approve, gate: approve}
"""


@pytest.fixture()
def installed(tmp_path, monkeypatch):
    """One installed pipeline (a single gate leg, so no engine or service door is
    involved), no pull worker, the account lookup answered without a store."""
    import cairn.core.engine.pipeline_resolve as R
    from cairn.core import pull_worker_door as pw

    d = tmp_path / "hookpack" / "pipelines"
    d.mkdir(parents=True)
    (d / "importlike.yaml").write_text(textwrap.dedent(_PIPELINE))
    monkeypatch.setattr(R, "all_pipeline_dirs", lambda: [str(d)])

    async def _never():
        return None

    monkeypatch.setattr(pw, "last_worker_checkin", _never)

    async def _account(workspace_id=None, **_kw):
        return f"acct-of-{workspace_id}"

    monkeypatch.setattr(W, "workspace_account", _account)
    return d


class _Bridge:
    """Records every start_pipeline — the launch path's only side effect."""

    def __init__(self):
        self.calls: list[dict] = []

    async def start_pipeline(self, plan, **kw):
        self.calls.append({"plan": plan, **kw})
        return SimpleNamespace(pipeline_id="pl_test", status="running", step_state={})


class _Hooks:
    """Stands in for the installed ``cairn.run_hooks`` providers."""

    def __init__(self, monkeypatch, **providers):
        self.seen: list[tuple[str, dict]] = []
        self.providers = providers
        monkeypatch.setattr(RH, "load_run_hooks", lambda: dict(sorted(self.providers.items())))

    def recorder(self, then=None):
        async def _hook(event, payload):
            self.seen.append((event, payload))
            if then is not None:
                then(event, payload)
        return _hook

    def launching(self) -> list[dict]:
        return [p for e, p in self.seen if e == RH.PIPELINE_LAUNCHING]


def _refuse(reason):
    def _then(event, payload):
        if event == RH.PIPELINE_LAUNCHING:
            raise RH.AdmissionRefused(reason)
    return _then


def _boom(event, payload):
    if event == RH.PIPELINE_LAUNCHING:
        raise RuntimeError("listener bug: internal detail")


# ── the event and its semantics in run_hooks ────────────────────────────────


def test_the_event_is_in_the_existing_group_and_refusable_and_fails_closed():
    assert RH.PIPELINE_LAUNCHING == "pipeline.launching"
    assert RH.PIPELINE_LAUNCHING in RH.EVENTS
    assert RH.ENTRY_POINT_GROUP == "cairn.run_hooks"  # no new group
    assert RH.REFUSABLE_EVENTS == (RH.RUN_ADMITTING, RH.PIPELINE_LAUNCHING)
    assert RH.FAIL_CLOSED_EVENTS == (RH.PIPELINE_LAUNCHING,)


async def test_a_failing_provider_refuses_pipeline_launching_with_a_generic_reason(monkeypatch):
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"a_broken": hooks.recorder(_boom), "b_never": hooks.recorder()}
    with pytest.raises(RH.AdmissionRefused) as ei:
        await RH.fire(RH.PIPELINE_LAUNCHING, pipeline_ref="pipeline:p/x")
    assert "'a_broken'" in ei.value.reason and "RuntimeError" in ei.value.reason
    assert "internal detail" not in ei.value.reason  # the provider's message is logged, not returned
    assert len(hooks.seen) == 1  # the pass ends at the failure


@pytest.mark.parametrize("event", [e for e in RH.EVENTS if e != RH.PIPELINE_LAUNCHING])
async def test_every_other_event_still_fails_open(monkeypatch, event):
    """Unchanged semantics: run.admitting included, a provider failure is logged and
    the others still run."""
    hooks = _Hooks(monkeypatch)

    def _raise(_e, _p):
        raise RuntimeError("boom")

    hooks.providers = {"a_broken": hooks.recorder(_raise), "b_fine": hooks.recorder()}
    assert await RH.fire(event, run_id="r1") == ["b_fine"]


async def test_a_refusal_on_an_event_that_cannot_refuse_is_still_ignored(monkeypatch):
    hooks = _Hooks(monkeypatch)

    def _no(_e, _p):
        raise RH.AdmissionRefused("no")

    hooks.providers = {"a": hooks.recorder(_no), "b": hooks.recorder()}
    assert await RH.fire(RH.RUN_FINISHED, run_id="r1") == ["b"]


# ── at the launch door ──────────────────────────────────────────────────────


async def test_no_provider_installed_fires_nothing_and_changes_nothing(installed, monkeypatch):
    monkeypatch.setattr(RH, "load_run_hooks", lambda: {})
    fired = []

    async def _spy(event, **kw):  # pragma: no cover — must not be reached
        fired.append(event)

    async def _no_account(*a, **k):  # pragma: no cover — must not be reached
        raise AssertionError("the account is resolved only when a provider is installed")

    monkeypatch.setattr(RH, "fire", _spy)
    monkeypatch.setattr(W, "workspace_account", _no_account)
    bridge = _Bridge()
    await launch_pipeline(PIPE, inputs={"path": "x"}, bridge=bridge)
    assert fired == [] and len(bridge.calls) == 1
    assert bridge.calls[0]["inputs"] == {"path": "x", "eval_path": "", "tags": "a"}
    assert bridge.calls[0]["bundle_ref"] == f"pipeline:{PIPE}"


async def test_the_provider_sees_the_launch_once_with_a_read_only_payload(installed, monkeypatch):
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder()}
    bridge = _Bridge()
    await launch_pipeline(PIPE, inputs={"path": "uploads/me/a.jsonl", "eval_path": "s3://b/k"},
                          bridge=bridge, workspace_id="ws-1", environment="staging",
                          trigger={"source": "console", "extra": [1, {"k": "v"}]})
    assert len(bridge.calls) == 1
    [p] = hooks.launching()
    assert len(hooks.seen) == 1
    assert p["event"] == "pipeline.launching"
    assert p["pipeline_ref"] == f"pipeline:{PIPE}"
    assert p["workspace_id"] == "ws-1" and p["account_id"] == "acct-of-ws-1"
    assert p["environment"] == "staging" and p["source"] == "console" and p["preset"] == ""
    assert dict(p["inputs"]) == {"path": "uploads/me/a.jsonl", "eval_path": "s3://b/k", "tags": "a"}
    assert dict(p["supplied"]) == {"path": "uploads/me/a.jsonl", "eval_path": "s3://b/k"}
    assert p["trigger"]["extra"][1]["k"] == "v"
    for m in (p["inputs"], p["supplied"], p["trigger"], p["trigger"]["extra"][1]):
        assert isinstance(m, MappingProxyType)
        with pytest.raises(TypeError):
            m["path"] = "changed"  # type: ignore[index]
    assert isinstance(p["trigger"]["extra"], tuple)
    # what the launch runs with is the door's own copy, not the provider's view
    assert bridge.calls[0]["inputs"]["path"] == "uploads/me/a.jsonl"


async def test_a_refusal_prevents_the_record_and_is_the_admission_refusal(installed, monkeypatch):
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder(_refuse("path must be one of your uploads"))}
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError) as ei:
        await launch_pipeline(PIPE, inputs={"path": "/srv/data"}, bridge=bridge, workspace_id="ws-1")
    assert bridge.calls == []
    assert isinstance(ei.value, ValueError)
    assert str(ei.value) == "admission door: path must be one of your uploads"
    rec = ei.value.decision.as_record()
    assert rec["verdict"] == REFUSED and rec["refusing_reasons"] == ["path must be one of your uploads"]
    assert rec["workspace_id"] == "ws-1"


async def test_a_failing_provider_refuses_the_launch(installed, monkeypatch):
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder(_boom)}
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError, match="'check' could not check this launch"):
        await launch_pipeline(PIPE, inputs={"path": "x"}, bridge=bridge)
    assert bridge.calls == []


async def test_an_unresolvable_account_is_empty_and_the_launch_proceeds(installed, monkeypatch):
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder()}

    async def _no(*a, **k):
        raise W.WorkspaceForbidden("ws-x")

    monkeypatch.setattr(W, "workspace_account", _no)
    bridge = _Bridge()
    await launch_pipeline(PIPE, inputs={"path": "x"}, bridge=bridge, workspace_id="ws-x")
    assert hooks.launching()[0]["account_id"] == "" and len(bridge.calls) == 1


async def test_an_earlier_refusal_wins_and_the_hook_is_not_asked(installed, monkeypatch):
    """Asked after the admission decision: a launch the door already refuses never
    reaches a provider."""
    from cairn.core import admission_door as AD

    async def _refused(inputs, **kw):
        return AD.AdmissionDecision(verdict=REFUSED, refusing_reasons=["holdout"])

    monkeypatch.setattr(AD, "decide_admission", _refused)
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder()}
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError, match="holdout"):
        await launch_pipeline(PIPE, inputs={"path": "x"}, bridge=bridge)
    assert hooks.seen == [] and bridge.calls == []


async def test_a_policy_deny_wins_and_the_hook_is_not_asked(installed, monkeypatch):
    from cairn.core.policy import pipeline_gates as PG

    def _deny(plan, environment=None):
        raise PG.PolicyDeniedError("policy no-launch denies it")

    monkeypatch.setattr(PG, "inject_policy_gates", _deny)
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder()}
    bridge = _Bridge()
    with pytest.raises(PG.PolicyDeniedError):
        await launch_pipeline(PIPE, inputs={"path": "x"}, bridge=bridge)
    assert hooks.seen == [] and bridge.calls == []


# ── every entry path reaches it ─────────────────────────────────────────────


async def test_a_schedule_fire_asks_the_hook_and_a_refusal_starts_nothing(installed, monkeypatch):
    from cairn.core.dispatch.scheduler import fire_scheduled_workflow

    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder()}
    bridge = _Bridge()
    disp = SimpleNamespace(pipeline_bridge=bridge)
    row = SimpleNamespace(workflow=f"pipeline:{PIPE}", inputs={"path": "x"}, environment="",
                          config_preset="", workspace_id="ws-s", id="sch-1", cron="* * * * *")
    await fire_scheduled_workflow(disp, row)
    [p] = hooks.launching()
    assert p["source"] == "schedule" and p["workspace_id"] == "ws-s" and len(bridge.calls) == 1

    hooks.providers = {"check": hooks.recorder(_refuse("not on a timer"))}
    with pytest.raises(AdmissionRefusedError, match="not on a timer"):
        await fire_scheduled_workflow(disp, row)
    assert len(bridge.calls) == 1


async def test_a_webhook_start_asks_the_hook(installed, monkeypatch):
    from cairn.core.dispatch.start import unattended_starter

    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder()}
    bridge = _Bridge()
    start = unattended_starter(SimpleNamespace(pipeline_bridge=bridge), source="webhook")
    await start(f"pipeline:{PIPE}", {"path": "x"})
    assert [p["source"] for p in hooks.launching()] == ["webhook"] and len(bridge.calls) == 1


async def test_an_operator_launch_asks_the_hook(installed, monkeypatch):
    from cairn.core.dispatch.dispatcher import RunDispatcher

    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder(_refuse("operators may not start this"))}
    bridge = _Bridge()
    handle = RunDispatcher.launch_handle(SimpleNamespace(_pipeline_bridge=bridge))
    with pytest.raises(AdmissionRefusedError, match="operators may not start this"):
        await handle(f"pipeline:{PIPE}", {"path": "x"}, {"source": "watch-op"})
    assert hooks.launching()[0]["source"] == "watch-op" and bridge.calls == []


def _app(bridge):
    from fastapi import FastAPI

    from cairn.core.triggers.routers.pipelines import build_pipelines_router
    from cairn.core.triggers.routers.start import build_start_router

    app = FastAPI()
    disp = SimpleNamespace(pipeline_bridge=bridge)
    app.include_router(build_start_router(disp, bridge))
    app.include_router(build_pipelines_router(disp, bridge))
    return app


@pytest.mark.parametrize("path,body,source", [
    ("/start", {"ref": f"pipeline:{PIPE}", "inputs": {"path": "/srv/x"}}, "start"),
    ("/pipelines/launch", {"ref": PIPE, "inputs": {"path": "/srv/x"}}, "console"),
])
def test_both_http_doors_answer_a_refusal_as_a_refused_admission(installed, monkeypatch, path, body, source):
    from fastapi.testclient import TestClient

    for k in ("OBS_REQUIRE_AUTH", "CAIRN_REQUIRE_AUTH"):
        monkeypatch.delenv(k, raising=False)
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder(_refuse("only your own uploads"))}
    bridge = _Bridge()
    with TestClient(_app(bridge)) as c:
        r = c.post(path, json=body)
    assert r.status_code == 403, r.text
    detail = r.json()["detail"]
    assert detail["error"] == "admission door: only your own uploads"
    assert detail["admission"]["refusing_reasons"] == ["only your own uploads"]
    assert [p["source"] for p in hooks.launching()] == [source] and bridge.calls == []


def test_the_cli_reports_a_refusal_as_a_refused_admission(installed, monkeypatch, tmp_path):
    from typer.testing import CliRunner

    from cairn.cli.commands import pipeline as cli_pipeline

    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{tmp_path / 'cli.db'}")
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "runs.db"))
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder(_refuse("not from a terminal"))}
    started = []

    async def _never_start(self, plan, **kw):  # pragma: no cover — must not be reached
        started.append(plan)

    from cairn.core.dispatch import pipeline_bridge as PB

    monkeypatch.setattr(PB.PipelineBridge, "start_pipeline", _never_start)
    r = CliRunner().invoke(cli_pipeline.app, ["run", f"pipeline:{PIPE}", "--input", "path=x"])
    assert r.exit_code == 3, r.output
    assert "not from a terminal" in r.output
    assert [p["source"] for p in hooks.launching()] == ["cli"] and started == []


async def test_a_template_run_wrapped_in_its_one_leg_pipeline_asks_the_hook(installed, monkeypatch):
    """The no-orphan chokepoint in ``run_template`` launches through the same door,
    stamped with the flow's address."""
    from cairn.core.dispatch import start as S
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.dispatch.pipeline_launch import resolve_or_raise

    d = resolve_or_raise(PIPE)
    monkeypatch.setattr(S, "synthesize_flow_pipeline", lambda ref, typed: d)
    hooks = _Hooks(monkeypatch)
    hooks.providers = {"check": hooks.recorder(_refuse("no wrapped runs here"))}
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError, match="no wrapped runs here"):
        await RunDispatcher._wrap_orphan_run(
            SimpleNamespace(pipeline_bridge=bridge), "hookpack/tmpl", {"path": "x"},
            environment=None, run_options={}, producer="test")
    [p] = hooks.launching()
    assert p["pipeline_ref"] == "flow:hookpack/tmpl" and p["source"] == "dispatch"
    assert bridge.calls == []
