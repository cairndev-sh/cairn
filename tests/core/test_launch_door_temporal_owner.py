"""#199 (ADR-0151 D10) — the launch door refuses, under ``CAIRN_RUNTIME=temporal``, a
plan the Temporal pipeline owner would start and not finish: a ``task:`` leg (the
owner's SUBMIT routes only flow legs to an engine and everything else to
``run_template``, #59) and a ``finalizer:`` step (the workflow schedules by
``depends_on`` only). Before any record exists, as a refused launch (403 / exit 3).

The refusal reads the owner's own declaration
(``cairn.core.runtime.temporal.PIPELINE_OWNER_*``), so it lifts when the owner learns
the step. Every refusal test asserts the bridge was never called: the bridge is where
the record is created.
"""
from __future__ import annotations

import textwrap
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from cairn.core.dispatch import pipeline_launch as PL
from cairn.core.policy.pipeline_gates import PolicyDeniedError
from cairn.core.runtime import temporal as owner

_TASK = """
name: with-task
kind: pipeline
steps:
  - {id: approve, gate: approve}
  - id: check
    task: monitor/threshold
    depends_on: [approve]
    with: {metric: rows, value: 1, threshold: 2, comparator: gt}
"""

_FLOW = """
name: flow-only
kind: pipeline
steps:
  - {id: approve, gate: approve}
  - {id: run, flow: summarise, depends_on: [approve]}
"""

_FINALIZER = """
name: with-finalizer
kind: pipeline
steps:
  - {id: run, flow: summarise}
  - {id: teardown, flow: summarise, finalizer: true}
"""


@pytest.fixture()
def installed(tmp_path, monkeypatch):
    """Three installed pipelines in one scratch pack; no pull worker."""
    import cairn.core.engine.pipeline_resolve as R
    from cairn.core import pull_worker_door as pw

    d = tmp_path / "ownerpack" / "pipelines"
    d.mkdir(parents=True)
    for name, text in (("with-task", _TASK), ("flow-only", _FLOW), ("with-finalizer", _FINALIZER)):
        (d / f"{name}.yaml").write_text(textwrap.dedent(text))
    monkeypatch.setattr(R, "all_pipeline_dirs", lambda: [str(d)])

    async def _never():
        return None

    monkeypatch.setattr(pw, "last_worker_checkin", _never)
    monkeypatch.delenv("CAIRN_PROFILE", raising=False)
    # The scratch pack ships no flows and the operator packs may not be loaded:
    # whether a leg's target resolves is `assert_runnable`'s refusal, not this one.
    monkeypatch.setattr(PL, "assert_runnable", lambda d: None)
    return d


class _Bridge:
    """Records every start_pipeline — the launch path's only side effect."""

    def __init__(self):
        self.calls: list[dict] = []

    async def start_pipeline(self, plan, **kw):
        self.calls.append({"plan": plan, **kw})
        return SimpleNamespace(pipeline_id="pl_test", status="running", step_state={})


async def test_temporal_refuses_a_task_leg_before_any_record(installed, monkeypatch):
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    bridge = _Bridge()
    with pytest.raises(PL.RuntimeOwnerRefusedError) as e:
        await PL.launch_pipeline("ownerpack/with-task", bridge=bridge)
    msg = str(e.value)
    assert "'check'" in msg and "task:" in msg and "monitor/threshold" in msg
    assert "CAIRN_RUNTIME=inprocess" in msg and "CAIRN_RUNTIME=dbos" in msg
    assert isinstance(e.value, PolicyDeniedError)  # every surface answers it 403 / exit 3
    assert bridge.calls == []


async def test_temporal_refuses_a_finalizer_step_before_any_record(installed, monkeypatch):
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    bridge = _Bridge()
    with pytest.raises(PL.RuntimeOwnerRefusedError, match="'teardown' is a finalizer"):
        await PL.launch_pipeline("ownerpack/with-finalizer", bridge=bridge)
    assert bridge.calls == []


async def test_temporal_launches_a_flow_only_pipeline_as_before(installed, monkeypatch):
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    bridge = _Bridge()
    await PL.launch_pipeline("ownerpack/flow-only", bridge=bridge)
    assert len(bridge.calls) == 1


@pytest.mark.parametrize("runtime", ["inprocess", "dbos"])
async def test_other_runtimes_launch_task_legs_and_finalizers(installed, monkeypatch, runtime):
    monkeypatch.setenv("CAIRN_RUNTIME", runtime)
    bridge = _Bridge()
    await PL.launch_pipeline("ownerpack/with-task", bridge=bridge)
    await PL.launch_pipeline("ownerpack/with-finalizer", bridge=bridge)
    assert len(bridge.calls) == 2


async def test_the_refusal_reads_the_owners_declaration(installed, monkeypatch):
    """When the owner learns task legs and finalizers (#59), the door lets them through."""
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    monkeypatch.setattr(owner, "PIPELINE_OWNER_LEG_KINDS", owner.PIPELINE_OWNER_LEG_KINDS | {"task"})
    monkeypatch.setattr(owner, "PIPELINE_OWNER_RUNS_FINALIZERS", True)
    bridge = _Bridge()
    await PL.launch_pipeline("ownerpack/with-task", bridge=bridge)
    await PL.launch_pipeline("ownerpack/with-finalizer", bridge=bridge)
    assert len(bridge.calls) == 2


def _app(bridge):
    from fastapi import FastAPI

    from cairn.core.triggers.routers.pipelines import build_pipelines_router
    from cairn.core.triggers.routers.start import build_start_router

    app = FastAPI()
    disp = SimpleNamespace(pipeline_bridge=bridge)
    app.include_router(build_start_router(disp, bridge))
    app.include_router(build_pipelines_router(disp, bridge))
    return app


@pytest.mark.parametrize("path,body", [
    ("/start", {"ref": "pipeline:ownerpack/with-task"}),
    ("/pipelines/launch", {"ref": "ownerpack/with-task"}),
])
def test_both_http_doors_answer_403(installed, monkeypatch, path, body):
    from fastapi.testclient import TestClient

    for k in ("OBS_REQUIRE_AUTH", "CAIRN_REQUIRE_AUTH"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    bridge = _Bridge()
    with TestClient(_app(bridge)) as c:
        r = c.post(path, json=body)
    assert r.status_code == 403, r.text
    assert "monitor/threshold" in str(r.json()["detail"])
    assert bridge.calls == []


def test_the_cli_exits_3(installed, monkeypatch, tmp_path):
    from cairn.cli.commands import pipeline as cli_pipeline
    from cairn.core.dispatch import pipeline_bridge as PB

    monkeypatch.setenv("OBS_STORAGE", f"sqlite://{tmp_path / 'cli.db'}")
    monkeypatch.setenv("OBS_RUN_DB", str(tmp_path / "runs.db"))
    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    started = []

    async def _never_start(self, plan, **kw):  # pragma: no cover — must not be reached
        started.append(plan)

    monkeypatch.setattr(PB.PipelineBridge, "start_pipeline", _never_start)
    r = CliRunner().invoke(cli_pipeline.app, ["run", "pipeline:ownerpack/with-task"])
    assert r.exit_code == 3, r.output
    assert "monitor/threshold" in r.output and started == []


async def test_explain_reports_the_refusal_by_the_doors_function(installed, monkeypatch):
    from cairn.core.engine.pipeline_explain import explain_pipeline

    monkeypatch.setenv("CAIRN_RUNTIME", "temporal")
    d = PL.resolve_or_raise("ownerpack/with-task")
    doc = await explain_pipeline(d, workspace_variables={})
    assert any("the runtime would refuse this launch" in p and "'check'" in p
               for p in doc["problems"])
    monkeypatch.setenv("CAIRN_RUNTIME", "inprocess")
    doc = await explain_pipeline(d, workspace_variables={})
    assert not any("the runtime would refuse" in p for p in doc["problems"])
