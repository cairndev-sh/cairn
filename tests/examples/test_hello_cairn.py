"""The example pack (`examples/hello-cairn`, #2423) runs on the public core alone.

The pack is what the public quickstart runs with zero private packages, so this
file drives it the way a user does: discovered through ``CAIRN_PACK_PATH`` (no
install, no entry point), parsed by the real pipeline and flow parsers, launched
through the ONE launch door (``launch_pipeline``) on a real dispatcher + bridge,
with the real builtin operators (`data/profile`, `storage/put`) and the builtin
dataflow engine for the flow leg. Nothing is mocked; no key, no network.

What it pins, in the order the pipeline reaches it:
  * the data gate passes with verdict ``data`` (answered by the profile's output);
  * the summary is redacted before anything stores it;
  * the person gate pauses the pipeline (``awaiting_approval``);
  * approving it reaches ``promote``, which pauses AGAIN: artifact/promote declares
    ``requires_approval`` and a pipeline cannot remove an operator's gate;
  * approving that tags the flow leg's registered artifact, publishes the file and
    writes a hash-chained audit row, and the pipeline ends ``done``;
  * a predicate that is false STOPS the pipeline (``gate_blocked``) before
    anything is written;
  * an estimate above the cap is refused at the flow leg's reservation, before
    its engine starts (``over-budget``).
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core import pack as pack_api
from cairn.core.dataflow.defs import load_flow_defs, parse_flow_def
from cairn.core.dispatch.dispatcher import RunDispatcher
from cairn.core.dispatch.pipeline_bridge import PipelineBridge
from cairn.core.dispatch.pipeline_launch import launch_pipeline
from cairn.core.engine.pipeline import parse_pipeline_def, plan_pipeline
from cairn.core.engine.pipeline_resolve import assert_runnable
from cairn.core.pack import loader as loader_mod
from cairn.core.storage import build_storage
from cairn.core.storage.pipeline_store import InMemoryPipelineStore
from tests.core.test_trigger_server_dispatch import FakeRunner

REPO = Path(__file__).resolve().parents[2]
PACK_DIR = REPO / "examples" / "hello-cairn"
PIPELINE = "hello-cairn/profile-and-publish"
OVER_BUDGET = "hello-cairn/over-budget"


@pytest.fixture
def hello_pack(monkeypatch, tmp_path):
    """Load the example as a dev pack under the sandbox profile; restore the
    real registry afterwards so the rest of the session is untouched."""
    monkeypatch.setenv(loader_mod.DEV_PACK_PATH_ENV, str(PACK_DIR))
    monkeypatch.setenv("OBS_PROFILE", str(REPO / "src" / "cairn" / "profiles" / "sandbox.toml"))
    monkeypatch.setenv("OBS_ARTIFACTS_DIR", str(tmp_path / "artifacts"))
    # promote moves a tag in the artifact registry, which needs a store; audit/store
    # appends to CAIRN_AUDIT_LOG (default ./audit.jsonl)
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{tmp_path / 'cairn.db'}")
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(tmp_path / "audit.jsonl"))
    # the pipeline's default `path` is repo-relative, as in the quickstart
    monkeypatch.chdir(REPO)
    pack_api.clear()
    loader_mod.load_all_packs()
    yield pack_api.get_pack("hello-cairn")
    pack_api.clear()
    monkeypatch.delenv(loader_mod.DEV_PACK_PATH_ENV)
    loader_mod.load_all_packs()


# ── discovery and parsing ───────────────────────────────────────────────────

def test_the_pack_loads_from_cairn_pack_path(hello_pack):
    assert hello_pack is not None, loader_mod.pack_load_failures()
    assert Path(hello_pack.pipelines_dir) == PACK_DIR / "pipelines"
    assert Path(hello_pack.flows_dir) == PACK_DIR / "flows"
    assert not [f for f in loader_mod.pack_load_failures() if f["name"] == "hello-cairn"]


def test_every_pipeline_and_flow_parses_and_plans(hello_pack):
    pipelines = sorted((PACK_DIR / "pipelines").glob("*.yaml"))
    flows = sorted((PACK_DIR / "flows").glob("*.yaml"))
    assert pipelines and flows
    for p in pipelines:
        d = parse_pipeline_def(p.read_text(), pack="hello-cairn", source=str(p))
        assert_runnable(d)              # tiny-finetune too: train-local is a builtin (WP3)
        plan = plan_pipeline(d)
        assert plan.steps
    for f in flows:
        assert parse_flow_def(f.read_text(), where=str(f)).stages
    # and the loader addresses them by `<pack>/<name>`, the ml flow included: its `train.lora`
    # kernel is the builtin train-local pack's (WP3)
    from cairn.core.dataflow.defs import unresolvable_flow_defs

    defs = load_flow_defs()
    assert {"hello-cairn/engine-check", "hello-cairn/summarise"} <= set(defs)
    assert "hello-cairn/lora" in defs and "hello-cairn/lora" not in unresolvable_flow_defs()


def test_the_pipeline_declares_what_the_example_is_for(hello_pack):
    d = parse_pipeline_def((PACK_DIR / "pipelines" / "profile-and-publish.yaml").read_text(),
                           pack="hello-cairn")
    steps = {s.name: s for s in d.steps}
    assert d.governance["budget"] == {"usd": 1}
    assert steps["data-ok"].predicate and not steps["data-ok"].approvers   # data gate
    assert steps["review"].approvers and steps["review"].show              # person gate
    assert steps["engine-check"].flow == "hello-cairn/engine-check"   # qualified at parse
    assert [t["kind"] for t in d.triggers] == ["watch", "schedule", "webhook"]
    assert not any(t["enabled"] for t in d.triggers)   # declared, never armed by default


# ── end to end through the launch door ──────────────────────────────────────

async def _bridge():
    d = RunDispatcher(FakeRunner(), backend=build_storage())
    bridge = PipelineBridge(d, store=InMemoryPipelineStore())
    await bridge.start()
    return d, bridge


async def _wait_for(bridge, pid, states, timeout=30.0):
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        rec = await bridge.store.get(pid)
        if rec.status in states:
            return rec
        if loop.time() > deadline:
            raise AssertionError(
                f"pipeline stuck in {rec.status!r}; steps="
                + str({k: (v.status, v.verdict) for k, v in rec.step_state.items()}))
        await asyncio.sleep(0.05)


async def test_data_gate_passes_person_gate_pauses_approval_publishes(hello_pack, tmp_path):
    _, bridge = await _bridge()
    try:
        rec = await launch_pipeline(PIPELINE, inputs={}, bridge=bridge, environment="sandbox")
        rec = await _wait_for(bridge, rec.pipeline_id, ("paused", "failed"))
        st = rec.step_state
        assert rec.status == "paused", {k: (v.status, v.verdict) for k, v in st.items()}
        assert st["profile"].status == "done"
        assert (st["data-ok"].status, st["data-ok"].verdict) == ("done", "data")
        assert st["redact"].status == "done"
        assert st["engine-check"].status == "done"
        assert st["stage"].status == "done"
        assert st["review"].status == "awaiting_approval"
        assert st["publish"].status == "pending"
        assert rec.governance["budget"] == {"usd": 1}
        published = tmp_path / "artifacts" / "hello-published" / "summary"
        assert (tmp_path / "artifacts" / "hello-staging" / "summary").is_file()
        assert not published.exists()          # nothing published before a person says so

        await bridge.advance_step(rec.pipeline_id, "review", approver="alice")
        rec = await _wait_for(bridge, rec.pipeline_id, ("paused", "done", "failed"))
        assert rec.step_state["promote"].status == "awaiting_approval"   # the operator's own gate
        assert not published.exists()

        await bridge.advance_step(rec.pipeline_id, "promote", approver="alice")
        rec = await _wait_for(bridge, rec.pipeline_id, ("done", "failed"))
        assert rec.status == "done", {k: (v.status, v.verdict) for k, v in rec.step_state.items()}
        assert (rec.step_state["review"].verdict, rec.step_state["review"].approver) == (
            "approved", "alice")
        assert published.read_text().startswith("20 records")
        assert rec.step_state["audit"].status == "done"
        assert (tmp_path / "audit.jsonl").read_text().count("hello.dataset.published") == 1
    finally:
        bridge.stop()


async def test_a_false_predicate_stops_the_pipeline(hello_pack, tmp_path):
    _, bridge = await _bridge()
    try:
        rec = await launch_pipeline(PIPELINE, inputs={"min_rows": 1000}, bridge=bridge,
                                    environment="sandbox")
        rec = await _wait_for(bridge, rec.pipeline_id, ("paused", "failed", "done"))
        st = rec.step_state
        assert rec.status == "failed"
        assert st["profile"].status == "done"        # the profile itself passed ...
        assert (st["data-ok"].status, st["data-ok"].verdict) == ("skipped", "gate_blocked")
        assert st["stage"].status == "pending"       # ... and nothing after the gate ran
        assert not (tmp_path / "artifacts").exists()
    finally:
        bridge.stop()


# ── a budget refuses a run: no key, no spend ────────────────────────────────

@pytest.fixture
def engine_submits(monkeypatch):
    """Every job handed to the builtin engine. Nothing can reach it unrecorded: the real
    ``submit`` still runs, so a leg that is admitted runs for real."""
    from operator_dataflow.engines import BuiltinLocalEngine

    submits, real = [], BuiltinLocalEngine.submit

    async def spy(self, spec, ctx):
        submits.append(spec)
        return await real(self, spec, ctx)

    monkeypatch.setattr(BuiltinLocalEngine, "submit", spy)
    return submits


@pytest.fixture
def audit_rows(tmp_path, monkeypatch):
    """The real chained writer as the audit sink, as ``hello.toml``'s ``audit.sink`` binds it."""
    from cairn.core.obs import audit as audit_mod
    from operator_audit_store.operators.audit_store import AuditStoreOp

    log = tmp_path / "audit.jsonl"
    monkeypatch.setenv("CAIRN_AUDIT_LOG", str(log))
    audit_mod.reset_audit_sink()
    monkeypatch.setattr(audit_mod, "_CACHE", AuditStoreOp())
    yield lambda: [json.loads(line) for line in log.read_text().splitlines()]
    audit_mod.reset_audit_sink()


async def test_a_budget_refuses_the_run_before_its_engine_starts(hello_pack, engine_submits, audit_rows):
    """The over-budget pipeline's own file, through the launch door, with the real reservation."""
    from cairn.core.budget_reservations import held_for_run

    dispatcher, bridge = await _bridge()
    try:
        rec = await launch_pipeline(OVER_BUDGET, inputs={}, bridge=bridge, environment="sandbox")
        rec = await _wait_for(bridge, rec.pipeline_id, ("failed", "done"))
        leg = rec.step_state["engine-check"]
        assert (rec.status, leg.status) == ("failed", "failed")
        assert engine_submits == []             # the engine was never handed the job
        assert leg.handle is None               # so there is no engine job on the record
        run = await dispatcher.store.get(leg.run_id)
        assert "this run's estimate $5.00 would exceed the $1.00 cap" in run.error
        assert await held_for_run(run.id) == []
        # the refusal is the leg's: the launch was admitted, so `pipeline.refused` has no row
        actions = [r["action"] for r in audit_rows()]
        assert "pipeline.refused" not in actions and "pipeline.admitted" in actions
        row = next(r for r in audit_rows() if r["action"] == "leg.budget_refused")
        assert row["outcome"] == "deny" and row["run_id"] == run.id
        assert (row["metadata"]["estimated_usd"], row["metadata"]["budget_usd"]) == (5.0, 1.0)
    finally:
        bridge.stop()


async def test_the_same_pipeline_runs_once_its_cap_covers_the_estimate(hello_pack, engine_submits):
    """The control for the test above: only the cap differs, the leg runs, and the engine
    spy sees it. So an empty spy above means the refusal stopped it, not that the spy is blind."""
    from cairn.core.dispatch.pipeline_launch import launch_pipeline_def

    d = parse_pipeline_def((PACK_DIR / "pipelines" / "over-budget.yaml").read_text(), pack="hello-cairn")
    d = d.model_copy(update={"governance": {**d.governance, "budget": {"usd": 5}}})
    _, bridge = await _bridge()
    try:
        rec = await launch_pipeline_def(d, inputs={}, bridge=bridge, environment="sandbox")
        # a real engine subprocess, which imports the stack before it starts on the stage
        rec = await _wait_for(bridge, rec.pipeline_id, ("failed", "done"), timeout=180.0)
        assert rec.status == "done", rec.step_state["engine-check"]
        assert len(engine_submits) == 1
    finally:
        bridge.stop()
