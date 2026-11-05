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
import yaml

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


# ── tiny-finetune's loss gate reads a loss over rows the adapter did not train on (#252) ──

def _tiny_finetune_def():
    return parse_pipeline_def((PACK_DIR / "pipelines" / "tiny-finetune.yaml").read_text(),
                              pack="hello-cairn")


def test_the_shipped_training_rows_are_distinct_so_no_held_out_row_is_also_a_training_row():
    """The dataset had 10 distinct rows repeated to 48, so all 6 held-out rows also sat among
    the 42 training rows. The trainer's split (`split_holdout`) is the one the stage uses."""
    from operator_train_local import finetune as F

    defaults = _tiny_finetune_def().inputs
    dataset = PACK_DIR / "data" / "train.jsonl"
    texts = F.load_texts(str(dataset), int(defaults["max_samples"]["default"]))
    assert len(texts) == 48 and len(set(texts)) == len(texts)
    train, held = F.split_holdout(texts)
    assert (len(train), len(held)) == (42, 6)
    assert not set(train) & set(held)


def test_the_trainers_split_leaves_no_held_out_row_among_the_training_rows(tmp_path):
    """The fixture the gate's figure rests on: distinct rows split into disjoint train and
    held-out sets, the held-out rows being the last `len // 8`."""
    from operator_train_local import finetune as F

    rows = tmp_path / "rows.jsonl"
    rows.write_text("".join(json.dumps({"text": f"row {n}"}) + "\n" for n in range(16)), encoding="utf-8")
    texts = F.load_texts(str(rows), 16)
    train, held = F.split_holdout(texts)
    assert held == ["row 14", "row 15"] and train == texts[:14]
    assert not set(train) & set(held)


def test_the_loss_gate_reads_the_trainers_held_out_loss_and_nothing_scores_training_rows(hello_pack):
    """`loss` is wired to the `train.lora` stage's own `eval_loss`, which `finetune()` computes
    over the held-out rows only. The `eval` leg (train/eval-loss scores the first `max_samples`
    rows with no split) is gone, so nothing in the pipeline scores rows the adapter trained on."""
    from types import SimpleNamespace

    from cairn.core.dispatch.pipeline_bridge import _flow_leg_output
    from cairn.core.engine.expr import Scopes, resolve_value

    d = _tiny_finetune_def()
    steps = {s.id or s.gate: s for s in d.steps}
    assert "eval" not in steps
    assert not [s.id for s in d.steps if s.task == "train/eval-loss"]
    gate = steps["loss-ok"]
    assert gate.depends_on == ["train"]
    assert "steps.train.findings.train_lora_eval_loss" in gate.predicate
    # that spelling resolves against the stats the runner reports for the flow leg
    plan = {s.id: s for s in plan_pipeline(d).steps}
    stats = {"train.lora.adapter_path": "/w/adapter", "train.lora.output": "/w/adapter",
             "train.lora.train_loss": 3.2, "train.lora.eval_loss": 3.4}
    out = _flow_leg_output(plan["train"], SimpleNamespace(ok=True, stats=stats))
    gated = resolve_value("$steps.train.train_lora_eval_loss", Scopes(state={}, steps={"train": out}), source="test")
    assert gated == 3.4                      # the held-out figure, not train_loss
    assert d.outputs["eval_loss"] == "train.train_lora_eval_loss"
    assert "train.train_lora_eval_loss" in steps["review"].show


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


# ── the Quickstart's data comes with the pack (#317) ────────────────────


def test_the_sample_preset_is_in_the_wheel_and_names_a_file_in_the_pack():
    pyproject = (PACK_DIR / "pyproject.toml").read_text()
    assert '"presets" = "cairn_pack_hello/presets"' in pyproject and '"data" = "cairn_pack_hello/data"' in pyproject
    preset = yaml.safe_load((PACK_DIR / "presets" / "profile-and-publish.yaml").read_text())
    assert preset["pipeline"] == "hello-cairn/profile-and-publish"
    sample = next(p for p in preset["presets"] if p["name"] == "sample")
    assert (PACK_DIR / sample["inputs"]["path"].removeprefix("$pack/")).is_file()


def test_the_quickstarts_download_no_sample_data():
    for page in (PACK_DIR.parents[1] / "README.md", PACK_DIR.parents[1] / "docs/site/src/content/docs/quickstart.md"):
        text = page.read_text()
        assert "data/sample.jsonl" not in text and "--preset sample" in text


# ── where tiny-finetune's adapter lives (#253) ──────────────────────────


def test_tiny_finetune_can_keep_its_adapter_outside_the_temp_workdir():
    pipeline = _tiny_finetune_def()
    train = next(s for s in pipeline.steps if s.id == "train")
    assert train.inputs["output_dir"] == "$input.output_dir"
    flow = parse_flow_def((PACK_DIR / "flows" / "lora.yaml").read_text(), where="lora.yaml")
    assert flow.stages[0].config["output_dir"] == "$config.output_dir"


def test_the_readme_says_where_the_adapter_lives():
    readme = (PACK_DIR / "README.md").read_text()
    assert "temp workdir" in readme and "-i output_dir=" in readme


def test_an_empty_output_dir_means_the_workdir(tmp_path):
    from operator_train_local.stages import train_lora

    data = PACK_DIR / "data" / "train.jsonl"
    out = train_lora(tmp_path, {"dataset": str(data), "base_model": "m", "output_dir": "", "mock": True}, [])
    assert out["mock"] is True

