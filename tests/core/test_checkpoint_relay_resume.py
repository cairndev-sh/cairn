"""#1248 H6b/H6c (ADR-0088) — checkpoint relay + resume selection.

The control-plane halves over the H6a runner: relay copies checkpoints into
durable storage + registers them; selection finds the newest one for a spec
key and builds ``resume_from``. The finale is the LOCAL analog of the H6d
acceptance: run, relay, DESTROY the original workdir ("the box died"), and
resume in a fresh workdir running only the remaining stages — through the
registry, not through any surviving box state.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import pytest

from cairn.core.dataflow import Budget, DataflowSpec, StageRef
from cairn.core.dataflow.checkpoints import (
    find_resume_checkpoint,
    relay_local_checkpoints,
    spec_key,
)
from cairn.core.storage.artifacts import SqliteArtifactStore

import cairn.core.dataflow.runner as runner_mod
from cairn.core.dataflow.runner import run_dataflow


CALLS: list[str] = []


def _writer(tag: str):
    def stage(workdir, config, inputs):
        CALLS.append(tag)
        (workdir / "current.jsonl").write_text(
            f'{{"from": "{tag}"}}\n', encoding="utf-8")
        return {"wrote": tag}
    return stage


@pytest.fixture(autouse=True)
def _stages(monkeypatch):
    CALLS.clear()
    monkeypatch.setattr(runner_mod, "load_stages", lambda: {
        "w.one": _writer("one"), "w.two": _writer("two")})


@pytest.fixture()
async def store():
    s = SqliteArtifactStore(path=":memory:")
    await s.ensure_schema()
    yield s
    await s.close()


def _spec() -> DataflowSpec:
    return DataflowSpec(stages=[
        StageRef(stage="w.one", checkpoint=True),
        StageRef(stage="w.two"),
    ], budget=Budget())


def test_spec_key_mirrors_the_runner(tmp_path):
    """ONE identity: the selection key must equal what the runner stamps
    into every checkpoint manifest."""
    run_dataflow(_spec(), tmp_path)
    manifest = json.loads(
        (tmp_path / "checkpoints" / "stage-00" / "manifest.json").read_text())
    assert manifest["spec_key"] == spec_key(_spec())


async def test_relay_registers_and_is_idempotent(tmp_path, store):
    wd = tmp_path / "wd"
    run_dataflow(_spec(), wd)
    root = tmp_path / "durable"
    new = await relay_local_checkpoints(wd, run_id="r1", store=store,
                                        dest_root=root)
    assert len(new) == 1 and new[0]["stage"] == "w.one"
    assert (root / "r1" / "stage-00" / "current.jsonl").is_file()
    recs = await store.list_artifacts(kind="dataset")
    assert len(recs) == 1 and recs[0].stats["checkpoint"] is True
    assert recs[0].stats["spec_key"] == spec_key(_spec())
    # second tick: nothing new, nothing duplicated
    assert await relay_local_checkpoints(wd, run_id="r1", store=store,
                                         dest_root=root) == []
    assert len(await store.list_artifacts(kind="dataset")) == 1


async def test_selection_finds_newest_and_misses_cleanly(tmp_path, store):
    wd = tmp_path / "wd"
    run_dataflow(_spec(), wd)
    await relay_local_checkpoints(wd, run_id="r1", store=store,
                                  dest_root=tmp_path / "durable")
    rf = await find_resume_checkpoint(store, spec_key(_spec()))
    assert rf is not None and rf.stage_index == 0
    assert rf.artifacts[0]["dest"] == "current.jsonl"
    # #1616 — assert the SUBSET under test, not the whole dict. `==` here meant
    # "stats contains exactly this and nothing else", so #1465 adding
    # `artifacts_bytes`/`artifacts_count` to stats broke a relay test that does
    # not care what else stats carries — it cares that `wrote` survived the
    # relay. An exact-equality assert on a payload dict is a landmine that fires
    # months later, in a file the person adding the field has no reason to read.
    assert rf.completed[-1]["stats"]["wrote"] == "one"
    assert await find_resume_checkpoint(store, "no-such-key") is None


async def test_box_death_resume_via_the_registry(tmp_path, store):
    """The local H6d analog: original workdir DESTROYED after relay; a fresh
    workdir resumes purely from durable state and runs ONLY stage two."""
    import shutil

    wd_a = tmp_path / "box-a"
    run_dataflow(_spec(), wd_a)
    await relay_local_checkpoints(wd_a, run_id="r1", store=store,
                                  dest_root=tmp_path / "durable")
    shutil.rmtree(wd_a)                       # the box died
    assert CALLS == ["one", "two"]

    CALLS.clear()
    spec = _spec()
    spec.resume_from = await find_resume_checkpoint(store, spec_key(spec))
    assert spec.resume_from is not None
    res = run_dataflow(spec, tmp_path / "box-b")
    assert CALLS == ["two"]                   # stage one never re-ran
    assert res.stats["resumed_from_stage"] == 0
    assert (tmp_path / "box-b" / "current.jsonl").read_text() == '{"from": "two"}\n'


async def test_engine_rewritten_spec_still_finds_its_checkpoint(tmp_path, store):
    """#1314 — the SSH engine REWRITES stage configs at submit
    (`_localize_inputs`: abs local file paths → uploaded + basenames), so the
    spec the runner hashes is not the spec the control plane hashed. The
    carried ``checkpoint_key`` makes identity control-plane-owned: the
    executor stamps it BEFORE submit, every engine-side rewrite is free, and
    the manifest key equals the lookup key.
    """
    f = tmp_path / "in.jsonl"
    f.write_text("{}\n", encoding="utf-8")
    spec = DataflowSpec(stages=[
        StageRef(stage="w.one", checkpoint=True, config={"path": str(f)}),
        StageRef(stage="w.two"),
    ], budget=Budget())

    key = spec_key(spec)                # control-side lookup key
    spec.checkpoint_key = key           # what the executor stamps pre-submit

    # what _localize_inputs does on the SSH path: abs file path → basename
    localized = spec.model_copy(update={"stages": [
        spec.stages[0].model_copy(update={"config": {"path": f.name}}),
        spec.stages[1],
    ]})
    assert localized.model_dump_json() != spec.model_dump_json()

    wd = tmp_path / "wd"
    run_dataflow(localized, wd)         # the box runs the REWRITTEN spec
    manifest = json.loads(
        (wd / "checkpoints" / "stage-00" / "manifest.json").read_text())
    assert manifest["spec_key"] == key  # stamped with the CARRIED key

    await relay_local_checkpoints(wd, run_id="r1", store=store,
                                  dest_root=tmp_path / "durable")
    rf = await find_resume_checkpoint(store, key)
    assert rf is not None and rf.stage_index == 0

    # and the resumed submission (carrying the same key) accepts it
    resumed = localized.model_copy(update={"resume_from": rf})
    res = run_dataflow(resumed, tmp_path / "wd2")
    assert res.stats["resumed_from_stage"] == 0


def test_checkpoint_key_is_excluded_from_the_hash():
    """Stamping the key must not change the key — and specs WITHOUT a stamp
    keep their pre-#1314 hashes (already-registered checkpoints stay valid)."""
    bare = _spec()
    stamped = _spec()
    stamped.checkpoint_key = spec_key(bare)
    assert spec_key(stamped) == spec_key(bare)


def test_sdk_and_yaml_declare_checkpoint(tmp_path):
    """Reachability: the declaration survives SDK → YAML → FlowDef → StageRef."""
    import yaml as _yaml

    pytest.importorskip("cairn_sdk", reason="the authoring SDK is the platform's (ADR-0135)")
    from cairn_sdk import compile_pipeline, flow, inp, pipeline

    @pipeline(series="h6", pack="t1248-h6")
    class F:
        class Inputs:
            dataset: str = "d.jsonl"

        clean = (flow.data("clean", path=inp.dataset)
                 .stage("noop", checkpoint=True)
                 .stage("noop.slow"))

    compiled = compile_pipeline(F)
    doc = _yaml.safe_load(compiled.flows["clean"])
    assert doc["stages"][0]["checkpoint"] is True
    assert "checkpoint" not in doc["stages"][1]

    from cairn.core.dataflow.defs import FlowDef
    d = FlowDef.model_validate(doc)
    assert d.stages[0].checkpoint is True


# ── resume stays inside the leg's workspace ──────────────────────────────────


class _Runs:
    """run_id → workspace, as the run store records it."""

    def __init__(self, workspaces: dict[str, str]) -> None:
        self.workspaces = workspaces

    async def get(self, run_id, *a, **kw):
        from types import SimpleNamespace

        ws = self.workspaces.get(run_id)
        return None if ws is None else SimpleNamespace(id=run_id, workspace_id=ws)


def _hooks(store, runs, run_id):
    from cairn.core.dispatch.leg_hooks import FlowLegHooks

    async def _emit(*a, **kw):
        return None

    return FlowLegHooks(pipeline_id="p", step_id="s", leg_ref="flow:x", run_id=run_id,
                        pipeline_store=None, run_store=runs, emit=_emit, artifact_store=store)


async def test_a_leg_resumes_only_from_its_own_workspaces_checkpoint(tmp_path, store):
    """The spec key is a hash of the spec, so two workspaces running the same
    spec share it. A leg in one workspace never resumes from a checkpoint a run
    of another workspace relayed; in its own workspace it does."""
    wd = tmp_path / "wd"
    run_dataflow(_spec(), wd)
    await relay_local_checkpoints(wd, run_id="run-a", store=store, dest_root=tmp_path / "durable",
                                  workspace_id="ws-a")
    runs = _Runs({"run-a": "ws-a", "leg-a": "ws-a", "leg-b": "ws-b"})
    key = spec_key(_spec())
    assert await _hooks(store, runs, "leg-b").resume_provider()(key) is None
    rf = await _hooks(store, runs, "leg-a").resume_provider()(key)
    assert rf is not None and rf.stage_index == 0


async def test_identical_checkpoint_bytes_resume_from_each_workspaces_own_copy(tmp_path, store):
    """Identical bytes are one content-addressed identity, but each workspace's
    relay is its own registration (ADR-0148): its own copy and its own producing
    run. Each workspace resumes from its own copy, never the other's."""
    for run_id, ws in (("run-a", "ws-a"), ("run-b", "ws-b")):
        wd = tmp_path / run_id
        run_dataflow(_spec(), wd)
        await relay_local_checkpoints(wd, run_id=run_id, store=store, dest_root=tmp_path / "durable",
                                      workspace_id=ws)
    for ws in ("ws-a", "ws-b"):
        assert len(await store.list_artifacts(kind="dataset", workspace_id=ws)) == 1
    assert await store.list_artifacts(kind="dataset", workspace_id="ws-c") == []
    runs = _Runs({"run-a": "ws-a", "run-b": "ws-b"})
    key = spec_key(_spec())
    for ws, run_id in (("ws-a", "run-a"), ("ws-b", "run-b")):
        rf = await find_resume_checkpoint(store, key, workspace_id=ws,
                                          run_workspace=lambda r: _ws(runs, r))
        assert rf is not None
        assert all(f"/durable/{run_id}/" in a["uri"] for a in rf.artifacts), rf.artifacts


async def test_a_leg_whose_producer_or_own_run_is_unreadable_starts_fresh(tmp_path, store):
    wd = tmp_path / "wd"
    run_dataflow(_spec(), wd)
    # relayed in the leg's own workspace, so only the unreadable-producer rule can refuse it
    await relay_local_checkpoints(wd, run_id="run-gone", store=store, dest_root=tmp_path / "durable",
                                  workspace_id="ws-a")
    assert len(await store.list_artifacts(kind="dataset", workspace_id="ws-a")) == 1
    key = spec_key(_spec())
    assert await _hooks(store, _Runs({"leg-a": "ws-a"}), "leg-a").resume_provider()(key) is None
    assert await _hooks(store, _Runs({"run-gone": "ws-a"}), "leg-x").resume_provider()(key) is None


async def _ws(runs, run_id):
    run = await runs.get(run_id)
    return None if run is None else run.workspace_id
