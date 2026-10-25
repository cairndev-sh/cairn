"""ADR-0141 — a stage's named outputs: a split's train, val and test files as three registered
products from one stage, each with the role it is for.

The runner turns ``outputs: [<name>, …]`` plus flat ``outputs.<name>.<field>`` stats into one
descriptor per name in ``DataflowResult.named_outputs``, siblings of the stage's own descriptor:
they share its parents, ``output_artifacts`` keeps one descriptor per stage (what the stage
contract re-check reads by position), and the next stage still descends from the stage. The leg
registers each with its ``register_role``, which is what the holdout door reads; a stage may give
only training / validation / test. Hermetic: in-process stages, sqlite, a fake engine."""
from __future__ import annotations

import json
import os
import types

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import cairn.core.dataflow.runner as runner_mod
from cairn.core.dataflow import Budget, DataflowSpec, StageRef
from cairn.core.dataflow.conformance_check import descriptor_problems
from cairn.core.dataflow.contract import DataflowResult
from cairn.core.dataflow.runner import run_dataflow
from cairn.core.storage.artifacts import SqliteArtifactStore

_SPLITS = {"train": "training", "val": "validation", "test": "test"}


def _ingest(workdir, config, inputs):
    (workdir / "current.jsonl").write_text('{"q": 1}\n{"q": 2}\n{"q": 3}\n', encoding="utf-8")
    return {"rows": 3, "content_hash": "h-ingest"}


def _split(workdir, config, inputs):
    stats: dict = {"rows": 3, "outputs": list(_SPLITS)}
    for name, role in _SPLITS.items():
        (workdir / f"{name}.jsonl").write_text('{"q": 1}\n', encoding="utf-8")
        stats.update({f"outputs.{name}.output": f"{name}.jsonl",
                      f"outputs.{name}.content_hash": f"h-{name}",
                      f"outputs.{name}.rows": 1,
                      f"outputs.{name}.register_as": f"facts-{name}",
                      f"outputs.{name}.register_role": config.get("role", role)})
    return stats


def _after(workdir, config, inputs):
    return {"rows": 3}


@pytest.fixture(autouse=True)
def _stages(monkeypatch):
    def broken(workdir, config, inputs):
        return {"outputs": ["train"], "outputs.train.output": "train.jsonl"}

    def not_a_list(workdir, config, inputs):
        return {"outputs": "train", "outputs.train.output": "train.jsonl"}

    def own_stat(workdir, config, inputs):
        return {"outputs": ["s3://b/model.safetensors"]}

    monkeypatch.setattr(runner_mod, "load_stages", lambda: {
        "t.ingest": _ingest, "t.split": _split, "t.after": _after,
        "t.broken": broken, "t.not_a_list": not_a_list, "t.own_stat": own_stat})


def _run(tmp_path, *stages: str, split_config: dict | None = None) -> DataflowResult:
    spec = DataflowSpec(stages=[StageRef(stage=s, config=(split_config or {}) if s == "t.split" else {})
                                for s in stages], budget=Budget())
    return run_dataflow(spec, tmp_path / "wd")


def test_each_named_output_is_a_sibling_of_its_stage(tmp_path):
    result = _run(tmp_path, "t.ingest", "t.split", "t.after")
    ingest, split, after = result.output_artifacts
    train, val, test = result.named_outputs
    assert [d.content_hash for d in (train, val, test)] == ["h-train", "h-val", "h-test"]
    for d in (train, val, test):
        assert d.parents == split.parents == ["h-ingest"]
        assert d.kind == "dataset" and d.rows == 1
        assert d.uri.startswith("file://") and d.uri.endswith(f"/wd/{d.stats['output_name']}.jsonl")
    assert test.stats["register_role"] == "test" and test.stats["register_as"] == "facts-test"
    assert after.parents == [split.content_hash]


def test_the_stage_contract_recheck_still_reads_one_descriptor_per_stage(tmp_path):
    """The leg's D5 re-check (unchanged) indexes `output_artifacts` by stage; named outputs do
    not shift it."""
    result = _run(tmp_path, "t.ingest", "t.split", "t.after")
    contracts = {"stages": {"1": {"stage": "t.split", "contract": {"output": "o"},
                                  "schemas": {"o": {"type": "object", "required": ["rows", "outputs"]}}}}}
    assert descriptor_problems(contracts, 3, result.output_artifacts) == []


def test_a_result_without_named_outputs_serialises_as_before(tmp_path):
    result = _run(tmp_path, "t.ingest", "t.after")
    assert result.named_outputs == [] and "named_outputs" not in result.model_dump()
    assert "named_outputs" in _run(tmp_path, "t.ingest", "t.split").model_dump()


def test_an_outputs_stat_of_a_kernels_own_is_left_alone(tmp_path):
    assert _run(tmp_path, "t.ingest", "t.own_stat").named_outputs == []


@pytest.mark.parametrize("stage, message", [
    ("t.broken", "needs outputs.train.output and outputs.train.content_hash"),
    ("t.not_a_list", "must be a list of distinct names"),
])
def test_a_malformed_named_output_fails_the_flow(tmp_path, stage, message):
    with pytest.raises(ValueError, match=message):
        _run(tmp_path, "t.ingest", stage)


class _Engine:
    """Succeeds at once and returns the result a box wrote, through its JSON form."""

    name = "fake"

    def __init__(self, result: DataflowResult) -> None:
        self._payload = json.loads(result.model_dump_json())

    def validate(self, spec):
        return []

    async def submit(self, spec, ctx):
        return types.SimpleNamespace(job_id="j1", engine=self.name, meta={})

    async def poll(self, handle):
        return types.SimpleNamespace(state="succeeded", metrics={}, error="", retriable=False)

    async def cancel(self, handle):
        pass

    async def result(self, handle):
        return DataflowResult.model_validate(self._payload)


async def _leg(monkeypatch, tmp_path, result: DataflowResult):
    from cairn.core.dispatch.leg_executor import execute_leg

    monkeypatch.setattr("cairn.core.dataflow.defs.load_flow_defs", lambda: {
        "demo/prepare": types.SimpleNamespace(kind="data")})
    monkeypatch.setattr("cairn.core.dataflow.defs.resolve_flow_ref",
                        lambda ref, cfg, kind="data": [StageRef(stage="t.split")])
    monkeypatch.setattr("cairn.core.dispatch.leg_executor._POLL_INTERVAL_S", 0.01)
    store = SqliteArtifactStore(str(tmp_path / "a.db"))
    out = await execute_leg("flow:demo/prepare", inputs={}, engine=_Engine(result), run_id="r1",
                            artifact_store=store)
    return out, store


async def test_the_leg_registers_each_named_output_with_its_role(monkeypatch, tmp_path):
    out, store = await _leg(monkeypatch, tmp_path, _run(tmp_path, "t.ingest", "t.split"))
    assert out.state == "succeeded", out.error
    assert out.stats["dataset_versions"] == ["facts-train@1", "facts-val@1", "facts-test@1"]
    for name, role in _SPLITS.items():
        assert (await store.get_dataset_version_meta(f"facts-{name}"))["role"] == role
    assert [v["name"] for v in await store.list_dataset_versions_by_role(["test"])] == ["facts-test"]
    await store.close()


async def test_a_named_model_never_displaces_the_stages_candidate(tmp_path):
    from cairn.core.dataflow.contract import ArtifactDescriptor
    from cairn.core.dispatch.leg_executor import register_leg_descriptors

    own = ArtifactDescriptor(uri="s3://b/adapter", kind="model", content_hash="h-model")
    named = ArtifactDescriptor(uri="s3://b/other", kind="model", content_hash="h-named",
                               stats={"output_name": "other", "stage": "t.split"})
    store = SqliteArtifactStore(str(tmp_path / "a.db"))
    out_stats: dict = {}
    await register_leg_descriptors([own, named], store, run_id="r1", out_stats=out_stats)
    assert out_stats["candidate_artifact_uri"] == "s3://b/adapter"
    await store.close()


async def test_a_stage_cannot_declare_a_golden_version_and_nothing_registers(monkeypatch, tmp_path):
    out, store = await _leg(monkeypatch, tmp_path,
                            _run(tmp_path, "t.ingest", "t.split", split_config={"role": "golden"}))
    assert out.state == "failed" and "may give only" in out.error
    assert await store.get_dataset_version("facts-test") is None
    assert await store.get("h-test") is None
    await store.close()
