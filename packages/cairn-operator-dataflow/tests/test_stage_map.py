"""#1360 (ADR-0090, G4 #1290) — `map:` stage fan-out through the #962 pool.

The record stays a LINEAR stage list; one stage fans out over a runtime list
of independent items (manifest shards or an explicit bounded list), dispatched
through the existing `map_shards_local` process pool, fanned back in by a
declared merge. Per-item `.done` markers make the fan-out resume-safe at item
granularity. Distinct from #1366's coordinated gang — the contract refuses a
stage carrying both.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from cairn.core import obs
from cairn.core.dataflow import DataflowSpec, StageMap, StageRef
from cairn.core.dataflow import stage_loader
from cairn.core.operator import batch
from cairn.core.dataflow import runner

from cairn.core.dataflow.runner import (
    BudgetExceeded,
    _item_marker,
    _merge_map_stats,
    _plan_map_items,
    _run_mapped_stage,
    run_dataflow,
)


def _mapped_ref(**map_kw) -> StageRef:
    return StageRef(stage="noop", config={"files": ["a", "b", "c"]},
                    map=StageMap(over="$config.files", **map_kw))


# ── grammar ──────────────────────────────────────────────────────────────────

def test_map_block_parses_with_defaults():
    ref = _mapped_ref()
    assert ref.map.as_ == "item"
    assert ref.map.max_parallel == 4
    assert ref.map.merge == "stats_sum"


def test_map_and_gang_cannot_combine():
    """ADR-0090 §2: mapped items never rendezvous; a gang is nothing but
    rendezvous. Refused at the CONTRACT, so no engine ever sees it."""
    with pytest.raises(Exception, match="coordinated gang"):
        StageRef(stage="train.qlora", map=StageMap(),
                 resources={"workers": 2})


# ── item planning ────────────────────────────────────────────────────────────

def test_config_list_items(tmp_path):
    assert _plan_map_items(_mapped_ref(), tmp_path) == ["a", "b", "c"]


def test_non_list_wraps_and_missing_is_empty(tmp_path):
    """MapBTNode semantics, kept identical across kinds on purpose."""
    one = StageRef(stage="noop", config={"files": "solo"},
                   map=StageMap(over="$config.files"))
    assert _plan_map_items(one, tmp_path) == ["solo"]
    none = StageRef(stage="noop", map=StageMap(over="$config.files"))
    assert _plan_map_items(none, tmp_path) == []


def test_shards_mode_without_a_manifest_is_a_named_error(tmp_path):
    ref = StageRef(stage="noop", map=StageMap(over="shards"))
    with pytest.raises(ValueError, match="manifest.json"):
        _plan_map_items(ref, tmp_path)


def test_unknown_over_is_refused(tmp_path):
    ref = StageRef(stage="noop", map=StageMap(over="stdin"))
    with pytest.raises(ValueError, match="'shards' or"):
        _plan_map_items(ref, tmp_path)


# ── merge ────────────────────────────────────────────────────────────────────

PARTS = [{"rows": 2, "ok": True, "name": "x"}, {"rows": 3}, {}]


def test_stats_sum_sums_numerics_only():
    out = _merge_map_stats(_mapped_ref(), PARTS, kernel_fn=object())
    assert out["rows"] == 5
    assert out["items_total"] == 3
    assert "ok" not in out and "name" not in out, "bools/strings must not sum"


def test_concat_keeps_the_per_item_stats():
    out = _merge_map_stats(_mapped_ref(merge="concat"), PARTS, kernel_fn=object())
    assert out["rows"] == 5
    assert out["items"][1] == {"rows": 3}


def test_kernel_declared_merge_is_an_attribute_on_the_kernel():
    def kernel(workdir, config, inputs):  # pragma: no cover — shape only
        return {}

    kernel.pick_max = lambda parts: {"rows": max(p.get("rows", 0) for p in parts)}
    out = _merge_map_stats(_mapped_ref(merge="pick_max"), PARTS, kernel_fn=kernel)
    assert out["rows"] == 3
    assert out["items_total"] == 3


def test_unknown_merge_is_refused():
    with pytest.raises(ValueError, match="stats_sum"):
        _merge_map_stats(_mapped_ref(merge="nope"), PARTS, kernel_fn=object())


# ── the real thing: run_dataflow with a mapped stage through the pool ───────

def test_mapped_stage_runs_items_through_the_pool(tmp_path):
    spec = DataflowSpec(stages=[_mapped_ref(max_parallel=2)])
    result = run_dataflow(spec, tmp_path)
    assert result.stats["noop.items_done"] == 3
    assert result.stats["noop.items_total"] == 3
    # per-item markers exist alongside the stage's own merged marker
    for j in range(3):
        assert _item_marker(tmp_path, 0, "noop", j).exists()
    # each item ran in its OWN subdir — a shared filesystem cannot interleave
    assert (tmp_path / "map-00" / "item-002").is_dir()


def test_item_markers_make_the_fanout_resume_safe(tmp_path):
    """A pre-existing item marker is trusted, not re-run — its (sentinel)
    stats survive into the merge, proving the skip read the marker."""
    _item_marker(tmp_path, 0, "noop", 1).write_text(
        json.dumps({"rows": 7}), encoding="utf-8")
    spec = DataflowSpec(stages=[_mapped_ref()])
    result = run_dataflow(spec, tmp_path)
    assert result.stats["noop.rows"] == 7      # noop items contribute 0 each
    assert result.stats["noop.items_done"] == 3


def test_zero_items_runs_the_merge_over_nothing(tmp_path):
    spec = DataflowSpec(stages=[
        StageRef(stage="noop", map=StageMap(over="$config.files"))])
    result = run_dataflow(spec, tmp_path)
    assert result.stats["noop.items_total"] == 0


def test_budget_breach_lands_between_waves(tmp_path):
    """The check has a boundary INSIDE the fan-out — an exhausted budget
    stops before dispatching the next wave, naming the item progress."""
    ref = _mapped_ref(max_parallel=1)
    spec = DataflowSpec.model_validate(
        {"stages": [json.loads(ref.model_dump_json())],
         "budget": {"max_seconds": 1}})
    with pytest.raises(BudgetExceeded, match="mid-fan-out"):
        _run_mapped_stage({"noop": object()}, spec.stages[0], 0, tmp_path,
                          spec, started=time.monotonic() - 10, on_progress=None)


def test_progress_carries_item_counts(tmp_path):
    seen: list[dict] = []
    spec = DataflowSpec(stages=[_mapped_ref(max_parallel=1)])
    run_dataflow(spec, tmp_path, on_progress=seen.append)
    with_items = [p for p in seen if "items_total" in p]
    assert with_items, "D8a additive keys must surface item progress"
    assert with_items[-1]["items_done"] == 3


def test_failed_mapped_item_keeps_cost_from_completed_and_failing_items(tmp_path, monkeypatch):
    calls = 0

    def paid_item(wd, config, inputs):
        nonlocal calls
        calls += 1
        obs.record_llm("gpt-4o-mini", 10, 5, 0.2, usd=0.2)
        if calls == 2:
            raise RuntimeError("mapped item failed after its paid call")
        return {}

    monkeypatch.setattr(stage_loader, "load_stages", lambda: {"paid": paid_item})
    monkeypatch.setattr(runner, "load_stages", lambda: {"paid": paid_item})

    def run_inline(fn, tasks, *, requested_workers):
        return [fn(task) for task in tasks]

    monkeypatch.setattr(batch, "map_shards_local", run_inline)
    spec = DataflowSpec(stages=[
        StageRef(stage="paid", config={"items": [1, 2]},
                 map=StageMap(over="$config.items")),
    ])

    with pytest.raises(RuntimeError, match="paid call"):
        runner.run_dataflow(spec, tmp_path)

    metrics = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["cost_usd"] == pytest.approx(0.4)
    assert metrics["items_done"] == 0
    assert metrics["items_total"] == 2

    calls = 0

    original_write_metrics = runner._write_metrics
    writes = 0

    def fail_metrics_write(workdir, payload):
        nonlocal writes
        writes += 1
        if writes > 1:
            raise OSError("progress disk unavailable")
        original_write_metrics(workdir, payload)

    monkeypatch.setattr(runner, "_write_metrics", fail_metrics_write)
    with pytest.raises(RuntimeError, match="mapped item failed after its paid call"):
        runner.run_dataflow(spec, tmp_path / "progress-write-fails")
