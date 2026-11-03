"""The runner enforces every ceiling the spec declares (#1678 layer 2).

`Budget` has carried `max_cost_usd` and `max_rows` since ADR-0081 — "Hard
ceilings the engine must respect; breach → abort + failed status … a conforming
engine self-enforces these" — but only `max_seconds` was ever read. These pin
that all three now bind, and that an unenforceable cost ceiling says so instead
of letting a run finish looking guarded.
"""
from __future__ import annotations

import json

import pytest

from cairn.core import obs
from cairn.core.dataflow.contract import Budget, DataflowSpec, StageRef

from cairn.core.dataflow import runner
from cairn.core.dataflow.runner import BudgetExceeded, run_dataflow


def _spec(stages: list[StageRef], **budget) -> DataflowSpec:
    return DataflowSpec(stages=stages, budget=Budget(**budget))


@pytest.fixture
def registry(monkeypatch):
    """A stage registry whose kernels report whatever the test wants."""
    reported: dict[str, dict] = {}

    def _install(**per_stage: dict):
        reported.clear()
        reported.update(per_stage)
        monkeypatch.setattr(
            runner, "load_stages",
            lambda: {name: (lambda wd, cfg, inputs, _s=stats: dict(_s))
                     for name, stats in reported.items()})
    return _install


# ── the ceilings that were declared and never enforced ─────────────────────

def test_a_cost_ceiling_now_aborts_the_dataflow(registry):
    registry(a={"cost_usd": 4.0}, b={"cost_usd": 4.0}, c={"cost_usd": 1.0})
    spec = _spec([StageRef(stage="a"), StageRef(stage="b"), StageRef(stage="c")],
                 max_cost_usd=6.0)
    with pytest.raises(BudgetExceeded) as e:
        run_dataflow(spec, workdir=_wd())
    assert "max_cost_usd" in str(e.value)
    assert "$8.0000" in str(e.value), "the refusal names what was actually spent"


def test_cost_is_summed_across_stages_not_compared_per_stage(registry):
    """No single stage breaches $6; together they do. A per-stage comparison
    would let an unbounded number of cheap stages spend without limit."""
    registry(a={"cost_usd": 3.0}, b={"cost_usd": 3.5})
    with pytest.raises(BudgetExceeded):
        run_dataflow(_spec([StageRef(stage="a"), StageRef(stage="b")], max_cost_usd=6.0), _wd())


def test_a_row_ceiling_now_aborts_the_dataflow(registry):
    registry(ingest={"rows": 5_000_000}, next_stage={"rows": 1})
    with pytest.raises(BudgetExceeded) as e:
        run_dataflow(_spec([StageRef(stage="ingest"), StageRef(stage="next_stage")],
                           max_rows=1_000_000), _wd())
    assert "max_rows" in str(e.value) and "5000000" in str(e.value)


def test_rows_is_a_peak_not_a_sum(registry):
    """Stages see the SAME rows in turn, so summing them would fail a dataflow
    that never processed more than the ceiling."""
    registry(a={"rows": 600}, b={"rows": 600}, c={"rows": 600})
    res = run_dataflow(_spec([StageRef(stage="a"), StageRef(stage="b"), StageRef(stage="c")],
                             max_rows=1000), _wd())
    assert res.stats["stages_done"] == 3


# ── a ceiling that cannot bind must not look like one that did ─────────────

def test_an_unenforceable_cost_ceiling_is_reported(registry):
    """No stage reported cost, so the ceiling never bound. Finishing silently
    would present an unguarded run as a guarded one."""
    registry(a={"rows": 10})
    res = run_dataflow(_spec([StageRef(stage="a")], max_cost_usd=5.0), _wd())
    assert res.stats.get("cost_budget_unenforced") is True


def test_a_bound_cost_ceiling_is_not_flagged(registry):
    registry(a={"cost_usd": 0.25})
    res = run_dataflow(_spec([StageRef(stage="a")], max_cost_usd=5.0), _wd())
    assert "cost_budget_unenforced" not in res.stats


def test_no_declared_ceiling_means_no_flag_and_no_abort(registry):
    registry(a={"rows": 10_000_000, "cost_usd": 999.0})
    res = run_dataflow(_spec([StageRef(stage="a")]), _wd())
    assert "cost_budget_unenforced" not in res.stats
    assert res.stats["stages_done"] == 1


def test_failed_stage_metrics_keep_cumulative_metered_spend(monkeypatch, tmp_path):
    def paid_stage(wd, config, inputs):
        obs.record_llm("gpt-4o-mini", 10, 5, 0.2, usd=0.2)
        return {}

    def failing_stage(wd, config, inputs):
        obs.record_llm("gpt-4o-mini", 10, 5, 0.3, usd=0.3)
        raise RuntimeError("stage failed after its paid call")

    monkeypatch.setattr(runner, "load_stages", lambda: {
        "paid": paid_stage,
        "fails": failing_stage,
    })

    with pytest.raises(RuntimeError, match="paid call"):
        run_dataflow(DataflowSpec(stages=[
            StageRef(stage="paid"), StageRef(stage="fails"),
        ]), tmp_path)

    metrics = json.loads((tmp_path / "metrics.json").read_text(encoding="utf-8"))
    assert metrics["cost_usd"] == pytest.approx(0.5)
    assert metrics["stages_done"] == 1


def test_builtin_failed_status_surfaces_partial_cost(tmp_path, monkeypatch):
    import asyncio

    from cairn.core.dataflow import EngineJobHandle
    from operator_dataflow import engines as engines_module
    from operator_dataflow.engines import BuiltinLocalEngine

    class _ExitedProcess:
        returncode = 1

    (tmp_path / "metrics.json").write_text('{"cost_usd": 0.5}', encoding="utf-8")
    monkeypatch.setitem(engines_module._PROCS, "failed-job", {
        "proc": _ExitedProcess(), "workdir": tmp_path, "deadline": None,
        "budget_hit": False, "cancelled": False,
    })

    status = asyncio.run(BuiltinLocalEngine().poll(
        EngineJobHandle(job_id="failed-job", engine="builtin", meta={}),
    ))

    assert status.state == "failed"
    assert status.metrics["cost_usd"] == 0.5


# ── malformed stats must not be the thing that kills a run ─────────────────

def test_a_non_numeric_stat_is_ignored_rather_than_fatal(registry):
    registry(a={"cost_usd": "not-a-number"}, b={"cost_usd": 1.0})
    res = run_dataflow(_spec([StageRef(stage="a"), StageRef(stage="b")],
                             max_cost_usd=5.0), _wd())
    assert res.stats["stages_done"] == 2


_WD = []


def _wd():
    """A fresh workdir per call (markers must not leak between cases)."""
    import tempfile
    from pathlib import Path

    d = tempfile.mkdtemp(prefix="cairn-budget-")
    _WD.append(d)
    return Path(d)
