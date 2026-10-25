"""ADR-0086 H2 (#1244) — leg-level retry, driven by the engine's `retriable` flag.

The flag existed on every engine failure status (`engines_ssh.py` poll) and was
consumed by nobody: a transient SSH drop failed the leg, and with it the
pipeline. execute_leg now runs a bounded, kind-aware attempt loop over the
EXISTING RetryPolicy shape — every attempt budgeted (a retry is new spend,
never free) and announced (`leg_retrying` run event via the on_retry hook).
"""
from __future__ import annotations

import os
import shutil
import types

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

from cairn.core.dataflow.contract import ResumeFrom, StageRef
from cairn.core.dispatch.leg_executor import execute_leg
from cairn.core.dispatch.leg_hooks import flow_leg_record
from cairn.core.engine.schema import TriggerEvent
from cairn.core.runtime.retry import RetryPolicy
from cairn.core.storage.memory.runs import InMemoryRunStore
from cairn.core.storage.protocols import Run


def _stub_resolution(monkeypatch, *, kind="data", stage="clean.run", checkpoint=False):
    stage = StageRef(stage=stage, checkpoint=checkpoint)
    monkeypatch.setattr("cairn.core.dataflow.defs.load_flow_defs", lambda: {
        "demo/clean": types.SimpleNamespace(kind=kind),
    })
    monkeypatch.setattr(
        "cairn.core.dataflow.defs.resolve_flow_ref",
        lambda ref, cfg, kind="data": [stage],
    )
    monkeypatch.setattr("cairn.core.dispatch.leg_executor._POLL_INTERVAL_S", 0.01)


class _FlakyEngine:
    """Fails `failures` times (retriable or not), then succeeds."""

    name = "flaky"

    def __init__(self, failures=1, *, retriable=True) -> None:
        self.failures = failures
        self.retriable = retriable
        self.submits = 0

    def validate(self, spec):
        return []

    async def submit(self, spec, ctx):
        self.submits += 1
        return types.SimpleNamespace(job_id=f"j{self.submits}", engine=self.name, meta={})

    async def poll(self, handle):
        if self.submits <= self.failures:
            return types.SimpleNamespace(
                state="failed", error="transient: connection reset",
                metrics={}, retriable=self.retriable,
            )
        return types.SimpleNamespace(state="succeeded", metrics={}, error="", retriable=False)

    async def cancel(self, handle):
        pass

    async def result(self, handle):
        return types.SimpleNamespace(stats={"clean.output": "s3://ok"})


async def _run(engine, *, kind="data", retry=None, monkeypatch=None):
    retries: list[tuple[int, str]] = []

    async def _on_retry(attempt: int, cause: str) -> None:
        retries.append((attempt, cause))

    _stub_resolution(monkeypatch, kind=kind)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1",
        retry=retry, on_retry=_on_retry,
    )
    return out, retries


async def test_retriable_failure_retries_once_and_succeeds(monkeypatch):
    engine = _FlakyEngine(failures=1, retriable=True)
    out, retries = await _run(engine, monkeypatch=monkeypatch)
    assert out.ok, out.error
    assert engine.submits == 2, "data-leg default is exactly one automatic retry"
    assert retries == [(1, "transient: connection reset")], "every retry must be announced"


async def test_non_retriable_failure_never_retries(monkeypatch):
    engine = _FlakyEngine(failures=5, retriable=False)
    out, retries = await _run(engine, monkeypatch=monkeypatch)
    assert not out.ok and engine.submits == 1 and retries == []


async def test_data_leg_default_is_two_attempts_total(monkeypatch):
    engine = _FlakyEngine(failures=5, retriable=True)
    out, retries = await _run(engine, monkeypatch=monkeypatch)
    assert not out.ok
    assert engine.submits == 2, "bounded: one retry, then the failure stands"
    assert len(retries) == 1


async def test_ml_leg_never_retries_without_opt_in(monkeypatch):
    """A failed train leg must not silently re-spend GPU dollars."""
    engine = _FlakyEngine(failures=5, retriable=True)
    out, retries = await _run(engine, kind="ml", monkeypatch=monkeypatch)
    assert not out.ok and engine.submits == 1 and retries == []


async def test_ml_leg_retries_with_explicit_policy(monkeypatch):
    engine = _FlakyEngine(failures=1, retriable=True)
    policy = RetryPolicy(maximum_attempts=3, initial_interval_seconds=0.0)
    out, retries = await _run(engine, kind="ml", retry=policy, monkeypatch=monkeypatch)
    assert out.ok and engine.submits == 2 and len(retries) == 1


async def test_budget_refusal_never_retries(monkeypatch):
    """Budget exhaustion is not transient — no retry, no reservation churn."""
    _stub_resolution(monkeypatch)
    calls = {"n": 0}

    async def _reserve(*a, **k):
        from cairn.core.budget_reservations import BudgetExceeded
        calls["n"] += 1
        raise BudgetExceeded("series budget exhausted")

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    engine = _FlakyEngine(failures=0, retriable=True)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1", series_id="s1",
    )
    assert not out.ok and "budget" in out.error.lower()
    assert calls["n"] == 1 and engine.submits == 0


async def test_each_attempt_reserves_fresh_budget(monkeypatch):
    """Governance invariant: a retry is NEW spend — one reservation per attempt."""
    _stub_resolution(monkeypatch)
    ledger = {"reserved": 0, "released": 0, "settled": 0}

    async def _reserve(*a, **k):
        ledger["reserved"] += 1
        return object()

    async def _release(res):
        ledger["released"] += 1

    async def _settle(res, **k):
        ledger["settled"] += 1

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    engine = _FlakyEngine(failures=1, retriable=True)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1", series_id="s1",
    )
    assert out.ok
    assert ledger == {"reserved": 2, "released": 1, "settled": 1}, (
        "attempt 1: reserved+released; attempt 2: reserved+settled"
    )


async def test_failed_retry_accumulates_and_settles_known_partial_cost(monkeypatch):
    _stub_resolution(monkeypatch)
    reservations: list[float] = []
    settlements: list[float] = []
    releases: list[object] = []

    async def _reserve(*args, **kwargs):
        reservations.append(kwargs["spent_usd"])
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        releases.append(reservation)

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _PaidFailures(_FlakyEngine):
        async def poll(self, handle):
            cost_usd = 0.2 if self.submits == 1 else 0.3
            return types.SimpleNamespace(
                state="failed", error="paid attempt failed", retriable=True,
                metrics={"cost_usd": cost_usd},
            )

    retries: list[tuple[int, str]] = []

    async def _on_retry(attempt: int, cause: str) -> None:
        retries.append((attempt, cause))

    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=_PaidFailures(failures=2, retriable=True),
        run_id="r1", series_id="s1", estimated_usd=0.5, spent_usd=0.0,
        budget_usd=2.0,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0),
        on_retry=_on_retry,
    )

    assert not out.ok and out.cost_usd == 0.5
    assert reservations == [0.0, 0.2]
    assert settlements == [0.2, 0.3]
    assert releases == []
    assert retries == [(1, "paid attempt failed")]
    record = flow_leg_record({"error": out.error}, out)
    assert record["cost"]["total_usd"] == 0.5
    assert record["result_payloads"][0]["llm_cost_usd"] == 0.5


async def test_cancelled_attempt_with_partial_cost_settles_without_retry(monkeypatch):
    _stub_resolution(monkeypatch)
    settlements: list[float] = []
    releases: list[object] = []
    retries: list[tuple[int, str]] = []

    async def _reserve(*args, **kwargs):
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        releases.append(reservation)

    async def _on_retry(attempt: int, cause: str) -> None:
        retries.append((attempt, cause))

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _CancelledPaid(_FlakyEngine):
        async def poll(self, handle):
            return types.SimpleNamespace(
                state="cancelled", error="cancelled after paid call",
                metrics={"cost_usd": 0.15}, retriable=True,
            )

    engine = _CancelledPaid(failures=0)
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine,
        run_id="r1", series_id="s1", estimated_usd=0.5,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0),
        on_retry=_on_retry,
    )

    assert out.state == "cancelled" and out.cost_usd == 0.15
    assert engine.submits == 1 and retries == []
    assert settlements == [0.15] and releases == []


def test_failed_leg_without_reported_cost_keeps_run_cost_unknown():
    from cairn.core.dispatch.leg_executor import LegOutcome

    record = flow_leg_record(
        {"error": "worker disappeared before reporting metrics"},
        LegOutcome(state="failed", error="worker disappeared before reporting metrics"),
    )

    assert "cost" not in record
    assert record["result_payloads"][0]["llm_cost_usd"] == 0.0


async def test_successful_retry_run_includes_cost_of_replaced_attempt(monkeypatch):
    _stub_resolution(monkeypatch)
    reservations: list[float] = []
    settlements: list[float] = []

    async def _reserve(*args, **kwargs):
        reservations.append(kwargs["spent_usd"])
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        pytest.fail("a paid attempt must be settled, not released")

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _PaidThenSuccess(_FlakyEngine):
        async def poll(self, handle):
            if self.submits == 1:
                return types.SimpleNamespace(
                    state="failed", error="paid attempt failed", retriable=True,
                    metrics={"cost_usd": 0.2},
                )
            return types.SimpleNamespace(
                state="succeeded", error="", retriable=False,
                metrics={"cost_usd": 0.3},
            )

    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=_PaidThenSuccess(failures=1),
        run_id="r1", series_id="s1", estimated_usd=0.5, spent_usd=0.0,
        budget_usd=2.0,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0),
    )

    assert out.ok and out.cost_usd == 0.5
    assert reservations == [0.0, 0.2]
    assert settlements == [0.2, 0.3]
    record = flow_leg_record({"output": "s3://ok"}, out)
    assert record["cost"]["total_usd"] == 0.5


def test_unrepresentable_reported_cost_is_treated_as_unknown():
    from cairn.core.dispatch.leg_executor import _reported_cost, _reported_metrics_cost

    huge_cost = 10**400
    assert _reported_cost(types.SimpleNamespace(cost_usd=huge_cost)) == 0.0
    assert _reported_metrics_cost({"cost_usd": huge_cost}) == 0.0


async def test_checkpoint_retry_does_not_settle_frozen_cost_twice(monkeypatch):
    _stub_resolution(monkeypatch, checkpoint=True)
    reserved_spend: list[float] = []
    settlements: list[float] = []
    lookups = 0

    async def _reserve(*args, **kwargs):
        reserved_spend.append(kwargs["spent_usd"])
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        pytest.fail("a paid attempt must be settled, not released")

    async def _resume(key):
        nonlocal lookups
        lookups += 1
        if lookups == 1:
            return None
        return ResumeFrom(
            spec_key=key,
            stage_index=0,
            completed=[{
                "index": 0,
                "stage": "clean.run",
                "stats": {"cost_usd": 0.2},
            }],
        )

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _CheckpointedRetry(_FlakyEngine):
        def __init__(self):
            super().__init__(failures=1)
            self.resumes: list[int | None] = []

        async def submit(self, spec, ctx):
            self.submits += 1
            resume = spec.resume_from
            self.resumes.append(resume.stage_index if resume is not None else None)
            return types.SimpleNamespace(job_id=f"j{self.submits}", engine=self.name, meta={})

        async def poll(self, handle):
            if self.submits == 1:
                return types.SimpleNamespace(
                    state="failed", error="checkpointed attempt failed", retriable=True,
                    metrics={"cost_usd": 0.2},
                )
            return types.SimpleNamespace(
                state="succeeded", error="", retriable=False,
                metrics={"cost_usd": 0.5},
            )

        async def result(self, handle):
            return types.SimpleNamespace(stats={}, cost_usd=0.5)

    engine = _CheckpointedRetry()
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1",
        series_id="s1", estimated_usd=0.5, spent_usd=0.0, budget_usd=2.0,
        retry=RetryPolicy(maximum_attempts=2, initial_interval_seconds=0.0),
        resume_provider=_resume,
    )

    assert out.ok and out.cost_usd == 0.5
    assert engine.resumes == [None, 0]
    assert reserved_spend == [0.0, 0.2]
    assert settlements == [0.2, 0.3]


async def test_timeout_settles_last_known_partial_cost(monkeypatch):
    _stub_resolution(monkeypatch)
    settlements: list[float] = []

    async def _reserve(*args, **kwargs):
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        pytest.fail("a known partial cost must be settled, not released")

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _RunningPaidEngine(_FlakyEngine):
        def __init__(self):
            super().__init__(failures=0)
            self.cancelled = False
            self.polls = 0

        async def poll(self, handle):
            self.polls += 1
            return types.SimpleNamespace(
                state="running", error="", retriable=False,
                metrics={"cost_usd": 0.15},
            )

        async def cancel(self, handle):
            self.cancelled = True

    engine = _RunningPaidEngine()
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1",
        series_id="s1", estimated_usd=0.5, spent_usd=0.0, budget_usd=2.0,
        poll_budget_s=1.0,
    )

    assert not out.ok and "TimeoutError" in out.error
    assert out.cost_usd == 0.15
    assert out.metrics["cost_usd"] == 0.15
    assert engine.cancelled and engine.polls > 0
    assert settlements == [0.15]


async def test_poll_exception_keeps_last_known_partial_cost(monkeypatch):
    _stub_resolution(monkeypatch)
    settlements: list[float] = []

    async def _reserve(*args, **kwargs):
        return object()

    async def _settle(reservation, *, actual_usd):
        settlements.append(actual_usd)

    async def _release(reservation):
        pytest.fail("a known partial cost must be settled, not released")

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)

    class _PollDropsAfterSpend(_FlakyEngine):
        def __init__(self):
            super().__init__(failures=0)
            self.polls = 0

        async def poll(self, handle):
            self.polls += 1
            if self.polls == 1:
                return types.SimpleNamespace(
                    state="running", error="", retriable=False,
                    metrics={"cost_usd": 0.27},
                )
            raise ConnectionError("poll disconnected")

    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=_PollDropsAfterSpend(), run_id="r1",
        series_id="s1", estimated_usd=0.5, spent_usd=0.0, budget_usd=2.0,
    )

    assert not out.ok and "poll disconnected" in out.error
    assert out.cost_usd == 0.27
    assert out.metrics["cost_usd"] == 0.27
    assert settlements == [0.27]


async def test_builtin_paid_stage_failure_reaches_run_cost(monkeypatch, tmp_path):
    from operator_dataflow import engines as engines_module
    from operator_dataflow.engines import BuiltinLocalEngine

    stage_name = "test.paid-failure"
    dist_info = tmp_path / "cairn_test_paid_stage-0.0.0.dist-info"
    dist_info.mkdir()
    (dist_info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: cairn-test-paid-stage\nVersion: 0.0.0\n\n",
        encoding="utf-8",
    )
    (dist_info / "entry_points.txt").write_text(
        f"[cairn.dataflow_stages]\n{stage_name} = paid_failure_stage:paid_failure\n",
        encoding="utf-8",
    )
    (tmp_path / "paid_failure_stage.py").write_text(
        "from cairn.core import obs\n"
        "def paid_failure(workdir, config, inputs):\n"
        "    obs.record_llm('test-model', 10, 20, 1.0, usd=0.23)\n"
        "    raise RuntimeError('failed after a billed model call')\n"
        "paid_failure.risk_class = 'read_only'\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(filter(None, (str(tmp_path), os.environ.get("PYTHONPATH", "")))),
    )
    _stub_resolution(monkeypatch, stage=stage_name)

    engine = BuiltinLocalEngine()
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="run-paid-failure",
        retry=RetryPolicy(maximum_attempts=1), poll_budget_s=30.0,
    )
    try:
        assert not out.ok
        assert out.cost_usd == 0.23

        store = InMemoryRunStore()
        await store.create(Run(
            id="run-paid-failure",
            trigger=TriggerEvent(source="manual", payload={}),
            status="running",
        ))
        await store.update(
            "run-paid-failure", status="failed", error=out.error,
            **flow_leg_record({"error": out.error}, out),
        )
        saved = await store.get("run-paid-failure")
        assert saved.cost["total_usd"] == 0.23
        assert saved.result_payloads[0]["llm_cost_usd"] == 0.23
    finally:
        if out.handle is not None:
            job = engines_module._PROCS.pop(out.handle.job_id, None)
            if job is not None:
                shutil.rmtree(job["workdir"], ignore_errors=True)


async def test_submit_engine_error_fails_the_leg_releases_and_never_retries(monkeypatch):
    """A refusal raised by ``engine.submit`` (e.g. the SSH engine's image mode
    with no image named, ADR-0150 D1) is a failed leg: its reservation is
    released, nothing is settled, and the default data-leg retry does not
    submit again — a configuration refusal is not transient."""
    from cairn.core.dataflow import EngineError

    _stub_resolution(monkeypatch)
    ledger = {"reserved": 0, "released": 0, "settled": 0}

    async def _reserve(*a, **k):
        ledger["reserved"] += 1
        return object()

    async def _release(res):
        ledger["released"] += 1

    async def _settle(res, **k):
        ledger["settled"] += 1

    monkeypatch.setattr("cairn.core.dispatch.leg_executor.reserve", _reserve)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.release", _release)
    monkeypatch.setattr("cairn.core.dispatch.leg_executor.settle", _settle)

    class _RefusingEngine(_FlakyEngine):
        async def submit(self, spec, ctx):
            self.submits += 1
            raise EngineError("image mode but no image is named")

    engine = _RefusingEngine()
    out = await execute_leg(
        "flow:demo/clean", inputs={}, engine=engine, run_id="r1", series_id="s1",
    )
    assert not out.ok and "no image is named" in out.error
    assert engine.submits == 1
    assert ledger == {"reserved": 1, "released": 1, "settled": 0}
